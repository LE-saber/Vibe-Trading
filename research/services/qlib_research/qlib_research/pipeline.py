"""Alpha158, 1D/3D/5D labels, rolling LightGBM, and reproducible artifacts."""

from __future__ import annotations

import importlib.metadata
import json
from pathlib import Path

import numpy as np
import pandas as pd

from qlib_research.converter import convert_canonical_parquet, file_sha256
from qlib_research.metrics import daily_ic, finite_or_none, summarize_prediction
from qlib_research.models import PredictionRecord, ResearchRequest, ResearchResult
from qlib_research.symbols import qlib_to_canonical


class QlibResearchPipeline:
    def run(self, request: ResearchRequest) -> ResearchResult:
        if not request.canonical_parquet.is_file() or not request.universe_parquet.is_file():
            raise FileNotFoundError("canonical market and PIT universe parquet files are required")
        manifest = convert_canonical_parquet(
            request.canonical_parquet, request.qlib_store, request.inference_as_of
        )
        frame, feature_names = self._load_qlib_frame(request)
        frame = self._apply_pit_universe(frame, request.universe_parquet)
        metrics: dict[str, dict] = {}
        prediction_frames: list[pd.DataFrame] = []
        factor_rows: list[dict] = []

        for horizon in request.horizons:
            label = f"LABEL{horizon}D"
            predictions = self._rolling_predict(frame, feature_names, label, request)
            prediction_frames.append(predictions.assign(horizon=horizon))
            test_predictions = predictions.loc[
                predictions["datetime"].between(
                    pd.Timestamp(request.window.test_start), pd.Timestamp(request.window.test_end)
                )
            ]
            metrics[str(horizon)] = summarize_prediction(test_predictions, "prediction", label)
            for feature in feature_names:
                values = frame.loc[
                    frame["datetime"].between(
                        pd.Timestamp(request.window.test_start), pd.Timestamp(request.window.test_end)
                    ),
                    ["datetime", feature, label],
                ]
                ic = daily_ic(values, feature, label, "pearson").dropna()
                rank_ic = daily_ic(values, feature, label, "spearman").dropna()
                factor_rows.append(
                    {
                        "horizon": horizon,
                        "alpha": feature,
                        "ic_mean": finite_or_none(ic.mean()) if len(ic) else None,
                        "rank_ic_mean": finite_or_none(rank_ic.mean()) if len(rank_ic) else None,
                        "coverage": float(values[[feature, label]].dropna().shape[0] / len(values))
                        if len(values)
                        else 0.0,
                    }
                )

        artifact_dir = request.artifact_dir.resolve() / request.run_id
        artifact_dir.mkdir(parents=True, exist_ok=False)
        all_predictions = pd.concat(prediction_frames, ignore_index=True)
        historical_path = artifact_dir / "historical-predictions.parquet"
        all_predictions.to_parquet(historical_path, index=False)
        factor_path = artifact_dir / "alpha158-metrics.csv"
        pd.DataFrame(factor_rows).to_csv(factor_path, index=False)
        score_path = artifact_dir / "competition-scores.json"
        score_records = self._latest_scores(all_predictions, request, metrics)
        score_path.write_text(
            json.dumps([item.model_dump(mode="json") for item in score_records], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        metrics_path = artifact_dir / "metrics.json"
        metrics_path.write_text(json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8")
        run_manifest = {
            "run_id": request.run_id,
            "canonical_sha256": file_sha256(request.canonical_parquet),
            "universe_sha256": file_sha256(request.universe_parquet),
            "conversion": manifest.model_dump(mode="json"),
            "request": request.model_dump(mode="json"),
            "artifacts": [path.name for path in (historical_path, factor_path, score_path, metrics_path)],
        }
        manifest_path = artifact_dir / "run-manifest.json"
        manifest_path.write_text(
            json.dumps(run_manifest, ensure_ascii=False, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        result = ResearchResult(
            run_id=request.run_id,
            framework_version=importlib.metadata.version("pyqlib"),
            alpha_count=len(feature_names),
            horizons=list(request.horizons),
            historical_window={
                "train": f"{request.window.train_start}/{request.window.train_end}",
                "valid": f"{request.window.valid_start}/{request.window.valid_end}",
                "test": f"{request.window.test_start}/{request.window.test_end}",
            },
            training_cutoff=request.training_cutoff,
            inference_as_of=request.inference_as_of,
            metrics=metrics,
            artifacts={
                "historical_predictions": str(historical_path),
                "alpha_metrics": str(factor_path),
                "competition_scores": str(score_path),
                "metrics": str(metrics_path),
                "manifest": str(manifest_path),
            },
        )
        (artifact_dir / "result.json").write_text(
            json.dumps(result.model_dump(mode="json"), ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return result

    @staticmethod
    def _load_qlib_frame(request: ResearchRequest) -> tuple[pd.DataFrame, list[str]]:
        import qlib
        from qlib.constant import REG_CN
        from qlib.contrib.data.loader import Alpha158DL
        from qlib.data import D

        qlib.init(provider_uri=str(request.qlib_store.resolve()), region=REG_CN)
        expressions, feature_names = Alpha158DL.get_feature_config()
        labels = [f"Ref($close, -{h})/$close-1" for h in request.horizons]
        label_names = [f"LABEL{h}D" for h in request.horizons]
        data = D.features(
            D.instruments("all"),
            [*expressions, *labels],
            start_time=str(request.window.train_start),
            end_time=str(request.inference_as_of),
            freq="day",
        )
        data.columns = [*feature_names, *label_names]
        data = data.reset_index()
        data["symbol"] = data["instrument"].map(qlib_to_canonical)
        data["datetime"] = pd.to_datetime(data["datetime"]).dt.normalize()
        return data, list(feature_names)

    @staticmethod
    def _apply_pit_universe(frame: pd.DataFrame, universe_path: Path) -> pd.DataFrame:
        universe = pd.read_parquet(universe_path)
        required = {"snapshot_date", "symbol", "is_eligible"}
        if not required.issubset(universe.columns):
            raise ValueError("PIT universe parquet requires snapshot_date, symbol, is_eligible")
        universe = universe.copy()
        universe["datetime"] = pd.to_datetime(universe["snapshot_date"], errors="raise").dt.normalize()
        if universe.duplicated(["datetime", "symbol"]).any():
            raise ValueError("PIT universe contains duplicate date/symbol rows")
        eligible = universe.loc[universe["is_eligible"].astype(bool), ["datetime", "symbol"]]
        filtered = frame.merge(eligible, on=["datetime", "symbol"], how="inner", validate="many_to_one")
        if filtered.empty:
            raise ValueError("PIT universe removed every research sample")
        return filtered.sort_values(["datetime", "symbol"]).reset_index(drop=True)

    @staticmethod
    def _rolling_predict(
        frame: pd.DataFrame, features: list[str], label: str, request: ResearchRequest
    ) -> pd.DataFrame:
        from lightgbm import LGBMRegressor

        dates = sorted(frame["datetime"].unique())
        start = pd.Timestamp(request.window.valid_start)
        end = pd.Timestamp(request.inference_as_of)
        predict_dates = [value for value in dates if start <= value <= end]
        outputs: list[pd.DataFrame] = []
        horizon = int(label.removeprefix("LABEL").removesuffix("D"))
        for offset in range(0, len(predict_dates), request.refit_every):
            chunk = predict_dates[offset : offset + request.refit_every]
            first = chunk[0]
            first_index = dates.index(first)
            embargo_index = first_index - horizon
            if embargo_index <= 0:
                continue
            train_end = dates[embargo_index - 1]
            train = frame.loc[
                (frame["datetime"] >= pd.Timestamp(request.window.train_start))
                & (frame["datetime"] <= train_end)
                & (frame["datetime"] <= pd.Timestamp(request.training_cutoff))
            ].dropna(subset=[label])
            predict = frame.loc[frame["datetime"].isin(chunk)].copy()
            if train.empty or predict.empty:
                continue
            usable = [name for name in features if train[name].notna().any()]
            x_train = train[usable].replace([np.inf, -np.inf], np.nan).fillna(0.0)
            x_predict = predict[usable].replace([np.inf, -np.inf], np.nan).fillna(0.0)
            model = LGBMRegressor(
                objective="regression",
                n_estimators=50,
                learning_rate=0.05,
                num_leaves=15,
                max_depth=5,
                subsample=0.9,
                colsample_bytree=0.9,
                random_state=20260901,
                deterministic=True,
                force_col_wise=True,
                n_jobs=1,
                verbosity=-1,
            )
            model.fit(x_train, train[label])
            predict["prediction"] = model.predict(x_predict)
            predict["training_end"] = pd.Timestamp(train_end)
            outputs.append(predict[["datetime", "symbol", label, "prediction", "training_end"]])
        if not outputs:
            raise ValueError(f"insufficient data for rolling {horizon}D prediction")
        return pd.concat(outputs, ignore_index=True)

    @staticmethod
    def _latest_scores(
        predictions: pd.DataFrame, request: ResearchRequest, metrics: dict[str, dict]
    ) -> list[PredictionRecord]:
        records: list[PredictionRecord] = []
        for horizon in request.horizons:
            subset = predictions.loc[predictions["horizon"] == horizon]
            latest_date = subset["datetime"].max()
            latest = subset.loc[subset["datetime"] == latest_date].sort_values(
                ["prediction", "symbol"], ascending=[False, True]
            )
            for rank, row in enumerate(latest.itertuples(), start=1):
                records.append(
                    PredictionRecord(
                        symbol=row.symbol,
                        prediction=float(row.prediction),
                        rank=rank,
                        horizon=horizon,
                        model_version=request.model_version,
                        as_of=pd.Timestamp(latest_date).date(),
                        training_window={
                            "start": str(request.window.train_start),
                            "end": str(pd.Timestamp(row.training_end).date()),
                        },
                        validation_metrics=metrics[str(horizon)],
                    )
                )
        return records
