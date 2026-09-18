"""Execute a bounded, stdlib-only quality audit and export its notebook.

No source calls or credentials. Cells run in order using this Python process;
Jupyter/nbformat are not dependencies of the production Core environment.
"""

from contextlib import redirect_stdout
from io import StringIO
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from competition_core.pipeline.artifacts import atomic_json


def main():
    cells = []
    scope = {}
    execution_count = 0

    def markdown(text):
        cells.append({"cell_type": "markdown", "metadata": {}, "source": text.splitlines(keepends=True)})

    def code(text):
        nonlocal execution_count
        execution_count += 1
        output = StringIO()
        with redirect_stdout(output):
            exec(compile(text, "suspension-audit", "exec"), scope)
        cells.append({"cell_type": "code", "metadata": {}, "execution_count": execution_count,
                      "source": text.splitlines(keepends=True),
                      "outputs": [{"output_type": "stream", "name": "stdout", "text": output.getvalue().splitlines(keepends=True)}]})

    markdown("# Compatible-source suspension audit\n## tl;dr\n"
             "Three zero-OHL/no-trade records on 2026-06-10 are independently confirmed full-day suspensions. "
             "They are not usable price bars. This narrow diagnosis does not certify the complete 120-session dataset or pipeline.")
    markdown("## Context & Methods\nGrain: symbol × trade date. Source: the configured t.xiaodefa.top compatible service.\n"
             "### Key Assumptions\nOnly O=H=L=0, C>0, vol=amount=0 plus same-day S with empty intraday timing qualifies. "
             "No missing price is filled and no volume is adjusted.\n"
             "Official reference: https://tushare.pro/document/1?doc_id=27 and https://tushare.pro/document/2?doc_id=214.")
    code("import json, hashlib\nfrom pathlib import Path\n"
         f"root = Path({str(ROOT)!r})\n"
         "paths = [root / 'storage/artifacts/pipeline-engineering/daily-20260610-invalid-price.json',\n"
         "         root / 'storage/artifacts/pipeline-engineering/suspensions-20260610.json']\n"
         "daily, suspensions = [json.loads(path.read_text(encoding='utf-8')) for path in paths]\n"
         "for path in paths:\n    print(path.name, hashlib.sha256(path.read_bytes()).hexdigest())\n")
    markdown("## Data\nThe first artifact records a 5,515-row source response and its three invalid-price rows. "
             "The second contains the separately fetched same-day suspension records. Capture times are not market timestamps.")
    code("rows = daily['nonpositive_rows']\n"
         "assert daily['row_count'] == 5515 and len(rows) == 3\n"
         "assert {r['trade_date'] for r in rows} == {'20260610'}\n"
         "confirmed = {r['ts_code'] for r in suspensions['rows'] if r['trade_date'] == '20260610' "
         "and r['suspend_type'] == 'S' and r['suspend_timing'] in (None, '')}\n"
         "for row in rows:\n"
         "    assert all(row[k] == 0 for k in ('open', 'high', 'low', 'vol', 'amount'))\n"
         "    assert row['close'] > 0 and row['ts_code'] in confirmed\n"
         "    print(row['ts_code'], 'CONFIRMED_FULL_DAY_SUSPENSION', row)\n")
    markdown("## Results")
    code("print({'affected_rows': len(rows), 'source_rows': daily['row_count'],\n"
         "       'affected_row_ratio': len(rows) / daily['row_count'], 'unconfirmed_rows': 0})\n")
    markdown("## Takeaways\nKeep the original source rows and independent proof immutable. Exclude only confirmed "
             "placeholders from the calculation panel and count the dates as missing history. Other invalid prices still fail.\n"
             "Validation: code cells executed sequentially with Python; no Jupyter kernel was installed or exercised. "
             "Rerun: `.venv/Scripts/python.exe scripts/build-suspension-audit.py`.")
    notebook = {"cells": cells, "metadata": {"language_info": {"name": "python", "version": sys.version.split()[0]}},
                "nbformat": 4, "nbformat_minor": 4}
    assert all(cell["cell_type"] in {"markdown", "code"} and isinstance(cell["source"], list) for cell in cells)
    target = ROOT / "storage/artifacts/pipeline-engineering/suspension-placeholder-audit.ipynb"
    atomic_json(target, notebook, immutable=True)
    print(target)


if __name__ == "__main__":
    main()
