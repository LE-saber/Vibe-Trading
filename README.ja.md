# Vibe-Trading

[English](README.md) · [简体中文](README.zh-CN.md) · [繁體中文](README.zh-TW.md) · [日本語](README.ja.md)

**ローカル研究、コンテスト用ダッシュボード、人による承認を伴う執行フローを統合するワークベンチ。**

## プロジェクトの状態

既定の `main` ブランチには現在、プロジェクト紹介が置かれています。アプリケーション実装は別の開発・レビューブランチにあります。`main` を取得するだけでは、インストール可能なアプリケーションは得られません。

| 場所 | 内容 |
| --- | --- |
| [`main`](https://github.com/LE-saber/Vibe-Trading/tree/main) | 概要と各言語の紹介 |
| [`codex/canonical-platform-consolidation`](https://github.com/LE-saber/Vibe-Trading/tree/codex/canonical-platform-consolidation) | 研究機能の統合、ローカル Web UI、承認・執行コード |
| [カーネルのレビュー：PR #1](https://github.com/LE-saber/Vibe-Trading/pull/1) | 先行する統合ワークベンチの提案とレビュー履歴 |

これらは開発資料への入口です。デプロイ済み、または外部口座で有効化済みであることを意味しません。

## 実装ブランチに含まれるもの

- 研究の進捗、口座情報、ポートフォリオ案を表示する Python バックエンドとローカル Web UI。
- ユニバース選定、段階的なスクリーニング、証拠整理、AI レビューを行う研究パッケージ。
- 口座、ランキング、ニュース、注文操作の明示的な Provider インターフェース。
- 計画の承認、注文状態の追跡、照合処理の境界。
- 合成データによるデモと、個別設定が必要なローカル模擬口座 Provider モード。

統合ブランチの通常ランチャーは書き込み有効化設定を消去します。認証情報を入力するだけでは口座操作は有効になりません。書き込みには別途検証済みの設定と、アプリケーションの承認条件が必要です。デモの価格、口座、結果を NineF の実データとして扱わないでください。

## 開発版を確認する

まずブランチの [README](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/README.md) と [デプロイ手順](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/DEPLOYMENT.md)を確認してください。このブランチは **Python 3.13** を指定し、Windows 用セットアップ・起動スクリプトを備えています。

実装コードを取得するには、対象ブランチを明示します。

```bash
git clone --branch codex/canonical-platform-consolidation https://github.com/LE-saber/Vibe-Trading.git
cd Vibe-Trading
```

以降は取得したブランチの手順に従ってください。文書だけを含む `main` には起動スクリプトがないため、開発ブランチの起動命令をそのまま実行できません。

## 関連資料

- [ユーザーガイド](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/USER_GUIDE.md)
- [アーキテクチャ](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/ARCHITECTURE.md)
- [ローカル統合](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/LOCAL_INTEGRATION.md)
- [セキュリティ上の境界](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/SECURITY.md)
- [ブランチの検証記録](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/VERIFICATION.md)

認証情報、口座のエクスポート、実行時データベースはローカルに保管してください。既定ブランチにはライセンスファイルがなく、この紹介自体がソフトウェアの利用許諾を与えるものではありません。
