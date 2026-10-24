# Vibe-Trading

[English](README.md) · [简体中文](README.zh-CN.md) · [繁體中文](README.zh-TW.md) · [日本語](README.ja.md)

**整合本機研究、競賽看板與人工核准執行流程的工作台。**

## 專案狀態

預設 `main` 分支目前存放專案介紹。應用程式實作位於獨立的開發審查分支；僅取出 `main` 並不會取得可安裝的應用程式。

| 位置 | 內容 |
| --- | --- |
| [`main`](https://github.com/LE-saber/Vibe-Trading/tree/main) | 專案概覽與各語言版本 |
| [`codex/canonical-platform-consolidation`](https://github.com/LE-saber/Vibe-Trading/tree/codex/canonical-platform-consolidation) | 研究整合、本機 Web 介面、核准與執行程式碼 |
| [核心審查：PR #1](https://github.com/LE-saber/Vibe-Trading/pull/1) | 較早的整合工作台提案及審查紀錄 |

這些是開發參考入口，不代表已部署或已對外部帳戶啟用。

## 實作分支包含什麼

- Python 後端與本機瀏覽器介面，呈現研究進度、帳戶背景與投資組合草案。
- 用於股票池選擇、分階段篩選、證據整理和 AI 複核的研究套件。
- 明確的帳戶、排行榜、新聞及訂單操作 Provider 介面。
- 計畫核准、訂單狀態追蹤與對帳界線。
- 合成資料示範模式，以及須另行設定的本機模擬帳戶 Provider 模式。

整合分支的一般啟動器會清除寫入啟用設定。填入憑證不會自動開啟帳戶操作；寫入能力仍須另行驗證設定並通過應用程式的核准檢查。合成示範行情、帳戶與結果不得當作 NineF 即時資料。

## 查看開發版本

先閱讀該分支的 [README](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/README.md) 與 [部署說明](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/DEPLOYMENT.md)。該分支要求 **Python 3.13**，並提供 Windows 安裝及啟動指令碼。

如需查看實際開發程式碼，請明確取出實作分支：

```bash
git clone --branch codex/canonical-platform-consolidation https://github.com/LE-saber/Vibe-Trading.git
cd Vibe-Trading
```

後續依取出分支的說明安裝。請勿在僅含文件的 `main` 目錄執行開發分支啟動命令，其中沒有對應檔案。

## 文件入口

- [使用手冊](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/USER_GUIDE.md)
- [架構](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/ARCHITECTURE.md)
- [本機整合](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/LOCAL_INTEGRATION.md)
- [安全界線](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/SECURITY.md)
- [分支驗證紀錄](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/VERIFICATION.md)

憑證、帳戶匯出與執行資料庫保留在本機。預設分支沒有授權條款檔案；本介紹不授予軟體使用授權。
