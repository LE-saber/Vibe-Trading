# Vibe-Trading

[English](README.md) · [简体中文](README.zh-CN.md) · [繁體中文](README.zh-TW.md) · [日本語](README.ja.md)

**集成本地研究、比赛看板和人工审批执行流程的工作台。**

## 项目状态

默认 `main` 分支目前保存项目介绍。应用实现位于独立的开发评审分支；仅检出 `main` 不能得到可安装的应用。

| 位置 | 内容 |
| --- | --- |
| [`main`](https://github.com/LE-saber/Vibe-Trading/tree/main) | 项目概览和各语言版本 |
| [`codex/canonical-platform-consolidation`](https://github.com/LE-saber/Vibe-Trading/tree/codex/canonical-platform-consolidation) | 研究集成、本地 Web 界面、审批与执行代码 |
| [内核评审：PR #1](https://github.com/LE-saber/Vibe-Trading/pull/1) | 较早的集成工作台方案及评审记录 |

这些是开发参考入口，不代表已经部署或已对外部账户启用。

## 实现分支包含什么

- Python 后端和本地浏览器界面，展示研究进度、账户上下文与组合草案。
- 用于股票池选择、分阶段筛选、证据整理和 AI 复核的研究包。
- 明确的账户、榜单、新闻与订单操作 Provider 接口。
- 计划审批、订单状态跟踪和对账边界。
- 合成数据演示模式，以及需单独配置的本地模拟账户 Provider 模式。

整合分支的普通启动器会清空写入启用设置。填入凭据不会自动开启账户操作；写能力仍需单独验证配置并通过应用的审批门禁。合成演示行情、账户和结果不能当作 NineF 实时数据。

## 查看开发版本

先阅读该分支的 [README](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/README.md) 和 [部署说明](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/DEPLOYMENT.md)。该分支要求 **Python 3.13**，并提供 Windows 安装和启动脚本。

要查看实际开发代码，应显式检出实现分支：

```bash
git clone --branch codex/canonical-platform-consolidation https://github.com/LE-saber/Vibe-Trading.git
cd Vibe-Trading
```

后续按检出分支的说明安装。不要在仅含文档的 `main` 目录执行开发分支启动命令，其中没有对应文件。

## 文档入口

- [使用手册](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/USER_GUIDE.md)
- [架构](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/ARCHITECTURE.md)
- [本地集成](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/LOCAL_INTEGRATION.md)
- [安全边界](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/SECURITY.md)
- [分支验证记录](https://github.com/LE-saber/Vibe-Trading/blob/codex/canonical-platform-consolidation/docs/VERIFICATION.md)

凭据、账户导出和运行数据库保留在本地。默认分支没有许可证文件；本介绍不授予软件许可。
