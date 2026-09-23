# Vibe-Trading A 股比赛研究工作台

这是 `LE-saber/Vibe-Trading` 的 canonical repository。仓库把原先分散的
Competition Universe、S0–S5 因子筛选/证据/AI 复核，与账户感知的 S6
组合规划、人工审批、订单状态机和 Web 工作台整理为一个可部署项目。

默认启动是只读研究模式。真实或模拟账户的买入、卖出、撤单不会因为
填入 token 自动开启。

## 能做什么

- 沪深 A 股 point-in-time Universe，排除 ST、退市风险、停牌和不可交易标的；
- `S0 全市场 → S1 300 → S2 60 → S3 20 → S4 10–15 → S5 逐股 AI 复核`；
- Tushare 官方服务或显式配置的兼容 base URL；
- 前复权价格与原始成交量分离，保留现有 qfq 安全检查；
- ninef 模拟比赛账户、持仓、委托、榜单、新闻和行情只读接入；
- 独立的新股冲刺池：仅筛选上市后的前五个无涨跌幅限制交易日，单独输出；
- 账户/费用/手数/价格限制约束下的 S6 当前→目标→差量草案；
- 不可变计划哈希、人工批准、发送前复核、UNKNOWN 防重试和成交对账；
- 无构建步骤的本地中文 Web UI。

## 五分钟启动

要求 Windows、Git 和 Python 3.13。首次运行：

```powershell
git clone git@github.com:LE-saber/Vibe-Trading.git
cd Vibe-Trading
powershell -ExecutionPolicy Bypass -File .\scripts\setup.ps1
Copy-Item .env.example .env
```

编辑 `.env`，至少填写一个长度不小于 12 的 `VIBE_UI_PASSWORD`。随后双击
`start-vibe-trading.cmd`，或执行：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start.ps1
```

浏览器打开 `http://127.0.0.1:8765`。完整真实研究还需要本地填写
Tushare、ninef 和 S5 模型配置；字段说明见 [部署文档](docs/DEPLOYMENT.md)。

如只想验证界面和安全状态机，不访问网络：

```powershell
.\.venv\Scripts\vibe-trading.exe serve --mode demo
```

DEMO 的股票、行情、费用、榜单、新闻和结果全部是合成数据，不能视为研究结论。

## 日常使用

1. 启动平台并登录。
2. 确认账户、榜单、新闻读取状态。
3. 点击开始研究，观察 S0–S5 的真实数量与进度。
4. 查看 S6 草案中的目标权重、目标股数、差量、费用和 blocker。
5. blocker 不为零时不得执行；更新账户/行情后重新生成计划。
6. 计划需要人工确认精确 hash。普通启动脚本不会开放 ninef 写能力。

详细流程见 [使用手册](docs/USER_GUIDE.md)。

## 项目结构

```text
src/vibe_trading/   Web/API、类型契约、S6、审批、执行与对账唯一入口
research/           Universe、S0–S5、证据、数据源与可选研究 sidecar
research/config/    比赛与研究参数（无凭据）
scripts/            安装、启动、验证、安全审计
tests/              工作台与执行边界测试
docs/               部署、使用、架构、开发、安全和验证文档
```

`research/` 中保留可独立测试的研究包，但根启动器不会启动它的旧执行服务；
任何组合批准或订单动作都必须经过 `src/vibe_trading` 的 canonical contract。

## 验证

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify.ps1
```

单独运行：

```powershell
.\.venv\Scripts\python.exe -m pytest -q
Push-Location research
..\.venv\Scripts\python.exe -m pytest -q
Pop-Location
```

无涨跌幅限制新股的冲刺观察池使用独立 CLI 和独立 JSON，不进入原 S0-S6：
[IPO Sprint 使用说明](research/docs/IPO_SPRINT.md)。

当前整理基线：工作台收集 140 项测试；研究包收集 1032 项测试。最终提交对应的
实测结果记录在 [验证报告](docs/VERIFICATION.md)。

## 安全

不要提交 `.env`、token、Cookie、Authorization header、账户导出、抓包、
数据库、`runs/`、`data/`、`storage/` 或模型原始响应。普通启动脚本会清空
`VIBE_ENABLE_NINEF_WRITES`，因此只能研究和生成草案。写能力必须在单独的、
经过本地验证的操作流程中显式启用，且仍需计划 hash 与人工确认。

参见 [安全边界](docs/SECURITY.md) 和 [架构](docs/ARCHITECTURE.md)。
