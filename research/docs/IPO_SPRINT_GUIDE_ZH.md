# 新股冲刺池：流程与使用手册

## 1. 模块定位

`IPO_SPRINT` 是一条独立、只读的新股冲刺路线，用来发现仍处于上市前五个
无涨跌幅限制交易日、同时具备增长动量、流动性、完整风险证据和实时可成交
盘口的新股。

它不会修改原有 `S0-S6` 筛选和组合流程，也不会把结果合并到原路线候选中。
输出始终保留 `route_origin: IPO_SPRINT`，因此可以明确区分：

- 原路线股票：来自正常 `S0-S5` 研究和 `S6` 组合规划；
- 新增路线股票：只来自本模块的独立 JSON；
- 实际交易：必须由操作者根据比赛奖线和账户状态另行确认，本模块不会下单。

## 2. 冲刺决策流程

```text
新股发行与上市数据
        ↓
第一层：冲刺初筛
确认沪深股票身份、上市日、交易日序号和前五日无涨跌幅制度
        ↓
第二层：硬风险与成交门
公告/监管/减持/业绩风险 + 流动性 + 新鲜双边盘口
        ↓
第三层：初始候选排序
单位交易日增长动量 → 累计收益 → 成交额
        ↓
第四层：预观察池（连续多次运行后由操作者确认）
只保留连续通过、增长没有明显衰减、盘口承接稳定的股票
        ↓
第五层：交易前复核
临近决策时间重新采集；旧报价和昨日结论一律不能直接使用
        ↓
第六层：最终重仓决策（人工、账户感知）
结合实时奖线差距、剩余交易日、单股上限和 T+1 风险决定是否使用冲刺仓
```

当前代码自动完成第一至第三层和第五层的实时硬门检查。第四层需要比较连续
生成的结果文件；第六层是人工比赛决策，尚未接入 `S6` 或订单系统。这个边界
是有意保留的，避免新增新股路线污染原筛选结果或自动触发高风险交易。

## 3. 自动筛选规则

采集器使用 Tushare 获取 `new_share`、`stock_basic`、`trade_cal`、`daily`、
`daily_basic` 和 `anns_d`，使用新浪 Level-1 行情验证当前双边盘口。默认配置位于
[`research/config/ipo-sprint.yaml`](../config/ipo-sprint.yaml)：

| 检查项 | 默认要求 |
| --- | --- |
| 上市窗口 | 当前交易日必须处于上市后第 1–5 个交易日且制度为 `UNBOUNDED` |
| 增长动量 | 至少一个有效收益窗口不低于 10% |
| 成交额 | 不低于 5,000 万元 |
| 换手率 | 不低于 1% |
| 量比 | 不低于 1 |
| 实时行情 | 不超过 60 秒，且必须有正数买一、卖一价格和数量 |
| 风险证据 | 公告、监管、减持、业绩四类全部完成且不超过 24 小时 |

任何身份冲突、交易日不确定、行情过期、停牌、价格锁死、单边盘口、流动性
不足或风险证据不完整都会失败关闭。评价结果按以下顺序排序：

1. `momentum_pct_per_session`：单位上市交易日增长动量；
2. `best_return_pct`：最佳有效窗口累计收益；
3. `turnover_amount`：当前成交额；
4. 股票代码：保证并列结果可重复。

排名靠前只代表优先观察，不代表适合立即买入。

## 4. 安装与凭据

先按仓库根目录 README 完成 Python 3.13 和 `.venv` 安装。Tushare token 只能
放在仓库外的本地文件，或临时放入 `TUSHARE_TOKEN` 环境变量；禁止提交 token、
原始账户数据或实时结果文件。

下面示例假设 token 位于 `D:\AI\secrets\Tushare-token.txt`，运行产物写入仓库
外的 `D:\AI\runtime\ipo-sprint`：

```powershell
$SprintOutput = 'D:\AI\runtime\ipo-sprint'
New-Item -ItemType Directory -Force -Path $SprintOutput | Out-Null

Push-Location research
..\.venv\Scripts\python.exe scripts\ipo-sprint.py collect `
  --token-file 'D:\AI\secrets\Tushare-token.txt' `
  --snapshot-output "$SprintOutput\snapshot.json"

..\.venv\Scripts\python.exe scripts\ipo-sprint.py evaluate `
  --input "$SprintOutput\snapshot.json" `
  --output "$SprintOutput\result.json"
Pop-Location
```

采集命令必须在沪深连续竞价时段执行：09:30–11:30 或 13:00–14:57（上海
时间）。`anns_d` 权限不可用、上一完成交易日的 `daily_basic` 缺失或实时行情
不满足条件时，命令会以退出码 `2` 失败，不会将未知状态当作安全状态。

## 5. 查看初始候选与淘汰原因

```powershell
$Result = Get-Content 'D:\AI\runtime\ipo-sprint\result.json' -Raw |
  ConvertFrom-Json

$Result.candidates |
  Select-Object symbol,name,listing_session_number,best_return_pct,`
    momentum_pct_per_session,turnover_amount,turnover_rate_pct,volume_ratio |
  Format-Table -AutoSize

$Result.rejected |
  Select-Object symbol,name,@{Name='reasons';Expression={$_.reason_codes -join ','}} |
  Format-Table -AutoSize

$Result.collection_excluded |
  Select-Object symbol,name,@{Name='reasons';Expression={$_.reason_codes -join ','}} |
  Format-Table -AutoSize
```

三个数组的含义不同：

- `candidates`：完成采集并通过全部硬门；
- `rejected`：证据完整，但在评价阶段未通过某个门；
- `collection_excluded`：在身份、日历或采集阶段已经排除。

结果中的 `read_only: true`、`execution_ready: false`、
`production_accepted: false` 和 `orders_submitted: 0` 不得被改写为可执行状态。

## 6. 从初筛到预观察池

预观察不能只看一次涨幅榜。建议在不同检查点分别保存文件，不要覆盖前一次
结果：

```powershell
# 示例文件名；每个检查点重新运行 collect 和 evaluate
snapshot-20260924-1000.json
result-20260924-1000.json
snapshot-20260924-1330.json
result-20260924-1330.json
snapshot-20260928-1000.json
result-20260928-1000.json
```

进入预观察池的建议条件是：

- 至少连续两次出现在 `candidates`，而不是单次脉冲；
- `momentum_pct_per_session` 没有发生明显衰减；
- 成交额、换手率和量比继续高于硬门，买一卖一都可成交；
- 没有新出现的公告、监管、减持或业绩风险；
- 上市交易日序号仍在无涨跌幅窗口内；
- 与其它预观察股票不过度集中在同一风险来源。

“连续两次”和“明显衰减”的判断当前属于操作规则，不是代码自动生成的字段。
如果股票只通过一次、随后跌出候选或风险审查变为不完整，应从预观察池移除。

## 7. 交易前增长趋势复核

最终决策不能复用上午或前一交易日的 `result.json`。在计划决策前重新运行一次
完整采集和评价，并同时比较最近两次结果：

- 候选仍通过全部永久硬门；
- 单位交易日动量保持为正且未出现快速衰减；
- 累计收益不是由单个极端首日完全贡献；
- 最新成交额和盘口能够承载计划仓位；
- 报价时间仍在 60 秒以内；
- 风险审查覆盖最新公告。

出现以下任一情况，不进入最终重仓候选：新风险公告、报价过期、停牌、价格
锁死、单边盘口、量比跌破门槛、上市窗口结束或连续检查中增长趋势反转。

## 8. 比赛时间表与仓位边界

本轮比赛可采用下列节奏：

| 日期 | 主要动作 | 注意事项 |
| --- | --- | --- |
| 9 月 24 日 | 建立初筛和预观察池 | 买入后跨休市，25–27 日无法处理风险 |
| 9 月 28 日 | 重新采集并形成主要目标组合 | 以实时奖线差距决定是否启用冲刺仓 |
| 9 月 29 日 | 最后一轮可控冲刺 | 当日买入的持仓 30 日仍可卖出 |
| 9 月 30 日 | 只做最终修正 | 当日新买受 T+1 约束，盘中转弱无法退出 |

平台单股上限为 20% 时，“最终重仓”仍不能突破单股上限；接近满仓至少需要五
个持仓。新股冲刺池只提供独立的进攻仓候选，不是把整个账户集中到一只新股：

- 奖线差距很小：优先保留原路线，不必为了波动而启用新股仓；
- 差距扩大且剩余时间不足：才考虑为通过最终复核的新股分配一个进攻席位；
- 只有进入最后一日且常规收益已经不足时，才评估更激进的冲刺配置；
- 无论模式如何，风险、流动性、行情新鲜度和成交能力硬门都不能放宽。

## 9. 代码与输出边界

```text
research/competition_core/ipo_sprint/collector.py   独立采集与证据绑定
research/competition_core/ipo_sprint/evaluation.py  校验、硬门、排序与结果契约
research/scripts/ipo-sprint.py                       命令行入口
research/config/ipo-sprint.yaml                      可审计阈值
research/tests/unit/test_ipo_sprint_*.py              回归测试
```

更详细的 JSON 字段契约见 [IPO_SPRINT.md](IPO_SPRINT.md)。模块不会读取或写入
正常研究 `runs` 目录，不创建 `Plan`，不调用 broker，也不会自动进入 S6。

## 10. 验证

从仓库根目录运行冲刺模块的聚焦测试：

```powershell
Push-Location research
..\.venv\Scripts\python.exe -m pytest -q `
  tests\unit\test_ipo_sprint_collector.py `
  tests\unit\test_ipo_sprint_evaluation.py
Pop-Location
```

发布前运行完整验证：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify.ps1
```
