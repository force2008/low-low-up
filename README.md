# openctp-ctp2tts - 低低UP量化工程

基于 OpenCTP TTS 柜台的量化交易系统

## 项目结构

```
openctp-ctp2tts/
├── config/           # 配置文件
│   ├── config.py
│   └── trading_time_config.py
├── ctp/              # CTP接口封装
│   ├── base_mdapi.py     # 行情API基类
│   ├── base_tdapi.py     # 交易API基类
│   ├── market_data/      # 行情API模块
│   └── trading/          # 交易API模块
├── data/             # 数据存储
│   ├── db/           # SQLite数据库
│   ├── manager/      # 数据管理器
│   └── contracts/    # 合约配置
├── strategies/       # 策略模块
│   ├── trend_reversal/   # 趋势反转策略族
│   ├── rebound/          # 反弹策略族
│   └── volatility/       # 波动率策略
├── backtest/         # 回测引擎
│   ├── engine.py
│   ├── indicators.py
│   ├── logic.py
│   └── models.py
├── signal/           # 信号检测
│   ├── detector.py
│   └── charts/       # 信号图表
├── trading/          # 交易执行
│   ├── ArbitrageTrading.py
│   └── VolatilitySwitchMonitor.py
├── utils/            # 工具模块
│   ├── KlineCollector.py
│   ├── GetMainContract.py
│   ├── GetMainContractWithVolume.py            # 主力合约Top-N生成脚本(方式A：实时持仓量/成交量排序)
│   ├── refresh_top3_main_contracts.bat         # 周度刷新：实际运行脚本（计划任务调用）
│   ├── register_weekly_task_RefreshTop3MainContracts.bat  # 一键注册「每周一 10:00」Windows 计划任务
│   └── feishu_notifier.py
├── logs/             # 日志目录
└── tests/            # 测试脚本
```

## 快速开始

### 安装依赖

```bash
pip install openctp-ctp==6.7.2.*
pip install openctp-ctp-channels pyautogui
pip install numpy pandas matplotlib
```

### 运行

```bash
# 切换到TTS通道
openctp-channels switch tts

# 运行K线采集
python KlineCollector.py online

回测信号
python .\backtest\strategy_backtest.py

所有的合约，昨天的信号回测

python simulate_signal_check_v2.py --all --days 1
```

## 运维 & 定时任务

### 1. 主力合约 Top-N 刷新脚本（方式 A：基于实时持仓量 / 成交量排序）

**入口脚本**：`utils/GetMainContractWithVolume.py`

- 第一步：CTP 交易端 `ReqQryInstrument` 拉取全量合约基本信息（新上市/下市合约会更新；`--refresh-instruments` 强制跳过缓存）
- 第二步：行情端 md_front 批量订阅所有在交易合约的深度行情（`--md-wait` 决定等多久收推送）
- 第三步：**每个品种按「OpenInterest 持仓量降序 → Volume 成交量降序」排序，取 Top-N** 作为 `main / main2 / main3 / main4 ...`

#### 命令行参数表

| 参数 | 默认值 | 说明 |
|---|---|---|
| `env` 位置参数 | `7x24` | 配置环境：`online` / `7x24` / `simu` / `test` |
| `--top-n` | **4** | 输出几级主力：`1`=只main；`3`=main+main2+main3；最大 `10` |
| `--md-wait` | `30` 秒 | 订阅行情后等待推送秒数。**交易时段建议 60~120；非交易时段/7x24 建议 120~180** |
| `--batch-size` | `100` | 每批订阅多少合约（防止一次订阅过多触发柜台频控） |
| `--refresh-instruments` | 关（提供即开启） | **建议每周至少开一次**：忽略 `data/contracts/instruments.json` 缓存，强制重新 `ReqQryInstrument` 更新新上市/下市合约 |
| `--output-dir` | `data/contracts` | 覆盖输出目录（备份、不同环境分开写时用） |

完整示例：
```bash
python utils\GetMainContractWithVolume.py online --refresh-instruments --md-wait 120 --top-n 3
```

#### 输出文件（全部写入 `data/contracts/`）

| 输出文件 | 格式 | 用途 |
|---|---|---|
| `main_contracts.json` | list（每 ProductID 一行） | **旧格式 100% 兼容**。保留 `MainContractID=Top1`；新增 `MainContractID2/3/4` 以及 `Top2/3/4 OpenInterest/Volume` |
| `main_contracts_by_product.json` | dict `{ProductID: {...}}` | **下游主用**。直接含 `main/main2/main3/main4`、对应 OI/Volume、`top_ranking[]` 完整榜单 |
| `main_contracts_top4.csv` | UTF-8 BOM CSV | Excel 直接打开核对。一眼判断某品种 main3/main4 是否真的活跃 |
| `instruments.json` | list（每合约一行） | 全量合约缓存。有了它下次不写 `--refresh-instruments` 跳过 ReqQryInstrument，启动快很多 |

#### 注意：非交易时段 vs 交易时段

- **周一 09:00~10:30 / 周内交易时段 online 运行（推荐）**：`OpenInterest` 和 `Volume` 是实盘最新值，Top-N 完全贴合市场
- **周末/夜盘非活跃 / 7x24 运行**：许多合约 OI/Vol 推送为 0 或保持昨收不变，脚本自动兜底（打印 `[无行情 OI=0] ... 按 InstrumentID 近月优先占位`，Top3 顺序大体正确但精确性略差，**建议周一 10:00 再跑 online 覆盖一次刷新 = 下面定时任务做的事**

---

### 2. 每周一 10:00 自动刷新 Top3 主力合约（Windows 计划任务）

> 固化自动执行的命令（已封装进 `utils/refresh_top3_main_contracts.bat`）：
```bat
cd /d E:\projects-02\low-low-up
call conda activate python310
python utils\GetMainContractWithVolume.py online --refresh-instruments --md-wait 120 --top-n 3
```

#### 一键注册（推荐）

**只需做一次**：

1. 打开 `E:\projects-02\low-low-up\utils\`
2. 右键 `register_weekly_task_RefreshTop3MainContracts.bat` → **以管理员身份运行**（schtasks /Create 需要管理员权限）
3. 看到 `[OK] 计划任务创建成功！` 即可

任务名：`low-low-up_周度刷新Top3主力合约`
- 触发：**每周一 10:00:00**（想改时间，编辑 bat 顶部 `SCHEDULE_TIME=10:00` 改完后右键管理员重跑，它自动删旧任务重注册）
- 身份：当前登录用户（即配置 CTP config 的用户）
- 失败自动重试：默认不开启；要开的话在任务计划程序 GUI → 任务属性 → 设置 → 「如果任务失败，按以下频率重新启动：5 分钟，尝试 3 次」打勾。

#### 常用命令行操作（cmd）

```bat
REM 查看状态详细配置
schtasks /Query /TN "low-low-up_周度刷新Top3主力合约" /FO LIST /V

REM 手动立刻跑（不等下周一），测完看 data\contracts 下 3 个输出文件是否刷新
schtasks /Run   /TN "low-low-up_周度刷新Top3主力合约"

REM 提前结束正在运行的实例
schtasks /End   /TN "low-low-up_周度刷新Top3主力合约"

REM 删除任务
schtasks /Delete /TN "low-low-up_周度刷新Top3主力合约" /F
```

#### 手动注册（不想用 bat，在 GUI 里配置）

1. `Win + R` → 输入 `taskschd.msc` → 打开「任务计划程序」
2. 右侧「创建基本任务...」
   - 名称：`low-low-up_周度刷新Top3主力合约`
   - 描述：每周一 10:00 刷新主力合约 Top3
   - 触发器：每周 / 周一 / 10:00
   - 操作：启动程序
     - 程序/脚本：`E:\projects-02\low-low-up\utils\refresh_top3_main_contracts.bat`
     - **起始于（Start in）非常重要，否则相对路径会漂移到 system32）**：`E:\projects-02\low-low-up`
3. 完成。后续按需要勾选「使用最高权限运行」。

---

### 3. 项目现有同类脚本风格参考

- `order-check/start_monitor.bat`：激活 python310 + 运行账户监控
- `utils/refresh_top3_main_contracts.bat`：本项目新增 —— 激活 python310 + 刷新 Top3 主力合约

> 统一风格：`call conda activate python310` + `cd /d <项目绝对路径>` + `python xxx.py`

## 文档

- [ENV_CONFIG_README.md](ENV_CONFIG_README.md) - 环境配置说明
- [KLINECOLLECTOR_STRATEGY_INTEGRATION.md](KLINECOLLECTOR_STRATEGY_INTEGRATION.md) - 策略集成说明
