# 项目概述
该项目是基于macd 60分钟的底底抬升的技术指标开发的程序化策略， 60分钟的macd在绿柱堆区间会形成一个低点，如果当前的绿柱堆里的dif拐头，且这个绿柱堆的最低价，高于前一个绿柱堆的最低价，则在5分钟的阳k线入里场， 止损价的确定是5分钟的前前绿柱堆的最低价为止损价，如果当前是5分钟的绿柱堆，则止损在前这个绿柱堆的往前一个绿柱堆里找出最低价做为止损价，如果当前是红柱堆，则在前前绿柱堆里的最低价做为止损价，如果选出来的止损价高于开仓价，则跳过本次开仓，因为5分钟是一个下降趋势，不应该开仓，这是一个只做多的策略

# 技术栈

基于 OpenCTP TTS 柜台的量化交易系统, 如果线上对接的是融航的api,但需要把so文件换成蓉航人so或dll文件
如果是本地的测试环境是用tts的系统对接，每天的下午16：00开始模拟环境的交易数据，推送的是前一天的tick数据。

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
│       ├── main_contracts.json
│       └── instruments.json
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
│   ├── VolatilitySwitchMonitor.py
│   ├── PositionSyncManager.py   # 持仓同步与委托执行核心
│   └── PositionManagerUI.py     # 持仓管理 Tkinter UI
├── order-check/      # 委托监控流水线
│   ├── run_pipeline.py          # 一键流水线：导出→对比→通知→交易
│   ├── compare_orders.py        # CSV 委托对比与 signal.json 生成
│   ├── automate_export.py       # CTP 客户端自动导出 CSV
│   ├── hold-std.json            # 标准持仓文件（由 compare_orders 生成）
│   ├── signal.json              # 待执行委托信号文件
│   └── orders_submitted.json    # 已提交委托记录
├── utils/            # 工具模块
│   ├── KlineCollector.py
│   ├── GetMainContract.py
│   └── feishu_notifier.py
├── rules/            # 项目规范文档
│   ├── style.md                 # 代码风格规范
│   ├── api_design.md            # API 设计规范
│   └── trading_rules.md         # 交易执行规则
├── logs/             # 日志目录
└── tests/            # 测试脚本
```

## 快速开始


### 安装依赖
- python用conda的安装一个3.10的版本，做到该python环境和其他的独立使用
- openctp-ctp
openctp-ctp是上期所的定义发布的接口协议，不同的期货公司有不同的ctp版本，所以要和期货公司的交易接口连上需要找到对应版本的库文件，这个工程用到的线上的是6.7.2的融航柜台的接口，里面的config里有融航可真实交易的账号和信息，现在暂不进行程序化交易，只把信号输出到飞书的通知里，待回测和优化成熟后，可接入自动化交易。工程里在libs下有对应的库文件，当安装完openctp-ctp-channels并切换完渠道后，可把libs文件放到 openctp-ctp-channels的lib目录这样就可以完成与融航柜台的联通了。
这个是库文件所以目录，安装完openctp-ctp后lib文件要替换成libs目录下的两个文件，如果文件名不同，要把文件名改动openctp_ctp.libs的相同的文件名
/home/ubuntu/miniconda3/envs/python310/lib/python3.10/site-packages/openctp_ctp.libs



```bash
pip install openctp-ctp==6.7.2.*
pip install openctp-ctp-channels
pip install numpy pandas matplotlib
```

### 数据获取
从tqsdk导数据到data/db/kline_data.db
python utils/ImportKlineToSqlite.py --source tqsdk

### 更新主力合约
该功能还不够键全，主力合约需要从交易所拿到所有合约数据，再从合约里找出成交量最大的合约才能找到主力合约，现在单单下面的功能，可能还不行
python utils/GetMainContractWithVolume.py
### 常用命令

```bash
- 切换到TTS通道
openctp-channels switch tts

- 运行K线采集
python KlineCollector.py online

- 回测信号
python .\backtest\strategy_backtest.py

- 回放命令，把04-07的k线进行一条一条的检查是否满足信号，并推送到飞书消息
python test_kline_playback.py --date 2026-04-07

- 运行委托监控流水线（导出CSV → 对比委托 → 飞书通知 → 持仓同步 → 委托执行）
python order-check/run_pipeline.py [online|simu|7x24]

- 运行持仓管理UI（可视化查看持仓与委托状态）
python -c "from trading.PositionManagerUI import main; main()"

# 代码规范
- 共用的代码在 strategies/ 目录下

## 委托监控流水线 (order-check/run_pipeline.py)

`run_pipeline.py` 是交易执行的核心入口，在开盘时间内每 30 秒循环执行：

1. **自动导出** (`automate_export.py`)：从 CTP 客户端自动导出持仓明细和所有委托 CSV
2. **委托对比** (`compare_orders.py`)：对比最新两份 CSV，检测新增委托，写入 `signal.json`
3. **生成标准持仓** (`compare_orders.generate_hold_std()`)：从持仓明细 CSV 生成 `hold-std.json`
4. **持仓同步** (`_do_sync` 后台线程)：对比实际持仓与 `hold-std.json`，缺额补单/超额平仓（30分钟冷却）
5. **委托执行** (`_do_execute` 后台线程)：从 `signal.json` 读取委托，在 CTP 上执行限价单（simu/online 环境使用文件价格）
6. **飞书通知**：委托变化、持仓汇总、交易结束等事件自动推送
7. **旧文件清理**：只保留最新 1-2 份 CSV，防止磁盘堆积

### 环境区分
- `online`：实盘环境，交易时段检查严格，日盘收盘后自动退出
- `simu`：仿真环境，使用委托文件价格，不限时运行
- `7x24`：TTS 测试环境，跳过时段检查，不限时运行

## 交易执行核心 (trading/PositionSyncManager.py)

`PositionSyncManager` 封装了 CTP 交易接口，主要功能：

- **持仓同步** (`sync_and_trade`)：对比 `hold-std.json` 与实际持仓，自动补单/平仓
- **委托执行** (`execute_orders`)：从 `signal.json` 读取委托并执行限价单
- **状态持久化**：`.processed_ids.json` 确保重启后已处理信号不丢失
- **自动撤单重挂**：后台线程每 10 秒检查未成交开仓委托，超时 60 秒自动撤单并用最新对手价重新挂单（最多重挂 2 次）
- **防重复机制**：
  - 已处理报单编号去重
  - 同合约方向有未成交委托时先撤旧单再重挂
  - 平仓单检查实际持仓是否足够
  - 1009（持仓不足）拒绝后自动撤销该合约所有未成交平仓委托，下次重新计算后重试
- **合约标准化**：自动处理 CZCE 4位→3位年月、GFEX/SHFE 小写恢复等格式问题

## 文档

- [ENV_CONFIG_README.md](ENV_CONFIG_README.md) - 环境配置说明
- [KLINECOLLECTOR_STRATEGY_INTEGRATION.md](KLINECOLLECTOR_STRATEGY_INTEGRATION.md) - 策略集成说明
- [rules/style.md](rules/style.md) - 代码风格规范
- [rules/api_design.md](rules/api_design.md) - API 设计规范
- [rules/trading_rules.md](rules/trading_rules.md) - 交易执行规则

## 文档

- [ENV_CONFIG_README.md](ENV_CONFIG_README.md) - 环境配置说明
- [KLINECOLLECTOR_STRATEGY_INTEGRATION.md](KLINECOLLECTOR_STRATEGY_INTEGRATION.md) - 策略集成说明