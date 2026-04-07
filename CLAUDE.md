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
│   └── feishu_notifier.py
├── logs/             # 日志目录
└── tests/            # 测试脚本
```

## 快速开始

### 安装依赖

```bash
pip install openctp-ctp==6.7.2.*
pip install openctp-ctp-channels
pip install numpy pandas matplotlib
```

### 常用命令

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

## 文档

- [ENV_CONFIG_README.md](ENV_CONFIG_README.md) - 环境配置说明
- [KLINECOLLECTOR_STRATEGY_INTEGRATION.md](KLINECOLLECTOR_STRATEGY_INTEGRATION.md) - 策略集成说明