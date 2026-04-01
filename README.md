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

### 运行

```bash
# 切换到TTS通道
openctp-channels switch tts

# 运行K线采集
python KlineCollector.py
```

## 文档

- [ENV_CONFIG_README.md](ENV_CONFIG_README.md) - 环境配置说明
- [KLINECOLLECTOR_STRATEGY_INTEGRATION.md](KLINECOLLECTOR_STRATEGY_INTEGRATION.md) - 策略集成说明
