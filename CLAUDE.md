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


# 代码规范
- 共用的代码在strateg里
```

## 文档

- [ENV_CONFIG_README.md](ENV_CONFIG_README.md) - 环境配置说明
- [KLINECOLLECTOR_STRATEGY_INTEGRATION.md](KLINECOLLECTOR_STRATEGY_INTEGRATION.md) - 策略集成说明