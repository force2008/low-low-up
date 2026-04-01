# 多时间框架实盘策略 v7 使用说明

## 策略概述

基于 backtest_v7.py 回测策略开发的实盘版本，保持回测逻辑不变，添加实盘交易所需功能。

### 策略核心逻辑

**60 分钟趋势确认：**
- MACD 绿柱堆内 DIF 拐头 + 底背离确认
- MACD 红柱堆内 DIF 拐头（趋势向上）
- 传统逻辑：绿柱堆结束转红 + 底背离

**5 分钟精确入场：**
- MACD 红柱 + 阳柱确认
- DIF 二次拐头 或 绿柱堆萎缩

**止损管理：**
- 初始止损：前前绿柱堆间 K 线低点
- 移动止损：每次绿柱转红后上移止损

## 文件结构

```
strategies/
├── TrendReversalV7LiveStrategy.py  # 实盘策略主文件
├── LIVE_STRATEGY_README.md         # 使用说明文档
└── ...其他策略文件
```

## 核心组件

### 1. LiveStrategyEngine（策略引擎）

主要功能：
- 管理 5 分钟和 60 分钟 K 线数据
- 从 5 分钟 K 线合成 60 分钟 K 线
- 执行策略逻辑检查
- 生成交易信号
- 管理持仓和止损

### 2. TrendReversalStrategy（策略逻辑）

包含所有策略判断逻辑：
- `check_60m_dif_turn_in_green()` - 检查 60m 绿柱堆内 DIF 拐头
- `check_60m_divergence()` - 检查 60m 底背离
- `check_60m_dif_turn_in_red()` - 检查 60m 红柱堆内 DIF 拐头
- `check_5m_entry()` - 检查 5 分钟入场条件
- `get_initial_stop_loss()` - 获取初始止损价
- `get_mobile_stop()` - 获取移动止损价

### 3. KlineSynthesizer（K 线合成器）

从 5 分钟 K 线合成 60 分钟 K 线：
- 实时合成：每根 5 分钟 K 线到来时更新当前 60 分钟 K 线
- 完成判断：55 分时确认 60 分钟 K 线完成
- 触发策略检查：60 分钟 K 线完成后执行策略

### 4. CTPExecutor（CTP 订单执行器）

负责与 CTP 接口对接：
- 连接 CTP 交易前置
- 发送订单
- 撤销订单
- 查询持仓

## 使用方法

### 基础使用

```python
from strategies.TrendReversalV7LiveStrategy import LiveStrategyEngine, LiveConfig

# 创建配置
config = LiveConfig()
config.TARGET_NOTIONAL = 100000  # 目标货值
config.COOLDOWN_HOURS = 4        # 冷却期

# 创建策略引擎
engine = LiveStrategyEngine("CZCE.CF605", config)

# 初始化（加载历史数据）
engine.initialize()

# 处理 5 分钟 K 线推送（实盘时由行情回调触发）
def on_5m_bar(bar):
    """
    bar: (datetime, open, high, low, close, volume)
    """
    engine.on_5m_bar(bar)

# 获取生成的信号
signals = engine.get_signals(clear=True)
for signal in signals:
    print(f"{signal.signal_type.value} | {signal.time} | {signal.price:.2f}")

# 获取当前持仓
position = engine.get_position()
if position:
    print(f"持仓：{position.symbol} | 入场价：{position.entry_price:.2f}")

# 获取策略状态
status = engine.get_status()
print(status)
```

### 与 CTP 行情接口集成

```python
from base_mdapi import CMdSpiBase
from base_tdapi import CTdSpiBase
from strategies.TrendReversalV7LiveStrategy import LiveStrategyEngine, SignalType

class StrategyMdSpi(CMdSpiBase):
    """行情回调"""
    
    def __init__(self, engine: LiveStrategyEngine):
        super().__init__()
        self.engine = engine
    
    def OnRtnDepthMarketData(self, pMarketData):
        """行情推送回调"""
        # 将行情转换为 K 线数据（需要 K 线合成逻辑）
        # 这里需要实现 K 线合成器
        pass

class StrategyTdSpi(CTdSpiBase):
    """交易回调"""
    
    def __init__(self, engine: LiveStrategyEngine):
        super().__init__()
        self.engine = engine
    
    def OnRtnOrder(self, pRtnOrder):
        """订单回报回调"""
        pass
    
    def OnRtnTrade(self, pRtnTrade):
        """成交回报回调"""
        pass

# 启动流程
# 1. 创建策略引擎
engine = LiveStrategyEngine("CZCE.CF605")
engine.initialize()

# 2. 创建行情和交易实例
md_spi = StrategyMdSpi(engine)
td_spi = StrategyTdSpi(engine)

# 3. 订阅行情
# md_spi.subscribe_market_data("CZCE.CF605")
```

### 完整实盘示例

```python
#!/usr/bin/env python3
"""
实盘策略启动脚本
"""

from strategies.TrendReversalV7LiveStrategy import (
    LiveStrategyEngine, LiveConfig, SignalType, CTPExecutor
)
from base_mdapi import CMdSpiBase
from base_tdapi import CTdSpiBase
import openctp_ctp
from openctp_ctp import mdapi, tdapi

class LiveTradingSystem:
    """实盘交易系统"""
    
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.config = LiveConfig()
        
        # 创建策略引擎
        self.engine = LiveStrategyEngine(symbol, self.config)
        
        # 创建 CTP 执行器
        self.executor = CTPExecutor(self.config)
        
        # 状态
        self.is_running = False
    
    def start(self):
        """启动交易系统"""
        print("启动交易系统...")
        
        # 初始化策略引擎
        self.engine.initialize()
        
        # 连接 CTP
        self.executor.connect()
        
        # 订阅行情
        # self.md_api.SubscribeMarketData([self.symbol])
        
        self.is_running = True
        print("交易系统启动完成")
    
    def on_market_data(self, bar_5m: tuple):
        """处理 5 分钟 K 线"""
        if not self.is_running:
            return
        
        # 更新策略
        self.engine.on_5m_bar(bar_5m)
        
        # 获取新生成的信号
        signals = self.engine.get_signals(clear=True)
        
        # 执行信号
        for signal in signals:
            self._execute_signal(signal)
    
    def _execute_signal(self, signal):
        """执行交易信号"""
        if signal.signal_type == SignalType.ENTRY_LONG:
            # 开多
            order_id = self.executor.send_order(signal)
            print(f"开多订单发送：{order_id}")
        
        elif signal.signal_type == SignalType.EXIT_LONG:
            # 平多
            order_id = self.executor.send_order(signal)
            print(f"平多订单发送：{order_id}")
    
    def stop(self):
        """停止交易系统"""
        self.is_running = False
        print("交易系统停止")


def main():
    system = LiveTradingSystem("CZCE.CF605")
    system.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        system.stop()


if __name__ == "__main__":
    main()
```

## 配置参数

### LiveConfig 类

| 参数 | 说明 | 默认值 |
|------|------|--------|
| DB_PATH | SQLite 数据库路径 | kline_data.db |
| CONTRACTS_PATH | 主力合约列表路径 | main_contracts.json |
| DURATION_5M | 5 分钟 K 线 duration | 300 |
| DURATION_60M | 60 分钟 K 线 duration | 3600 |
| MAX_5M_BARS | 最大 5 分钟 K 线数量 | 5000 |
| TARGET_NOTIONAL | 目标货值（元） | 100000 |
| COOLDOWN_HOURS | 冷却期（小时） | 4 |
| MAX_POSITION | 最大持仓手数 | 1 |
| SIGNAL_EXPIRY_MINUTES | 信号有效期（分钟） | 30 |

### CTP 配置

```python
config.TD_FRONT = "tcp://你的交易前置地址"
config.MD_FRONT = "tcp://你的行情前置地址"
config.BROKER_ID = "你的经纪公司 ID"
config.USER_ID = "你的用户 ID"
config.PASSWORD = "你的密码"
config.APP_ID = "应用 ID"
config.AUTH_CODE = "授权码"
config.USER_PRODUCT_INFO = "产品信息"
```

## 信号类型

| 信号类型 | 说明 |
|----------|------|
| ENTRY_LONG | 做多入场 |
| EXIT_LONG | 平多出场 |
| ENTRY_SHORT | 做空入场（预留） |
| EXIT_SHORT | 平空出场（预留） |

## 数据结构

### Signal（信号）

```python
@dataclass
class Signal:
    signal_type: SignalType      # 信号类型
    symbol: str                  # 合约代码
    price: float                 # 价格
    time: str                    # 时间
    reason: str                  # 信号原因
    stop_loss: float             # 止损价
    take_profit: float           # 止盈价
    position_size: int           # 手数
    expiry_time: Optional[str]   # 过期时间
    extra_data: dict             # 额外数据
```

### Position（持仓）

```python
@dataclass
class Position:
    symbol: str                  # 合约代码
    direction: str               # 方向（long/short）
    entry_time: str              # 入场时间
    entry_price: float           # 入场价
    position_size: int           # 手数
    initial_stop: float          # 初始止损价
    current_stop: float          # 当前止损价
    stop_reason: str             # 止损原因
```

## 注意事项

1. **数据依赖**：策略依赖 SQLite 数据库中的 K 线数据，确保 KlineCollector 正常运行
2. **CTP 环境**：实盘需要配置正确的 CTP 环境和账户信息
3. **冷却期**：默认 4 小时冷却期，避免频繁交易
4. **止损管理**：策略使用移动止损，确保及时锁定利润
5. **信号有效期**：信号 30 分钟过期，避免延迟执行

## 与回测版本的差异

| 特性 | 回测版 (backtest_v7.py) | 实盘版 (TrendReversalV7LiveStrategy.py) |
|------|------------------------|----------------------------------------|
| 数据来源 | SQLite 历史数据 | 实时 K 线推送 |
| 60 分钟 K 线 | 直接加载 | 从 5 分钟合成 |
| 信号输出 | 打印日志 | Signal 对象列表 |
| 订单执行 | 无 | CTPExecutor 发送订单 |
| 持仓管理 | 内部状态 | Position 对象 |
| 止损检查 | 回测循环中 | 每根 K 线触发 |

## 测试方法

```bash
# 运行测试
cd /home/ubuntu/quant/ctp.examples/openctp-ctp2tts
python strategies/TrendReversalV7LiveStrategy.py
```

## 下一步开发

1. 完善 CTPExecutor 的 CTP 接口实现
2. 添加 K 线合成器与行情接口的集成
3. 添加订单状态管理
4. 添加风险控制模块
5. 添加日志和监控