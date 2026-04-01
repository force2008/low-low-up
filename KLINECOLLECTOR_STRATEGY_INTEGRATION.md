# KlineCollector 与 TrendReversalV7LiveStrategy 集成说明

## 概述

本次集成将 `KlineCollector.py`（K 线数据采集和合成程序）与 `TrendReversalV7LiveStrategy.py`（60 分钟 MACD 底背离策略）相结合，实现：
1. 在 5 分钟 K 线合成完成后自动执行策略检查
2. 当策略生成开仓信号时，通过飞书机器人发送通知

## 修改内容

### 1. feishu_notifier.py

新增 `send_strategy_signal` 方法和 `send_feishu_strategy_signal` 快捷函数：

```python
def send_feishu_strategy_signal(symbol: str, signal_data: dict) -> bool:
    """
    快捷函数：发送策略开仓信号
    
    Args:
        symbol: 合约代码
        signal_data: 策略信号数据
            - signal_type: 信号类型 (ENTRY_LONG/EXIT_LONG)
            - price: 价格
            - stop_loss: 止损价
            - position_size: 手数
            - reason: 信号原因
            - time: 信号时间
    
    Returns:
        bool: 发送是否成功
    """
```

**飞书消息格式示例：**

```
📈 策略开仓信号 - 做多
━━━━━━━━━━━━━━━━━━━━
🟢 合约：CZCE.CF605
信号类型：ENTRY_LONG
入场价格：15450.00
止损价格：15380.00
开仓手数：3 手

信号原因：60m 底背离确认 + 5m DIF 二次拐头

信号时间：2026-03-22 21:30:00
```

### 2. KlineCollector.py

#### 2.1 导入新增模块

```python
from feishu_notifier import FeishuNotifier, send_feishu_signal, send_feishu_high_volatility_alert, send_feishu_strategy_signal
from strategies.TrendReversalV7LiveStrategy import LiveStrategyEngine, LiveConfig, SignalType
```

#### 2.2 KlineAggregator 初始化策略引擎

```python
def __init__(self, db_manager, instruments, vol_calculator=None, signal_manager=None, breakout_detector=None, enable_strategy=True):
    # ... 其他初始化代码 ...
    
    # 策略引擎配置
    self.enable_strategy = enable_strategy
    self.strategy_engines = {}  # {symbol: LiveStrategyEngine}
    if enable_strategy:
        print_log("初始化策略引擎...")
        # 为每个合约创建策略引擎
        for inst_id in self.instrument_map.keys():
            exchange_id = self.instrument_map[inst_id].get("ExchangeID", "")
            symbol = f"{exchange_id}.{inst_id}"
            try:
                engine = LiveStrategyEngine(symbol)
                engine.initialize()
                self.strategy_engines[symbol] = engine
                print_log(f"  策略引擎已初始化：{symbol}")
            except Exception as e:
                print_log(f"  策略引擎初始化失败 {symbol}: {e}")
        print_log(f"策略引擎初始化完成，共 {len(self.strategy_engines)} 个合约")
```

#### 2.3 新增 check_strategy_signal 方法

```python
def check_strategy_signal(self, symbol: str):
    """检查趋势反转策略信号（60 分钟 MACD 底背离策略）
    
    在 5 分钟 K 线完成后调用策略引擎检查信号
    """
    if not self.enable_strategy:
        return
    
    if symbol not in self.strategy_engines:
        return
    
    engine = self.strategy_engines[symbol]
    
    try:
        # 获取最新的 5 分钟 K 线
        bar = self.get_latest_5m_bar(symbol)
        if bar is None:
            return
        
        # 处理 5 分钟 K 线（策略引擎会合成 60 分钟 K 线并检查策略）
        engine.on_5m_bar(bar)
        
        # 获取生成的信号
        signals = engine.get_signals(clear=True)
        
        # 发送信号
        for signal in signals:
            if signal.signal_type == SignalType.ENTRY_LONG:
                # 开仓信号
                signal_data = {
                    'signal_type': 'ENTRY_LONG',
                    'price': signal.price,
                    'stop_loss': signal.stop_loss,
                    'position_size': signal.position_size,
                    'reason': signal.reason,
                    'time': signal.time
                }
                print_log(f"📈 {symbol} 策略开仓信号：{signal_data}")
                
                # 发送飞书通知
                try:
                    send_feishu_strategy_signal(symbol, signal_data)
                    print_log(f"✓ {symbol} 飞书开仓信号已发送")
                except Exception as e:
                    print_log(f"✗ {symbol} 飞书开仓信号发送失败：{e}")
            
            elif signal.signal_type == SignalType.EXIT_LONG:
                # 平仓信号
                signal_data = {
                    'signal_type': 'EXIT_LONG',
                    'price': signal.price,
                    'stop_loss': 0,
                    'position_size': 0,
                    'reason': signal.reason,
                    'time': signal.time
                }
                print_log(f"📉 {symbol} 策略平仓信号：{signal_data}")
                
                # 发送飞书通知
                try:
                    send_feishu_strategy_signal(symbol, signal_data)
                    print_log(f"✓ {symbol} 飞书平仓信号已发送")
                except Exception as e:
                    print_log(f"✗ {symbol} 飞书平仓信号发送失败：{e}")
    
    except Exception as e:
        print_log(f"✗ {symbol} 策略信号检查失败：{e}")
```

#### 2.4 新增 get_latest_5m_bar 方法

```python
def get_latest_5m_bar(self, symbol: str) -> tuple:
    """获取最新的 5 分钟 K 线
    
    返回：(datetime, open, high, low, close, volume)
    """
    try:
        df = self.get_kline_history(symbol, limit=1, duration=300)
        if len(df) > 0:
            row = df.iloc[0]
            return (
                row['datetime'].strftime('%Y-%m-%d %H:%M:%S'),
                float(row['open']),
                float(row['high']),
                float(row['low']),
                float(row['close']),
                int(row['volume'])
            )
    except Exception as e:
        print_log(f"获取最新 K 线失败 {symbol}: {e}")
    return None
```

#### 2.5 在 save_kline 方法中调用策略检查

```python
def save_kline(self, instrument_name, kline, duration=300):
    # ... 保存 K 线到数据库 ...
    
    # 只在 5 分钟 K 线保存后检查波动率切换、突破信号和策略信号
    if duration == 300:
        print_log(f"保存 K 线：{symbol} {date_time_str} O={kline['open']:.2f} H={kline['high']:.2f} L={kline['low']:.2f} C={kline['close']:.2f} V={kline['vol']} OI={kline['open_interest']}")
        self.check_volatility_switch(symbol)
        self.check_breakout_signal(symbol)
        self.check_strategy_signal(symbol)  # 新增：策略信号检查
```

## 工作流程

```
┌─────────────────────────────────────────────────────────────┐
│                    KlineCollector.py                        │
│                                                             │
│  1. 接收 Tick 数据                                           │
│     ↓                                                       │
│  2. 合成 5 分钟 K 线                                           │
│     ↓                                                       │
│  3. 保存 K 线到数据库                                         │
│     ↓                                                       │
│  4. 调用 check_strategy_signal(symbol)                      │
│     ↓                                                       │
│  5. 获取最新 5 分钟 K 线 bar                                    │
│     ↓                                                       │
│  6. 调用 engine.on_5m_bar(bar)                              │
│     ↓                                                       │
│  7. 策略引擎合成 60 分钟 K 线                                    │
│     ↓                                                       │
│  8. 策略引擎检查 MACD 底背离条件                               │
│     ↓                                                       │
│  9. 如果满足条件，生成 Signal 对象                              │
│     ↓                                                       │
│  10. 调用 send_feishu_strategy_signal(symbol, signal_data) │
│     ↓                                                       │
│  11. 飞书机器人发送通知                                      │
└─────────────────────────────────────────────────────────────┘
```

## 策略逻辑回顾

### 60 分钟入场条件（满足任一即可）

1. **绿柱堆内 DIF 拐头 + 底背离**
   - 60 分钟 MACD 在绿柱堆内（hist < 0）
   - DIF 二次拐头：dif_3 > dif_2 < dif_1 < dif_0
   - 底背离：当前绿柱堆 K 线低点 >= 前一个绿柱堆 K 线低点

2. **红柱堆内 DIF 拐头**
   - 60 分钟 MACD 在红柱堆内（hist > 0）
   - DIF 拐头：dif_5 > dif_3 < dif_4
   - 无需底背离（趋势已向上）

3. **传统逻辑：绿柱堆结束转红**
   - 前一根是绿柱（hist < 0），当前是红柱（hist > 0）
   - 底背离条件满足

### 5 分钟入场条件

- MACD 红柱（hist > 0）
- 阳柱确认（close > open）
- DIF 二次拐头 或 绿柱堆萎缩

### 止损管理

- **初始止损**：前前绿柱堆间 K 线低点
- **移动止损**：每次绿柱转红后，移动止损到前前绿柱堆间 K 线低点

## 配置说明

### 飞书 Webhook

在 `feishu_notifier.py` 中配置：

```python
FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/你的 webhook 地址"
```

### 数据库路径配置（重要）

**注意**：`KlineCollector.py` 使用线上数据库（`kline_data.db`），而 `TrendReversalV7LiveStrategy.py` 默认可能使用不同的数据库。

集成时，`KlineCollector.py` 会将自身的数据库路径传递给策略引擎，确保使用同一数据库：

```python
# KlineCollector.py 中
# 获取数据库路径（使用 KlineCollector 的配置）
self.db_path = db_manager.db_path
self.contracts_path = "main_contracts.json"

# 传入 KlineCollector 的数据库路径
engine = LiveStrategyEngine(symbol, db_path=self.db_path, contracts_path=self.contracts_path)
```

### 策略引擎配置

在 `TrendReversalV7LiveStrategy.py` 中的 `LiveConfig` 类配置（默认值，可被外部传入覆盖）：

```python
class LiveConfig:
    # 数据库配置（默认值，可通过构造函数覆盖）
    DB_PATH = "kline_data.db"  # 默认使用当前目录的数据库
    CONTRACTS_PATH = "main_contracts.json"  # 默认使用当前目录的合约配置
    
    DURATION_5M = 300   # 5 分钟
    DURATION_60M = 3600 # 60 分钟
    MAX_5M_BARS = 5000  # 最多保留 5000 根 5 分钟 K 线
    
    TARGET_NOTIONAL = 100000  # 目标货值（10 万）
    COOLDOWN_HOURS = 4        # 冷却期 4 小时
    MAX_POSITION = 1          # 最大持仓手数
```

### 飞书通知冷却时间

在 `feishu_notifier.py` 中：

```python
# 5 分钟内不重复发送同一合约的同类型信号
if last_time and (now - last_time).total_seconds() < 300:
    return False
```

## 日志输出示例

```
2026-03-22 21:30:00 [INFO] 保存 K 线：CZCE.CF605 2026-03-22 21:30:00 O=15445.00 H=15455.00 L=15440.00 C=15450.00 V=58393815 OI=12345
2026-03-22 21:30:01 [INFO] 📈 CZCE.CF605 策略开仓信号：{'signal_type': 'ENTRY_LONG', 'price': 15450.0, 'stop_loss': 15380.0, 'position_size': 3, 'reason': '60m 底背离确认 + 5m DIF 二次拐头', 'time': '2026-03-22 21:30:00'}
2026-03-22 21:30:02 [INFO] ✓ CZCE.CF605 飞书开仓信号已发送
```

## 测试方法

### 1. 测试飞书通知

```bash
cd /home/ubuntu/quant/ctp.examples/openctp-ctp2tts
python -c "from feishu_notifier import send_feishu_strategy_signal; send_feishu_strategy_signal('TEST', {'signal_type': 'ENTRY_LONG', 'price': 10000, 'stop_loss': 9900, 'position_size': 1, 'reason': '测试信号', 'time': '2026-03-22 21:00:00'})"
```

### 2. 测试策略引擎

```bash
cd /home/ubuntu/quant/ctp.examples/openctp-ctp2tts
python strategies/TrendReversalV7LiveStrategy.py
```

### 3. 运行 KlineCollector

```bash
cd /home/ubuntu/quant/ctp.examples/openctp-ctp2tts
python KlineCollector.py
```

## 注意事项

1. **数据依赖**：策略依赖 SQLite 数据库中的 K 线数据，确保 KlineCollector 正常运行
2. **初始化时间**：策略引擎初始化需要加载历史数据，可能需要几秒钟
3. **冷却时间**：飞书通知有 5 分钟冷却时间，避免重复发送
4. **错误处理**：所有策略检查和飞书发送都有 try-except 包裹，不会因异常导致程序崩溃
5. **性能影响**：策略检查在 5 分钟 K 线保存后执行，对性能影响很小

## 禁用策略检查

如果只需要 K 线采集功能，可以禁用策略检查：

```python
# 修改 KlineCollector.py 中的 KlineAggregator 初始化
kline_aggregator = KlineAggregator(
    db_manager, 
    instruments, 
    vol_calculator, 
    signal_manager, 
    breakout_detector,
    enable_strategy=False  # 设置为 False 禁用策略
)
```

## 文件清单

| 文件 | 说明 |
|------|------|
| `KlineCollector.py` | K 线数据采集和合成程序（已修改） |
| `feishu_notifier.py` | 飞书通知模块（已修改） |
| `strategies/TrendReversalV7LiveStrategy.py` | 实盘策略引擎（新增） |
| `strategies/LIVE_STRATEGY_README.md` | 策略使用说明（新增） |
| `KLINECOLLECTOR_STRATEGY_INTEGRATION.md` | 本文档（新增） |