# KlineCollector_v2 与 TrendReversal 策略集成说明

## 概述

本次集成将 `KlineCollector_v2.py`（K 线数据采集和合成程序）与 `TrendReversal` 策略（60 分钟 MACD 底背离策略）相结合，实现：

1. **预检测信号机制**：60 分钟 K 线完成后生成预检测信号
2. **5 分钟入场检查**：5 分钟 K 线完成后检查是否满足入场条件
3. **飞书通知**：策略产生开仓/平仓信号时通过飞书机器人发送通知

## 核心架构变化

### 旧版本 (KlineCollector.py)
```
KlineCollector.py → TrendReversalV7LiveStrategy.py (独立策略引擎)
```

### 新版本 (KlineCollector_v2.py)
```
KlineCollector_v2.py (内置策略逻辑，无需外部策略引擎)
         ↓
    60分钟K线 → 生成预检测信号 (precheck_signals_green/red)
         ↓
    5分钟K线 → 检查预检测信号 → 满足条件则入场
```

## 修改内容

### 1. feishu_notifier.py

`send_feishu_strategy_signal` 方法保持不变：

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

### 2. KlineCollector_v2.py

#### 2.1 导入模块

```python
from utils.feishu_notifier import FeishuNotifier, send_feishu_strategy_signal, send_feishu_test

# 从拆分后的模块导入（公共类）
from strategy import MACDCalculator, ATRCalculator, StackIdentifier
```

#### 2.2 KlineAggregator 初始化

```python
def __init__(self, db_manager, instruments, strategy_signal_manager=None):
    self.db_manager = db_manager
    self.instruments = instruments

    # 策略信号管理器
    self.strategy_signal_manager = strategy_signal_manager

    # 策略配置
    self.db_path = db_manager.db_path
    self.contracts_path = "./data/contracts/main_contracts.json"

    # 预检测信号队列（60分钟产生，5分钟检查）
    self.precheck_signals_green = {}  # {symbol: [signal, ...]}
    self.precheck_signals_red = {}    # {symbol: [signal, ...]}

    # 持仓状态 {symbol: position_info}
    self.positions = {}

    # 上次入场时间 {symbol: datetime}
    self.last_entry_times = {}

    # 信号冷却时间（小时）
    self.cooldown_hours = 4

    # 记录上次处理的 60m bar 时间（避免重复处理）
    self.last_60m_bar_times = {}

    # 60分钟索引映射（5m索引 -> 60m索引）
    self.index_map_60m = {}  # {symbol: [idx_60m, ...]}
```

#### 2.3 预检测信号生成 (60 分钟 K 线完成后)

```python
def check_60m_signal_v2(self, symbol: str, end_time: str = None):
    """检查 60 分钟 K 线完成后是否产生预检测信号"""
    
    # 从数据库读取 60 分钟 K 线数据
    data_60m = self.db_manager.get_kline_data(symbol, MAX_60M_BARS, 3600, end_time)
    
    # 计算 MACD
    data_60m_with_macd = MACDCalculator.calculate(data_60m)
    
    # 识别绿柱堆
    _, green_stacks_60m, _ = StackIdentifier.identify(data_60m_with_macd)
    
    # 绿柱堆内 DIF 拐头 + 底背离
    if hist_60m < 0:
        dif_turn, _ = strategy.check_60m_dif_turn_in_green(data_60m_with_macd, idx_60m, green_stacks_60m)
        if dif_turn:
            diver_ok, diver_reason, _, _ = strategy.check_60m_divergence(data_60m_with_macd, idx_60m)
            if diver_ok:
                # 生成预检测信号（绿柱堆）
                if symbol not in self.precheck_signals_green:
                    self.precheck_signals_green[symbol] = []
                self.precheck_signals_green[symbol].append({
                    'type': 'green',
                    'sub_type': sub_type,
                    'created_time': current_60m_time,
                    'reason': f"60m DIF拐头 + {diver_reason}"
                })
    
    # 红柱堆内 DIF 拐头
    if hist_60m > 0:
        # 类似逻辑，生成预检测信号（红柱堆）
        ...
```

#### 2.4 入场信号检查 (5 分钟 K 线完成后)

```python
def check_strategy_signal_v2(self, symbol: str, end_time: str = None):
    """检查策略信号（每次从数据库读取数据）"""
    
    # 检查冷却时间
    if not position and symbol in self.last_entry_times:
        hours_passed = (current_time - last_entry).total_seconds() / 3600
        if hours_passed < self.cooldown_hours:
            return
    
    # 检查预检测信号
    all_precheck = []
    if symbol in self.precheck_signals_green:
        all_precheck.extend(self.precheck_signals_green[symbol])
    if symbol in self.precheck_signals_red:
        all_precheck.extend(self.precheck_signals_red[symbol])
    
    # 过滤过期信号（超过 8 小时）
    valid_precheck = []
    for sig in all_precheck:
        hours_old = (current_time - sig_time).total_seconds() / 3600
        if hours_old < 8:
            valid_precheck.append(sig)
    
    if not valid_precheck:
        return
    
    # 从数据库读取 5 分钟和 60 分钟 K 线
    data_5m = self.db_manager.get_kline_data(symbol, MAX_5M_BARS, 300, end_time)
    data_60m = self.db_manager.get_kline_data(symbol, MAX_60M_BARS, 3600, end_time)
    
    # 计算 MACD 和 ATR
    data_5m_with_macd = MACDCalculator.calculate(data_5m)
    data_60m_with_macd = MACDCalculator.calculate(data_60m)
    data_5m_with_atr = ATRCalculator.calculate(data_5m_with_macd, 14)
    
    # 构建60分钟索引映射
    if symbol not in self.index_map_60m:
        self.index_map_60m[symbol] = IndexMapper.precompute_60m_index(data_5m_with_macd, data_60m_with_macd)
    
    # 检查 5 分钟入场条件
    idx_5m = len(data_5m_with_macd) - 1
    
    # 如果有持仓，检查止损
    if position:
        self._check_stop_loss_v2(symbol, data_5m_with_atr, position)
        return
    
    # 检查入场条件（遍历所有有效预检测信号）
    for sig in valid_precheck:
        if sig_type == 'green':
            # 检查绿柱堆预检测信号的5分钟入场条件
            entry_ok, entry_reason = strategy.check_5m_entry_for_green(
                data_5m_with_atr, idx_5m, green_stacks_5m, data_60m_with_macd, idx_60m
            )
            if entry_ok:
                # 生成入场信号
                ...
                # 发送飞书通知
                send_feishu_strategy_signal(symbol, signal_data)
        
        elif sig_type == 'red':
            # 检查红柱堆预检测信号的5分钟入场条件
            ...
```

#### 2.5 在 save_kline 方法中调用

```python
def save_kline(self, instrument_name, kline, duration=300):
    # ... 保存 K 线到数据库 ...
    
    if duration == 300:
        # 5 分钟 K 线完成后检查策略信号
        print_log(f"保存 K 线：{symbol} {date_time_str} O={kline['open']:.2f} H={kline['high']:.2f} L={kline['low']:.2f} C={kline['close']:.2f} V={kline['vol']}")
        self.check_strategy_signal_v2(symbol)
    else:
        # 60 分钟 K 线完成后，检查是否产生预检测信号
        if duration == 3600:
            self.check_60m_signal_v2(symbol, end_time=date_time_str)
```

## 工作流程

```
┌─────────────────────────────────────────────────────────────┐
│                    KlineCollector_v2.py                     │
│                                                             │
│  1. 接收 Tick 数据                                           │
│     ↓                                                       │
│  2. 合成 K 线（5min/30min/60min/day）                          │
│     ↓                                                       │
│  3. 保存 K 线到数据库                                         │
│     ↓                                                       │
├─────────────────────────────────────────────────────────────┤
│  路径 A：60 分钟 K 线完成后                                    │
│     ↓                                                       │
│  4. 调用 check_60m_signal_v2(symbol)                        │
│     ↓                                                       │
│  5. 从数据库读取 60 分钟 K 线                                  │
│     ↓                                                       │
│  6. 计算 MACD，识别绿柱堆                                      │
│     ↓                                                       │
│  7. 检查 DIF 拐头 + 底背离条件                                 │
│     ↓                                                       │
│  8. 生成预检测信号（存入 precheck_signals_green/red）          │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  路径 B：5 分钟 K 线完成后                                     │
│     ↓                                                       │
│  9. 调用 check_strategy_signal_v2(symbol)                   │
│     ↓                                                       │
│ 10. 读取有效预检测信号（8小时内）                               │
│     ↓                                                       │
│ 11. 从数据库读取 5 分钟和 60 分钟 K 线                          │
│     ↓                                                       │
│ 12. 计算 MACD/ATR，构建索引映射                                │
│     ↓                                                       │
│ 13. 检查 5 分钟入场条件                                       │
│     ↓                                                       │
│ 14. 满足条件则生成 ENTRY_LONG 信号                             │
│     ↓                                                       │
│ 15. 调用 send_feishu_strategy_signal(symbol, signal_data)   │
│     ↓                                                       │
│ 16. 飞书机器人发送通知                                        │
└─────────────────────────────────────────────────────────────┘
```

## 策略逻辑回顾

### 预检测信号（60 分钟）

#### 绿柱堆内 DIF 拐头 + 底背离
- 60 分钟 MACD 在绿柱堆内（hist < 0）
- DIF 二次拐头：dif_3 > dif_2 < dif_1 < dif_0
- 底背离：当前绿柱堆 K 线低点 >= 前一个绿柱堆 K 线低点

#### 红柱堆内 DIF 拐头
- 60 分钟 MACD 在红柱堆内（hist > 0）
- DIF 拐头：dif_5 > dif_3 < dif_4
- 无需底背离（趋势已向上）

### 入场信号（5 分钟）

#### 绿柱堆预检测 → 5 分钟入场
- MACD 红柱（hist > 0）
- 阳柱确认（close > open）
- DIF 二次拐头 或 绿柱堆萎缩

#### 红柱堆预检测 → 5 分钟入场
- MACD 红柱（hist > 0）
- 阳柱确认（close > open）
- DIF 拐头

### 止损管理

- **初始止损**：前前绿柱堆间 K 线低点
- **移动止损**：每次绿柱转红后，移动止损到前前绿柱堆间 K 线低点

## 配置说明

### 飞书 Webhook

在 `utils/feishu_notifier.py` 中配置：

```python
FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/你的 webhook 地址"
```

### 策略参数配置

在 `KlineCollector_v2.py` 中：

```python
# 信号冷却时间（小时）
self.cooldown_hours = 4

# 预检测信号有效期（小时）
PRECHECK_SIGNAL_EXPIRE_HOURS = 8
```

### 数据库路径配置

```python
# 策略配置
self.db_path = db_manager.db_path
self.contracts_path = "./data/contracts/main_contracts.json"
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
2026-03-22 21:00:00 [INFO] 保存60分钟K 线：CZCE.CF605 2026-03-22 21:00:00
2026-03-22 21:00:01 [INFO] ✓ CZCE.CF605 60分钟预检测信号已生成

2026-03-22 21:30:00 [INFO] 保存 K 线：CZCE.CF605 2026-03-22 21:30:00 O=15445.00 H=15455.00 L=15440.00 C=15450.00 V=58393815
2026-03-22 21:30:01 [INFO] 📈 CZCE.CF605 策略开仓信号：{'signal_type': 'ENTRY_LONG', 'price': 15450.0, 'stop_loss': 15380.0, 'position_size': 3, 'reason': '60m 底背离确认 + 5m DIF 二次拐头', 'time': '2026-03-22 21:30:00'}
2026-03-22 21:30:02 [INFO] ✓ CZCE.CF605 飞书开仓信号已发送
```

## 测试方法

### 1. 测试飞书通知

```bash
cd /home/ubuntu/low-low-up
python -c "from utils.feishu_notifier import send_feishu_strategy_signal; send_feishu_strategy_signal('TEST', {'signal_type': 'ENTRY_LONG', 'price': 10000, 'stop_loss': 9900, 'position_size': 1, 'reason': '测试信号', 'time': '2026-03-22 21:00:00'})"
```

### 2. 测试 KlineCollector_v2

```bash
cd /home/ubuntu/low-low-up
python KlineCollector_v2.py
```

### 3. 使用回放脚本测试

```bash
cd /home/ubuntu/low-low-up
python test_kline_playback.py
```

## 注意事项

1. **数据依赖**：策略依赖 SQLite 数据库中的 K 线数据，确保 KlineCollector 正常运行
2. **预检测信号机制**：新版本采用 60 分钟预检测 + 5 分钟入场的两级机制，提高信号准确性
3. **索引映射**：使用 IndexMapper 正确映射 5 分钟和 60 分钟数据的对应关系
4. **冷却时间**：飞书通知和入场信号都有冷却时间控制，避免重复
5. **信号有效期**：预检测信号有效期为 8 小时，超时后自动失效

## 与旧版本对比

| 特性 | 旧版本 (KlineCollector.py) | 新版本 (KlineCollector_v2.py) |
|------|---------------------------|------------------------------|
| 策略引擎 | 外部 LiveStrategyEngine 类 | 内置策略逻辑 |
| 信号机制 | 5分钟K线直接触发策略检查 | 60分钟预检测 + 5分钟入场 |
| 索引映射 | 策略引擎内部处理 | 使用 IndexMapper 显式映射 |
| 数据读取 | 策略引擎独立读取 | 聚合器统一从数据库读取 |
| 代码结构 | 依赖外部模块 | 自包含，无需外部策略类 |

## 文件清单

| 文件 | 说明 |
|------|------|
| `KlineCollector_v2.py` | K 线数据采集和合成程序（已更新） |
| `utils/feishu_notifier.py` | 飞书通知模块 |
| `strategy/__init__.py` | 策略公共模块（MACDCalculator, ATRCalculator, StackIdentifier, IndexMapper） |
| `KLINECOLLECTOR_STRATEGY_INTEGRATION.md` | 本文档 |