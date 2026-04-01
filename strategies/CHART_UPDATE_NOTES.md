# 信号图表优化说明

## 更新内容

已按照 `plot_all_contracts.py` 的方式优化 TrendReversalV2 策略的信号图表。

## 主要改进

### 1. 使用索引作为 x 轴
- ❌ 之前：使用时间轴，非交易时间导致显示不连续
- ✅ 现在：使用 K-line Index，连续显示

### 2. 图表布局优化
- ❌ 之前：4 个子图 (K 线+MACD+RSI+ 成交量)，16x12
- ✅ 现在：2 个子图 (K 线 + 成交量)，16x7，更宽更扁

### 3. 动态配色方案
- ❌ 之前：固定红涨绿跌
- ✅ 现在：根据总盈亏动态调整
  - 盈利：红色 K 线 (上涨) / 蓝色 (下跌)
  - 亏损：绿色 K 线 (上涨) / 红色 (下跌)

### 4. 信号标记优化
- ❌ 之前：简单的 ▲▼ 标记
- ✅ 现在：带边框和背景的箭头
  - 入场：金色↑箭头，黑色背景
  - 出场：TP(绿色)/SL(红色)/TO(灰色)，白色背景

### 5. 布林带显示
- ✅ MA20 (蓝色实线)
- ✅ 上轨 (红色虚线)
- ✅ 下轨 (绿色虚线)

### 6. 字体优化
- ✅ 使用 SimHei 支持中文
- ✅ 标签使用英文避免乱码

## 文件结构

```
strategies/
├── plot_trend_reversal_signals.py   # 优化版图表生成脚本
├── backtest_trend_reversal_v2.py    # 回测脚本 (已更新)
└── CHART_UPDATE_NOTES.md            # 本说明文档
```

## 使用方法

### 方式 1: 回测时自动生成
```bash
python3 strategies/backtest_trend_reversal_v2.py CFFEX.IF2603
```

### 方式 2: 单独生成图表
```bash
python3 strategies/plot_trend_reversal_signals.py CFFEX.IF2603
```

### 方式 3: 批量生成
```bash
python3 strategies/plot_trend_reversal_signals.py --batch
```

## 输出示例

```
backtest/CFFEX_IF2603/signal_chart.png
```

图表标题示例：
```
CFFEX.IF2603 - WIN | 2 trades | PnL: +65,100
```

## 对比

| 特性 | 旧版 | 新版 |
|------|------|------|
| x 轴 | 时间 | K-line Index |
| 子图数量 | 4 | 2 |
| 图表尺寸 | 16x12 | 16x7 |
| 配色 | 固定 | 动态 |
| 信号标记 | 简单 | 带边框背景 |
| 布林带 | ✓ | ✓ |
| MACD | ✓ | ✗ |
| RSI | ✓ | ✗ |

## 注意事项

- 移除了 MACD 和 RSI 子图，使图表更简洁
- 如需查看完整指标，可使用 `plot_signal.py`
- 图表自动保存到 `backtest/{SYMBOL}/signal_chart.png`

---
最后更新：2026-03-17

## 2026-03-17 更新：MACD 指标

✅ 底部子图已从成交量 (VOL) 改为 MACD 指标

### MACD 显示内容
- **蓝色线**: MACD 线 (DIF)
- **橙色线**: Signal 线 (DEA)
- **柱状图**: MACD Histogram (绿涨红跌)
- **灰色水平线**: 零线

### 图表结构
```
┌─────────────────────────────────────────┐
│  K 线图 + 信号标记 + 布林带              │
│  ↑入场 TP/SL 出场                        │
├─────────────────────────────────────────┤
│  MACD                                   │
│  蓝=MACD 橙=Signal 柱=Histogram         │
└─────────────────────────────────────────┘
```

### 使用方法不变
```bash
python3 strategies/plot_trend_reversal_signals.py CFFEX.IF2603
python3 strategies/plot_trend_reversal_signals.py --batch
```
