# 信号图表生成说明

## 功能

生成 TrendReversalV2 策略的信号图表，包含:

- **K 线蜡烛图** - 红涨绿跌
- **买卖信号** - 红色▲买入，绿色▼止盈，橙色▼止损，灰色▼到期
- **MACD 指标** - 蓝色 MACD 线，橙色 Signal 线，柱状图
- **RSI 指标** - 紫色 RSI(14)，超买/超卖线
- **成交量** - 红绿柱状图 + 20 周期均线

## 使用方法

### 方式 1: 回测时自动生成

```bash
cd /home/ubuntu/quant/ctp.examples/openctp-ctp2tts
python3 strategies/backtest_trend_reversal_v2.py CFFEX.IF2603
# 自动生成：
# - backtest_CFFEX_IF2603.json (回测数据)
# - backtest_report_CFFEX_IF2603.html (HTML 报告)
# - signal_chart_CFFEX_IF2603.png (信号图表)
```

### 方式 2: 单独生成图表

```bash
python3 strategies/generate_signal_chart.py CFFEX.IF2603
```

### 批量生成

```bash
python3 strategies/backtest_trend_reversal_v2.py CFFEX.IF2603 CFFEX.IM2603 CZCE.MA605
```

## 输出示例

```
signal_chart_CFFEX_IF2603.png  (200-300KB)
```

## 图表说明

```
┌─────────────────────────────────────────┐
│  CFFEX.IF2603 - Signal Chart            │
├─────────────────────────────────────────┤
│  ┌───────────────────────────────────┐  │
│  │  K 线图 + 买卖信号                  │  │
│  │  ▲ = 买入  ▼ = 卖出                │  │
│  └───────────────────────────────────┘  │
├─────────────────────────────────────────┤
│  MACD                                   │
│  蓝=MACD 橙=Signal 柱=Histogram         │
├─────────────────────────────────────────┤
│  RSI(14)                                │
│  虚线=70/30 (超买/超卖)                 │
├─────────────────────────────────────────┤
│  Volume                                 │
│  蓝线=MA20                              │
└─────────────────────────────────────────┘
```

## 依赖

- matplotlib (绘图)
- numpy (计算)
- sqlite3 (数据库)

## 文件位置

- 脚本：`/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/strategies/`
- 输出：`/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/`

---
最后更新：2026-03-17
