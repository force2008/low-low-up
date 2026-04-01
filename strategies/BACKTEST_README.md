# TrendReversalV2 策略回测系统

## 功能特性

- ✅ 数据源：SQLite 数据库 `kline_data.db`
- ✅ 合约配置：`main_contracts.json`
- ✅ K 线数据：5 分钟 500 条 + 60 分钟 200 条
- ✅ 输出：JSON 结果 + HTML 可视化报告
- ✅ 支持批量回测多个合约

## 策略逻辑

1. **趋势过滤** - 只做上升趋势或震荡，避开明显下跌
2. **60 分钟超跌** - 价格从近期高点回调超过 3%
3. **5 分钟 MACD 背离** - MACD 指标上升，显示动能转强
4. **止损止盈** - 止损 3%，止盈 5%
5. **最大持有** - 100 个 5 分钟 bar（约 8 小时）

## 使用方法

### 基本用法

```bash
cd /home/ubuntu/quant/ctp.examples/openctp-ctp2tts

# 回测单个合约
python3 strategies/backtest_trend_reversal_v2.py CFFEX.IC2606

# 回测多个合约
python3 strategies/backtest_trend_reversal_v2.py CFFEX.IF2603 CFFEX.IM2603 CZCE.MA605

# 交互式选择合约
python3 strategies/backtest_trend_reversal_v2.py
```

### 自定义策略参数

```bash
python3 strategies/backtest_trend_reversal_v2.py CFFEX.IC2606 --config '{"stop_loss_pct":0.02,"take_profit_pct":0.06}'
```

## 输出文件

- `backtest_{SYMBOL}.json` - 详细回测数据（JSON 格式）
- `backtest_report_{SYMBOL}.html` - 可视化报告（浏览器打开）

## 回测结果示例

| 合约 | 收益率 | 交易次数 | 胜率 | 盈亏比 | 最大回撤 |
|------|--------|----------|------|--------|----------|
| CFFEX.IF2603 | +65.10% | 7 | 85.71% | 5.67 | 4.23% |
| CZCE.MA605 | +8.18% | 11 | 81.82% | 1.30 | 0.82% |
| DCE.m2605 | +2.57% | 3 | 66.67% | 5.08 | 0.27% |

## 注意事项

⚠️ **风险提示**
- 回测结果基于历史数据，不代表未来表现
- 未考虑交易成本、滑点等因素
- 实际交易中可能存在流动性限制
- 本策略仅供学习研究，不构成投资建议

## 数据库说明

数据库路径：`/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/kline_data.db`

表结构：
```sql
CREATE TABLE kline_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    datetime TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    duration INTEGER NOT NULL  -- 300=5 分钟，3600=60 分钟
);
```

## 合约配置

配置文件：`main_contracts.json`

包含字段：
- `ProductID` - 品种代码
- `MainContractID` - 主力合约代码
- `VolumeMultiple` - 合约乘数
- `PriceTick` - 最小价格变动

---

最后更新：2026-03-17
