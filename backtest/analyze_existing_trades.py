#!/usr/bin/env python3
"""
从现有交易CSV + 实时K线数据计算indicator值并分析

每笔交易：根据entry_time去K线数据里查那个时刻的RSI等indicator值
"""

import sys
import os
import sqlite3
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from backtest.cumulative_curve import CumulativeCurveAnalyzer, TradePnL
from backtest.indicators import RSI, SMA, EMA
from utils.strategy_config import Config


def load_trades_from_csv(csv_path: str) -> list:
    """从CSV加载交易"""
    trades = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        header = f.readline().strip().split(',')
        for line in f:
            parts = line.strip().split(',')
            if len(parts) < 9:
                continue
            try:
                trade = {
                    'symbol': parts[0],
                    'entry_time': parts[1],
                    'entry_price': float(parts[2]),
                    'exit_time': parts[3],
                    'exit_price': float(parts[4]),
                    'pnl': float(parts[7]),
                }
                trades.append(trade)
            except (ValueError, IndexError):
                continue
    return trades


def get_klines_before_time(db_path: str, symbol: str, entry_time: str,
                        lookback: int = 100) -> list:
    """获取entry_time之前的K线数据，用于计算indicator"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 查找该时刻之前的数据
    t = entry_time[:19]
    cursor.execute("""
        SELECT open, high, low, close, volume
        FROM kline_data
        WHERE symbol = ? AND datetime <= ? AND duration = 300
        ORDER BY datetime DESC
        LIMIT ?
    """, (symbol, t, lookback))

    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return []

    # 反转，最新的在最后
    rows.reverse()
    return rows


def calculate_rsi_at_entry(klines: list, period: int = 14) -> float:
    """计算RSI"""
    if not klines or len(klines) < period:
        return None

    # 只取close价格
    closes = [row[3] for row in klines]
    values = RSI.calculate_simple(closes, period)

    # 返回最后一个（非None的值）
    for v in reversed(values):
        if v is not None:
            return v
    return None


def calculate_sma_at_entry(klines: list, period: int = 20) -> float:
    """计算SMA"""
    if not klines or len(klines) < period:
        return None

    closes = [row[3] for row in klines]
    values = SMA.calculate_simple(closes, period)

    for v in reversed(values):
        if v is not None:
            return v
    return None


def main():
    csv_path = './trading/backtest_trades_0328.csv'
    db_path = Config.DB_PATH

    print(f"加载交易记录...")
    trades = load_trades_from_csv(csv_path)
    print(f"共 {len(trades)} 笔交易")

    # 分析每个indicator
    indicators = {
        'RSI_14': lambda klines: calculate_rsi_at_entry(klines, 14),
        'RSI_20': lambda klines: calculate_rsi_at_entry(klines, 20),
        'SMA_20': lambda klines: calculate_sma_at_entry(klines, 20),
    }

    results = {}

    for ind_name, calc_func in indicators.items():
        print(f"\n分析 {ind_name}...")

        indicator_values = []
        pnl_values = []

        processed = 0
        for trade in trades:
            symbol = trade['symbol']
            entry_time = trade['entry_time']

            # 获取K线数据
            klines = get_klines_before_time(db_path, symbol, entry_time, 100)
            if not klines:
                continue

            # 计算indicator
            ind_val = calc_func(klines)
            if ind_val is None:
                continue

            indicator_values.append(ind_val)
            pnl_values.append(trade['pnl'])
            processed += 1

        if not indicator_values:
            print(f"  无法计算 {ind_name}")
            continue

        print(f"  有效数据: {processed} 笔交易")

        # 分析
        analyzer = CumulativeCurveAnalyzer()

        # 峰值法
        lower, upper, max_p = analyzer.find_best_boundary(indicator_values, pnl_values)

        # 0线穿越法
        left, right, crossings = analyzer.find_zero_crossing_bounds(indicator_values, pnl_values)

        # 统计
        total_pnl = sum(pnl_values)
        winning = sum(1 for p in pnl_values if p > 0)
        win_rate = winning / len(pnl_values) * 100

        results[ind_name] = {
            'trades': len(pnl_values),
            'total_pnl': total_pnl,
            'win_rate': win_rate,
            'lower_bound': lower,
            'upper_bound': upper,
            'profit_zone': (left, right),
            'crossings': len(crossings),
        }

    # 输出结果
    print(f"\n{'=' * 60}")
    print("分析结果汇总")
    print(f"{'=' * 60}")

    for name, r in results.items():
        print(f"\n{name}:")
        print(f"  有效交易数: {r['trades']}")
        print(f"  总PnL: {r['total_pnl']:,.0f}")
        print(f"  胜率: {r['win_rate']:.1f}%")
        print(f"  峰值法边界: [{r['lower_bound']:.1f}, {r['upper_bound']:.1f}]")
        print(f"  0线法盈利区间: [{r['profit_zone'][0]:.1f}, {r['profit_zone'][1]:.1f}]")
        print(f"  0线穿越点: {r['crossings']}")


if __name__ == '__main__':
    main()