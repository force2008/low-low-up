#!/usr/bin/env python3
"""
过滤噪音数据后的详细分析
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import sqlite3
from backtest.indicators import RSI, SMA
from backtest.cumulative_curve import CumulativeCurveAnalyzer
from utils.strategy_config import Config


def load_trades(csv_path):
    trades = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        f.readline()
        for line in f:
            parts = line.strip().split(',')
            if len(parts) >= 8:
                try:
                    trades.append({
                        'symbol': parts[0],
                        'entry_time': parts[1],
                        'pnl': float(parts[7]),
                    })
                except:
                    continue
    return trades


def get_klines(db_path, symbol, entry_time, lookback=100):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    t = entry_time[:19]
    cursor.execute("""
        SELECT close
        FROM kline_data
        WHERE symbol = ? AND datetime <= ? AND duration = 300
        ORDER BY datetime DESC
        LIMIT ?
    """, (symbol, t, lookback))
    rows = cursor.fetchall()
    conn.close()
    rows.reverse()
    return [r[0] for r in rows]


def main():
    csv_path = './trading/backtest_trades_0328.csv'
    db_path = Config.DB_PATH

    print("加载交易...")
    trades = load_trades(csv_path)
    print(f"共 {len(trades)} 笔\n")

    # 计算 RSI_14
    rsi_values = []
    pnl_values = []

    for trade in trades:
        closes = get_klines(db_path, trade['symbol'], trade['entry_time'], 50)
        if not closes or len(closes) < 20:
            continue

        values = RSI.calculate_simple(closes, 14)
        rsi = None
        for v in reversed(values):
            if v is not None and v > 10:  # 过滤无效值
                rsi = v
                break

        if rsi is None or rsi <= 10:
            continue

        rsi_values.append(rsi)
        pnl_values.append(trade['pnl'])

    print(f"有效数据: {len(rsi_values)} 笔（过滤了RSI<=10）")

    # 排序
    paired = list(zip(rsi_values, pnl_values))
    paired.sort(key=lambda x: x[0])

    sorted_rsi = [x[0] for x in paired]
    sorted_pnl = [x[1] for x in paired]

    # 累积曲线
    cumsum = []
    total = 0.0
    for pnl in sorted_pnl:
        total += pnl
        cumsum.append(total)

    print(f"\n总PnL: {total:,.0f}")

    # 找峰值
    max_idx = cumsum.index(max(cumsum))
    min_idx = cumsum.index(min(cumsum))

    print(f"\n峰值位置:")
    print(f"  最高点: index={max_idx}, RSI={sorted_rsi[max_idx]:.1f}, cumsum={cumsum[max_idx]:,.0f}")
    print(f"  最低点: index={min_idx}, RSI={sorted_rsi[min_idx]:.1f}, cumsum={cumsum[min_idx]:,.0f}")

    # 0线穿越点（有意义的）
    print(f"\n0线穿越点（累积从负变正 或 从正变负）:")
    meaningful_crossings = []
    for i in range(1, len(cumsum)):
        before = cumsum[i - 1]
        after = cumsum[i]
        # 只取RSI > 20的有意义点
        if sorted_rsi[i] > 20:
            if (before < 0 <= after) or (before > 0 >= after):
                meaningful_crossings.append((i, sorted_rsi[i], before, after))
                print(f"  RSI={sorted_rsi[i]:.1f}: cumsum {before:,.0f} -> {after:,.0f}")

    # 最终边界
    print(f"\n{'=' * 60}")
    print("最终边界结论")
    print(f"{'=' * 60}")

    # 方法1：峰值法
    print(f"方法1 - 峰值法:")
    print(f"  盈利区间: RSI 在 {sorted_rsi[min_idx]:.1f} 到 {sorted_rsi[max_idx]:.1f}")

    # 方法2：0线法
    if len(meaningful_crossings) >= 2:
        # 第一个正->负（上限）和最后一个负->正（下限）
        lower_rsi = meaningful_crossings[-1][1]  # 最后一个（负->正）
        upper_rsi = meaningful_crossings[0][1]  # 第一个（正->负）
        print(f"方法2 - 0线法:")
        print(f"  盈利区间: RSI 在 {lower_rsi:.1f} 到 {upper_rsi:.1f}")
    else:
        print(f"方法2 - 0线法: 无法确定（穿越点不足）")


if __name__ == '__main__':
    main()