#!/usr/bin/env python3
"""
最终版本：正确找到边界
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import sqlite3
from backtest.indicators import RSI
from utils.strategy_config import Config


def load_trades(csv_path):
    trades = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        f.readline()
        for line in f:
            parts = line.strip().split(',')
            if len(parts) >= 8:
                try:
                    trades.append({'symbol': parts[0], 'entry_time': parts[1], 'pnl': float(parts[7])})
                except:
                    continue
    return trades


def get_klines(db_path, symbol, entry_time, lookback=100):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT close FROM kline_data
        WHERE symbol = ? AND datetime <= ? AND duration = 300
        ORDER BY datetime DESC LIMIT ?
    """, (symbol, entry_time[:19], lookback))
    rows = cursor.fetchall()
    conn.close()
    return [r[0] for r in rows]


def main():
    csv_path = './trading/backtest_trades_0328.csv'
    db_path = Config.DB_PATH
    trades = load_trades(csv_path)
    print(f"共 {len(trades)} 笔\n")

    rsi_values, pnl_values = [], []
    for trade in trades:
        closes = get_klines(db_path, trade['symbol'], trade['entry_time'], 50)
        if not closes or len(closes) < 20:
            continue
        values = RSI.calculate_simple(closes, 14)
        rsi = next((v for v in reversed(values) if v and v > 10), None)
        if rsi:
            rsi_values.append(rsi)
            pnl_values.append(trade['pnl'])

    # 排序、累积
    paired = sorted(zip(rsi_values, pnl_values), key=lambda x: x[0])
    sorted_rsi = [x[0] for x in paired]
    sorted_pnl = [x[1] for x in paired]

    cumsum = []
    for p in sorted_pnl:
        cumsum.append(cumsum[-1] + p if cumsum else p)

    total_pnl = cumsum[-1]
    print(f"总PnL: {total_pnl:,}")

    max_cumsum = max(cumsum)
    max_idx = cumsum.index(max_cumsum)

    # 方法：从峰值向两边找到累积下降50%的位置
    threshold = max_cumsum * 0.5
    print(f"\n累积峰值: {max_cumsum:,.0f} (在RSI={sorted_rsi[max_idx]:.1f})")
    print(f"50%阈值: {threshold:,.0f}")

    # 向左找
    for i in range(max_idx, 0, -1):
        if cumsum[i] < threshold:
            print(f"  左边界: RSI={sorted_rsi[i]:.1f} (累积={cumsum[i]:,.0f})")
            left = sorted_rsi[i]
            break

    # 向右找
    for i in range(max_idx, len(cumsum)-1):
        if cumsum[i] < threshold:
            print(f"  右边界: RSI={sorted_rsi[i]:.1f} (累积={cumsum[i]:,.0f})")
            right = sorted_rsi[i]
            break

    print("\n" + "=" * 60)
    print("结论")
    print("=" * 60)
    try:
        print(f"\n盈利区间: RSI 在 {left:.1f} 到 {right:.1f}")
    except:
        print("\n无法确定边界")


if __name__ == '__main__':
    main()