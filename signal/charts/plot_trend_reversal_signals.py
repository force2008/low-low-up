#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TrendReversalV2 策略信号图表 - 优化版 (MACD)
参考 plot_all_contracts.py 的绘图方式
"""

import sys
import os
import sqlite3
import json
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

plt.rcParams['font.sans-serif'] = ['SimHei', 'WenQuanYi Micro Hei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False


def load_kline_data(db_path: str, symbol: str, duration: int = 300, limit: int = 500) -> list:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT datetime, open, high, low, close, volume 
        FROM kline_data WHERE symbol = ? AND duration = ?
        ORDER BY datetime DESC LIMIT ?
    """, (symbol, duration, limit))
    rows = cursor.fetchall()
    conn.close()
    return [{'time': row[0], 'open': row[1], 'high': row[2], 'low': row[3], 
             'close': row[4], 'volume': row[5]} for row in reversed(rows)]


def calc_macd(closes: np.ndarray, fast=12, slow=26, signal=9):
    """计算 MACD"""
    n = len(closes)
    
    # EMA
    ema12 = np.zeros(n)
    ema26 = np.zeros(n)
    
    ema12[0] = closes[0]
    ema26[0] = closes[0]
    
    for i in range(1, n):
        ema12[i] = (closes[i] - ema12[i-1]) * 2/(fast+1) + ema12[i-1]
        ema26[i] = (closes[i] - ema26[i-1]) * 2/(slow+1) + ema26[i-1]
    
    macd_line = ema12 - ema26
    
    # Signal line
    sig_line = np.zeros(n)
    sig_line[0] = macd_line[0]
    for i in range(1, n):
        sig_line[i] = (macd_line[i] - sig_line[i-1]) * 2/(signal+1) + sig_line[i-1]
    
    # Histogram
    histogram = macd_line - sig_line
    
    return macd_line, sig_line, histogram


def plot_symbol_with_signals(symbol: str, data: list, trades: list, output_path: str = None):
    if len(data) < 50:
        print(f"⚠️ 数据不足：{symbol}")
        return None
    
    x = np.arange(len(data))
    closes = np.array([d['close'] for d in data])
    opens = np.array([d['open'] for d in data])
    highs = np.array([d['high'] for d in data])
    lows = np.array([d['low'] for d in data])
    
    # 布林带
    period = 20
    bb_mid, bb_up, bb_low = [], [], []
    for i in range(len(closes)):
        if i < period - 1:
            bb_mid.append(np.nan)
            bb_up.append(np.nan)
            bb_low.append(np.nan)
        else:
            m = np.mean(closes[i-period+1:i+1])
            std = np.std(closes[i-period+1:i+1])
            bb_mid.append(m)
            bb_up.append(m + 2*std)
            bb_low.append(m - 2*std)
    
    bb_mid = np.array(bb_mid)
    bb_up = np.array(bb_up)
    bb_low = np.array(bb_low)
    
    # MACD
    macd_line, sig_line, histogram = calc_macd(closes)
    
    # 总盈亏决定颜色
    total_pnl = sum(t.get('pnl_amount', 0) if isinstance(t, dict) else t.pnl_amount for t in trades) if trades else 0
    is_winning = total_pnl > 0
    up_color = '#d62728' if is_winning else '#2ca02c'
    down_color = '#1f77b4' if is_winning else '#d62728'
    
    # 图表
    fig = plt.figure(figsize=(16, 7))
    gs = fig.add_gridspec(2, 1, height_ratios=[3, 1], hspace=0.05)
    ax1 = fig.add_subplot(gs[0])
    
    # K 线
    for i in range(len(data)):
        color = up_color if closes[i] >= opens[i] else down_color
        ax1.plot([x[i], x[i]], [lows[i], highs[i]], color=color, linewidth=0.8)
        h = closes[i] - opens[i]
        if abs(h) < 0.0001: h = 0.0001
        rect = Rectangle((x[i] - 0.3, min(opens[i], closes[i])), 0.6, h,
                        facecolor=color, edgecolor=color, linewidth=0)
        ax1.add_patch(rect)
    
    # 布林带
    valid_mask = ~np.isnan(bb_mid)
    if np.any(valid_mask):
        ax1.plot(x[valid_mask], bb_mid[valid_mask], 'b-', linewidth=1.2, label='MA20', alpha=0.7)
        ax1.plot(x[valid_mask], bb_up[valid_mask], 'r--', linewidth=0.8, label='Upper', alpha=0.5)
        ax1.plot(x[valid_mask], bb_low[valid_mask], 'g--', linewidth=0.8, label='Lower', alpha=0.5)
    
    # 信号
    if trades:
        for trade in trades:
            entry_idx = int(trade.get('entry_idx', 0) if isinstance(trade, dict) else trade.entry_idx)
            exit_reason = trade.get('exit_reason', '') if isinstance(trade, dict) else trade.exit_reason
            pnl = trade.get('pnl_amount', 0) if isinstance(trade, dict) else trade.pnl_amount
            exit_time = trade.get('exit_time', '') if isinstance(trade, dict) else str(trade.exit_time)
            
            exit_bar_idx = entry_idx + 50
            if exit_time:
                for i in range(entry_idx + 1, min(entry_idx + 300, len(data))):
                    if str(data[i]['time'])[:19] == str(exit_time)[:19]:
                        exit_bar_idx = i
                        break
            
            # 入场
            ax1.annotate('↑', xy=(entry_idx, lows[entry_idx] * 0.998), 
                        color='gold', fontsize=14, fontweight='bold', ha='center', va='top',
                        bbox=dict(boxstyle='round,pad=0.3', facecolor='black', edgecolor='gold', linewidth=1.5))
            # 出场
            exit_color = 'green' if pnl > 0 else 'red'
            exit_label = 'TP' if exit_reason == '止盈' else ('SL' if exit_reason == '止损' else 'TO')
            ax1.annotate(exit_label, xy=(exit_bar_idx, highs[exit_bar_idx] * 1.002), 
                        color=exit_color, fontsize=10, fontweight='bold', ha='center', va='bottom',
                        bbox=dict(boxstyle='round,pad=0.2', facecolor='white', edgecolor=exit_color, linewidth=1.5))
    
    # 标题
    status = 'WIN' if total_pnl > 0 else 'LOSS' if trades else ''
    status_color = '#2ca02c' if is_winning else '#d62728'
    title = f"{symbol} - {status} | {len(trades)} trades | PnL: {total_pnl:+,.0f}" if trades else symbol
    ax1.set_title(title, fontsize=13, fontweight='bold', color=status_color)
    ax1.set_ylabel('Price', fontsize=11)
    ax1.legend(loc='upper left', fontsize=9)
    ax1.grid(True, alpha=0.25)
    
    tick_step = max(1, len(x) // 10)
    ax1.set_xticks(list(range(0, len(x), tick_step)))
    ax1.set_xlabel('K-line Index', fontsize=10)
    
    # MACD 子图
    ax2 = fig.add_subplot(gs[1], sharex=ax1)
    
    # MACD 柱状图
    bar_colors = ['green' if h >= 0 else 'red' for h in histogram]
    ax2.bar(x, histogram, color=bar_colors, alpha=0.7, width=0.6)
    
    # MACD 线和 Signal 线
    ax2.plot(x, macd_line, 'blue', linewidth=1.5, label='MACD')
    ax2.plot(x, sig_line, 'orange', linewidth=1.5, label='Signal')
    
    # 零线
    ax2.axhline(y=0, color='gray', linestyle='-', linewidth=0.5)
    
    ax2.set_ylabel('MACD', fontsize=10)
    ax2.legend(loc='upper left', fontsize=8)
    ax2.grid(True, alpha=0.25)
    
    plt.subplots_adjust(left=0.06, right=0.98, top=0.92, bottom=0.10, hspace=0.05)
    
    if not output_path:
        symbol_safe = symbol.replace('.', '_')
        output_path = f'/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/backtest/{symbol_safe}/signal_chart.png'
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"✅ 图表：{output_path}")
    return output_path


def main():
    db_path = '/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/kline_data.db'
    
    if len(sys.argv) < 2:
        print("用法：python3 plot_trend_reversal_signals.py CFFEX.IF2603")
        print("   或：python3 plot_trend_reversal_signals.py --batch")
        return
    
    if sys.argv[1] == '--batch':
        backtest_dir = '/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/backtest'
        for symbol_dir in os.listdir(backtest_dir):
            dir_path = os.path.join(backtest_dir, symbol_dir)
            if os.path.isdir(dir_path):
                json_file = os.path.join(dir_path, 'backtest_results.json')
                if os.path.exists(json_file):
                    symbol = symbol_dir.replace('_', '.')
                    with open(json_file, 'r') as f:
                        data = json.load(f)
                    kline_data = load_kline_data(db_path, symbol, 300, 500)
                    trades = data.get('trades', [])
                    plot_symbol_with_signals(symbol, kline_data, trades, os.path.join(dir_path, 'signal_chart.png'))
        return
    
    symbol = sys.argv[1]
    symbol_safe = symbol.replace('.', '_')
    json_file = f'/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/backtest/{symbol_safe}/backtest_results.json'
    
    kline_data = load_kline_data(db_path, symbol, 300, 500)
    trades = []
    if os.path.exists(json_file):
        with open(json_file, 'r') as f:
            data = json.load(f)
            trades = data.get('trades', [])
    
    plot_symbol_with_signals(symbol, kline_data, trades)


if __name__ == '__main__':
    main()
