#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TrendReversalV3 策略信号图表生成器
"""

import sys
import os
import sqlite3
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from TrendReversalV2Strategy_V3 import TrendReversalV3Strategy


def load_kline_data(db_path: str, symbol: str, duration: int, limit: int) -> list:
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


def plot_v3_signals(symbol: str, data: list, signals: list, output_path: str = None):
    """绘制 V3 策略信号图表"""
    
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
    def calc_macd(closes, fast=12, slow=26, signal=9):
        n = len(closes)
        ema12 = np.zeros(n)
        ema26 = np.zeros(n)
        ema12[0] = closes[0]
        ema26[0] = closes[0]
        for i in range(1, n):
            ema12[i] = (closes[i] - ema12[i-1]) * 2/13 + ema12[i-1]
            ema26[i] = (closes[i] - ema26[i-1]) * 2/27 + ema26[i-1]
        macd_line = ema12 - ema26
        sig_line = np.zeros(n)
        sig_line[0] = macd_line[0]
        for i in range(1, n):
            sig_line[i] = (macd_line[i] - sig_line[i-1]) * 2/10 + sig_line[i-1]
        histogram = macd_line - sig_line
        return macd_line, sig_line, histogram
    
    macd_line, sig_line, histogram = calc_macd(closes)
    
    # 总盈亏决定颜色
    total_pnl = sum(s.pnl_amount for s in signals) if signals else 0
    is_winning = total_pnl > 0
    up_color = '#d62728' if is_winning else '#2ca02c'
    down_color = '#1f77b4' if is_winning else '#d62728'
    
    # 图表
    fig = plt.figure(figsize=(16, 9))
    gs = fig.add_gridspec(3, 1, height_ratios=[3, 1, 1], hspace=0.05)
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
    
    # V3 信号
    if signals:
        for idx, sig in enumerate(signals, 1):
            entry_idx = int(sig.entry_idx)
            exit_reason = sig.exit_reason
            pnl = sig.pnl_amount
            exit_time = str(sig.exit_time)
            
            # 找到出场点
            exit_idx = entry_idx + 50
            for i in range(entry_idx + 1, min(entry_idx + 300, len(data))):
                if str(data[i]['time'])[:19] == exit_time[:19]:
                    exit_idx = i
                    break
            
            # 入场 - 金色向上箭头
            ax1.annotate('↑', xy=(entry_idx, lows[entry_idx] * 0.998), 
                        color='gold', fontsize=14, fontweight='bold', ha='center', va='top',
                        bbox=dict(boxstyle='round,pad=0.3', facecolor='black', 
                                 edgecolor='gold', linewidth=1.5))
            
            # 出场 - TP/SL/TO
            exit_color = 'green' if pnl > 0 else 'red'
            exit_label = 'TP' if exit_reason == '止盈' else ('SL' if exit_reason == '止损' else 'TO')
            if 0 <= exit_idx < len(x):
                ax1.annotate(exit_label, xy=(exit_idx, highs[exit_idx] * 1.002), 
                            color=exit_color, fontsize=11, fontweight='bold', ha='center', va='bottom',
                            bbox=dict(boxstyle='round,pad=0.2', facecolor='white', 
                                     edgecolor=exit_color, linewidth=1.5))
    
    # 标题
    status = 'WIN' if total_pnl > 0 else 'LOSS' if signals else ''
    status_color = '#2ca02c' if is_winning else '#d62728'
    title = f"{symbol} - V3 Strategy | {status} | {len(signals)} trades | PnL: {total_pnl:+,.0f}" if signals else f"{symbol} - V3 Strategy"
    ax1.set_title(title, fontsize=13, fontweight='bold', color=status_color)
    ax1.set_ylabel('Price', fontsize=11)
    ax1.legend(loc='upper left', fontsize=9)
    ax1.grid(True, alpha=0.25)
    
    tick_step = max(1, len(x) // 10)
    ax1.set_xticks(list(range(0, len(x), tick_step)))
    ax1.set_xlabel('K-line Index', fontsize=10)
    
    # MACD
    ax2 = fig.add_subplot(gs[1], sharex=ax1)
    bar_colors = ['green' if h >= 0 else 'red' for h in histogram]
    ax2.bar(x, histogram, color=bar_colors, alpha=0.7, width=0.6)
    ax2.plot(x, macd_line, 'blue', linewidth=1.5, label='MACD')
    ax2.plot(x, sig_line, 'orange', linewidth=1.5, label='Signal')
    ax2.axhline(y=0, color='gray', linestyle='-', linewidth=0.5)
    ax2.set_ylabel('MACD', fontsize=10)
    ax2.legend(loc='upper left', fontsize=8)
    ax2.grid(True, alpha=0.25)
    
    # 成交量
    ax3 = fig.add_subplot(gs[2], sharex=ax1)
    vol_colors = [up_color if closes[i] >= opens[i] else down_color for i in range(len(data))]
    ax3.bar(x, [d['volume'] for d in data], color=vol_colors, alpha=0.7, width=0.6)
    vol_ma = np.convolve([d['volume'] for d in data], np.ones(20)/20, mode='same')
    ax3.plot(x, vol_ma, 'blue', linewidth=1.5, label='MA20')
    ax3.set_ylabel('Volume', fontsize=10)
    ax3.set_xlabel('K-line Index', fontsize=10)
    ax3.legend(loc='upper left', fontsize=8)
    ax3.grid(True, alpha=0.25)
    
    plt.subplots_adjust(left=0.06, right=0.98, top=0.92, bottom=0.08, hspace=0.05)
    
    if not output_path:
        symbol_safe = symbol.replace('.', '_')
        output_path = f'/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/backtest/{symbol_safe}/signal_chart_v3.png'
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"✅ V3 图表：{output_path}")
    return output_path


def main():
    db_path = '/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/kline_data.db'
    contracts_path = '/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/main_contracts.json'
    
    print("\n📊 生成 V3 策略信号图表\n")
    
    test_symbols = ['CFFEX.IF2603', 'CFFEX.IC2606', 'CZCE.MA605', 'DCE.m2605']
    
    strategy = TrendReversalV3Strategy()
    strategy.load_contracts(contracts_path)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    for symbol in test_symbols:
        print(f"\n{'='*60}")
        print(f"📈 {symbol}")
        print(f"{'='*60}")
        
        cursor.execute("SELECT datetime, open, high, low, close, volume FROM kline_data WHERE symbol = ? AND duration = 300 ORDER BY datetime DESC LIMIT 500", (symbol,))
        data_5min = [{'time': row[0], 'open': row[1], 'high': row[2], 'low': row[3], 'close': row[4], 'volume': row[5]} for row in reversed(cursor.fetchall())]
        
        cursor.execute("SELECT datetime, open, high, low, close, volume FROM kline_data WHERE symbol = ? AND duration = 3600 ORDER BY datetime DESC LIMIT 200", (symbol,))
        data_60min = [{'time': row[0], 'open': row[1], 'high': row[2], 'low': row[3], 'close': row[4], 'volume': row[5]} for row in reversed(cursor.fetchall())]
        
        if len(data_5min) < 200 or len(data_60min) < 50:
            print(f"⚠️ 数据不足，跳过")
            continue
        
        # V3 回测
        signals = strategy.run_backtest(symbol, data_5min, data_60min) or []
        
        # 生成图表
        plot_v3_signals(symbol, data_5min, signals)
    
    conn.close()
    
    print(f"\n{'='*60}")
    print("✅ V3 图表生成完成!")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
