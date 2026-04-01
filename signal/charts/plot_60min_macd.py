#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
绘制 60 分钟 K 线和 MACD 指标图 (无乱码版本)
用于观察 V5 策略的信号捕捉
"""

import sys
import os
import sqlite3
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

# 只使用英文字体，避免乱码
plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial']
plt.rcParams['axes.unicode_minus'] = False

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def load_kline_data(db_path: str, symbol: str, duration: int, limit: int) -> list:
    """加载 K 线数据"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT datetime, open, high, low, close, volume 
        FROM kline_data WHERE symbol = ? AND duration = ?
        ORDER BY datetime ASC LIMIT ?
    """, (symbol, duration, limit))
    rows = cursor.fetchall()
    conn.close()
    return [{'time': row[0], 'open': row[1], 'high': row[2], 'low': row[3], 
             'close': row[4], 'volume': row[5]} for row in rows]


def calc_macd(closes: np.ndarray) -> tuple:
    """计算 MACD"""
    n = len(closes)
    
    ema12 = np.zeros(n)
    ema26 = np.zeros(n)
    
    ema12[0] = closes[0]
    ema26[0] = closes[0]
    
    for i in range(1, n):
        ema12[i] = (closes[i] - ema12[i-1]) * 2/13 + ema12[i-1]
        ema26[i] = (closes[i] - ema26[i-1]) * 2/27 + ema26[i-1]
    
    dif = ema12 - ema26
    
    dea = np.zeros(n)
    dea[0] = dif[0]
    for i in range(1, n):
        dea[i] = (dif[i] - dea[i-1]) * 2/10 + dea[i-1]
    
    histogram = dif - dea
    
    return dif, dea, histogram


def find_red_bar_stacks(histogram: np.ndarray, min_bars: int = 3) -> list:
    """找红柱堆 (histogram < 0)"""
    red_stacks = []
    n = len(histogram)
    
    i = 0
    while i < n:
        if histogram[i] >= 0:
            i += 1
            continue
        
        stack_start = i
        while i < n and histogram[i] < 0:
            i += 1
        stack_end = i - 1
        
        stack_bars = stack_end - stack_start + 1
        
        if stack_bars >= min_bars:
            red_stacks.append({
                'start_idx': stack_start,
                'end_idx': stack_end,
                'bars': stack_bars,
            })
    
    return red_stacks


def plot_60min_macd(symbol: str, db_path: str, output_path: str = None, limit: int = 200):
    """绘制 60 分钟 MACD 图"""
    
    print(f"\n📊 Plotting {symbol} 60min MACD Chart...\n")
    
    # 加载数据
    data = load_kline_data(db_path, symbol, 3600, limit)
    
    if len(data) < 50:
        print(f"⚠️ Insufficient data")
        return None
    
    # 准备数据
    times = [d['time'] for d in data]
    x = np.arange(len(data))
    closes = np.array([d['close'] for d in data])
    opens = np.array([d['open'] for d in data])
    highs = np.array([d['high'] for d in data])
    lows = np.array([d['low'] for d in data])
    
    # 计算 MACD
    dif, dea, histogram = calc_macd(closes)
    
    # 找红柱堆
    red_stacks = find_red_bar_stacks(histogram, min_bars=3)
    
    # 创建图表
    fig = plt.figure(figsize=(16, 10))
    gs = fig.add_gridspec(3, 1, height_ratios=[2, 1, 1], hspace=0.05)
    
    # ========== 子图 1: K 线图 ==========
    ax1 = fig.add_subplot(gs[0])
    
    # K 线
    up_color = '#d62728'  # 红
    down_color = '#2ca02c'  # 绿
    
    for i in range(len(data)):
        color = up_color if closes[i] >= opens[i] else down_color
        ax1.plot([x[i], x[i]], [lows[i], highs[i]], color=color, linewidth=0.8)
        h = closes[i] - opens[i]
        if abs(h) < 0.0001: h = 0.0001
        rect = Rectangle((x[i] - 0.3, min(opens[i], closes[i])), 0.6, h,
                        facecolor=color, edgecolor=color, linewidth=0)
        ax1.add_patch(rect)
    
    # 标记红柱堆
    for stack_idx, stack in enumerate(red_stacks, 1):
        # 找红柱堆期间的最低价
        stack_low_idx = np.argmin(lows[stack['start_idx']:stack['end_idx']+1])
        stack_low_global_idx = stack['start_idx'] + stack_low_idx
        stack_low = lows[stack_low_global_idx]
        
        # 标记最低价
        ax1.scatter([stack_low_global_idx], [stack_low], 
                   color='blue', marker='v', s=100, zorder=5,
                   label=f'Stack #{stack_idx} Low {stack_low:.0f}' if stack_idx <= 3 else "")
        
        # 标记红柱堆区域
        ax1.axvspan(stack['start_idx'], stack['end_idx'], 
                   alpha=0.1, color='red',
                   label=f'Stack #{stack_idx} ({stack["bars"]} bars)' if stack_idx <= 3 else "")
    
    # 标题
    ax1.set_title(f'{symbol} - 60min K-Line + Red Bar Stacks', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Price')
    ax1.legend(loc='upper left', fontsize=9)
    ax1.grid(True, alpha=0.25)
    
    # 时间轴标签
    tick_step = max(1, len(x) // 10)
    tick_positions = list(range(0, len(x), tick_step))
    tick_labels = [times[i][5:16] for i in tick_positions]
    ax1.set_xticks(tick_positions)
    ax1.set_xticklabels(tick_labels, rotation=45, ha='right', fontsize=8)
    
    # ========== 子图 2: MACD DIF 和 DEA ==========
    ax2 = fig.add_subplot(gs[1], sharex=ax1)
    
    ax2.plot(x, dif, 'blue', linewidth=1.5, label='DIF')
    ax2.plot(x, dea, 'orange', linewidth=1.5, label='DEA')
    ax2.axhline(y=0, color='gray', linestyle='-', linewidth=0.5)
    
    # 标记 DIF 低点
    for stack_idx, stack in enumerate(red_stacks[:3], 1):
        stack_dif_min_idx = np.argmin(dif[stack['start_idx']:stack['end_idx']+1])
        stack_dif_min_global_idx = stack['start_idx'] + stack_dif_min_idx
        stack_dif_min = dif[stack_dif_min_global_idx]
        
        ax2.scatter([stack_dif_min_global_idx], [stack_dif_min], 
                   color='purple', marker='v', s=80, zorder=5,
                   label=f'DIF Low #{stack_idx} {stack_dif_min:.2f}')
    
    ax2.set_ylabel('MACD')
    ax2.legend(loc='upper left', fontsize=9)
    ax2.grid(True, alpha=0.25)
    
    # ========== 子图 3: MACD 柱状图 ==========
    ax3 = fig.add_subplot(gs[2], sharex=ax1)
    
    bar_colors = ['green' if h >= 0 else 'red' for h in histogram]
    ax3.bar(x, histogram, color=bar_colors, alpha=0.7, width=0.8)
    ax3.axhline(y=0, color='gray', linestyle='-', linewidth=0.5)
    
    ax3.set_ylabel('Histogram')
    ax3.set_xlabel('Time')
    ax3.grid(True, alpha=0.25)
    
    # 保存图表
    if not output_path:
        symbol_safe = symbol.replace('.', '_')
        output_path = f'/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/backtest/{symbol_safe}/macd_60min.png'
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"✅ Chart saved: {output_path}\n")
    
    # 打印红柱堆详情
    print(f"{'='*100}")
    print(f"📊 Red Bar Stacks Details")
    print(f"{'='*100}\n")
    
    for i, stack in enumerate(red_stacks, 1):
        stack_low = np.min(lows[stack['start_idx']:stack['end_idx']+1])
        stack_low_idx = np.argmin(lows[stack['start_idx']:stack['end_idx']+1])
        stack_low_global_idx = stack['start_idx'] + stack_low_idx
        
        stack_dif_min = np.min(dif[stack['start_idx']:stack['end_idx']+1])
        stack_dif_min_idx = np.argmin(dif[stack['start_idx']:stack['end_idx']+1])
        stack_dif_min_global_idx = stack['start_idx'] + stack_dif_min_idx
        
        print(f"Stack #{i}:")
        print(f"  Index: [{stack['start_idx']}] - [{stack['end_idx']}]")
        print(f"  Time: {times[stack['start_idx']][5:16]} - {times[stack['end_idx']][5:16]}")
        print(f"  Bars: {stack['bars']}")
        print(f"  Low Price: {stack_low:.2f} (Idx [{stack_low_global_idx}], {times[stack_low_global_idx][5:16]})")
        print(f"  DIF Min: {stack_dif_min:.2f} (Idx [{stack_dif_min_global_idx}])")
        
        if i >= 2:
            prev_stack = red_stacks[i-2]
            prev_low = np.min(lows[prev_stack['start_idx']:prev_stack['end_idx']+1])
            threshold = prev_low * 0.98
            is_rising = stack_low > threshold
            
            status = "✅ Rising" if is_rising else "❌ Lower"
            print(f"  vs Prev: {stack_low:.2f} > {prev_low:.2f}×0.98={threshold:.2f} = {status}")
        
        print()
    
    print(f"{'='*100}\n")
    
    return output_path


def main():
    db_path = '/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/kline_data.db'
    
    print("\n📊 60min MACD Chart Generator\n")
    
    # 测试品种
    test_symbols = ['CFFEX.IC2606', 'CFFEX.IF2603', 'CZCE.MA605', 'DCE.m2605']
    
    for symbol in test_symbols:
        plot_60min_macd(symbol, db_path, limit=200)
    
    print("✅ Charts generated!\n")


if __name__ == '__main__':
    main()
