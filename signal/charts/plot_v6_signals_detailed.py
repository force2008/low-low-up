#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V6 策略详细信号图表 - 60 分钟 MACD 能量背离
包含：K 线图、MACD、红柱堆能量对比、5 分钟信号说明
"""

import sys
import os
import sqlite3
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial']
plt.rcParams['axes.unicode_minus'] = False

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from TrendReversalV2Strategy_V6 import TrendReversalV6Strategy


def load_kline_data(db_path: str, symbol: str, duration: int, limit: int) -> list:
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
    n = len(closes)
    ema12 = np.zeros(n)
    ema26 = np.zeros(n)
    ema12[0] = ema26[0] = closes[0]
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
            energy_sum = np.sum(np.abs(histogram[stack_start:stack_end+1]))
            red_stacks.append({
                'start_idx': stack_start,
                'end_idx': stack_end,
                'bars': stack_bars,
                'energy_sum': energy_sum,
            })
        i = stack_start - 1
    return red_stacks


def plot_v6_signals_detailed(symbol: str, db_path: str, output_path: str = None):
    print(f"\n{'='*100}")
    print(f"📊 V6 Strategy Signal Analysis - {symbol}")
    print(f"{'='*100}\n")
    
    # 加载 60 分钟数据
    data_60 = load_kline_data(db_path, symbol, 3600, 200)
    
    if len(data_60) < 50:
        print("⚠️ Insufficient data")
        return None
    
    # 准备数据
    times_60 = [d['time'] for d in data_60]
    x_60 = np.arange(len(data_60))
    closes_60 = np.array([d['close'] for d in data_60])
    opens_60 = np.array([d['open'] for d in data_60])
    highs_60 = np.array([d['high'] for d in data_60])
    lows_60 = np.array([d['low'] for d in data_60])
    
    # 计算 MACD
    dif_60, dea_60, hist_60 = calc_macd(closes_60)
    
    # 找红柱堆
    red_stacks = find_red_bar_stacks(hist_60, min_bars=3)
    
    # 创建图表
    fig = plt.figure(figsize=(16, 12))
    gs = fig.add_gridspec(4, 1, height_ratios=[2, 1, 1, 1], hspace=0.05)
    
    # ========== 子图 1: 60 分钟 K 线图 ==========
    ax1 = fig.add_subplot(gs[0])
    
    up_color = '#d62728'
    down_color = '#2ca02c'
    
    for i in range(len(data_60)):
        color = up_color if closes_60[i] >= opens_60[i] else down_color
        ax1.plot([x_60[i], x_60[i]], [lows_60[i], highs_60[i]], color=color, linewidth=0.8)
        h = closes_60[i] - opens_60[i]
        if abs(h) < 0.0001: h = 0.0001
        rect = Rectangle((x_60[i] - 0.3, min(opens_60[i], closes_60[i])), 0.6, h,
                        facecolor=color, edgecolor=color, linewidth=0)
        ax1.add_patch(rect)
    
    # 标记红柱堆
    for stack_idx, stack in enumerate(red_stacks[:5], 1):
        stack_lows = lows_60[stack['start_idx']:stack['end_idx']+1]
        stack_low_idx = np.argmin(stack_lows)
        stack_low_global_idx = stack['start_idx'] + stack_low_idx
        stack_low = lows_60[stack_low_global_idx]
        
        ax1.scatter([stack_low_global_idx], [stack_low], 
                   color='blue', marker='v', s=100, zorder=5,
                   label=f'Stack #{stack_idx} Low {stack_low:.0f}' if stack_idx <= 3 else "")
        
        ax1.axvspan(stack['start_idx'], stack['end_idx'], 
                   alpha=0.1, color='red',
                   label=f'Stack #{stack_idx} ({stack["bars"]} bars, E={stack["energy_sum"]:.1f})' if stack_idx <= 3 else "")
    
    ax1.set_title(f'{symbol} - V6 Strategy (60min K-Line + Red Bar Stacks)', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Price')
    ax1.legend(loc='upper left', fontsize=9)
    ax1.grid(True, alpha=0.25)
    
    tick_step = max(1, len(x_60) // 10)
    ax1.set_xticks(list(range(0, len(x_60), tick_step)))
    ax1.set_xticklabels([times_60[i][5:16] for i in range(0, len(x_60), tick_step)], rotation=45, ha='right', fontsize=8)
    
    # ========== 子图 2: MACD DIF 和 DEA ==========
    ax2 = fig.add_subplot(gs[1], sharex=ax1)
    
    ax2.plot(x_60, dif_60, 'blue', linewidth=1.5, label='DIF')
    ax2.plot(x_60, dea_60, 'orange', linewidth=1.5, label='DEA')
    ax2.axhline(y=0, color='gray', linestyle='-', linewidth=0.5)
    
    # 标记 DIF 拐点
    for stack_idx, stack in enumerate(red_stacks[:3], 1):
        stack_dif_min = np.min(dif_60[stack['start_idx']:stack['end_idx']+1])
        stack_dif_min_idx = np.argmin(dif_60[stack['start_idx']:stack['end_idx']+1])
        stack_dif_min_global_idx = stack['start_idx'] + stack_dif_min_idx
        
        ax2.scatter([stack_dif_min_global_idx], [stack_dif_min], 
                   color='purple', marker='v', s=80, zorder=5,
                   label=f'DIF Low #{stack_idx} {stack_dif_min:.2f}')
    
    ax2.set_ylabel('MACD')
    ax2.legend(loc='upper left', fontsize=9)
    ax2.grid(True, alpha=0.25)
    
    # ========== 子图 3: MACD 柱状图 ==========
    ax3 = fig.add_subplot(gs[2], sharex=ax1)
    
    bar_colors = ['green' if h >= 0 else 'red' for h in hist_60]
    ax3.bar(x_60, hist_60, color=bar_colors, alpha=0.7, width=0.8)
    ax3.axhline(y=0, color='gray', linestyle='-', linewidth=0.5)
    
    ax3.set_ylabel('Histogram')
    ax3.grid(True, alpha=0.25)
    
    # ========== 子图 4: 红柱堆能量对比 ==========
    ax4 = fig.add_subplot(gs[3], sharex=ax1)
    
    if len(red_stacks) >= 2:
        energies = [stack['energy_sum'] for stack in red_stacks]
        stack_centers = [(stack['start_idx'] + stack['end_idx']) / 2 for stack in red_stacks]
        
        # 标记能量减弱的红柱堆
        colors = []
        for i in range(len(energies)):
            if i == 0:
                colors.append('gray')
            elif energies[i] < energies[i-1] * 0.8:
                colors.append('green')  # 能量减弱，背离信号
            else:
                colors.append('red')
        
        ax4.bar(stack_centers, energies, width=8, color=colors, alpha=0.7)
        
        # 添加能量比值标注
        for i in range(1, len(energies)):
            ratio = energies[i] / energies[i-1] if energies[i-1] > 0 else 1.0
            ax4.text(stack_centers[i], energies[i] * 1.1, f'{ratio:.2f}', 
                    ha='center', va='bottom', fontsize=8,
                    color='green' if ratio < 0.8 else 'gray')
    
    ax4.set_ylabel('Energy Sum')
    ax4.set_xlabel('Stack Index')
    ax4.set_title('Red Bar Stack Energy Comparison (Green = Divergence < 0.8)', fontsize=10)
    ax4.grid(True, alpha=0.25)
    
    # 保存图表
    if not output_path:
        symbol_safe = symbol.replace('.', '_')
        output_path = f'/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/backtest/{symbol_safe}/v6_signals_detailed.png'
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"✅ Chart saved: {output_path}\n")
    
    # ========== 打印 5 分钟信号详情 ==========
    print(f"{'='*100}")
    print(f"📊 60 分钟 MACD 红柱堆分析")
    print(f"{'='*100}\n")
    
    for i, stack in enumerate(red_stacks, 1):
        stack_lows = lows_60[stack['start_idx']:stack['end_idx']+1]
        stack_low = np.min(stack_lows)
        stack_low_idx = np.argmin(stack_lows)
        stack_low_global_idx = stack['start_idx'] + stack_low_idx
        
        stack_dif_min = np.min(dif_60[stack['start_idx']:stack['end_idx']+1])
        stack_dif_min_idx = np.argmin(dif_60[stack['start_idx']:stack['end_idx']+1])
        stack_dif_min_global_idx = stack['start_idx'] + stack_dif_min_idx
        
        print(f"Stack #{i}:")
        print(f"  Index: [{stack['start_idx']}] - [{stack['end_idx']}]")
        print(f"  Time: {times_60[stack['start_idx']][5:16]} - {times_60[stack['end_idx']][5:16]}")
        print(f"  Bars: {stack['bars']}")
        print(f"  Energy Sum: {stack['energy_sum']:.2f}")
        print(f"  Low Price: {stack_low:.2f} (Idx [{stack_low_global_idx}], {times_60[stack_low_global_idx][5:16]})")
        print(f"  DIF Min: {stack_dif_min:.2f} (Idx [{stack_dif_min_global_idx}])")
        
        if i >= 2:
            prev_stack = red_stacks[i-2]
            prev_energy = prev_stack['energy_sum']
            ratio = stack['energy_sum'] / prev_energy if prev_energy > 0 else 1.0
            status = "✅ DIVERGENCE" if ratio < 0.8 else "➖ No Divergence"
            print(f"  vs Prev: {stack['energy_sum']:.2f} / {prev_energy:.2f} = {ratio:.2f} {status}")
        
        print()
    
    print(f"{'='*100}")
    print(f"📊 5 分钟入场信号说明")
    print(f"{'='*100}\n")
    
    print("V6 策略 5 分钟入场条件:")
    print("  1. 绿柱堆 (histogram > 0) 连续 ≥ 3 根")
    print("  2. 放量：成交量 > MA20 × 1.5")
    print("  3. 阳柱：close > open")
    print("  4. 入场：绿柱堆 + (放量 或 阳柱)")
    print("  5. 止损：绿柱堆期间最低价 - 2 ticks\n")
    
    print("当 60 分钟出现以下信号时，在 5 分钟周期寻找入场:")
    print("  ✅ 红柱能量减弱：当前红柱堆能量 < 前红柱堆能量 × 0.8")
    print("  ✅ DIF 负区拐头：DIF 从下降转为上升，且 DIF < 0\n")
    
    print("示例:")
    print("  60 分钟：红柱能量减弱 (0.17 < 0.8) → 背离信号 ✅")
    print("  5 分钟：绿柱堆 (3 根) + 放量 → 入场信号 ✅")
    print("  止损：绿柱最低价 - 2 ticks\n")
    
    print(f"{'='*100}\n")
    
    return output_path


def main():
    db_path = '/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/kline_data.db'
    
    print("\n📊 V6 Strategy Detailed Signal Chart Generator\n")
    
    # 测试品种
    test_symbols = ['CFFEX.IC2606', 'CFFEX.IF2603', 'CZCE.MA605', 'DCE.m2605']
    
    for symbol in test_symbols:
        plot_v6_signals_detailed(symbol, db_path)
    
    print("✅ Charts generated!\n")


if __name__ == '__main__':
    main()
