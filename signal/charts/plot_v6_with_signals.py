#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V6 策略综合信号图
包含：5 分钟 500 根 K 线 + 60 分钟 K 线 + 60 分钟 MACD
标记：E1/X1, E2/X2, E3/X3 入场/出场信号
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

if len(sys.argv) < 2:
    print("Usage: python3 plot_v6_with_signals.py <symbol>")
    sys.exit(1)

symbol = sys.argv[1]
db_path = '/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/kline_data.db'

print(f"\n{'='*80}")
print(f"V6 Strategy - {symbol} - Comprehensive Chart")
print(f"{'='*80}\n")

# Load 5min data (500 bars)
print("Loading 5min data (500 bars)...")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute('SELECT datetime, open, high, low, close, volume FROM kline_data WHERE symbol = ? AND duration = 300 ORDER BY datetime DESC LIMIT 500', (symbol,))
data5 = [{'time': row[0], 'open': row[1], 'high': row[2], 'low': row[3], 'close': row[4], 'volume': row[5]} for row in cursor.fetchall()]
conn.close()
data5 = data5[::-1]
print(f"Loaded {len(data5)} bars: {data5[0]['time']} to {data5[-1]['time']}")

# Load 60min data (200 bars)
print("Loading 60min data (200 bars)...")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute('SELECT datetime, open, high, low, close FROM kline_data WHERE symbol = ? AND duration = 3600 ORDER BY datetime DESC LIMIT 200', (symbol,))
data60 = [{'time': row[0], 'open': row[1], 'high': row[2], 'low': row[3], 'close': row[4]} for row in cursor.fetchall()]
conn.close()
data60 = data60[::-1]
print(f"Loaded {len(data60)} bars: {data60[0]['time']} to {data60[-1]['time']}\n")

# V6 signals
signals = [
    {'entry': '2026-03-06 10:35', 'entry_price': 8171.20, 'exit': '2026-03-09 09:35', 'exit_price': 8055.00, 'pnl': -23240, 'name': 'E1/X1'},
    {'entry': '2026-03-09 14:05', 'entry_price': 8097.00, 'exit': '2026-03-11 14:30', 'exit_price': 8218.60, 'pnl': 24320, 'name': 'E2/X2'},
    {'entry': '2026-03-16 11:05', 'entry_price': 7863.00, 'exit': '2026-03-17 14:05', 'exit_price': 7855.60, 'pnl': -1480, 'name': 'E3/X3'},
]

# Prepare 5min data
times5 = [d['time'] for d in data5]
x5 = np.arange(len(data5))
closes5 = np.array([d['close'] for d in data5])
opens5 = np.array([d['open'] for d in data5])
highs5 = np.array([d['high'] for d in data5])
lows5 = np.array([d['low'] for d in data5])
vols5 = np.array([d['volume'] for d in data5])

# Prepare 60min data
times60 = [d['time'] for d in data60]
x60 = np.arange(len(data60))
closes60 = np.array([d['close'] for d in data60])
opens60 = np.array([d['open'] for d in data60])
highs60 = np.array([d['high'] for d in data60])
lows60 = np.array([d['low'] for d in data60])

# 60min MACD
n60 = len(closes60)
ema12_60 = np.zeros(n60)
ema26_60 = np.zeros(n60)
ema12_60[0] = ema26_60[0] = closes60[0]
for i in range(1, n60):
    ema12_60[i] = (closes60[i] - ema12_60[i-1]) * 2/13 + ema12_60[i-1]
    ema26_60[i] = (closes60[i] - ema26_60[i-1]) * 2/27 + ema26_60[i-1]
dif60 = ema12_60 - ema26_60
dea60 = np.zeros(n60)
dea60[0] = dif60[0]
for i in range(1, n60):
    dea60[i] = (dif60[i] - dea60[i-1]) * 2/10 + dea60[i-1]
hist60 = dif60 - dea60

# Find signal indices (5min)
print("Finding signal indices...")
for sig in signals:
    sig['entry_idx5'] = next((i for i, t in enumerate(times5) if t[:16] == sig['entry'][:16]), -1)
    sig['exit_idx5'] = next((i for i, t in enumerate(times5) if t[:16] == sig['exit'][:16]), -1)
    sig['entry_idx60'] = next((i for i, t in enumerate(times60) if t[:10] == sig['entry'][:10]), -1)
    sig['exit_idx60'] = next((i for i, t in enumerate(times60) if t[:10] == sig['exit'][:10]), -1)
    print(f"{sig['name']}: 5min Entry idx={sig['entry_idx5']}, Exit idx={sig['exit_idx5']} | 60min Entry idx={sig['entry_idx60']}, Exit idx={sig['exit_idx60']}")

# Create figure with 5 subplots
fig = plt.figure(figsize=(16, 14))
gs = fig.add_gridspec(5, 1, height_ratios=[2, 2, 1, 1, 1], hspace=0.05)

# ========== Plot 1: 5min K-Line ==========
print("\nPlotting 5min K-Line...")
ax1 = fig.add_subplot(gs[0])

for i in range(len(data5)):
    color = '#d62728' if closes5[i] >= opens5[i] else '#2ca02c'
    ax1.plot([x5[i], x5[i]], [lows5[i], highs5[i]], color=color, lw=0.5)
    h = closes5[i] - opens5[i]
    if abs(h) < 0.0001: h = 0.0001
    rect = Rectangle((x5[i]-0.3, min(opens5[i], closes5[i])), 0.6, h, facecolor=color, edgecolor=color)
    ax1.add_patch(rect)

# Mark signals on 5min
for sig in signals:
    if sig['entry_idx5'] >= 0:
        ax1.scatter([sig['entry_idx5']], [sig['entry_price']], color='gold', marker='^', s=80, zorder=5)
        ax1.annotate(f"{sig['name']}\n{sig['entry_price']:.0f}", xy=(sig['entry_idx5'], sig['entry_price']*0.995), 
                    color='black', fontsize=8, fontweight='bold', ha='center', va='top',
                    bbox=dict(boxstyle='round,pad=0.15', facecolor='gold', edgecolor='black', linewidth=0.5))
    
    if sig['exit_idx5'] >= 0:
        exit_color = 'green' if sig['pnl'] > 0 else 'red'
        ax1.scatter([sig['exit_idx5']], [sig['exit_price']], color=exit_color, marker='v', s=80, zorder=5)
        ax1.annotate(f"{sig['pnl']:+,.0f}", xy=(sig['exit_idx5'], sig['exit_price']*1.005), 
                    color='white', fontsize=8, fontweight='bold', ha='center', va='bottom',
                    bbox=dict(boxstyle='round,pad=0.15', facecolor=exit_color, edgecolor='black', linewidth=0.5))

ax1.set_title(f'{symbol} - V6 Strategy (5min K-Line, 500 bars)')
ax1.set_ylabel('Price')
ax1.legend(loc='upper left', fontsize=8)
ax1.grid(True, alpha=0.25)
ax1.set_xticks([])

# ========== Plot 2: 60min K-Line ==========
print("Plotting 60min K-Line...")
ax2 = fig.add_subplot(gs[1])

for i in range(len(data60)):
    color = '#d62728' if closes60[i] >= opens60[i] else '#2ca02c'
    ax2.plot([x60[i], x60[i]], [lows60[i], highs60[i]], color=color, lw=0.8)
    h = closes60[i] - opens60[i]
    if abs(h) < 0.0001: h = 0.0001
    rect = Rectangle((x60[i]-0.3, min(opens60[i], closes60[i])), 0.6, h, facecolor=color, edgecolor=color)
    ax2.add_patch(rect)

# Mark signals on 60min
for sig in signals:
    if sig['entry_idx60'] >= 0:
        ax2.scatter([sig['entry_idx60']], [sig['entry_price']], color='gold', marker='^', s=100, zorder=5)
        ax2.annotate(f"{sig['name']}", xy=(sig['entry_idx60'], sig['entry_price']*0.995), 
                    color='black', fontsize=9, fontweight='bold', ha='center', va='top',
                    bbox=dict(boxstyle='round,pad=0.2', facecolor='gold', edgecolor='black', linewidth=1))
    
    if sig['exit_idx60'] >= 0:
        exit_color = 'green' if sig['pnl'] > 0 else 'red'
        ax2.scatter([sig['exit_idx60']], [sig['exit_price']], color=exit_color, marker='v', s=100, zorder=5)
        ax2.annotate(f"{sig['name']}", xy=(sig['exit_idx60'], sig['exit_price']*1.005), 
                    color='white', fontsize=9, fontweight='bold', ha='center', va='bottom',
                    bbox=dict(boxstyle='round,pad=0.2', facecolor=exit_color, edgecolor='black', linewidth=1))

ax2.set_title(f'{symbol} - V6 Strategy (60min K-Line, 200 bars)')
ax2.set_ylabel('Price')
ax2.grid(True, alpha=0.25)
ax2.set_xticks([])

# ========== Plot 3: 60min MACD DIF/DEA ==========
print("Plotting 60min MACD DIF/DEA...")
ax3 = fig.add_subplot(gs[2], sharex=ax2)

ax3.plot(x60, dif60, 'blue', lw=1.5, label='DIF')
ax3.plot(x60, dea60, 'orange', lw=1.5, label='DEA')
ax3.axhline(y=0, color='gray', lw=0.5)
ax3.set_ylabel('MACD')
ax3.legend(loc='upper left', fontsize=9)
ax3.grid(True, alpha=0.25)
ax3.set_xticks([])

# ========== Plot 4: 60min MACD Histogram ==========
print("Plotting 60min MACD Histogram...")
ax4 = fig.add_subplot(gs[3], sharex=ax2)

hist_colors = ['green' if h >= 0 else 'red' for h in hist60]
ax4.bar(x60, hist60, color=hist_colors, alpha=0.7, width=0.8)
ax4.axhline(y=0, color='gray', lw=0.5)
ax4.set_ylabel('Histogram')
ax4.grid(True, alpha=0.25)
ax4.set_xticks([])

# ========== Plot 5: 5min Volume ==========
print("Plotting 5min Volume...")
ax5 = fig.add_subplot(gs[4], sharex=ax1)

vol_colors = ['#d62728' if closes5[i] >= opens5[i] else '#2ca02c' for i in range(len(data5))]
ax5.bar(x5, vols5, color=vol_colors, alpha=0.7, width=0.8)
ax5.set_ylabel('Volume')
ax5.set_xlabel('Time')
ax5.grid(True, alpha=0.25)

# X-axis labels for 5min
tick_step5 = max(1, len(x5) // 20)
ax5.set_xticks(list(range(0, len(x5), tick_step5)))
ax5.set_xticklabels([times5[i][11:16] for i in range(0, len(x5), tick_step5)], rotation=45, ha='right', fontsize=7)

# X-axis labels for 60min (shared)
tick_step60 = max(1, len(x60) // 15)
ax4.set_xticks(list(range(0, len(x60), tick_step60)))
ax4.set_xticklabels([times60[i][5:16] for i in range(0, len(x60), tick_step60)], rotation=45, ha='right', fontsize=8)

plt.tight_layout()

# Save
output_dir = f'/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/backtest/{symbol.replace(".", "_")}'
os.makedirs(output_dir, exist_ok=True)
output = f'{output_dir}/v6_comprehensive.png'
plt.savefig(output, dpi=150, bbox_inches='tight')
plt.close()

print(f"\n{'='*80}")
print(f"✅ Chart saved: {output}")
print(f"{'='*80}\n")
