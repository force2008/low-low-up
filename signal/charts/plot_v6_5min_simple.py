#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V6 策略 - 5 分钟 K 线信号图 (简化版)
只绘制 5 分钟 K 线和交易信号
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
    print("Usage: python3 plot_v6_5min_simple.py <symbol>")
    sys.exit(1)

symbol = sys.argv[1]
db_path = '/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/kline_data.db'

print(f"\n{'='*80}")
print(f"V6 Strategy - {symbol} - 5min K-Line Signals")
print(f"{'='*80}\n")

# Load 5min data (500 bars)
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute('SELECT datetime, open, high, low, close, volume FROM kline_data WHERE symbol = ? AND duration = 300 ORDER BY datetime DESC LIMIT 500', (symbol,))
data5 = [{'time': row[0], 'open': row[1], 'high': row[2], 'low': row[3], 'close': row[4], 'volume': row[5]} for row in cursor.fetchall()]
conn.close()
data5 = data5[::-1]

print(f"Loaded {len(data5)} bars: {data5[0]['time']} to {data5[-1]['time']}\n")

# V6 signals
signals = [
    {'entry': '2026-03-06 10:35', 'entry_price': 8171.20, 'exit': '2026-03-09 09:35', 'exit_price': 8055.00, 'pnl': -23240, 'cond60': 'DIF 拐头'},
    {'entry': '2026-03-09 14:05', 'entry_price': 8097.00, 'exit': '2026-03-11 14:30', 'exit_price': 8218.60, 'pnl': 24320, 'cond60': 'DIF 拐头'},
    {'entry': '2026-03-16 11:05', 'entry_price': 7863.00, 'exit': '2026-03-17 14:05', 'exit_price': 7855.60, 'pnl': -1480, 'cond60': '红柱背离'},
]

# Prepare data
times5 = [d['time'] for d in data5]
x5 = np.arange(len(data5))
closes5 = np.array([d['close'] for d in data5])
opens5 = np.array([d['open'] for d in data5])
highs5 = np.array([d['high'] for d in data5])
lows5 = np.array([d['low'] for d in data5])
vols5 = np.array([d['volume'] for d in data5])

# 5min MACD
n5 = len(closes5)
ema12_5 = np.zeros(n5)
ema26_5 = np.zeros(n5)
ema12_5[0] = ema26_5[0] = closes5[0]
for i in range(1, n5):
    ema12_5[i] = (closes5[i] - ema12_5[i-1]) * 2/13 + ema12_5[i-1]
    ema26_5[i] = (closes5[i] - ema26_5[i-1]) * 2/27 + ema26_5[i-1]
dif5 = ema12_5 - ema26_5
dea5 = np.zeros(n5)
dea5[0] = dif5[0]
for i in range(1, n5):
    dea5[i] = (dif5[i] - dea5[i-1]) * 2/10 + dea5[i-1]
hist5 = dif5 - dea5

# Find signal indices
for sig in signals:
    sig['entry_idx'] = next((i for i, t in enumerate(times5) if t[:16] == sig['entry'][:16]), -1)
    sig['exit_idx'] = next((i for i, t in enumerate(times5) if t[:16] == sig['exit'][:16]), -1)

# Create figure with 3 subplots
fig = plt.figure(figsize=(16, 10))
gs = fig.add_gridspec(3, 1, height_ratios=[3, 1, 1], hspace=0.05)

# ========== Plot 1: 5min K-Line with Signals ==========
ax1 = fig.add_subplot(gs[0])

for i in range(len(data5)):
    color = '#d62728' if closes5[i] >= opens5[i] else '#2ca02c'
    ax1.plot([x5[i], x5[i]], [lows5[i], highs5[i]], color=color, lw=0.6)
    h = closes5[i] - opens5[i]
    if abs(h) < 0.0001: h = 0.0001
    rect = Rectangle((x5[i]-0.3, min(opens5[i], closes5[i])), 0.6, h, facecolor=color, edgecolor=color)
    ax1.add_patch(rect)

# Mark signals
for i, sig in enumerate(signals, 1):
    if sig['entry_idx'] >= 0:
        ax1.scatter([sig['entry_idx']], [sig['entry_price']], color='gold', marker='^', s=100, zorder=5, label='Entry' if i==1 else "")
        ax1.annotate(f'E{i}\n{sig["entry_price"]:.0f}\n({sig["cond60"]})', 
                    xy=(sig['entry_idx'], sig['entry_price']*0.995), 
                    color='black', fontsize=9, fontweight='bold', ha='center', va='top',
                    bbox=dict(boxstyle='round,pad=0.2', facecolor='gold', edgecolor='black', linewidth=1))
    
    if sig['exit_idx'] >= 0:
        exit_color = 'green' if sig['pnl'] > 0 else 'red'
        pnl_text = f'+{sig["pnl"]/1000:.0f}K' if sig['pnl'] > 0 else f'{sig["pnl"]/1000:.0f}K'
        ax1.scatter([sig['exit_idx']], [sig['exit_price']], color=exit_color, marker='v', s=100, zorder=5, label='Exit' if i==1 else "")
        ax1.annotate(f'X{i}\n{pnl_text}', 
                    xy=(sig['exit_idx'], sig['exit_price']*1.005), 
                    color='white', fontsize=9, fontweight='bold', ha='center', va='bottom',
                    bbox=dict(boxstyle='round,pad=0.2', facecolor=exit_color, edgecolor='black', linewidth=1))

ax1.set_title(f'{symbol} - V6 Strategy (5min K-Line, 500 bars)')
ax1.set_ylabel('Price')
ax1.legend(loc='upper left', fontsize=9)
ax1.grid(True, alpha=0.25)
ax1.set_xticks([])

# ========== Plot 2: 5min MACD ==========
ax2 = fig.add_subplot(gs[1], sharex=ax1)

ax2.plot(x5, dif5, 'blue', lw=1.2, label='DIF')
ax2.plot(x5, dea5, 'orange', lw=1.2, label='DEA')
ax2.axhline(y=0, color='gray', lw=0.5)
ax2.set_ylabel('MACD 5min')
ax2.legend(loc='upper left', fontsize=8)
ax2.grid(True, alpha=0.25)
ax2.set_xticks([])

# ========== Plot 3: 5min Volume ==========
ax3 = fig.add_subplot(gs[2], sharex=ax1)

vol_colors = ['#d62728' if closes5[i] >= opens5[i] else '#2ca02c' for i in range(len(data5))]
ax3.bar(x5, vols5, color=vol_colors, alpha=0.7, width=0.8)
ax3.set_ylabel('Volume')
ax3.set_xlabel('Time')
ax3.grid(True, alpha=0.25)

# X-axis labels
tick_step = max(1, len(x5) // 20)
ax3.set_xticks(list(range(0, len(x5), tick_step)))
ax3.set_xticklabels([times5[i][11:16] for i in range(0, len(x5), tick_step)], rotation=45, ha='right', fontsize=8)

plt.tight_layout()

# Save
output_dir = f'/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/backtest/{symbol.replace(".", "_")}'
os.makedirs(output_dir, exist_ok=True)
output = f'{output_dir}/v6_5min_simple.png'
plt.savefig(output, dpi=150, bbox_inches='tight')
plt.close()

# Print 60min conditions
print(f"{'='*80}")
print(f"V6 策略 60 分钟过滤条件 (严格要求)")
print(f"{'='*80}\n")

print("条件 1: 红柱能量背离")
print("  - 当前红柱堆能量 < 前红柱堆能量 × 0.8")
print("  - 红柱堆：MACD histogram < 0 的连续 K 线 (≥3 根)")
print("  - 能量：红柱期间 |histogram| 之和\n")

print("条件 2: DIF 二次拐头")
print("  - DIF 连续下降 ≥ 3 根 K 线")
print("  - 当前 DIF 拐头向上 (DIF[i] > DIF[i-1])")
print("  - DIF < 0 (负值区域)")
print("  - DEA > DIF (准备金叉)\n")

print("价格过滤:")
print("  - 价格 ≥ MA60 × 0.95 (不能低于 MA60 超过 5%)\n")

print(f"{'='*80}")
print(f"本图表 3 笔交易的 60 分钟触发条件")
print(f"{'='*80}\n")

for i, sig in enumerate(signals, 1):
    print(f"交易 #{i} ({sig['entry'][:10]}):")
    print(f"  60 分钟条件：{sig['cond60']}")
    print(f"  入场：{sig['entry']} @ {sig['entry_price']:.2f}")
    print(f"  出场：{sig['exit']} @ {sig['exit_price']:.2f}")
    print(f"  盈亏：{sig['pnl']:+,.0f}元 {'✅' if sig['pnl'] > 0 else '❌'}")
    print()

print(f"{'='*80}")
print(f"✅ Chart saved: {output}")
print(f"{'='*80}\n")
