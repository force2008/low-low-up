#!/usr/bin/env python3
import sys, os, sqlite3, numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial']
plt.rcParams['axes.unicode_minus'] = False

if len(sys.argv) < 2:
    print("Usage: python3 plot_v6_60min.py <symbol>")
    sys.exit(1)

symbol = sys.argv[1]
db_path = '/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/kline_data.db'

print(f"\n{'='*80}")
print(f"V6 Strategy - {symbol} - 60min Chart")
print(f"{'='*80}\n")

# Load data
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute('SELECT datetime, open, high, low, close, volume FROM kline_data WHERE symbol = ? AND duration = 3600 ORDER BY datetime ASC LIMIT 200', (symbol,))
data = [{'time': row[0], 'open': row[1], 'high': row[2], 'low': row[3], 'close': row[4], 'volume': row[5]} for row in cursor.fetchall()]
conn.close()

if len(data) < 50:
    print('Insufficient data')
    sys.exit(1)

times = [d['time'] for d in data]
x = np.arange(len(data))
closes = np.array([d['close'] for d in data])
opens = np.array([d['open'] for d in data])
highs = np.array([d['high'] for d in data])
lows = np.array([d['low'] for d in data])

# MACD
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
hist = dif - dea

# Find red bar stacks
red_stacks = []
i = 0
while i < n:
    if hist[i] >= 0:
        i += 1
        continue
    start = i
    while i < n and hist[i] < 0:
        i += 1
    end = i - 1
    bars = end - start + 1
    if bars >= 3:
        energy = np.sum(np.abs(hist[start:end+1]))
        red_stacks.append({'start': start, 'end': end, 'bars': bars, 'energy': energy})
    i = start - 1

print(f"Found {len(red_stacks)} red bar stacks\n")

# Plot
fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, figsize=(16, 12), sharex=True)

# K-line
for i in range(len(data)):
    color = '#d62728' if closes[i] >= opens[i] else '#2ca02c'
    ax1.plot([x[i], x[i]], [lows[i], highs[i]], color=color, lw=0.8)
    h = closes[i] - opens[i]
    if abs(h) < 0.0001: h = 0.0001
    rect = Rectangle((x[i]-0.3, min(opens[i], closes[i])), 0.6, h, facecolor=color, edgecolor=color)
    ax1.add_patch(rect)

for idx, stack in enumerate(red_stacks[:5], 1):
    stack_lows = lows[stack['start']:stack['end']+1]
    low_idx = np.argmin(stack_lows)
    low_global = stack['start'] + low_idx
    ax1.scatter([low_global], [lows[low_global]], color='blue', marker='v', s=100, zorder=5, label=f'Stack #{idx} Low')
    ax1.axvspan(stack['start'], stack['end'], alpha=0.1, color='red', label=f'Stack #{idx} ({stack["bars"]} bars)')

ax1.set_title(f'{symbol} - V6 Strategy (60min K-Line + Red Bar Stacks)')
ax1.set_ylabel('Price')
ax1.legend(loc='upper left', fontsize=9)
ax1.grid(True, alpha=0.25)

# MACD
ax2.plot(x, dif, 'blue', lw=1.5, label='DIF')
ax2.plot(x, dea, 'orange', lw=1.5, label='DEA')
ax2.axhline(y=0, color='gray', lw=0.5)
ax2.set_ylabel('MACD')
ax2.legend()
ax2.grid(True, alpha=0.25)

# Histogram
colors = ['green' if h >= 0 else 'red' for h in hist]
ax3.bar(x, hist, color=colors, alpha=0.7, width=0.8)
ax3.axhline(y=0, color='gray', lw=0.5)
ax3.set_ylabel('Histogram')
ax3.grid(True, alpha=0.25)

# Energy
if len(red_stacks) >= 2:
    energies = [s['energy'] for s in red_stacks]
    centers = [(s['start'] + s['end']) / 2 for s in red_stacks]
    colors = ['green' if i > 0 and energies[i] < energies[i-1]*0.8 else 'red' for i in range(len(energies))]
    colors[0] = 'gray'
    ax4.bar(centers, energies, width=8, color=colors, alpha=0.7)
    for i in range(1, len(energies)):
        ratio = energies[i] / energies[i-1] if energies[i-1] > 0 else 1.0
        ax4.text(centers[i], energies[i]*1.1, f'{ratio:.2f}', ha='center', fontsize=8, color='green' if ratio < 0.8 else 'gray')

ax4.set_ylabel('Energy')
ax4.set_title('Red Bar Stack Energy (Green = Divergence < 0.8)')
ax4.grid(True, alpha=0.25)

plt.tight_layout()
output = f'/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/backtest/{symbol.replace(".", "_")}/v6_60min.png'
os.makedirs(os.path.dirname(output), exist_ok=True)
plt.savefig(output, dpi=150, bbox_inches='tight')
plt.close()

print(f"Chart saved: {output}\n")

# Print details
print(f"{'='*80}")
print(f"Red Bar Stacks Details")
print(f"{'='*80}\n")

for i, stack in enumerate(red_stacks, 1):
    stack_lows = lows[stack['start']:stack['end']+1]
    low = np.min(stack_lows)
    low_idx = np.argmin(stack_lows) + stack['start']
    stack_dif = np.min(dif[stack['start']:stack['end']+1])
    dif_idx = np.argmin(dif[stack['start']:stack['end']+1]) + stack['start']
    
    print(f"Stack #{i}:")
    print(f"  Index: [{stack['start']}] - [{stack['end']}]")
    print(f"  Time: {times[stack['start']][5:16]} - {times[stack['end']][5:16]}")
    print(f"  Bars: {stack['bars']}")
    print(f"  Energy: {stack['energy']:.2f}")
    print(f"  Low: {low:.2f} (Idx [{low_idx}])")
    print(f"  DIF Min: {stack_dif:.2f} (Idx [{dif_idx}])")
    
    if i >= 2:
        prev_e = red_stacks[i-2]['energy']
        ratio = stack['energy'] / prev_e if prev_e > 0 else 1.0
        status = "DIVERGENCE" if ratio < 0.8 else "No"
        print(f"  vs Prev: {stack['energy']:.2f} / {prev_e:.2f} = {ratio:.2f} [{status}]")
    print()

print(f"{'='*80}")
print(f"5min Entry Signal Description")
print(f"{'='*80}\n")

print("V6 5min Entry Conditions:")
print("  1. Green bar stack (hist > 0) >= 3 bars")
print("  2. High volume: vol > MA20 x 1.5")
print("  3. Yang candle: close > open")
print("  4. Entry: Green stack + (high vol OR yang)")
print("  5. Stop loss: Green stack low - 2 ticks\n")

print("60min triggers 5min search when:")
print("  [OK] Red energy divergence: current < prev x 0.8")
print("  [OK] DIF turning up from negative\n")

print(f"{'='*80}\n")
