import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
import os

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

output_dir = '/home/ubuntu/low-low-up/Levi_K线图形示例'
os.makedirs(output_dir, exist_ok=True)

# 辅助函数：创建K线图
def create_chart(data, title, filename, annotations=None, hlines=None, vlines=None):
    df = pd.DataFrame(data)
    df['Date'] = pd.date_range(start='2026-07-01', periods=len(df), freq='D')
    df.set_index('Date', inplace=True)
    df.index.name = 'Date'

    kwargs = {
        'type': 'candle',
        'title': title,
        'ylabel': 'Price',
        'volume': True,
        'style': 'charles',
        'figsize': (12, 8),
        'returnfig': True,
        'panel_ratios': (3, 1)
    }
    if hlines is not None:
        kwargs['hlines'] = hlines
    if vlines is not None:
        kwargs['vlines'] = vlines

    fig, axes = mpf.plot(df, **kwargs)

    # 添加文字注释
    if annotations:
        ax = axes[0]
        for x, y, text, color in annotations:
            ax.text(x, y, text, fontsize=12, color=color, fontweight='bold',
                    bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7))

    plt.savefig(os.path.join(output_dir, filename), dpi=150, bbox_inches='tight')
    plt.close()
    print(f'Generated: {filename}')

# 1. 真突破 - 平台整理后放量大阳线突破
print("Generating 真突破图形...")
data1 = {
    'Open':  [100, 101, 100, 102, 101, 102, 101, 102, 101, 102,
              103, 102, 103, 102, 103, 104, 105, 106, 108, 112],
    'High':  [102, 103, 102, 104, 103, 104, 103, 104, 103, 104,
              104, 103, 104, 103, 104, 106, 107, 108, 110, 115],
    'Low':   [99, 100, 99, 100, 100, 101, 100, 101, 100, 101,
              102, 101, 102, 101, 102, 103, 104, 105, 107, 111],
    'Close': [101, 102, 101, 103, 102, 103, 102, 103, 102, 103,
              103, 102, 103, 102, 103, 105, 106, 107, 109, 114],
    'Volume':[1000, 1100, 900, 1000, 1100, 1000, 1100, 1000, 1100, 1000,
              1200, 1000, 1100, 1000, 1100, 1300, 1400, 2500, 1800, 1600]
}
annotations1 = [(18, 116, 'Real Breakout!\nHigh Volume Long Yang', 'red')]
hlines1 = [104]
create_chart(data1, 'True Breakout: Platform Consolidation + High Volume Long Yang',
             '01_true_breakout_platform.png', annotations1, hlines1)

# 2. 真突破 - 下跌趋势中放量大阴线突破
data2 = {
    'Open':  [110, 109, 108, 107, 106, 107, 106, 105, 106, 105,
              104, 105, 104, 103, 104, 103, 102, 101, 100, 95],
    'High':  [111, 110, 109, 108, 107, 108, 107, 106, 107, 106,
              105, 106, 105, 104, 105, 104, 103, 102, 101, 98],
    'Low':   [109, 108, 107, 106, 105, 106, 105, 104, 105, 104,
              103, 104, 103, 102, 103, 102, 101, 100, 99, 93],
    'Close': [109, 108, 107, 106, 105, 106, 105, 104, 105, 104,
              103, 104, 103, 102, 103, 102, 101, 100, 99, 94],
    'Volume':[1000, 1100, 1000, 1100, 1000, 1100, 1000, 1100, 1000, 1100,
              1200, 1000, 1100, 1000, 1100, 1200, 1300, 1400, 2500, 1800]
}
annotations2 = [(18, 91, 'Real Breakout!\nHigh Volume Long Yin', 'green')]
hlines2 = [103]
create_chart(data2, 'True Breakout: Downtrend + High Volume Long Yin',
             '02_true_breakout_downtrend.png', annotations2, hlines2)

# 3. 假突破 - 长上影线突破后回落
print("Generating 假突破图形...")
data3 = {
    'Open':  [100, 101, 100, 102, 101, 102, 101, 102, 101, 102,
              103, 102, 103, 102, 103, 104, 105, 106, 108, 103],
    'High':  [102, 103, 102, 104, 103, 104, 103, 104, 103, 104,
              104, 103, 104, 103, 104, 106, 107, 108, 112, 104],
    'Low':   [99, 100, 99, 100, 100, 101, 100, 101, 100, 101,
              102, 101, 102, 101, 102, 103, 104, 105, 107, 102],
    'Close': [101, 102, 101, 103, 102, 103, 102, 103, 102, 103,
              103, 102, 103, 102, 103, 105, 106, 107, 103, 103],
    'Volume':[1000, 1100, 900, 1000, 1100, 1000, 1100, 1000, 1100, 1000,
              1200, 1000, 1100, 1000, 1100, 1300, 1400, 1500, 1600, 1200]
}
annotations3 = [(18, 113, 'Fake Breakout!\nLong Upper Shadow', 'green')]
hlines3 = [104]
create_chart(data3, 'Fake Breakout: Long Upper Shadow After Breakout',
             '03_fake_breakout_long_upper_shadow.png', annotations3, hlines3)

# 4. 假突破 - 毛刺突破
data4 = {
    'Open':  [100, 101, 100, 102, 101, 102, 101, 102, 101, 102,
              103, 102, 103, 102, 103, 104, 103, 102, 103, 102],
    'High':  [102, 103, 102, 104, 103, 104, 103, 104, 103, 104,
              104, 103, 104, 103, 104, 106, 104, 103, 104, 103],
    'Low':   [99, 100, 99, 100, 100, 101, 100, 101, 100, 101,
              102, 101, 102, 101, 102, 102, 102, 101, 102, 101],
    'Close': [101, 102, 101, 103, 102, 103, 102, 103, 102, 103,
              103, 102, 103, 102, 103, 103, 103, 102, 103, 102],
    'Volume':[1000, 1100, 900, 1000, 1100, 1000, 1100, 1000, 1100, 1000,
              1200, 1000, 1100, 1000, 1100, 1300, 1100, 1000, 1100, 1000]
}
annotations4 = [(15, 107, 'Fake Breakout!\nSpike Then Pull Back', 'green')]
hlines4 = [104]
create_chart(data4, 'Fake Breakout: Spike Above Resistance Then Pull Back',
             '04_fake_breakout_spike.png', annotations4, hlines4)

# 5. 假突破 - 无量小阳线突破
data5 = {
    'Open':  [100, 101, 100, 102, 101, 102, 101, 102, 101, 102,
              103, 102, 103, 102, 103, 104, 104, 104, 105, 105],
    'High':  [102, 103, 102, 104, 103, 104, 103, 104, 103, 104,
              104, 103, 104, 103, 104, 105, 105, 105, 106, 106],
    'Low':   [99, 100, 99, 100, 100, 101, 100, 101, 100, 101,
              102, 101, 102, 101, 102, 103, 103, 103, 104, 104],
    'Close': [101, 102, 101, 103, 102, 103, 102, 103, 102, 103,
              103, 102, 103, 102, 103, 105, 105, 104, 105, 105],
    'Volume':[1000, 1100, 900, 1000, 1100, 1000, 1100, 1000, 1100, 1000,
              1200, 1000, 1100, 1000, 1100, 900, 800, 700, 850, 800]
}
annotations5 = [(15, 107, 'Fake Breakout!\nLow Volume Small Yang', 'green')]
hlines5 = [104]
create_chart(data5, 'Fake Breakout: Low Volume Small Yang Breakout',
             '05_fake_breakout_low_volume.png', annotations5, hlines5)

# 6. 假突破 - 尾盘突击突破
data6 = {
    'Open':  [100, 101, 100, 102, 101, 102, 101, 102, 101, 102,
              103, 102, 103, 102, 103, 103, 103, 103, 104, 105],
    'High':  [102, 103, 102, 104, 103, 104, 103, 104, 103, 104,
              104, 103, 104, 103, 104, 104, 104, 104, 107, 106],
    'Low':   [99, 100, 99, 100, 100, 101, 100, 101, 100, 101,
              102, 101, 102, 101, 102, 102, 102, 102, 103, 103],
    'Close': [101, 102, 101, 103, 102, 103, 102, 103, 102, 103,
              103, 102, 103, 102, 103, 103, 103, 103, 106, 104],
    'Volume':[1000, 1100, 900, 1000, 1100, 1000, 1100, 1000, 1100, 1000,
              1200, 1000, 1100, 1000, 1100, 1000, 900, 800, 2000, 1200]
}
annotations6 = [(18, 108, 'Fake Breakout!\nEnd-of-Day Spike', 'green')]
hlines6 = [104]
create_chart(data6, 'Fake Breakout: End-of-Day Spike, Next Day Pull Back',
             '06_fake_breakout_end_of_day.png', annotations6, hlines6)

# 7. 真突破 - 回踩确认后再突破
data7 = {
    'Open':  [100, 101, 100, 102, 101, 102, 101, 102, 101, 102,
              103, 105, 104, 106, 105, 107, 106, 108, 110, 112],
    'High':  [102, 103, 102, 104, 103, 104, 103, 104, 103, 104,
              105, 107, 106, 108, 107, 109, 108, 110, 112, 115],
    'Low':   [99, 100, 99, 100, 100, 101, 100, 101, 100, 101,
              102, 104, 103, 105, 104, 106, 105, 107, 109, 111],
    'Close': [101, 102, 101, 103, 102, 103, 102, 103, 102, 103,
              105, 106, 104, 107, 106, 108, 107, 109, 111, 114],
    'Volume':[1000, 1100, 900, 1000, 1100, 1000, 1100, 1000, 1100, 1000,
              2200, 1800, 1200, 2000, 1300, 2200, 1400, 1800, 2000, 1600]
}
annotations7 = [(12, 108, 'Breakout\nRetest\nConfirms Support', 'red'),
                (17, 113, 'Second Leg Up', 'red')]
hlines7 = [104]
create_chart(data7, 'True Breakout: Breakout + Retest + Continuation',
             '07_true_breakout_retest.png', annotations7, hlines7)

print(f"\nAll charts saved to: {output_dir}")
print("Files:")
for f in sorted(os.listdir(output_dir)):
    print(f"  - {f}")
