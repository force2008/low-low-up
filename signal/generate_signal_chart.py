#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TrendReversalV2 策略信号图表生成器 (无 pandas 版本)
- K 线蜡烛图
- 买卖信号标记
- MACD 指标子图
- RSI 指标
- 成交量
"""

import sqlite3
import sys
import os
from datetime import datetime
import math

# 设置 matplotlib
import matplotlib
matplotlib.use('Agg')
matplotlib.rcParams['font.sans-serif'] = ['DejaVu Sans', 'SimHei', 'Arial Unicode MS']
matplotlib.rcParams['axes.unicode_minus'] = False

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import numpy as np


def get_kline_data(db_path: str, symbol: str, duration: int, limit: int) -> list:
    """从数据库获取 K 线数据"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT datetime, open, high, low, close, volume 
        FROM kline_data 
        WHERE symbol = ? AND duration = ?
        ORDER BY datetime DESC 
        LIMIT ?
    """, (symbol, duration, limit))
    
    rows = cursor.fetchall()
    conn.close()
    
    return [{'time': row[0], 'open': row[1], 'high': row[2], 'low': row[3], 
             'close': row[4], 'volume': row[5]} for row in reversed(rows)]


def calc_ema(values: list, period: int) -> np.ndarray:
    """计算指数移动平均"""
    result = np.zeros(len(values))
    multiplier = 2 / (period + 1)
    
    result[0] = values[0]
    for i in range(1, len(values)):
        result[i] = (values[i] - result[i-1]) * multiplier + result[i-1]
    
    return result


def calc_macd(data: list, fast=12, slow=26, signal=9):
    """计算 MACD"""
    closes = np.array([d['close'] for d in data])
    
    ema12 = calc_ema(closes.tolist(), fast)
    ema26 = calc_ema(closes.tolist(), slow)
    
    macd_line = ema12 - ema26
    signal_line = calc_ema(macd_line.tolist(), signal)
    histogram = macd_line - signal_line
    
    return macd_line, signal_line, histogram


def calc_rsi(data: list, period=14):
    """计算 RSI"""
    closes = np.array([d['close'] for d in data])
    delta = np.diff(closes)
    
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    
    # 填充到原长度
    gain = np.concatenate([[0], gain])
    loss = np.concatenate([[0], loss])
    
    # 滚动平均
    rsi = np.zeros(len(closes))
    for i in range(period, len(closes)):
        avg_gain = np.mean(gain[i-period+1:i+1])
        avg_loss = np.mean(loss[i-period+1:i+1])
        
        if avg_loss == 0:
            rsi[i] = 100
        else:
            rs = avg_gain / avg_loss
            rsi[i] = 100 - (100 / (1 + rs))
    
    return rsi


def calc_bollinger(data: list, period=20, std_dev=2):
    """计算布林带"""
    closes = np.array([d['close'] for d in data])
    n = len(closes)
    
    bb_mid = np.zeros(n)
    bb_upper = np.zeros(n)
    bb_lower = np.zeros(n)
    
    for i in range(period-1, n):
        window = closes[i-period+1:i+1]
        mid = np.mean(window)
        std = np.std(window)
        
        bb_mid[i] = mid
        bb_upper[i] = mid + std_dev * std
        bb_lower[i] = mid - std_dev * std
    
    return bb_upper, bb_mid, bb_lower


def generate_signal_chart(symbol: str, db_path: str, contracts_path: str, 
                          output_path: str = None, signals: list = None):
    """生成信号图表"""
    
    print(f"\n📈 生成信号图表：{symbol}")
    
    # 获取数据
    data_5min = get_kline_data(db_path, symbol, 300, 500)
    data_60min = get_kline_data(db_path, symbol, 3600, 200)
    
    if len(data_5min) < 100:
        print("❌ 数据不足")
        return None
    
    # 导入策略
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from TrendReversalV2Strategy import TrendReversalV2Strategy
    
    # 初始化策略
    strategy = TrendReversalV2Strategy()
    strategy.load_contracts(contracts_path)
    
    # 如果没有传入信号，运行回测获取
    if signals is None:
        signals = strategy.run_backtest(symbol, data_5min, data_60min)
        if signals:
            print(f"✅ 找到 {len(signals)} 个交易信号")
        else:
            print("⚠️ 无交易信号，仍生成指标图表")
            signals = []
    
    # 准备数据
    times = []
    opens, highs, lows, closes, volumes = [], [], [], [], []
    
    for d in data_5min:
        try:
            dt = datetime.strptime(d['time'], '%Y-%m-%d %H:%M:%S')
            times.append(dt)
            opens.append(d['open'])
            highs.append(d['high'])
            lows.append(d['low'])
            closes.append(d['close'])
            volumes.append(d['volume'])
        except:
            continue
    
    times = np.array(times)
    closes = np.array(closes)
    
    # 计算指标
    macd_line, signal_line, histogram = calc_macd(data_5min)
    rsi = calc_rsi(data_5min)
    bb_upper, bb_mid, bb_lower = calc_bollinger(data_5min)
    
    # 创建图表
    fig = plt.figure(figsize=(16, 12))
    gs = fig.add_gridspec(4, 1, height_ratios=[3, 1, 1, 1], hspace=0.08)
    
    # === 子图 1: K 线图 + 买卖信号 + 布林带 ===
    ax1 = fig.add_subplot(gs[0])
    
    # 绘制蜡烛图
    for i in range(len(times)):
        x = mdates.date2num(times[i])
        open_p = opens[i]
        high_p = highs[i]
        low_p = lows[i]
        close_p = closes[i]
        
        color = 'red' if close_p >= open_p else 'green'
        
        # 影线
        ax1.plot([x, x], [low_p, high_p], color=color, linewidth=1)
        # 实体
        height = close_p - open_p
        if abs(height) < 0.0001:
            height = 0.0001
        rect = Rectangle((x - 0.3, min(open_p, close_p)), 
                        0.6, height,
                        facecolor=color, edgecolor=color)
        ax1.add_patch(rect)
    
    # 布林带
    ax1.plot(times, bb_upper, 'gray', linestyle='--', linewidth=1, alpha=0.5)
    ax1.plot(times, bb_mid, 'gray', linestyle='-', linewidth=1, alpha=0.3)
    ax1.plot(times, bb_lower, 'gray', linestyle='--', linewidth=1, alpha=0.5)
    ax1.fill_between(times, bb_upper, bb_lower, alpha=0.1, color='gray')
    
    # 买卖信号标记
    for idx, sig in enumerate(signals):
        try:
            sig_time = datetime.strptime(sig.entry_time, '%Y-%m-%d %H:%M:%S')
            sig_price = sig.entry_price
            
            # 买入信号
            ax1.scatter([sig_time], [sig_price], color='red', marker='^', s=200, zorder=5, 
                       label='买入信号' if idx == 0 else "")
            
            # 卖出信号
            exit_time = datetime.strptime(sig.exit_time, '%Y-%m-%d %H:%M:%S')
            exit_price = sig.exit_price
            
            if sig.exit_reason == '止盈':
                color = 'green'
                label = '止盈'
            elif sig.exit_reason == '止损':
                color = 'orange'
                label = '止损'
            else:
                color = 'gray'
                label = '到期'
            
            ax1.scatter([exit_time], [exit_price], color=color, marker='v', s=200, zorder=5,
                       label=label if idx == 0 else "")
        except Exception as e:
            print(f"⚠️ 信号绘制失败：{e}")
    
    ax1.set_ylabel('价格', fontsize=12)
    ax1.set_title(f'{symbol} - TrendReversalV2 策略信号图', fontsize=14, fontweight='bold')
    ax1.legend(loc='upper left', fontsize=9, framealpha=0.9)
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    ax1.xaxis.set_major_locator(mdates.HourLocator(interval=12))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # === 子图 2: MACD ===
    ax2 = fig.add_subplot(gs[1], sharex=ax1)
    
    ax2.plot(times, macd_line, 'blue', linewidth=1.5, label='MACD')
    ax2.plot(times, signal_line, 'orange', linewidth=1.5, label='Signal')
    
    # MACD 柱状图
    colors = ['green' if h >= 0 else 'red' for h in histogram]
    ax2.bar(times, histogram, color=colors, alpha=0.5, width=0.3)
    
    ax2.axhline(y=0, color='gray', linestyle='-', linewidth=0.5)
    ax2.set_ylabel('MACD', fontsize=10)
    ax2.legend(loc='upper left', fontsize=8)
    ax2.grid(True, alpha=0.3)
    
    # === 子图 3: RSI ===
    ax3 = fig.add_subplot(gs[2], sharex=ax1)
    
    ax3.plot(times, rsi, 'purple', linewidth=1.5, label='RSI(14)')
    ax3.axhline(y=70, color='red', linestyle='--', linewidth=1, alpha=0.5, label='超买 (70)')
    ax3.axhline(y=30, color='green', linestyle='--', linewidth=1, alpha=0.5, label='超卖 (30)')
    ax3.fill_between(times, 30, 70, alpha=0.1, color='purple')
    
    ax3.set_ylabel('RSI', fontsize=10)
    ax3.set_ylim(0, 100)
    ax3.legend(loc='upper left', fontsize=8)
    ax3.grid(True, alpha=0.3)
    
    # === 子图 4: 成交量 ===
    ax4 = fig.add_subplot(gs[3], sharex=ax1)
    
    vol_colors = ['red' if closes[i] >= opens[i] else 'green' for i in range(len(times))]
    ax4.bar(times, volumes, color=vol_colors, alpha=0.7, width=0.3)
    
    # 成交量均线
    vol_ma = np.convolve(volumes, np.ones(20)/20, mode='same')
    ax4.plot(times, vol_ma, 'blue', linewidth=1.5, label='MA20')
    
    ax4.set_ylabel('成交量', fontsize=10)
    ax4.legend(loc='upper left', fontsize=8)
    ax4.grid(True, alpha=0.3)
    ax4.set_xlabel('时间', fontsize=10)
    
    plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45, ha='right')
    ax4.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    ax4.xaxis.set_major_locator(mdates.HourLocator(interval=12))
    
    # 保存图表
    if not output_path:
        symbol_safe = symbol.replace('.', '_').replace('/', '_')
        output_path = f"/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/signal_chart_{symbol_safe}.png"
    
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"✅ 信号图表已保存：{output_path}")
    return output_path


def main():
    db_path = "/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/kline_data.db"
    contracts_path = "/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/main_contracts.json"
    
    print("\n🎯 TrendReversalV2 策略信号图表生成器\n")
    
    if len(sys.argv) > 1:
        target_symbols = sys.argv[1:]
    else:
        # 获取可用合约列表
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT symbol FROM kline_data 
            WHERE duration = 300 
            ORDER BY symbol 
            LIMIT 20
        """)
        symbols = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        print("可用合约列表 (前 20 个):")
        for idx, sym in enumerate(symbols, 1):
            print(f"  {idx}. {sym}")
        print()
        
        choice = input("选择合约 (序号/代码，空格分隔，留空生成前 3 个): ").strip()
        
        if not choice:
            target_symbols = symbols[:3]
        else:
            target_symbols = []
            for item in choice.split():
                if item.isdigit():
                    idx = int(item) - 1
                    if 0 <= idx < len(symbols):
                        target_symbols.append(symbols[idx])
                else:
                    target_symbols.append(item)
    
    print(f"\n🚀 开始生成图表：{target_symbols}\n")
    
    for symbol in target_symbols:
        generate_signal_chart(symbol, db_path, contracts_path)
    
    print("\n✅ 图表生成完成!\n")


if __name__ == "__main__":
    main()
