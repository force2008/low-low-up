#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V2 vs V3 回测对比图表生成器
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
from TrendReversalV2Strategy import TrendReversalV2Strategy
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


def plot_backtest_comparison(symbol: str, data_5min: list, data_60min: list,
                             signals_v2: list, signals_v3: list,
                             output_path: str = None):
    """绘制 V2 vs V3 回测对比图"""
    
    if len(data_5min) < 50:
        print(f"⚠️ 数据不足：{symbol}")
        return None
    
    x = np.arange(len(data_5min))
    closes = np.array([d['close'] for d in data_5min])
    opens = np.array([d['open'] for d in data_5min])
    highs = np.array([d['high'] for d in data_5min])
    lows = np.array([d['low'] for d in data_5min])
    
    # 计算总盈亏
    total_pnl_v2 = sum(s.pnl_amount for s in signals_v2) if signals_v2 else 0
    total_pnl_v3 = sum(s.pnl_amount for s in signals_v3) if signals_v3 else 0
    is_winning = (total_pnl_v2 + total_pnl_v3) > 0
    up_color = '#d62728' if is_winning else '#2ca02c'
    down_color = '#1f77b4' if is_winning else '#d62728'
    
    # 创建图表
    fig = plt.figure(figsize=(16, 12))
    gs = fig.add_gridspec(3, 1, height_ratios=[2, 1, 1], hspace=0.05)
    
    ax1 = fig.add_subplot(gs[0])
    
    # K 线图
    for i in range(len(data_5min)):
        color = up_color if closes[i] >= opens[i] else down_color
        ax1.plot([x[i], x[i]], [lows[i], highs[i]], color=color, linewidth=0.8)
        h = closes[i] - opens[i]
        if abs(h) < 0.0001: h = 0.0001
        rect = Rectangle((x[i] - 0.3, min(opens[i], closes[i])), 0.6, h,
                        facecolor=color, edgecolor=color, linewidth=0)
        ax1.add_patch(rect)
    
    # V2 信号 (蓝色)
    for sig in signals_v2:
        entry_idx = int(sig.entry_idx)
        exit_reason = sig.exit_reason
        pnl = sig.pnl_amount
        
        ax1.annotate('V2↑', xy=(entry_idx, lows[entry_idx] * 0.998), 
                    color='blue', fontsize=10, fontweight='bold',
                    ha='center', va='top',
                    bbox=dict(boxstyle='round,pad=0.2', facecolor='white', 
                             edgecolor='blue', linewidth=1.5))
    
    # V3 信号 (红色)
    for sig in signals_v3:
        entry_idx = int(sig.entry_idx)
        exit_reason = sig.exit_reason
        pnl = sig.pnl_amount
        
        ax1.annotate('V3↑', xy=(entry_idx, highs[entry_idx] * 1.002), 
                    color='red', fontsize=10, fontweight='bold',
                    ha='center', va='bottom',
                    bbox=dict(boxstyle='round,pad=0.2', facecolor='white', 
                             edgecolor='red', linewidth=1.5))
    
    # 标题
    title = f"{symbol} - V2 vs V3 对比 | V2: {total_pnl_v2:+,.0f}元 ({len(signals_v2)}笔) | V3: {total_pnl_v3:+,.0f}元 ({len(signals_v3)}笔)"
    ax1.set_title(title, fontsize=12, fontweight='bold')
    ax1.set_ylabel('Price')
    ax1.grid(True, alpha=0.25)
    ax1.set_xticks([])
    
    # V2 权益曲线
    ax2 = fig.add_subplot(gs[1], sharex=ax1)
    if signals_v2:
        equity = 100000
        equity_curve = [100000]
        x_curve = [0]
        for sig in signals_v2:
            equity += sig.pnl_amount
            equity_curve.append(equity)
            x_curve.append(sig.entry_idx)
        ax2.plot(x_curve, equity_curve, 'b-', linewidth=2, label=f'V2 ({total_pnl_v2:+,.0f})')
        ax2.fill_between(x_curve, 100000, equity_curve, alpha=0.3, color='blue')
    ax2.set_ylabel('V2 权益')
    ax2.legend(loc='upper left')
    ax2.grid(True, alpha=0.25)
    ax2.set_xticks([])
    
    # V3 权益曲线
    ax3 = fig.add_subplot(gs[2], sharex=ax1)
    if signals_v3:
        equity = 100000
        equity_curve = [100000]
        x_curve = [0]
        for sig in signals_v3:
            equity += sig.pnl_amount
            equity_curve.append(equity)
            x_curve.append(sig.entry_idx)
        ax3.plot(x_curve, equity_curve, 'r-', linewidth=2, label=f'V3 ({total_pnl_v3:+,.0f})')
        ax3.fill_between(x_curve, 100000, equity_curve, alpha=0.3, color='red')
    ax3.set_ylabel('V3 权益')
    ax3.set_xlabel('K-line Index')
    ax3.legend(loc='upper left')
    ax3.grid(True, alpha=0.25)
    
    plt.subplots_adjust(left=0.06, right=0.98, top=0.92, bottom=0.08, hspace=0.05)
    
    if not output_path:
        symbol_safe = symbol.replace('.', '_')
        output_path = f'/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/backtest/{symbol_safe}/backtest_comparison.png'
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"✅ 对比图：{output_path}")
    return output_path


def main():
    db_path = '/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/kline_data.db'
    contracts_path = '/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/main_contracts.json'
    
    print("\n📊 生成 V2 vs V3 回测对比图表\n")
    
    # 测试品种
    test_symbols = ['CFFEX.IF2603', 'CFFEX.IC2606', 'CZCE.MA605', 'DCE.m2605']
    
    strategy_v2 = TrendReversalV2Strategy()
    strategy_v2.load_contracts(contracts_path)
    
    strategy_v3 = TrendReversalV3Strategy()
    strategy_v3.load_contracts(contracts_path)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    for symbol in test_symbols:
        print(f"\n{'='*60}")
        print(f"📈 {symbol}")
        print(f"{'='*60}")
        
        # 加载数据
        cursor.execute("SELECT datetime, open, high, low, close, volume FROM kline_data WHERE symbol = ? AND duration = 300 ORDER BY datetime DESC LIMIT 500", (symbol,))
        data_5min = [{'time': row[0], 'open': row[1], 'high': row[2], 'low': row[3], 'close': row[4], 'volume': row[5]} for row in reversed(cursor.fetchall())]
        
        cursor.execute("SELECT datetime, open, high, low, close, volume FROM kline_data WHERE symbol = ? AND duration = 3600 ORDER BY datetime DESC LIMIT 200", (symbol,))
        data_60min = [{'time': row[0], 'open': row[1], 'high': row[2], 'low': row[3], 'close': row[4], 'volume': row[5]} for row in reversed(cursor.fetchall())]
        
        if len(data_5min) < 200 or len(data_60min) < 50:
            print(f"⚠️ 数据不足，跳过")
            continue
        
        # V2 回测
        signals_v2 = strategy_v2.run_backtest(symbol, data_5min, data_60min) or []
        
        # V3 回测
        signals_v3 = strategy_v3.run_backtest(symbol, data_5min, data_60min) or []
        
        # 生成对比图
        plot_backtest_comparison(symbol, data_5min, data_60min, signals_v2, signals_v3)
    
    conn.close()
    
    print(f"\n{'='*60}")
    print("✅ 图表生成完成!")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
