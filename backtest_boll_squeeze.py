#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
布林带收口 + 突破位置标记工具

功能：
- 读取 K 线数据，计算布林带
- 标记出"布林带收口后向上/向下突破"的位置
- 不执行交易，只画图观察这些位置后续是否有行情
- 输出每个标记点后续 N 根 K 线的收益率，方便人工验证

用法：
    python backtest_boll_squeeze.py [test|online] [--symbol SHFE.rb2505] [--period 3600] [--forward 10]

参数：
    --period: K线周期秒数，3600=60分钟，86400=日线，900=15分钟
    --forward: 标记后观察几根K线的收益，默认 10
"""

import json
import os
import sys
import argparse
import sqlite3

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from strategy.boll import BOLLCalculator


DB_PATHS = {
    "test": os.path.join(PROJECT_ROOT, "data", "db", "kline_data_test.db"),
    "online": os.path.join(PROJECT_ROOT, "data", "db", "kline_data.db"),
}
MAIN_CONTRACTS_PATH = os.path.join(PROJECT_ROOT, "data", "contracts", "main_contracts.json")

PALETTE = {
    "primary": "#2563EB",
    "secondary": "#7C3AED",
    "positive": "#059669",
    "negative": "#DC2626",
    "neutral": "#6B7280",
    "surface": "#F9FAFB",
    "grid": "#E5E7EB",
    "text": "#111827",
    "muted": "#6B7280",
}


def load_main_contracts(json_file: str) -> list:
    if not os.path.exists(json_file):
        return []
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data if isinstance(data, list) else data.get("main_contracts", [])


def get_kline_data(conn, symbol: str, duration: int) -> pd.DataFrame:
    query = """
        SELECT datetime, open, high, low, close, volume
        FROM kline_data
        WHERE symbol = ? AND duration = ?
        ORDER BY datetime ASC
    """
    df = pd.read_sql_query(query, conn, params=(symbol, duration))
    df['datetime'] = pd.to_datetime(df['datetime'])
    return df


def find_squeeze_breakouts(
    df: pd.DataFrame,
    boll_period: int = 20,
    boll_multiplier: float = 2.0,
    squeeze_lookback: int = 20,
    squeeze_threshold_pct: float = 0.05,
    volume_ratio: float = 1.0,
) -> pd.DataFrame:
    """
    标记布林带收口区间以及收口后的突破方向。

    逻辑：
    1. 找出 bandwidth 处于近 lookback 根低位（收口）的 K 线
    2. 把连续的收口 K 线合并为一个 squeeze 区间
    3. 区间结束后，价格突破区间最高价 → UP；跌破区间最低价 → DOWN

    返回 DataFrame 增加列：
    - upper/middle/lower/bandwidth
    - is_squeeze: 是否处于收口状态
    - squeeze_id: 收口区间编号
    - breakout: 'UP' / 'DOWN' / ''
    """
    if len(df) < boll_period + squeeze_lookback + 5:
        return df

    data = list(df.itertuples(index=False, name=None))
    data_with_boll = BOLLCalculator.calculate(data, period=boll_period, multiplier=boll_multiplier)

    boll_df = pd.DataFrame(
        data_with_boll,
        columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'ma', 'upper', 'middle', 'lower']
    )
    boll_df['datetime'] = pd.to_datetime(boll_df['datetime'])
    boll_df['bandwidth'] = (boll_df['upper'] - boll_df['lower']) / boll_df['middle']
    boll_df['volume_ma5'] = boll_df['volume'].rolling(window=5).mean()

    boll_df['is_squeeze'] = False
    boll_df['squeeze_id'] = 0
    boll_df['breakout'] = ''

    # 标记 is_squeeze
    for i in range(squeeze_lookback + boll_period, len(boll_df)):
        recent = boll_df['bandwidth'].iloc[i - squeeze_lookback:i]
        min_bw = recent.min()
        is_squeeze = boll_df.at[i, 'bandwidth'] <= min_bw * (1 + squeeze_threshold_pct)
        volume_ok = boll_df.at[i, 'volume'] >= boll_df.at[i, 'volume_ma5'] * volume_ratio
        boll_df.at[i, 'is_squeeze'] = is_squeeze and volume_ok

    # 合并连续 squeeze K 线为区间，并检测区间后的突破
    squeeze_id = 0
    in_squeeze = False
    squeeze_start = -1
    squeeze_high = 0.0
    squeeze_low = float('inf')

    for i in range(len(boll_df)):
        if boll_df.at[i, 'is_squeeze']:
            if not in_squeeze:
                in_squeeze = True
                squeeze_id += 1
                squeeze_start = i
                squeeze_high = boll_df.at[i, 'high']
                squeeze_low = boll_df.at[i, 'low']
            else:
                squeeze_high = max(squeeze_high, boll_df.at[i, 'high'])
                squeeze_low = min(squeeze_low, boll_df.at[i, 'low'])
            boll_df.at[i, 'squeeze_id'] = squeeze_id
        else:
            if in_squeeze:
                # 当前 K 线是 squeeze 区间后的第一根非收口 K 线，检查是否突破区间
                if boll_df.at[i, 'close'] > squeeze_high:
                    boll_df.at[i, 'breakout'] = 'UP'
                elif boll_df.at[i, 'close'] < squeeze_low:
                    boll_df.at[i, 'breakout'] = 'DOWN'
                in_squeeze = False
                squeeze_high = 0.0
                squeeze_low = float('inf')
            else:
                # 已经离开 squeeze 区间，后续不再标记
                pass

    return boll_df


def compute_forward_returns(df: pd.DataFrame, forward_bars: int = 10) -> pd.DataFrame:
    """计算每个 breakout 点未来 N 根 K 线的收益率"""
    df = df.copy()
    df['fwd_return'] = np.nan
    df['fwd_max_return'] = np.nan
    df['fwd_max_drawdown'] = np.nan

    breakout_idx = df[df['breakout'] != ''].index
    for idx in breakout_idx:
        if idx + forward_bars >= len(df):
            continue
        entry_price = df.at[idx, 'close']
        future = df.iloc[idx + 1:idx + forward_bars + 1]

        # 按突破方向计算收益
        if df.at[idx, 'breakout'] == 'UP':
            df.at[idx, 'fwd_return'] = (future['close'].iloc[-1] - entry_price) / entry_price
            df.at[idx, 'fwd_max_return'] = (future['high'].max() - entry_price) / entry_price
            df.at[idx, 'fwd_max_drawdown'] = (future['low'].min() - entry_price) / entry_price
        else:
            df.at[idx, 'fwd_return'] = (entry_price - future['close'].iloc[-1]) / entry_price
            df.at[idx, 'fwd_max_return'] = (entry_price - future['low'].min()) / entry_price
            df.at[idx, 'fwd_max_drawdown'] = (entry_price - future['high'].max()) / entry_price

    return df


def plot_markings(df: pd.DataFrame, symbol: str, forward_bars: int, output_path: str):
    """绘制价格、布林带和突破标记"""
    plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False

    fig, axes = plt.subplots(3, 1, figsize=(16, 12), facecolor=PALETTE['surface'])
    fig.suptitle(f'布林带收口突破位置标记 - {symbol}（后续 {forward_bars} 根 K 线收益）',
                 fontsize=16, color=PALETTE['text'], y=0.98)

    # 1. 价格 + 布林带 + 突破标记
    ax1 = axes[0]
    ax1.plot(df['datetime'], df['close'], color=PALETTE['text'], linewidth=1.2, label='收盘价')
    ax1.plot(df['datetime'], df['upper'], color=PALETTE['primary'], linewidth=0.8, alpha=0.7, label='上轨')
    ax1.plot(df['datetime'], df['middle'], color=PALETTE['neutral'], linewidth=0.8, linestyle='--', label='中轨')
    ax1.plot(df['datetime'], df['lower'], color=PALETTE['primary'], linewidth=0.8, alpha=0.7, label='下轨')

    up_breaks = df[df['breakout'] == 'UP']
    down_breaks = df[df['breakout'] == 'DOWN']

    ax1.scatter(up_breaks['datetime'], up_breaks['close'],
                color=PALETTE['positive'], marker='^', s=100, label=f'向上突破 ({len(up_breaks)})', zorder=5)
    ax1.scatter(down_breaks['datetime'], down_breaks['close'],
                color=PALETTE['negative'], marker='v', s=100, label=f'向下突破 ({len(down_breaks)})', zorder=5)

    ax1.set_title('价格走势与突破标记', fontsize=12, color=PALETTE['text'])
    ax1.set_ylabel('价格', color=PALETTE['muted'])
    ax1.legend(loc='upper left', frameon=False)
    ax1.grid(True, color=PALETTE['grid'], linestyle='-', linewidth=0.5)
    ax1.set_facecolor(PALETTE['surface'])

    # 2. 布林带带宽 + squeeze 区域
    ax2 = axes[1]
    ax2.plot(df['datetime'], df['bandwidth'] * 100, color=PALETTE['secondary'], linewidth=1.2, label='Bandwidth (%)')
    squeeze_points = df[df['is_squeeze']]
    ax2.scatter(squeeze_points['datetime'], squeeze_points['bandwidth'] * 100,
                color=PALETTE['neutral'], marker='o', s=15, alpha=0.5, label=f'收口区域 ({len(squeeze_points)})')
    ax2.set_title('布林带带宽与收口区域', fontsize=12, color=PALETTE['text'])
    ax2.set_ylabel('Bandwidth (%)', color=PALETTE['muted'])
    ax2.legend(loc='upper left', frameon=False)
    ax2.grid(True, color=PALETTE['grid'], linestyle='-', linewidth=0.5)
    ax2.set_facecolor(PALETTE['surface'])

    # 3. 后续收益分布
    ax3 = axes[2]
    breakout_df = df[df['breakout'] != ''].copy()
    if len(breakout_df) > 0:
        colors = [PALETTE['positive'] if r > 0 else PALETTE['negative'] for r in breakout_df['fwd_return']]
        bars = ax3.bar(range(len(breakout_df)), breakout_df['fwd_return'] * 100, color=colors, width=0.6)
        ax3.axhline(0, color=PALETTE['neutral'], linewidth=1)
        ax3.set_title(f'标记点后续 {forward_bars} 根 K 线收益率分布', fontsize=12, color=PALETTE['text'])
        ax3.set_xlabel('突破事件序号', color=PALETTE['muted'])
        ax3.set_ylabel('收益率（%）', color=PALETTE['muted'])
        ax3.grid(True, color=PALETTE['grid'], linestyle='-', linewidth=0.5, axis='y')
        ax3.set_facecolor(PALETTE['surface'])

        # 在柱子上标注收益
        for j, bar in enumerate(bars):
            height = bar.get_height()
            if not np.isnan(height):
                ax3.annotate(f'{height:.1f}%',
                            xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 3 if height >= 0 else -10),
                            textcoords="offset points",
                            ha='center', va='bottom' if height >= 0 else 'top',
                            fontsize=7, color=PALETTE['text'])

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.savefig(output_path, dpi=150, facecolor=PALETTE['surface'])
    print(f"[图表] 已保存到 {output_path}")
    plt.close()


def main():
    parser = argparse.ArgumentParser(description='布林带收口突破位置标记工具')
    parser.add_argument('env', nargs='?', default='test', choices=['test', 'online'], help='数据库环境')
    parser.add_argument('--symbol', type=str, default=None, help='指定合约，如 SHFE.rb2505')
    parser.add_argument('--period', type=int, default=3600,
                        help='K线周期秒数：300=5分钟, 900=15分钟, 3600=60分钟, 86400=日线')
    parser.add_argument('--forward', type=int, default=10, help='标记后观察几根K线的收益，默认 10')
    parser.add_argument('--squeeze-threshold', type=float, default=0.20, help='收口阈值，默认 0.20（当前带宽 <= 近lookback最低带宽的120%即视为收口）')
    parser.add_argument('--volume-ratio', type=float, default=1.0, help='放量倍数，默认 1.0（不强制放量）')
    parser.add_argument('--output', type=str, default='boll_squeeze_markings.png', help='输出图片路径')
    args = parser.parse_args()

    db_path = DB_PATHS.get(args.env)
    if not os.path.exists(db_path):
        print(f"[错误] 数据库不存在：{db_path}")
        return

    symbol = args.symbol
    if not symbol:
        contracts = load_main_contracts(MAIN_CONTRACTS_PATH)
        if contracts:
            inst = contracts[0]
            symbol = f"{inst.get('ExchangeID', '')}.{inst.get('MainContractID', '')}".strip('.')
        else:
            symbol = "SHFE.rb2610"

    print(f"[标记] 合约：{symbol} | 周期：{args.period}秒 | 后续观察：{args.forward}根K线")

    conn = sqlite3.connect(db_path)
    try:
        df = get_kline_data(conn, symbol, args.period)
        if len(df) < 100:
            print(f"[错误] {symbol} 数据量不足（{len(df)} 根）")
            return

        print(f"[数据] 读取到 {len(df)} 根 K 线")

        df = find_squeeze_breakouts(
            df,
            squeeze_threshold_pct=args.squeeze_threshold,
            volume_ratio=args.volume_ratio,
        )
        df = compute_forward_returns(df, forward_bars=args.forward)

        up_count = len(df[df['breakout'] == 'UP'])
        down_count = len(df[df['breakout'] == 'DOWN'])
        print(f"[结果] 向上突破：{up_count} 次 | 向下突破：{down_count} 次")

        if up_count + down_count == 0:
            print("[提示] 未找到任何突破标记点")
            return

        # 输出每个标记点的后续收益
        print(f"\n========== 标记点后续 {args.forward} 根 K 线收益 ==========")
        breakout_df = df[df['breakout'] != ''].copy()
        for idx, row in breakout_df.iterrows():
            direction = "多" if row['breakout'] == 'UP' else "空"
            fwd = row['fwd_return'] * 100 if not pd.isna(row['fwd_return']) else 0
            max_gain = row['fwd_max_return'] * 100 if not pd.isna(row['fwd_max_return']) else 0
            max_dd = row['fwd_max_drawdown'] * 100 if not pd.isna(row['fwd_max_drawdown']) else 0
            print(f"  [{row['datetime']}] {direction} | 收盘收益：{fwd:+.2f}% | "
                  f"最大有利：{max_gain:+.2f}% | 最大不利：{max_dd:+.2f}%")

        # 统计汇总
        valid = breakout_df.dropna(subset=['fwd_return'])
        if len(valid) > 0:
            win_rate = (valid['fwd_return'] > 0).mean() * 100
            avg_return = valid['fwd_return'].mean() * 100
            print(f"\n汇总：总标记 {len(valid)} 次 | 收盘胜率：{win_rate:.1f}% | 平均收益：{avg_return:+.2f}%")
        print("====================================================\n")

        plot_markings(df, symbol, args.forward, args.output)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
