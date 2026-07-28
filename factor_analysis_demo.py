#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
单因子测试 Demo

以 BOLL %B 因子为例，演示量化因子测试的完整流程：
1. 读取日 K 线数据
2. 计算因子值（BOLL %B）
3. 计算下期收益率
4. 计算 IC（信息系数）
5. 分组测试（按因子值分 5 组）
6. 绘制 IC 序列、分组收益、累计收益

用法：
    python factor_analysis_demo.py [test|online] [--symbol SHFE.rb2505]
"""

import json
import os
import sys
import argparse
import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# 项目根目录加入路径
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from strategy.boll import BOLLCalculator


DB_PATHS = {
    "test": os.path.join(PROJECT_ROOT, "data", "db", "kline_data_test.db"),
    "online": os.path.join(PROJECT_ROOT, "data", "db", "kline_data.db"),
}

MAIN_CONTRACTS_PATH = os.path.join(PROJECT_ROOT, "data", "contracts", "main_contracts.json")

# 设计系统占位配色（brand-neutral placeholder palette）
PALETTE = {
    "primary": "#2563EB",      # 主色：蓝
    "secondary": "#7C3AED",    # 辅色：紫
    "accent": "#DC2626",       # 强调：红
    "positive": "#059669",     # 正收益：绿
    "negative": "#DC2626",     # 负收益：红
    "neutral": "#6B7280",      # 中性灰
    "surface": "#F9FAFB",      # 图表背景
    "grid": "#E5E7EB",         # 网格线
    "text": "#111827",         # 主文字
    "muted": "#6B7280",        # 次要文字
}


def load_main_contracts(json_file: str) -> list:
    """加载主力合约列表"""
    if not os.path.exists(json_file):
        return []
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data if isinstance(data, list) else data.get("main_contracts", [])


def get_kline_data(conn, symbol: str, duration: int = 86400) -> pd.DataFrame:
    """从数据库读取 K 线数据"""
    query = """
        SELECT datetime, open, high, low, close, volume
        FROM kline_data
        WHERE symbol = ? AND duration = ?
        ORDER BY datetime ASC
    """
    df = pd.read_sql_query(query, conn, params=(symbol, duration))
    df['datetime'] = pd.to_datetime(df['datetime'])
    return df


def compute_boll_percent_b(df: pd.DataFrame, period: int = 20, multiplier: float = 2.0) -> pd.DataFrame:
    """
    计算 BOLL %B 因子

    %B = (close - lower) / (upper - lower)
    - %B > 1: 价格突破上轨
    - %B = 1: 价格在上轨
    - %B = 0.5: 价格在中轨
    - %B = 0: 价格在下轨
    - %B < 0: 价格跌破下轨
    """
    data = list(df.itertuples(index=False, name=None))
    data_with_boll = BOLLCalculator.calculate(data, period=period, multiplier=multiplier)

    result = pd.DataFrame(
        data_with_boll,
        columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'ma', 'upper', 'middle', 'lower']
    )
    result['datetime'] = pd.to_datetime(result['datetime'])

    # 计算 %B 因子
    band_width = result['upper'] - result['lower']
    result['pct_b'] = (result['close'] - result['lower']) / band_width

    return result


def factor_test(df: pd.DataFrame, factor_col: str = 'pct_b', n_groups: int = 5, hold_days: int = 1):
    """
    单因子测试主函数

    返回：
        df: 包含因子、下期收益、分组标签的 DataFrame
        ic_series: 滚动 IC 序列
        group_returns: 各组平均收益
    """
    # 计算下期收益率（日频，持有 1 天）
    df['next_return'] = df['close'].shift(-hold_days) / df['close'] - 1

    # 去掉因子值缺失和未来收益缺失的行
    df = df.dropna(subset=[factor_col, 'next_return']).copy()

    # 按因子值分 n 组（1=最低，n=最高）
    df['group'] = pd.qcut(df[factor_col], n_groups, labels=False, duplicates='drop') + 1

    # 计算 IC：因子值与下期收益的 Spearman 秩相关系数
    ic = df[factor_col].corr(df['next_return'], method='spearman')

    # 滚动 IC（20 日窗口，Pearson 相关系数；滚动 Spearman 需要自定义 apply）
    df['ic_rolling'] = df[factor_col].rolling(window=20).corr(df['next_return'])

    # 分组平均收益
    group_returns = df.groupby('group')['next_return'].mean()

    # Top / Bottom 组累计收益
    top_df = df[df['group'] == n_groups].copy()
    bottom_df = df[df['group'] == 1].copy()
    top_df['cum_return'] = (1 + top_df['next_return']).cumprod() - 1
    bottom_df['cum_return'] = (1 + bottom_df['next_return']).cumprod() - 1

    return df, ic, group_returns, top_df, bottom_df


def plot_results(df, ic, group_returns, top_df, bottom_df, symbol, output_path='factor_demo.png'):
    """绘制因子测试结果"""
    plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False

    fig, axes = plt.subplots(2, 2, figsize=(14, 10), facecolor=PALETTE['surface'])
    fig.suptitle(f'BOLL %B 因子测试 - {symbol}', fontsize=16, color=PALETTE['text'], y=0.98)

    # 1. 因子值时间序列
    ax1 = axes[0, 0]
    ax1.plot(df['datetime'], df['pct_b'], color=PALETTE['primary'], linewidth=1.2, label='BOLL %B')
    ax1.axhline(0.5, color=PALETTE['neutral'], linestyle='--', linewidth=1, label='中轨(0.5)')
    ax1.axhline(1.0, color=PALETTE['accent'], linestyle='--', linewidth=1, alpha=0.6, label='上轨(1.0)')
    ax1.axhline(0.0, color=PALETTE['accent'], linestyle='--', linewidth=1, alpha=0.6, label='下轨(0.0)')
    ax1.set_title('因子值时间序列', fontsize=12, color=PALETTE['text'])
    ax1.set_ylabel('BOLL %B', color=PALETTE['muted'])
    ax1.legend(loc='upper left', frameon=False)
    ax1.grid(True, color=PALETTE['grid'], linestyle='-', linewidth=0.5)
    ax1.set_facecolor(PALETTE['surface'])

    # 2. 滚动 IC
    ax2 = axes[0, 1]
    valid_ic = df.dropna(subset=['ic_rolling'])
    ax2.plot(valid_ic['datetime'], valid_ic['ic_rolling'], color=PALETTE['secondary'], linewidth=1.2)
    ax2.axhline(0, color=PALETTE['neutral'], linestyle='-', linewidth=1)
    ax2.axhline(0.05, color=PALETTE['positive'], linestyle='--', linewidth=1, alpha=0.6)
    ax2.axhline(-0.05, color=PALETTE['negative'], linestyle='--', linewidth=1, alpha=0.6)
    ax2.set_title(f'滚动 IC (20日窗口) | 整体 IC={ic:.4f}', fontsize=12, color=PALETTE['text'])
    ax2.set_ylabel('IC', color=PALETTE['muted'])
    ax2.grid(True, color=PALETTE['grid'], linestyle='-', linewidth=0.5)
    ax2.set_facecolor(PALETTE['surface'])

    # 3. 分组收益
    ax3 = axes[1, 0]
    colors = [PALETTE['negative'] if v < 0 else PALETTE['positive'] for v in group_returns.values]
    bars = ax3.bar(group_returns.index.astype(str), group_returns.values * 100, color=colors, width=0.6)
    ax3.axhline(0, color=PALETTE['neutral'], linewidth=1)
    ax3.set_title('分组平均收益（%）', fontsize=12, color=PALETTE['text'])
    ax3.set_xlabel('因子分组（1=最低，5=最高）', color=PALETTE['muted'])
    ax3.set_ylabel('平均收益（%）', color=PALETTE['muted'])
    ax3.grid(True, color=PALETTE['grid'], linestyle='-', linewidth=0.5, axis='y')
    ax3.set_facecolor(PALETTE['surface'])
    # 在柱子上添加数值标签
    for bar in bars:
        height = bar.get_height()
        ax3.annotate(f'{height:.2f}%',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3 if height >= 0 else -12),
                    textcoords="offset points",
                    ha='center', va='bottom' if height >= 0 else 'top',
                    fontsize=9, color=PALETTE['text'])

    # 4. Top / Bottom 组累计收益
    ax4 = axes[1, 1]
    ax4.plot(top_df['datetime'], top_df['cum_return'] * 100,
             color=PALETTE['positive'], linewidth=1.5, label='Top组(因子最高)')
    ax4.plot(bottom_df['datetime'], bottom_df['cum_return'] * 100,
             color=PALETTE['negative'], linewidth=1.5, label='Bottom组(因子最低)')
    ax4.axhline(0, color=PALETTE['neutral'], linewidth=1)
    ax4.set_title('Top/Bottom 组累计收益（%）', fontsize=12, color=PALETTE['text'])
    ax4.set_xlabel('日期', color=PALETTE['muted'])
    ax4.set_ylabel('累计收益（%）', color=PALETTE['muted'])
    ax4.legend(loc='upper left', frameon=False)
    ax4.grid(True, color=PALETTE['grid'], linestyle='-', linewidth=0.5)
    ax4.set_facecolor(PALETTE['surface'])

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.savefig(output_path, dpi=150, facecolor=PALETTE['surface'])
    print(f"[图表] 已保存到 {output_path}")
    plt.close()


def main():
    parser = argparse.ArgumentParser(description='单因子测试 Demo')
    parser.add_argument('env', nargs='?', default='test', choices=['test', 'online'], help='数据库环境')
    parser.add_argument('--symbol', type=str, default=None, help='指定合约，如 SHFE.rb2505')
    parser.add_argument('--output', type=str, default='factor_demo.png', help='输出图片路径')
    args = parser.parse_args()

    db_path = DB_PATHS.get(args.env)
    if not os.path.exists(db_path):
        print(f"[错误] 数据库不存在：{db_path}")
        return

    # 如果没有指定合约，取 main_contracts.json 第一个
    symbol = args.symbol
    if not symbol:
        contracts = load_main_contracts(MAIN_CONTRACTS_PATH)
        if contracts:
            inst = contracts[0]
            symbol = f"{inst.get('ExchangeID', '')}.{inst.get('MainContractID', '')}".strip('.')
        else:
            symbol = "SHFE.rb2505"  # 默认示例

    print(f"[因子测试] 合约：{symbol} | 数据库：{db_path}")

    conn = sqlite3.connect(db_path)
    try:
        df = get_kline_data(conn, symbol,3600)
        if len(df) < 50:
            print(f"[错误] {symbol} 数据量不足（{len(df)} 根），需要至少 50 根日 K 线")
            return

        print(f"[数据] 读取到 {len(df)} 根日 K 线")

        # 计算因子
        df = compute_boll_percent_b(df)
        print("[因子] 已计算 BOLL %B 因子")

        # 因子测试
        df, ic, group_returns, top_df, bottom_df = factor_test(df)

        # 输出统计结果
        print("\n========== 因子测试结果 ==========")
        print(f"整体 IC（Spearman）：{ic:.4f}")
        print(f"滚动 IC 均值（Pearson，20日窗口）：{df['ic_rolling'].mean():.4f}")
        print(f"滚动 IC > 0 占比：{(df['ic_rolling'] > 0).mean() * 100:.2f}%")
        print(f"\n分组平均收益：")
        for group, ret in group_returns.items():
            print(f"  组 {int(group)}: {ret * 100:.4f}%")
        print(f"\nTop组（因子最高）累计收益：{top_df['cum_return'].iloc[-1] * 100:.2f}%")
        print(f"Bottom组（因子最低）累计收益：{bottom_df['cum_return'].iloc[-1] * 100:.2f}%")
        print(f"多空对冲累计收益：{(top_df['cum_return'].iloc[-1] - bottom_df['cum_return'].iloc[-1]) * 100:.2f}%")
        print("==================================\n")

        # 绘图
        plot_results(df, ic, group_returns, top_df, bottom_df, symbol, args.output)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
