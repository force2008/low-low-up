#!/usr/bin/env python3
"""
K线时间回放测试 - 模拟 KlineCollector_v2 的 save_kline 信号检测逻辑

与 save_kline 的对应关系：
┌─────────────────────────────────────────────────────────────────────────┐
│ save_kline (实盘)                    │ test_kline_playback (回放)           │
├─────────────────────────────────────────────────────────────────────────┤
│ duration=300 时:                     │ 每根 5m K线:                       │
│   check_strategy_signal_v2(symbol)   │   check_strategy_signal_v2(symbol)  │
├─────────────────────────────────────────────────────────────────────────┤
│ duration=3600 时:                   │ 60分钟K线时间点(分钟=0)时:           │
│   check_60m_signal_v2(              │   check_60m_signal_v2(              │
│       symbol,                        │       symbol,                       │
│       end_time=date_time_str)         │       end_time=prev_60m_time)         │
│   (使用当前K线时间作为截止时间)         │   (使用前一小时作为截止时间)           │
└─────────────────────────────────────────────────────────────────────────┘

关键说明：
1. 每根5分钟K线都会检查5分钟信号
2. 只有在60分钟K线完成时（分钟=0），才检查60分钟信号
3. 60分钟信号使用"前一小时"的K线数据，而不是当前小时的
4. 检查60分钟信号时，只取回放时间点之前的60m数据（通过end_time限制）

用法:
    python test_kline_playback.py CFFEX.TL2606 "2026-04-02 10:00:00"
"""
import sys
import os
import sqlite3
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from KlineCollector_v2 import (
    DatabaseManager, KlineAggregator, MACDCalculator,
    StackIdentifier, Strategy, MAX_5M_BARS, MAX_60M_BARS
)


def load_5m_upto(db_path: str, symbol: str, end_time: str, limit: int = 2000):
    """
    加载指定截止时间之前的5分钟K线数据（用于预热历史数据）

    参数:
        end_time: 截止时间，在此之前的K线都会被加载
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT datetime, open, high, low, close, volume
        FROM kline_data
        WHERE symbol = ? AND duration = 300 AND datetime <= ?
        ORDER BY datetime DESC
        LIMIT ?
    ''', (symbol, end_time, limit))
    rows = cursor.fetchall()
    conn.close()

    bars = []
    for row in reversed(rows):
        dt_str = row[0] if isinstance(row[0], str) else row[0].strftime('%Y-%m-%d %H:%M:%S')
        bars.append({'datetime': dt_str, 'open': float(row[1]), 'high': float(row[2]),
                     'low': float(row[3]), 'close': float(row[4]), 'volume': int(row[5])})
    return bars


def load_60m_upto(db_path: str, symbol: str, end_time: str, limit: int = 500):
    """
    加载指定截止时间之前的60分钟K线数据（用于回放时验证数据）

    注意：回放时不调用此函数，check_60m_signal_v2 直接从数据库读取
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT datetime, open, high, low, close, volume
        FROM kline_data
        WHERE symbol = ? AND duration = 3600 AND datetime <= ?
        ORDER BY datetime DESC
        LIMIT ?
    ''', (symbol, end_time, limit))
    rows = cursor.fetchall()
    conn.close()

    bars = []
    for row in reversed(rows):
        dt_str = row[0] if isinstance(row[0], str) else row[0].strftime('%Y-%m-%d %H:%M:%S')
        bars.append({'datetime': dt_str, 'open': float(row[1]), 'high': float(row[2]),
                     'low': float(row[3]), 'close': float(row[4]), 'volume': int(row[5])})
    return bars


def is_60m_bar_time(bar_time: str) -> bool:
    """
    判断是否是60分钟K线的时间点

    60分钟K线在每小时的00分完成，如 10:00:00, 11:00:00
    对应 save_kline 中 duration=3600 的情况
    """
    dt = datetime.strptime(bar_time[:19], '%Y-%m-%d %H:%M:%S')
    return dt.minute == 0


def get_previous_60m_time(bar_time: str) -> str:
    """
    获取前一小时的60分钟K线时间

    关键：60分钟信号检查使用的是前一小时完成的60m K线
    例如：在 10:00:00 检查60m信号，使用的是 09:00:00 完成的60m K线

    对应 save_kline 中：
    - 当 duration=3600 时，date_time_str 是当前完成的60m时间
    - 但 check_60m_signal_v2 需要的是"前一小时"的数据
    """
    dt = datetime.strptime(bar_time[:19], '%Y-%m-%d %H:%M:%S')
    prev = dt - timedelta(hours=1)
    prev = prev.replace(minute=0, second=0)
    return prev.strftime('%Y-%m-%d %H:%M:%S')


def test_playback(symbol: str, playback_time: str):
    """
    回放测试：模拟 KlineCollector_v2 的 save_kline 信号检测流程

    对应关系：
    ┌─────────────────────────────────────────────────────────────────────────┐
    │ save_kline 逻辑 (KlineCollector_v2.py line ~688)                     │
    ├─────────────────────────────────────────────────────────────────────────┤
    │ if duration == 300:                                                   │
    │     check_strategy_signal_v2(symbol)  # 每5分钟检查一次5分钟信号      │
    │ else:  # duration == 3600                                            │
    │     check_60m_signal_v2(symbol, end_time=date_time_str)  # 每小时检查│
    └─────────────────────────────────────────────────────────────────────────┘
    """
    db_path = './data/db/kline_data.db'

    print("=" * 70)
    print(f"K线时间回放测试")
    print("=" * 70)
    print(f"合约: {symbol}")
    print(f"回放时间点: {playback_time}")

    # 1. 找到起始5分钟K线（定位回放起点）
    #    相当于实盘启动时，等待第一个5m K线到来
    print("\n步骤1: 查找...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT datetime FROM kline_data
        WHERE symbol = ? AND duration = 300 AND datetime >= ?
        ORDER BY datetime ASC LIMIT 1
    ''', (symbol, playback_time))
    row = cursor.fetchone()
    if not row:
        print("  未找到数据")
        conn.close()
        return (0, 0)
    start_bar_time = row[0]
    print(f"  起始5m: {start_bar_time}")
    conn.close()

    # 2. 加载数据（预热历史数据）
    #    实盘启动时，需要先加载历史K线用于计算MACD等指标
    print("\n步骤2: 加载数据...")
    all_5m = load_5m_upto(db_path, symbol, "2099-12-31", MAX_5M_BARS)
    print(f"  5m: {len(all_5m)} 根")

    if len(all_5m) < 20:
        print("  数据不足")
        return (0, 0)

    # 3. 初始化（与 KlineCollector 启动时一致）
    print("\n步骤3: 初始化...")
    instrument_name = symbol.split('.')[-1]
    instruments = [{"InstrumentID": instrument_name, "ExchangeID": symbol.split('.')[0],
                   "MainContractID": instrument_name}]

    db_manager = DatabaseManager(db_path=db_path, use_online=True)
    db_manager.init_database()

    class MockSignalManager:
        def __init__(self):
            self.signals = []
        def add_signal(self, symbol, signal_data):
            self.signals.append(signal_data)
            print(f"\n  >>> 信号: {signal_data['signal_type']} @ {signal_data['time']}")
            print(f"      价格:{signal_data['price']} 止损:{signal_data.get('stop_loss','N/A')}")
            print(f"      原因:{signal_data['reason']}")

    signal_manager = MockSignalManager()
    aggregator = KlineAggregator(db_manager, instruments, signal_manager)
    aggregator.instrument_map = {instrument_name: instruments[0]}

    # 4. 回放（核心逻辑）
    print("\n步骤4: 回放...")
    print("-" * 50)

    # 找到起始索引（定位到回放时间点）
    # 支持只有日期的时间格式（如 "2026-04-02"）或完整时间（如 "2026-04-02 10:00:00"）
    start_idx = None
    playback_dt_str = playback_time[:19]  # 取前19字符
    # 补全时间为 00:00:00
    if len(playback_time) <= 10:
        playback_dt_str = playback_time + " 00:00:00"
    playback_dt = datetime.strptime(playback_dt_str, '%Y-%m-%d %H:%M:%S')
    for i, bar in enumerate(all_5m):
        bar_dt = datetime.strptime(bar['datetime'][:19], '%Y-%m-%d %H:%M:%S')
        if bar_dt >= playback_dt:
            start_idx = i
            break

    if start_idx is None:
        start_idx = len(all_5m) - 1

    print(f"从第{start_idx}根 ({all_5m[start_idx]['datetime']})")
    print(f"共{len(all_5m) - start_idx}根需要回放")

    # ========== 核心回放循环 ==========
    for i in range(start_idx, min(start_idx + 200, len(all_5m))):
        bar = all_5m[i]
        bar_time = bar['datetime'][:19]  # 当前5m K线时间

        # 设置当前时间（模拟实盘时间推进）
        current_time = datetime.strptime(bar_time, '%Y-%m-%d %H:%M:%S')
        aggregator.current_time = current_time

        # 【对应 save_kline: if duration == 300】
        # 每根5分钟K线都检查5分钟信号
        # 实盘：每次收到新的5m K线（duration=300），调用 check_strategy_signal_v2
        #
        # 关键修复：传入当前 bar 时间作为 end_time
        # 原因：check_strategy_signal_v2 默认会用预检测信号时间限制60m数据范围
        # 这会导致回放时使用旧数据，无法看到最新的60m状态
        # 传入当前 bar 时间可以确保使用到回放时间点的最新数据
        bar_time_full = bar['datetime']
        aggregator.check_strategy_signal_v2(symbol, end_time=bar_time_full)

        # 【对应 save_kline: if duration == 3600】
        # 只有在60分钟K线完成时才检查60分钟信号
        # 60分钟K线完成的时间点是分钟=0（如 10:00, 11:00）
        if is_60m_bar_time(bar_time):
            # ================================================================
            # 关键修改：使用当前整点时间作为 end_time，而非前一小时
            #
            # 原因：
            # - 实盘时：60m K线完成后，MACD 指标已经基于这根完整的60m计算好了
            # - 回测时：回测到 11:00 时间点，会加载包含 11:00 的 60m 数据
            # - 原回放：使用 prev_60m_time (10:00) 导致看不到 11:00 的 MACD 状态
            #
            # 示例：
            # - 在 11:00 整点：加载 end_time='2026-04-03 11:00:00' 的 60m 数据
            #   这时 60m 的 hist 可能已经从 -4.44 变成 -2.51（DIF 拐头）
            # - 在 10:00 整点：加载 end_time='2026-04-03 10:00:00' 的 60m 数据
            #   这时 60m 的 hist 是 -4.44（DIF 还在下跌，未拐头）
            # ================================================================
            current_60m_time = bar_time  # 当前整点时间，如 11:00:00
            # 补全格式以便字符串比较正确
            current_60m_time_full = current_60m_time + '.000000'
            print(f"\n[60m信号] {bar_time} -> 使用60m: {current_60m_time_full}")

            # 调用60分钟信号检查，传入当前整点时间作为截止时间
            # 这样可以计算包含当前整点60m K线在内的 MACD 指标
            aggregator.check_60m_signal_v2(symbol, end_time=current_60m_time_full)
            precheck = aggregator.precheck_signals_green.get(symbol, [])
            print(f"  预检测信号: {len(precheck)} 个")

        # 打印进度
        if (i - start_idx) % 20 == 0:
            print(f"  回放 {i - start_idx + 1} 根...")
    # ========== 回放循环结束 ==========

    # 5. 结果
    print("\n" + "=" * 70)
    print("结果")
    print("=" * 70)

    entry_signals = [s for s in signal_manager.signals if s.get('signal_type') == 'ENTRY_LONG']
    exit_signals = [s for s in signal_manager.signals if s.get('signal_type') == 'EXIT_LONG']

    print(f"入场: {len(entry_signals)} 个, 平仓: {len(exit_signals)} 个")

    precheck = aggregator.precheck_signals_green.get(symbol, [])
    print(f"预检测: {len(precheck)} 个")
    for s in precheck:
        print(f"  - {s['created_time']}")

    db_manager.close()

    # 返回信号数量，供批量回放汇总
    return len(entry_signals), len(exit_signals)


if __name__ == '__main__':
    import argparse
    import json

    parser = argparse.ArgumentParser(description='K线时间回放测试')
    parser.add_argument('symbol', nargs='?', default=None, help='合约代码，如 DCE.a2605')
    parser.add_argument('datetime', nargs='?', default='2026-04-02 10:00:00', help='回放时间点')
    parser.add_argument('--date', '-d', type=str, default=None, help='指定日期，回放所有合约在这一天的信号')
    args = parser.parse_args()

    if args.date:
        # 指定日期，回放所有合约
        json_file = "./data/contracts/main_contracts.json"
        with open(json_file, 'r', encoding='utf-8') as f:
            contracts = json.load(f)

        print(f"=" * 70)
        print(f"批量回放：{args.date} 所有合约")
        print(f"=" * 70)

        total_entry = 0
        total_exit = 0

        for i, inst in enumerate(contracts):
            exchange = inst.get('ExchangeID', '')
            main_contract = inst.get('MainContractID', '')
            if not exchange or not main_contract:
                continue

            symbol = f"{exchange}.{main_contract}"
            print(f"\n[{i+1}/{len(contracts)}] {symbol}", flush=True)

            # 调用 test_playback
            result = test_playback(symbol, args.date)
            if result:
                entry_count, exit_count = result
                total_entry += entry_count
                total_exit += exit_count

        print(f"\n" + "=" * 70)
        print(f"汇总：入场 {total_entry} 个，平仓 {total_exit} 个")
        print(f"=" * 70)
    else:
        # 单个合约回放
        symbol = args.symbol or 'CFFEX.TL2606'
        test_playback(symbol, args.datetime)