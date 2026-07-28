#!/usr/bin/env python3
"""
K线时间回放测试 - 与回测逻辑一致

与回测 strategy_backtest.py 的对应关系：
- 同样的遍历方式：遍历每根5m K线，检查60m状态
- 同样的信号判断：直接在同一时间点判断底背离和5m阳柱
- 同样的冷却时间：入场后4小时才能再次入场

用法:
    python test_kline_playback.py CFFEX.TL2606 "2026-04-02 10:00:00"
    python test_kline_playback.py --date 2026-04-17
"""
import sys
import os
import sqlite3
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.strategy_config import Config
from strategy.index_map import IndexMapper
from strategy.macd import MACDCalculator, ATRCalculator
from strategy.stack import StackIdentifier
from strategies.low_low_up.StrategyLowLowUp import StrategyLowLowUp as Strategy


def load_kline_from_db(db_path: str, symbol: str, duration: int, limit: int = None):
    """从数据库加载K线数据（获取最近的limit条，升序返回）"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 与回测逻辑一致：用DESC取最新数据，再反转成ASC
    query = f"""SELECT datetime, open, high, low, close, volume
               FROM kline_data
               WHERE symbol = ? AND duration = ?
               ORDER BY datetime DESC
               LIMIT {limit if limit else 100000}"""

    cursor.execute(query, [symbol, duration])
    rows = cursor.fetchall()
    conn.close()

    # 反转成升序
    return [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows][::-1]


def test_playback(symbol: str, playback_time: str):
    """回放测试 - 与回测逻辑一致"""
    db_path = './data/db/kline_data.db'
    config = Config()

    print("=" * 70)
    print(f"K线时间回放测试（与回测逻辑一致）")
    print("=" * 70)
    print(f"合约: {symbol}")
    print(f"回放时间点: {playback_time}")

    # 解析时间
    playback_dt_str = playback_time[:19]
    if len(playback_time) <= 10:
        playback_dt_str = playback_time + " 00:00:00"
    playback_dt = datetime.strptime(playback_dt_str, '%Y-%m-%d %H:%M:%S')

    # 加载数据
    print("\n加载数据...")
    df_5m_raw = load_kline_from_db(db_path, symbol, 300, config.MAX_5M_BARS)
    df_60m_raw = load_kline_from_db(db_path, symbol, 3600, config.MAX_60M_BARS)

    if not df_5m_raw or not df_60m_raw:
        print("数据不足")
        return (0, 0)

    print(f"  5m: {len(df_5m_raw)} 根, 60m: {len(df_60m_raw)} 根")

    # 计算 MACD 和识别柱堆
    df_5m = MACDCalculator.calculate(df_5m_raw)
    df_5m, green_stacks_5m, green_gaps_5m = StackIdentifier.identify(df_5m)
    df_5m = ATRCalculator.calculate(df_5m, period=14)

    df_60m = MACDCalculator.calculate(df_60m_raw)
    df_60m, green_stacks_60m, green_gaps_60m = StackIdentifier.identify(df_60m)

    # 构建索引映射
    index_map = IndexMapper.precompute_60m_index(df_5m, df_60m)

    # 获取合约信息
    exchange = symbol.split('.')[0]
    instrument_name = symbol.split('.')[-1]
    instrument_info = {
        'ProductID': instrument_name[:2],
        'InstrumentName': instrument_name,
        'ExchangeID': exchange,
        'MainContractID': instrument_name
    }
    strategy = Strategy(instrument_info)

    # 找到回放起始位置（找到date当天最早的数据，向后遍历）
    start_idx = None
    end_idx = None
    playback_date_str = playback_dt.strftime('%Y-%m-%d')

    # 从前往后找，找到date当天最早的数据
    for i in range(len(df_5m_raw)):
        time_str = df_5m_raw[i][0][:10]
        if time_str == playback_date_str:
            start_idx = i
            break

    if start_idx is None:
        print(f"  未找到当天数据")
        return (0, 0)

    # 从start_idx往后找，找到date当天最后的数据
    for i in range(start_idx, len(df_5m_raw)):
        time_str = df_5m_raw[i][0][:10]
        if time_str != playback_date_str:
            end_idx = i
            break

    if end_idx is None:
        end_idx = len(df_5m_raw)

    print(f"  从第{start_idx}根 ({df_5m_raw[start_idx][0][:19]}) 到第{end_idx-1}根 ({df_5m_raw[end_idx-1][0][:19]})")
    print(f"  共{end_idx - start_idx}根K线")

    # 冷却时间
    last_entry_time = None
    entry_signals = []
    date_prefix = playback_dt.strftime('%Y-%m-%d')

    # 遍历5m K线（与回测逻辑一致）
    print("\n遍历K线...")
    for i in range(start_idx, end_idx):
        row_5m = df_5m[i]
        time_str = row_5m[0][:19]

        if not time_str.startswith(date_prefix):
            continue

        idx_60m = index_map[i] if i < len(index_map) else len(df_60m) - 1

        if idx_60m < 4:
            continue

        hist_60m = df_60m[idx_60m][8]
        hist_60m_prev = df_60m[idx_60m - 1][8] if idx_60m > 0 else 0

        # 冷却时间检查
        if last_entry_time is not None:
            entry_dt = datetime.strptime(last_entry_time[:19], '%Y-%m-%d %H:%M:%S')
            current_dt = datetime.strptime(time_str[:19], '%Y-%m-%d %H:%M:%S')
            hours_passed = (current_dt - entry_dt).total_seconds() / 3600
            if hours_passed < config.COOLDOWN_HOURS:
                continue

        # 绿柱堆 DIF 拐头 + 底背离
        if hist_60m < 0:
            dif_turn, _ = strategy.check_60m_dif_turn_in_green(df_60m, idx_60m, green_stacks_60m)
            if dif_turn:
                diver_ok, diver_reason, curr_low, prev_low = strategy.check_60m_divergence(df_60m, idx_60m)
                if diver_ok:
                    cond_5m, reason_5m = strategy.check_5m_entry(df_5m, i, green_stacks_5m)
                    if cond_5m:
                        # 检查过滤条件
                        if strategy.check_60m_all_limits(df_60m, idx_60m):
                            continue
                        should_filter, filter_reason = strategy.is_large_60m_drop(df_60m, row_5m[4], df_5m)
                        if should_filter:
                            continue

                        initial_stop, stop_reason = strategy.get_initial_stop_loss(
                            df_5m, i, green_stacks_5m, green_gaps_5m, df_60m, green_stacks_60m)
                        if initial_stop:
                            signal = {
                                'time': time_str,
                                'type': '绿柱堆信号',
                                'price': row_5m[4],
                                'stop_loss': initial_stop,
                                'reason': f"{diver_reason} + {reason_5m}",
                            }
                            entry_signals.append(signal)
                            print(f"  >>> [{time_str}] 绿柱堆信号 @ {row_5m[4]} | 止损:{initial_stop}")
                            last_entry_time = time_str

        # 红柱转绿柱信号（hist从负变正）
        if hist_60m > 0 and hist_60m_prev < 0:
            diver_ok, diver_reason, curr_low, prev_low = strategy.check_60m_divergence(df_60m, idx_60m)
            if diver_ok:
                cond_5m, reason_5m = strategy.check_5m_entry(df_5m, i, green_stacks_5m)
                if cond_5m:
                    initial_stop, _ = strategy.get_initial_stop_loss(
                        df_5m, i, green_stacks_5m, green_gaps_5m, df_60m, green_stacks_60m)
                    if initial_stop:
                        print(f"  >>> [{time_str}] 绿柱转红柱信号 @ {row_5m[4]} | 止损:{initial_stop}")
                        last_entry_time = time_str

        # 红柱堆 DIF 拐头 + 底背离
        if hist_60m > 0 and hist_60m_prev > 0:
            dif_turn_red, _ = strategy.check_60m_dif_turn_in_red(df_60m, idx_60m)
            if dif_turn_red:
                diver_ok, diver_reason, curr_low, prev_low = strategy.check_60m_bottom_rise_in_red(df_60m, idx_60m)
                if diver_ok:
                    cond_5m, reason_5m = strategy.check_5m_entry(df_5m, i, green_stacks_5m)
                    if cond_5m:
                        cond_filter, reason_filter = strategy.check_5m_green_stack_filter(df_5m, i, green_stacks_5m)
                        should_filter, filter_reason = strategy.is_large_60m_drop(df_60m, row_5m[4], df_5m)
                        if should_filter:
                            break
                        if cond_filter:
                            initial_stop, stop_reason = strategy.get_initial_stop_loss(
                                df_5m, i, green_stacks_5m, green_gaps_5m, df_60m, green_stacks_60m)
                            if initial_stop:
                                signal = {
                                    'time': time_str,
                                    'type': '红柱堆信号',
                                    'price': row_5m[4],
                                    'stop_loss': initial_stop,
                                    'reason': f"{diver_reason} + {reason_filter} + {reason_5m}",
                                }
                                entry_signals.append(signal)
                                print(f"  >>> [{time_str}] 红柱堆信号 @ {row_5m[4]} | 止损:{initial_stop}")
                                last_entry_time = time_str

    # 结果
    print("\n" + "=" * 70)
    print("结果")
    print("=" * 70)
    print(f"入场: {len(entry_signals)} 个")

    return len(entry_signals), 0


if __name__ == '__main__':
    import argparse
    import json

    parser = argparse.ArgumentParser(description='K线时间回放测试')
    parser.add_argument('symbol', nargs='?', default=None, help='合约代码，如 DCE.a2605')
    parser.add_argument('datetime', nargs='?', default='2026-04-02 10:00:00', help='回放时间点')
    parser.add_argument('--date', '-d', type=str, default=None, help='指定日期，回放所有合约在这一天的信号')
    args = parser.parse_args()

    if args.date:
        json_file = "./data/contracts/main_contracts.json"
        with open(json_file, 'r', encoding='utf-8') as f:
            contracts = json.load(f)

        print("=" * 70)
        print(f"批量回放：{args.date} 所有合约")
        print("=" * 70)

        total_entry = 0

        for i, inst in enumerate(contracts):
            exchange = inst.get('ExchangeID', '')
            main_contract = inst.get('MainContractID', '')
            if not exchange or not main_contract:
                continue

            symbol = f"{exchange}.{main_contract}"
            print(f"\n[{i+1}/{len(contracts)}] {symbol}", flush=True)

            result = test_playback(symbol, args.date)
            if result:
                entry_count, _ = result
                total_entry += entry_count

        print(f"\n" + "=" * 70)
        print(f"汇总：入场 {total_entry} 个")
        print(f"=" * 70)
    else:
        symbol = args.symbol or 'CFFEX.TL2606'
        test_playback(symbol, args.datetime)