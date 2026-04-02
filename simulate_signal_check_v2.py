#!/usr/bin/env python3
"""
KlineCollector 信号检查模拟器 V2

使用与实盘一致的 API：engine.on_5m_bar()
模拟实盘时逐根 5m bar 检测信号的完整流程。
"""
import sys
import os
import sqlite3
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backtest'))
from backtest.strategy_engine import LiveStrategyEngine
from backtest.strategy_utils import Config
from backtest.strategy_models import SignalType


def load_5m_bars(db_path: str, symbol: str, limit: int = None):
    """从数据库加载 5 分钟 K 线"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    if limit:
        query = f"""SELECT datetime, open, high, low, close, volume
                    FROM (
                        SELECT datetime, open, high, low, close, volume
                        FROM kline_data
                        WHERE symbol = ? AND duration = 300
                        ORDER BY datetime DESC
                        LIMIT {limit}
                    ) sub
                    ORDER BY datetime ASC"""
    else:
        query = """SELECT datetime, open, high, low, close, volume
                   FROM kline_data
                   WHERE symbol = ? AND duration = 300
                   ORDER BY datetime ASC"""

    cursor.execute(query, [symbol])
    rows = cursor.fetchall()
    conn.close()

    bars = []
    for row in rows:
        dt_str = row[0] if isinstance(row[0], str) else row[0].strftime('%Y-%m-%d %H:%M:%S')
        bars.append((dt_str, float(row[1]), float(row[2]), float(row[3]), float(row[4]), int(row[5])))
    return bars


def simulate(symbol: str, db_path: str, contracts_path: str,
             start_time: str = None, end_time: str = None,
             limit: int = None):
    print("=" * 70)
    print(f"信号模拟 V2：{symbol}")
    print("=" * 70)

    # 加载 5 分钟数据
    all_5m = load_5m_bars(db_path, symbol, limit=limit)

    if not all_5m:
        print(f"数据不足")
        return

    print(f"共加载 {len(all_5m)} 根 5 分钟 K 线（{all_5m[0][0]} ~ {all_5m[-1][0]}）")

    # 确定模拟区间
    sim_start_idx = 0
    sim_end_idx = len(all_5m)

    if start_time:
        for i, bar in enumerate(all_5m):
            if bar[0] >= start_time:
                sim_start_idx = i
                break

    if end_time:
        end_extended = end_time + ' 23:59:59' if ' ' not in end_time else end_time
        for i, bar in enumerate(all_5m):
            if bar[0] > end_extended:
                sim_end_idx = i
                break
    else:
        # 默认从中间开始模拟
        sim_start_idx = len(all_5m) // 2

    warmup_5m = all_5m[:sim_start_idx]
    sim_5m = all_5m[sim_start_idx:sim_end_idx]

    print(f"\n预热: {len(warmup_5m)} 根 5m K 线")
    print(f"模拟: {len(sim_5m)} 根 5m K 线（{sim_5m[0][0] if sim_5m else 'N/A'} ~ {sim_5m[-1][0] if sim_5m else 'N/A'}）")

    if not sim_5m:
        return

    # 初始化引擎
    config = Config()
    config.DB_PATH = db_path
    config.CONTRACTS_PATH = contracts_path

    engine = LiveStrategyEngine(symbol, config)

    # 预填充预热数据
    engine.df_5m = list(warmup_5m)
    engine.df_5m_with_macd = list(warmup_5m)
    engine.df_5m_with_atr = list(warmup_5m)
    engine.green_stacks_5m = {}
    engine.green_gaps_5m = {}
    engine.df_60m = []
    engine.df_60m_with_macd = []
    engine.green_stacks_60m = {}
    engine.green_gaps_60m = {}
    engine._current_60m_bar = None
    engine.last_60m_bar_time = None
    engine.position = None
    engine.signals = []
    engine.last_entry_time = None
    engine.last_signal_60m_idx = None
    engine.precheck_signals_green = []
    engine.precheck_signals_red = []

    entry_signals = []
    exit_signals = []

    print("\n开始模拟...")
    print("-" * 70)

    # 遍历每根 5m bar，调用统一的 API（与 KlineCollector 一致）
    for bar in sim_5m:
        # 调用与实盘一致的 API
        engine.on_5m_bar(bar)

        # 获取生成的信号
        signals = engine.get_signals(clear=True)

        for signal in signals:
            if signal.signal_type == SignalType.ENTRY_LONG:
                entry_signals.append({
                    'time': signal.time,
                    'price': signal.price,
                    'stop_loss': signal.stop_loss,
                    'reason': signal.reason
                })
                print(f"[ENTRY] {signal.time} | 价格:{signal.price} | 止损:{signal.stop_loss} | {signal.reason}")

            elif signal.signal_type == SignalType.EXIT_LONG:
                exit_signals.append({
                    'time': signal.time,
                    'price': signal.price,
                    'reason': signal.reason
                })
                print(f"[EXIT] {signal.time} | 价格:{signal.price} | {signal.reason}")

    print("-" * 70)
    print(f"\n模拟完成")
    print(f"入场信号：{len(entry_signals)} 个")
    print(f"平仓信号：{len(exit_signals)} 个")

    if entry_signals:
        print(f"\n入场汇总：")
        for s in entry_signals:
            print(f"  [{s['time']}] 价格:{s['price']} 止损:{s['stop_loss']} {s['reason']}")

    if exit_signals:
        print(f"\n平仓汇总：")
        for s in exit_signals:
            print(f"  [{s['time']}] 价格:{s['price']} {s['reason']}")

    return entry_signals, exit_signals


def simulate_all_contracts(db_path: str, contracts_path: str, days: int = 1):
    """模拟所有合约"""
    import json

    with open(contracts_path, 'r', encoding='utf-8') as f:
        contracts = json.load(f)

    # 过滤可交易的合约
    trading_contracts = [c for c in contracts if c.get('IsTrading', 0) == 1]

    print(f"开始模拟 {len(trading_contracts)} 个合约...")

    all_entry_signals = []
    all_exit_signals = []

    for contract in trading_contracts[:10]:  # 限制数量
        product_id = contract['ProductID']
        exchange_id = contract['ExchangeID']
        main_contract = contract['MainContractID']

        symbol = f"{exchange_id}.{main_contract}"

        try:
            entry, exit_ = simulate(symbol, db_path, contracts_path, limit=5000)
            if entry:
                all_entry_signals.extend(entry)
            if exit_:
                all_exit_signals.extend(exit_)
        except Exception as e:
            print(f"模拟失败 {symbol}: {e}")

    print("\n" + "=" * 70)
    print("全部合约模拟完成")
    print(f"总入场信号：{len(all_entry_signals)} 个")
    print(f"总平仓信号：{len(all_exit_signals)} 个")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='K线信号模拟器 V2')
    parser.add_argument('symbol', nargs='?', default=None, help='合约代码，不指定则模拟所有合约')
    parser.add_argument('start_time', nargs='?', default=None, help='模拟起始时间 YYYY-MM-DD HH:MM:SS')
    parser.add_argument('end_time', nargs='?', default=None, help='模拟结束时间 YYYY-MM-DD HH:MM:SS')
    parser.add_argument('--limit', '-l', type=int, default=10000, help='最多加载的5分钟K线数量，默认10000')
    parser.add_argument('--db', '-d', type=str, default=None, help='数据库路径')
    parser.add_argument('--contracts', '-c', type=str, default=None, help='合约配置文件路径')
    parser.add_argument('--all', '-a', action='store_true', help='模拟所有合约')

    args = parser.parse_args()

    config = Config()
    db_path = args.db or config.DB_PATH
    contracts_path = args.contracts or config.CONTRACTS_PATH

    if args.all:
        simulate_all_contracts(db_path, contracts_path)
    elif args.symbol:
        simulate(args.symbol, db_path, contracts_path,
                start_time=args.start_time, end_time=args.end_time,
                limit=args.limit)
    else:
        print("用法:")
        print("  python simulate_signal_check_v2.py <symbol> [start_time] [end_time]")
        print("  python simulate_signal_check_v2.py --all")
        print("示例:")
        print("  python simulate_signal_check_v2.py CFFEX.IC2606")
        print("  python simulate_signal_check_v2.py CFFEX.IC2606 '2026-01-01' '2026-03-01'")
        print("  python simulate_signal_check_v2.py --all")
