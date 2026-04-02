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
from backtest.strategy_indicators import MACDCalculator, StackIdentifier, ATRCalculator


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


def load_60m_bars(db_path: str, symbol: str, limit: int = None):
    """从数据库加载 60 分钟 K 线"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    if limit:
        query = f"""SELECT datetime, open, high, low, close, volume
                    FROM (
                        SELECT datetime, open, high, low, close, volume
                        FROM kline_data
                        WHERE symbol = ? AND duration = 3600
                        ORDER BY datetime DESC
                        LIMIT {limit}
                    ) sub
                    ORDER BY datetime ASC"""
    else:
        query = """SELECT datetime, open, high, low, close, volume
                   FROM kline_data
                   WHERE symbol = ? AND duration = 3600
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

    # 加载 60 分钟数据
    all_60m = load_60m_bars(db_path, symbol, limit=limit)
    print(f"共加载 {len(all_60m)} 根 60 分钟 K 线（{all_60m[0][0]} ~ {all_60m[-1][0]}）")

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

    # 预热数据 = 模拟开始位置之前的 K 线（最多 1000 根）
    warmup_5m = all_5m[max(0, sim_start_idx - 1000):sim_start_idx]
    # 模拟数据 = start_time 到 end_time 之间的 K 线
    sim_5m = all_5m[sim_start_idx:sim_end_idx]

    # 预热数据对应的 60m 数据（基于时间范围）
    warmup_60m = [bar for bar in all_60m if bar[0] <= warmup_5m[-1][0]] if warmup_5m and all_60m else []
    # 模拟期间的 60m 数据
    sim_60m = [bar for bar in all_60m if sim_5m[0][0] <= bar[0] <= sim_5m[-1][0]] if sim_5m and all_60m else []

    print(f"\n预热: {len(warmup_5m)} 根 5m K 线，{len(warmup_60m)} 根 60m K 线")
    print(f"模拟: {len(sim_5m)} 根 5m K 线（{sim_5m[0][0] if sim_5m else 'N/A'} ~ {sim_5m[-1][0] if sim_5m else 'N/A'}）")
    print(f"      {len(sim_60m)} 根 60m K 线")

    if not sim_5m:
        return

    # 初始化引擎
    config = Config()
    config.DB_PATH = db_path
    config.CONTRACTS_PATH = contracts_path

    engine = LiveStrategyEngine(symbol, config)

    # 预填充预热数据（需要经过 MACD 和 ATR 计算）
    engine.df_5m = list(warmup_5m)
    engine.df_5m_with_macd, engine.green_stacks_5m, engine.green_gaps_5m = StackIdentifier.identify(
        MACDCalculator.calculate(warmup_5m)
    )
    engine.df_5m_with_atr = ATRCalculator.calculate(warmup_5m, period=14)
    engine.df_60m = list(warmup_60m)
    engine.df_60m_with_macd, engine.green_stacks_60m, engine.green_gaps_60m = StackIdentifier.identify(
        MACDCalculator.calculate(warmup_60m)
    ) if warmup_60m else ([], {}, {})
    engine.last_60m_bar_time = warmup_60m[-1][0] if warmup_60m else None
    engine.position = None
    engine.signals = []
    engine.last_entry_time = None
    engine.last_signal_60m_idx = None
    engine.precheck_signals_green = []
    engine.precheck_signals_red = []

    entry_signals = []
    exit_signals = []

    # 构建 60m bar 索引（按时间排序），用于在模拟过程中触发
    next_60m_idx = 0
    pending_60m_bars = sorted(sim_60m, key=lambda x: x[0]) if sim_60m else []

    print("\n开始模拟...")
    print("-" * 70)

    # 遍历每根 5m bar，调用统一的 API（与 KlineCollector 一致）
    for bar in sim_5m:
        bar_time = bar[0]

        # 检查是否有 60m bar 需要在当前 5m bar 之前处理
        while next_60m_idx < len(pending_60m_bars) and pending_60m_bars[next_60m_idx][0] <= bar_time:
            bar_60m = pending_60m_bars[next_60m_idx]
            engine.on_60m_bar(bar_60m)
            next_60m_idx += 1

        # 调用与实盘一致的 API
        engine.on_5m_bar(bar)

        # 获取生成的信号
        signals = engine.get_signals(clear=True)

        for signal in signals:
            if signal.signal_type == SignalType.ENTRY_LONG:
                entry_signals.append({
                    'symbol': symbol,
                    'time': signal.time,
                    'price': signal.price,
                    'stop_loss': signal.stop_loss,
                    'reason': signal.reason
                })
                print(f"[ENTRY] {signal.time} | 价格:{signal.price} | 止损:{signal.stop_loss} | {signal.reason}")

            elif signal.signal_type == SignalType.EXIT_LONG:
                exit_signals.append({
                    'symbol': symbol,
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
    """模拟所有合约最近 N 天的数据"""
    import json
    from datetime import datetime, timedelta

    with open(contracts_path, 'r', encoding='utf-8') as f:
        contracts = json.load(f)

    # 过滤可交易的合约
    trading_contracts = [c for c in contracts if c.get('IsTrading', 0) == 1]

    # 计算结束时间（今天）和开始时间（今天 - days 天）
    end_time = datetime.now().strftime('%Y-%m-%d')
    start_time = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    print(f"开始模拟 {len(trading_contracts)} 个合约（最近 {days} 天：{start_time} ~ {end_time}）...")

    all_entry_signals = []
    all_exit_signals = []

    for contract in trading_contracts:
        product_id = contract['ProductID']
        exchange_id = contract['ExchangeID']
        main_contract = contract['MainContractID']

        symbol = f"{exchange_id}.{main_contract}"

        # 加载足够的预热数据（用于计算 MACD 指标）
        # 每天约 288 根 5 分钟 K 线，预热需要至少 500 根以上才能有足够的 MACD 数据
        limit = 2000  # 固定加载 2000 根用于预热

        try:
            entry, exit_ = simulate(symbol, db_path, contracts_path,
                                    start_time=start_time, end_time=end_time,
                                    limit=limit)
            if entry:
                all_entry_signals.extend(entry)
            if exit_:
                all_exit_signals.extend(exit_)
        except Exception as e:
            print(f"模拟失败 {symbol}: {e}")

    print("\n" + "=" * 70)
    print(f"全部合约模拟完成（最近 {days} 天）")
    print(f"总入场信号：{len(all_entry_signals)} 个")
    print(f"总平仓信号：{len(all_exit_signals)} 个")

    if all_entry_signals:
        print(f"\n入场信号汇总（按时间排序）：")
        sorted_entries = sorted(all_entry_signals, key=lambda x: x['time'])
        for s in sorted_entries[:20]:  # 只显示前20个
            print(f"  [{s['time']}] {s.get('symbol', 'N/A')} 价格:{s['price']} 止损:{s['stop_loss']} {s['reason']}")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='K线信号模拟器 V2')
    parser.add_argument('symbol', nargs='?', default=None, help='合约代码，不指定则模拟所有合约')
    parser.add_argument('start_time', nargs='?', default=None, help='模拟起始时间 YYYY-MM-DD HH:MM:SS')
    parser.add_argument('end_time', nargs='?', default=None, help='模拟结束时间 YYYY-MM-DD HH:MM:SS')
    parser.add_argument('--limit', '-l', type=int, default=10000, help='最多加载的5分钟K线数量，默认10000')
    parser.add_argument('--db', '-d', type=str, default=None, help='数据库路径')
    parser.add_argument('--contracts', '-c', type=str, default=None, help='合约配置文件路径')
    parser.add_argument('--all', '-a', action='store_true', help='模拟所有合约最近 N 天')
    parser.add_argument('--days', type=int, default=7, help='--all 时模拟最近天数，默认 7 天')

    args = parser.parse_args()

    config = Config()
    db_path = args.db or config.DB_PATH
    contracts_path = args.contracts or config.CONTRACTS_PATH

    if args.all:
        simulate_all_contracts(db_path, contracts_path, days=args.days)
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
