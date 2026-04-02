#!/usr/bin/env python3
"""
KlineCollector 信号检查模拟器

模拟实盘时逐根 5m bar 检测信号的完整流程。
逻辑和 backtest/strategy_backtest.py 的主循环一致。
"""
import sys
import os
import sqlite3
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backtest'))
from backtest.strategy_engine import LiveStrategyEngine
from backtest.strategy_utils import Config
from backtest.strategy_models import SignalType
from backtest.strategy_indicators import MACDCalculator, StackIdentifier, ATRCalculator, IndexMapper


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


def load_60m_bars(db_path: str, symbol: str):
    """从数据库加载 60 分钟 K 线"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""SELECT datetime, open, high, low, close, volume
                      FROM kline_data
                      WHERE symbol = ? AND duration = 3600
                      ORDER BY datetime ASC""", [symbol])
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
    print(f"信号模拟：{symbol}")
    print("=" * 70)

    # 加载数据
    all_5m = load_5m_bars(db_path, symbol, limit=limit)
    all_60m = load_60m_bars(db_path, symbol)

    if not all_5m or not all_60m:
        print(f"数据不足")
        return

    print(f"共加载 {len(all_5m)} 根 5 分钟 K 线（{all_5m[0][0]} ~ {all_5m[-1][0]}）")
    print(f"共加载 {len(all_60m)} 根 60 分钟 K 线（{all_60m[0][0]} ~ {all_60m[-1][0]}）")

    # 计算 MACD
    df_5m_macd = MACDCalculator.calculate(all_5m)
    df_5m_with_macd, green_stacks_5m, green_gaps_5m = StackIdentifier.identify(df_5m_macd)
    df_5m_with_atr = ATRCalculator.calculate(df_5m_macd, period=14)

    df_60m_macd = MACDCalculator.calculate(all_60m)
    df_60m_with_macd, green_stacks_60m, green_gaps_60m = StackIdentifier.identify(df_60m_macd)

    # 建立 5m 到 60m 的索引映射
    index_map = IndexMapper.precompute_60m_index(df_5m_with_macd, df_60m_with_macd)

    # 确定模拟区间
    if start_time:
        sim_start_idx = 0
        for i, bar in enumerate(df_5m_with_macd):
            if bar[0] >= start_time:
                sim_start_idx = i
                break
    else:
        sim_start_idx = len(df_5m_with_macd) // 2

    if end_time:
        end_extended = end_time + ' 23:59:59' if ' ' not in end_time else end_time
        sim_end_idx = len(df_5m_with_macd)
        for i, bar in enumerate(df_5m_with_macd):
            if bar[0] > end_extended:
                sim_end_idx = i
                break
    else:
        sim_end_idx = len(df_5m_with_macd)

    warmup_5m = df_5m_with_macd[:sim_start_idx]
    warmup_5m_atr = df_5m_with_atr[:sim_start_idx] if df_5m_with_atr else []
    sim_5m = df_5m_with_macd[sim_start_idx:sim_end_idx]
    sim_5m_atr = df_5m_with_atr[sim_start_idx:sim_end_idx] if df_5m_with_atr else []
    sim_index_map = index_map[sim_start_idx:sim_end_idx]

    print(f"\n预热: {len(warmup_5m)} 根 5m K 线")
    print(f"模拟: {len(sim_5m)} 根 5m K 线（{sim_5m[0][0] if sim_5m else 'N/A'} ~ {sim_5m[-1][0] if sim_5m else 'N/A'}）")

    if not sim_5m:
        return

    # 初始化引擎
    config = Config()
    config.DB_PATH = db_path
    config.CONTRACTS_PATH = contracts_path

    engine = LiveStrategyEngine(symbol, config)

    # 预填充数据
    engine.df_5m = list(warmup_5m)
    engine.df_5m_with_macd = list(warmup_5m)
    engine.df_5m_with_atr = list(warmup_5m_atr) if warmup_5m_atr else []
    engine.green_stacks_5m = dict(green_stacks_5m)
    engine.green_gaps_5m = dict(green_gaps_5m)
    engine.df_60m = list(df_60m_with_macd)
    engine.df_60m_with_macd = list(df_60m_with_macd)
    engine.green_stacks_60m = dict(green_stacks_60m)
    engine.green_gaps_60m = dict(green_gaps_60m)
    engine._current_60m_bar = None
    engine.last_60m_bar_time = df_60m_with_macd[-1][0] if df_60m_with_macd else None
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

    for i, (bar_5m, bar_5m_atr) in enumerate(zip(sim_5m, sim_5m_atr)):
        current_time = bar_5m[0][:19]
        current_dt = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')
        idx_60m = sim_index_map[i] if i < len(sim_index_map) else len(df_60m_with_macd) - 1

        # 冷却期检查
        if engine.last_entry_time:
            hours_passed = (current_dt - engine.last_entry_time).total_seconds() / 3600
            if hours_passed < config.COOLDOWN_HOURS:
                continue

        # ========== 预检测信号创建（每根 5m bar 都检查） ==========
        if idx_60m >= 4:
            hist_60m = df_60m_with_macd[idx_60m][8]
            hist_60m_prev = df_60m_with_macd[idx_60m - 1][8]

            # 绿柱堆内 DIF 拐头
            if hist_60m < 0:
                dif_turn, _ = engine.strategy.check_60m_dif_turn_in_green(
                    df_60m_with_macd, idx_60m, green_stacks_60m
                )
                if dif_turn:
                    diver_ok, diver_reason, curr_low, prev_low = engine.strategy.check_60m_divergence(
                        df_60m_with_macd, idx_60m
                    )
                    if diver_ok:
                        existing = next((s for s in engine.precheck_signals_green
                                       if s['created_time'] == bar_5m[0][:19]), None)
                        if not existing:
                            engine.precheck_signals_green.append({
                                'type': 'green',
                                'created_time': bar_5m[0][:19],
                                'expiry_time': bar_5m[0][:19],
                            })

            # 绿柱堆转红柱堆（hist 从负转正）
            elif hist_60m > 0 and hist_60m_prev < 0:
                diver_ok, diver_reason, curr_low, prev_low = engine.strategy.check_60m_divergence(
                    df_60m_with_macd, idx_60m
                )
                if diver_ok:
                    existing = next((s for s in engine.precheck_signals_green
                                   if s['created_time'] == bar_5m[0][:19]), None)
                    if not existing:
                        engine.precheck_signals_green.append({
                            'type': 'green',
                            'created_time': bar_5m[0][:19],
                            'expiry_time': bar_5m[0][:19],
                        })

            # 红柱堆内 DIF 拐头
            elif hist_60m > 0:
                dif_turn_red, _ = engine.strategy.check_60m_dif_turn_in_red(
                    df_60m_with_macd, idx_60m
                )
                if dif_turn_red:
                    diver_ok, diver_reason, curr_low_60m, prev_prev_low_60m = \
                        engine.strategy.check_60m_bottom_rise_in_red(
                            df_60m_with_macd, idx_60m
                        )
                    if diver_ok:
                        existing = next((s for s in engine.precheck_signals_red
                                       if s['created_time'] == bar_5m[0][:19]), None)
                        if not existing:
                            engine.precheck_signals_red.append({
                                'type': 'red',
                                'created_time': bar_5m[0][:19],
                                'expiry_time': bar_5m[0][:19],
                            })

        # ========== 追加 5m bar 到 engine ==========
        engine.df_5m.append(bar_5m)
        engine.df_5m_with_macd.append(bar_5m)
        engine.df_5m_with_atr.append(bar_5m_atr)
        if len(engine.df_5m) > config.MAX_5M_BARS:
            engine.df_5m.pop(0)
        if len(engine.df_5m_with_macd) > config.MAX_5M_BARS:
            engine.df_5m_with_macd.pop(0)
        if len(engine.df_5m_with_atr) > config.MAX_5M_BARS:
            engine.df_5m_with_atr.pop(0)

        # ========== 预检测信号确认（5m 阳柱确认） ==========
        if engine.position is None:
            # 过滤过期信号（30分钟）
            engine.precheck_signals_green = [
                s for s in engine.precheck_signals_green
                if datetime.strptime(s['expiry_time'], '%Y-%m-%d %H:%M:%S') + timedelta(minutes=30) > current_dt
            ]
            engine.precheck_signals_red = [
                s for s in engine.precheck_signals_red
                if datetime.strptime(s['expiry_time'], '%Y-%m-%d %H:%M:%S') + timedelta(minutes=30) > current_dt
            ]

            all_signals = engine.precheck_signals_green + engine.precheck_signals_red

            for signal in all_signals[:]:
                signal_type = signal.get('type', 'unknown')

                # 绿柱用 divergence，红柱用 bottom_rise_in_red
                if signal_type == 'green':
                    diver_ok, diver_reason, _, _ = engine.strategy.check_60m_divergence(
                        df_60m_with_macd, idx_60m
                    )
                    signal_source = "绿柱堆内 DIF 拐头"
                else:
                    diver_ok, diver_reason, curr_low_60m, prev_prev_low_60m = engine.strategy.check_60m_bottom_rise_in_red(
                        df_60m_with_macd, idx_60m
                    )
                    signal_source = "红柱堆内 DIF 拐头"

                if not diver_ok:
                    if signal in engine.precheck_signals_green:
                        engine.precheck_signals_green.remove(signal)
                    if signal in engine.precheck_signals_red:
                        engine.precheck_signals_red.remove(signal)
                    continue

                # 5m 阳柱确认
                cond_5m, reason_5m = engine.strategy.check_5m_entry(
                    engine.df_5m_with_macd, len(engine.df_5m_with_macd) - 1, green_stacks_5m
                )

                if cond_5m:
                    initial_stop_loss, stop_reason = engine.strategy.get_initial_stop_loss(
                        engine.df_5m_with_atr,
                        len(engine.df_5m_with_macd) - 1,
                        green_stacks_5m, green_gaps_5m,
                        df_60m_with_macd, green_stacks_60m
                    )

                    if initial_stop_loss is not None:
                        entry_price = engine.df_5m_with_macd[-1][4]

                        engine._create_entry_signal(
                            entry_price, initial_stop_loss, stop_reason,
                            f"{diver_reason} + {reason_5m}",
                            current_time, signal_source, idx_60m
                        )

                        entry_signals.append({
                            'time': current_time,
                            'price': entry_price,
                            'stop_loss': initial_stop_loss,
                            'reason': f"{diver_reason} + {reason_5m}",
                            'source': signal_source
                        })
                        print(f"[ENTRY] {current_time} | 价格:{entry_price} | 止损:{initial_stop_loss} | {diver_reason} + {reason_5m}")
                        print(f"       来源:{signal_source}")

                        if signal in engine.precheck_signals_green:
                            engine.precheck_signals_green.remove(signal)
                        if signal in engine.precheck_signals_red:
                            engine.precheck_signals_red.remove(signal)
                        break

        # ========== 止损检查 ==========
        if engine.position is not None:
            current_low = bar_5m[2]

            if current_low <= engine.position.current_stop:
                exit_price = engine.position.current_stop
                exit_reason = f"止损触发 ({engine.position.stop_reason})"
                engine._create_exit_signal(exit_price, exit_reason, current_time)
                exit_signals.append({
                    'time': current_time,
                    'price': exit_price,
                    'reason': exit_reason
                })
                print(f"[EXIT] {current_time} | 价格:{exit_price} | {exit_reason}")
                continue

            # 更新移动止损
            mobile_stop, stop_reason = engine.strategy.get_mobile_stop(
                engine.df_5m_with_macd, len(engine.df_5m_with_macd) - 1,
                green_stacks_5m, green_gaps_5m
            )
            if mobile_stop and mobile_stop > engine.position.current_stop:
                engine.position.current_stop = mobile_stop
                engine.position.stop_reason = stop_reason

    print("-" * 70)
    print(f"\n模拟完成")
    print(f"入场信号：{len(entry_signals)} 个")
    print(f"平仓信号：{len(exit_signals)} 个")

    if entry_signals:
        print(f"\n入场汇总：")
        for s in entry_signals:
            print(f"  [{s['time']}] 价格:{s['price']} 止损:{s['stop_loss']} {s['reason']} | 来源:{s['source']}")


def simulate_all_contracts(db_path: str, contracts_path: str, days: int = 1):
    """对所有活跃合约运行最近N天的信号模拟

    Args:
        db_path: 数据库路径
        contracts_path: 合约配置文件路径
        days: 模拟最近几天，默认为1天
    """
    from backtest.strategy_utils import DataLoader

    loader = DataLoader(db_path, contracts_path)
    contracts = loader.load_main_contracts()

    # 找到所有合约的最近5m数据日期
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""SELECT symbol, MAX(datetime) as last_dt
                      FROM kline_data
                      WHERE duration = 300
                      GROUP BY symbol""")
    symbol_last_dates = {row[0]: row[1] for row in cursor.fetchall()}
    conn.close()

    # 找出最近的数据日期
    latest_date = max(dt for dt in symbol_last_dates.values()) if symbol_last_dates else None
    if not latest_date:
        print("没有找到5分钟K线数据")
        return

    latest_date = latest_date[:10]  # 取日期部分 YYYY-MM-DD
    print(f"最近数据日期：{latest_date}")
    print(f"=" * 70)

    all_results = []

    for product_id, contract in contracts.items():
        exchange = contract.get('ExchangeID', '')
        symbol = f"{exchange}.{contract['MainContractID']}"

        if symbol not in symbol_last_dates:
            continue

        last_dt = symbol_last_dates[symbol][:19]  # YYYY-MM-DD HH:MM:SS
        last_date = last_dt[:10]

        # 只模拟最近days天的数据
        if last_date < latest_date:
            continue

        print(f"\n{'=' * 60}")
        print(f"模拟合约：{symbol}")
        print(f"{'=' * 60}")

        # 模拟最近days天
        start_date = (datetime.strptime(latest_date, '%Y-%m-%d') - timedelta(days=days - 1)).strftime('%Y-%m-%d')

        try:
            simulate(
                symbol=symbol,
                db_path=db_path,
                contracts_path=contracts_path,
                start_time=start_date,
                end_time=latest_date,
                limit=10000
            )
        except Exception as e:
            print(f"模拟出错：{e}")

    print(f"\n{'=' * 70}")
    print("全部合约模拟完成")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='K线Collector信号模拟器')
    parser.add_argument('symbol', nargs='?', default=None, help='合约代码，不指定则模拟所有合约')
    parser.add_argument('start_time', nargs='?', default=None, help='模拟起始时间 YYYY-MM-DD HH:MM:SS')
    parser.add_argument('end_time', nargs='?', default=None, help='模拟结束时间 YYYY-MM-DD HH:MM:SS')
    parser.add_argument('--limit', '-l', type=int, default=10000, help='最多加载的5分钟K线数量，默认10000')
    parser.add_argument('--db', '-d', type=str, default=None, help='数据库路径')
    parser.add_argument('--contracts', '-c', type=str, default=None, help='合约配置文件路径')
    parser.add_argument('--all', '-a', action='store_true', help='模拟所有合约最近一天')

    args = parser.parse_args()

    config = Config()
    db_path = args.db or config.DB_PATH
    contracts_path = args.contracts or config.CONTRACTS_PATH

    print(f"数据库：{db_path}")
    print(f"合约配置：{contracts_path}")

    if args.all:
        # 模拟所有合约最近7天，确保有足够数据触发信号
        simulate_all_contracts(db_path, contracts_path, days=2)
    elif args.symbol:
        # 模拟指定合约
        simulate(
            symbol=args.symbol,
            db_path=db_path,
            contracts_path=contracts_path,
            start_time=args.start_time,
            end_time=args.end_time,
            limit=args.limit
        )