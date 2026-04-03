#!/usr/bin/env python3
"""
策略回测代码
"""

import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(__file__))
# 添加项目根目录到路径，以便从 strategy 模块导入
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# 优先使用 backtest 目录中的本地文件
from utils.strategy_config import Config, DataLoader
# from strategy_models import Trade, SignalType
from utils.strategy_models import Signal, Position, Trade, SignalType
# from strategy_indicators import MACDCalculator, StackIdentifier, IndexMapper, ATRCalculator
from strategy.index_map import IndexMapper
from strategy.macd import MACDCalculator, ATRCalculator
from strategy.stack import StackIdentifier
from strategy_logic import Strategy


# ============== 回测某一天信号功能 ==============

def backtest_date_signals(date_str: str = None, db_path: str = None, contracts_path: str = None):
    """回测某一天的所有信号"""
    from datetime import date

    if date_str is None:
        date_str = date.today().strftime('%Y-%m-%d')

    print("=" * 60)
    print(f"回测信号分析 - {date_str}")
    print("=" * 60)

    config = Config()
    if db_path:
        config.DB_PATH = db_path
    if contracts_path:
        config.CONTRACTS_PATH = contracts_path

    loader = DataLoader(config.DB_PATH, config.CONTRACTS_PATH)

    # 加载合约
    contracts = loader.load_main_contracts()
    print(f"加载 {len(contracts)} 个活跃合约")

    results = []
    for product_id, contract in contracts.items():
        exchange = contract.get('ExchangeID', '')
        symbol = f"{exchange}.{contract['MainContractID']}"
        symbol_info = contract

        # 加载数据
        df_5m_raw = loader.load_kline_fast(symbol, 300, config.MAX_5M_BARS)
        df_60m_raw = loader.load_kline_fast(symbol, 3600, config.MAX_60M_BARS)

        if not df_5m_raw or not df_60m_raw:
            continue

        # 计算 MACD
        df_5m = MACDCalculator.calculate(df_5m_raw)
        df_5m, green_stacks_5m, green_gaps_5m = StackIdentifier.identify(df_5m)

        # 计算 ATR（添加到第11列，索引10）- 必须在 MACD 计算之后
        df_5m = ATRCalculator.calculate(df_5m, period=14)

        df_60m = MACDCalculator.calculate(df_60m_raw)
        df_60m, green_stacks_60m, green_gaps_60m = StackIdentifier.identify(df_60m)

        index_map = IndexMapper.precompute_60m_index(df_5m, df_60m)
        strategy = Strategy(symbol_info)

        # 遍历所有 5 分钟 K 线，检测当天的信号
        date_prefix = date_str
        day_signals = []

        # 冷却时间跟踪：入场后 4 小时才能再次入场
        last_entry_time = None

        for i, row_5m in enumerate(df_5m):
            time_str = row_5m[0][:19]
            if not time_str.startswith(date_prefix):
                continue

            idx_60m = index_map[i] if i < len(index_map) else len(df_60m) - 1

            # 检查 60 分钟底背离
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
                        # 5分钟阳柱确认即可入场
                        cond_5m, reason_5m = strategy.check_5m_entry(df_5m, i, green_stacks_5m)
                        if cond_5m:
                            initial_stop, stop_reason = strategy.get_initial_stop_loss(df_5m, i, green_stacks_5m, green_gaps_5m, df_60m, green_stacks_60m)
                            if initial_stop:
                                day_signals.append({
                                    'time': time_str,
                                    'type': '绿柱堆信号',
                                    'price': row_5m[4],
                                    'stop_loss': initial_stop,
                                    'reason': f"{diver_reason} + {reason_5m}",
                                    'source': '绿柱堆内 DIF 拐头'
                                })

            # 红柱转绿柱时刻
            if hist_60m > 0 and hist_60m_prev < 0:
                diver_ok, diver_reason, curr_low, prev_low = strategy.check_60m_divergence(df_60m, idx_60m)
                if diver_ok:
                    # 5分钟阳柱确认即可入场
                    cond_5m, reason_5m = strategy.check_5m_entry(df_5m, i, green_stacks_5m)
                    if cond_5m:
                        initial_stop, stop_reason = strategy.get_initial_stop_loss(df_5m, i, green_stacks_5m, green_gaps_5m, df_60m, green_stacks_60m)
                        if initial_stop:
                            day_signals.append({
                                'time': time_str,
                                'type': '绿柱堆转红柱信号',
                                'price': row_5m[4],
                                'stop_loss': initial_stop,
                                'reason': f"{diver_reason} + {reason_5m}",
                                'source': '绿柱堆结束转红'
                            })

            # 红柱堆 DIF 拐头 + 底背离（信号队列）
            if hist_60m > 0 and hist_60m_prev > 0:
                dif_turn_red, _ = strategy.check_60m_dif_turn_in_red(df_60m, idx_60m)
                if dif_turn_red:
                    # 红柱堆 DIF 拐头：检查当前红柱堆最低价 vs 前一个绿柱堆最低价（底部抬升）
                    diver_ok, diver_reason, curr_low, prev_low = strategy.check_60m_bottom_rise_in_red(df_60m, idx_60m)
                    if diver_ok:
                        # 5分钟阳柱确认即可入场
                        cond_5m, reason_5m = strategy.check_5m_entry(df_5m, i, green_stacks_5m)
                        if cond_5m:
                            cond_filter, reason_filter = strategy.check_5m_green_stack_filter(df_5m, i, green_stacks_5m)
                            if cond_filter:
                                initial_stop, stop_reason = strategy.get_initial_stop_loss(df_5m, i, green_stacks_5m, green_gaps_5m, df_60m, green_stacks_60m)
                                if initial_stop:
                                    day_signals.append({
                                        'time': time_str,
                                        'type': '红柱堆信号',
                                        'price': row_5m[4],
                                        'stop_loss': initial_stop,
                                        'reason': f"{diver_reason} + {reason_filter} + {reason_5m}",
                                        'source': '红柱堆内 DIF 拐头'
                                    })

        if day_signals:
            print(f"  {symbol}: {len(day_signals)} 个信号")
            for sig in day_signals:
                print(f"    [{sig['time']}] {sig['type']} @ {sig['price']} | 止损:{sig['stop_loss']} | {sig['reason']}")
                # 更新冷却时间
                last_entry_time = sig['time']

            results.append({
                'symbol': symbol,
                'signals': day_signals,
                'symbol_info': symbol_info
            })

    print(f"\n📊 {date_str} 回测汇总：{len(results)} 个合约产生信号，共 {sum(len(r['signals']) for r in results)} 个")
    return results


# ============== 主函数（批量回测） ==============

def main():
    print("=" * 60)
    print("多时间框架策略回测")
    print("=" * 60)

    config = Config()
    loader = DataLoader(config.DB_PATH, config.CONTRACTS_PATH)

    symbols_to_test = []
    if len(sys.argv) > 1:
        symbols_to_test = sys.argv[1:]
        print(f"\n回测指定合约：{symbols_to_test}")
    else:
        print("\n加载主力合约列表...")
        contracts = loader.load_main_contracts()
        print(f"找到 {len(contracts)} 个活跃合约")

        for product_id, contract in contracts.items():
            exchange = contract.get('ExchangeID', '')
            symbol = f"{exchange}.{contract['MainContractID']}"
            symbols_to_test.append(symbol)

    all_results = []

    for idx, symbol in enumerate(symbols_to_test):
        print(f"\n{'=' * 60}")
        print(f"回测合约：{symbol} ({idx + 1}/{len(symbols_to_test)})")
        print(f"{'=' * 60}")

        symbol_info = loader.get_symbol_info(symbol)
        if symbol_info:
            print(f"合约信息：Tick={symbol_info.get('PriceTick')} | 乘数={symbol_info.get('VolumeMultiple')}")

        df_5m_raw = loader.load_kline_fast(symbol, 300, config.MAX_5M_BARS)
        df_60m_raw = loader.load_kline_fast(symbol, 3600, config.MAX_60M_BARS)

        if not df_5m_raw or not df_60m_raw:
            print(f"❌ {symbol} 数据不足，跳过")
            continue

        # 计算 MACD
        df_5m = MACDCalculator.calculate(df_5m_raw)
        df_5m, green_stacks_5m, green_gaps_5m = StackIdentifier.identify(df_5m)

        df_60m = MACDCalculator.calculate(df_60m_raw)
        df_60m, green_stacks_60m, green_gaps_60m = StackIdentifier.identify(df_60m)

        index_map = IndexMapper.precompute_60m_index(df_5m, df_60m)

        strategy = Strategy(symbol_info)

        position = None
        last_entry_time = None
        initial_stop_loss = None
        stop_updates = []

        precheck_signals_green = []
        precheck_signals_red = []

        volume_multiple = symbol_info.get('VolumeMultiple', 1) if symbol_info else 1

        total_bars = len(df_5m)
        trades = []

        for i, row_5m in enumerate(df_5m):
            if (i + 1) % 1000 == 0:
                print(f"  进度：{i + 1}/{total_bars} ({(i + 1) / total_bars * 100:.1f}%)")

            idx_60m = index_map[i] if i < len(index_map) else len(df_60m) - 1

            if position is None:
                if last_entry_time is not None:
                    entry_dt = datetime.strptime(last_entry_time[:19], '%Y-%m-%d %H:%M:%S')
                    current_dt = datetime.strptime(row_5m[0][:19], '%Y-%m-%d %H:%M:%S')
                    hours_passed = (current_dt - entry_dt).total_seconds() / 3600

                    if hours_passed < config.COOLDOWN_HOURS:
                        continue

                if idx_60m >= 4:
                    hist_60m = df_60m[idx_60m][8]

                    if hist_60m < 0:
                        dif_turn, _ = strategy.check_60m_dif_turn_in_green(df_60m, idx_60m, green_stacks_60m)

                        if dif_turn:
                            diver_ok, diver_reason, current_green_low, prev_prev_green_low = strategy.check_60m_divergence(df_60m, idx_60m)

                            if diver_ok:
                                expiry_time = row_5m[0][:19]
                                precheck_signals_green.append({
                                    'type': 'green',
                                    'created_time': row_5m[0][:19],
                                    'expiry_time': expiry_time,
                                })

                    elif hist_60m > 0:
                        dif_turn_red, _ = strategy.check_60m_dif_turn_in_red(df_60m, idx_60m)

                        if dif_turn_red:
                            expiry_time = row_5m[0][:19]
                            precheck_signals_red.append({
                                'type': 'red',
                                'created_time': row_5m[0][:19],
                                'expiry_time': expiry_time,
                            })

                precheck_entry_done = False

                all_signals = precheck_signals_green + precheck_signals_red

                if all_signals:
                    current_time = row_5m[0][:19]
                    current_dt = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S.%f') if '.' in current_time else datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')

                    precheck_signals_green = [
                        s for s in precheck_signals_green
                        if datetime.strptime(s['expiry_time'], '%Y-%m-%d %H:%M:%S') + timedelta(minutes=30) > current_dt
                    ]
                    precheck_signals_red = [
                        s for s in precheck_signals_red
                        if datetime.strptime(s['expiry_time'], '%Y-%m-%d %H:%M:%S') + timedelta(minutes=30) > current_dt
                    ]

                    for signal in (precheck_signals_green + precheck_signals_red)[:]:
                        signal_type = signal.get('type', 'unknown')

                        # 绿柱堆内信号使用check_60m_divergence，红柱堆内信号使用check_60m_bottom_rise_in_red
                        if signal_type == 'green':
                            diver_ok, diver_reason, curr_low_60m, prev_prev_low_60m = strategy.check_60m_divergence(df_60m, idx_60m)
                        else:
                            # 红柱堆内DIF拐头，检查底部是否抬升
                            diver_ok, diver_reason, curr_low_60m, prev_prev_low_60m = strategy.check_60m_bottom_rise_in_red(df_60m, idx_60m)

                        if not diver_ok:
                            if signal in precheck_signals_green:
                                precheck_signals_green.remove(signal)
                            if signal in precheck_signals_red:
                                precheck_signals_red.remove(signal)
                            continue

                        if signal_type == 'green':
                            signal_source = "绿柱堆内 DIF 拐头"
                        else:
                            signal_source = "红柱堆内 DIF 拐头"

                        cond_5m, reason_5m = strategy.check_5m_entry(df_5m, i, green_stacks_5m)

                        if cond_5m:
                            initial_stop_loss, stop_reason = strategy.get_initial_stop_loss(df_5m, i, green_stacks_5m, green_gaps_5m)

                            if initial_stop_loss is None:
                                continue

                            entry_price = row_5m[4]
                            contract_value = entry_price * volume_multiple

                            if contract_value > config.TARGET_NOTIONAL:
                                continue

                            position_size = max(1, int(config.TARGET_NOTIONAL / contract_value))

                            position = {
                                'entry_idx': i,
                                'entry_time': row_5m[0],
                                'entry_price': entry_price,
                                'position_size': position_size,
                                'stop_reason': stop_reason
                            }
                            last_entry_time = row_5m[0]

                            stop_updates = [{
                                'time': row_5m[0],
                                'entry_price': entry_price,
                                'stop_price': initial_stop_loss,
                                'type': '初始止损',
                                'reason': stop_reason,
                                'pnl': ''
                            }]

                            entry_conditions = f"{diver_reason} + {reason_5m} [{signal_source}]"
                            print(f"📈 入场：{row_5m[0]} @ {entry_price:.2f} | 止损:{initial_stop_loss:.2f} ({stop_reason}) | {entry_conditions}")

                            precheck_signals_green.clear()
                            precheck_signals_red.clear()
                            precheck_entry_done = True
                            position['entry_conditions'] = entry_conditions
                            break

                if precheck_entry_done:
                    continue

                hist_60m = df_60m[idx_60m][8]
                hist_60m_prev = df_60m[idx_60m - 1][8] if idx_60m > 0 else 0

                if hist_60m > 0 and hist_60m_prev < 0:
                    diver_ok, diver_reason, curr_low_60m, prev_low_60m = strategy.check_60m_divergence(df_60m, idx_60m)

                    if diver_ok:
                        # 5分钟阳柱确认即可入场
                        cond_5m, reason_5m = strategy.check_5m_entry(df_5m, i, green_stacks_5m)

                        if cond_5m:
                            initial_stop_loss, stop_reason = strategy.get_initial_stop_loss(df_5m, i, green_stacks_5m, green_gaps_5m)

                            if initial_stop_loss is None:
                                continue

                            entry_price = row_5m[4]
                            contract_value = entry_price * volume_multiple

                            if contract_value > config.TARGET_NOTIONAL:
                                continue

                            position_size = max(1, int(config.TARGET_NOTIONAL / contract_value))

                            position = {
                                'entry_idx': i,
                                'entry_time': row_5m[0],
                                'entry_price': entry_price,
                                'position_size': position_size,
                                'stop_reason': stop_reason
                            }
                            last_entry_time = row_5m[0]

                            stop_updates = [{
                                'time': row_5m[0],
                                'entry_price': entry_price,
                                'stop_price': initial_stop_loss,
                                'type': '初始止损',
                                'reason': stop_reason,
                                'pnl': ''
                            }]

                            entry_conditions = f"{diver_reason} + {reason_5m}"
                            print(f"📈 入场：{row_5m[0]} @ {entry_price:.2f} | 止损:{initial_stop_loss:.2f} ({stop_reason}) | {entry_conditions}")
                            position['entry_conditions'] = entry_conditions

            else:
                current_low = row_5m[3]

                if initial_stop_loss is not None and current_low <= initial_stop_loss:
                    price_diff = initial_stop_loss - position['entry_price']
                    pnl = price_diff * position['position_size'] * volume_multiple
                    pnl_pct = pnl / (position['entry_price'] * position['position_size'] * volume_multiple) * 100

                    stop_detail = position.get('stop_reason', '初始止损')

                    trade = Trade(
                        entry_time=position['entry_time'],
                        entry_price=position['entry_price'],
                        exit_time=row_5m[0],
                        exit_price=initial_stop_loss,
                        position_size=position['position_size'],
                        pnl=pnl,
                        pnl_pct=pnl_pct,
                        exit_reason=f"初始止损 ({stop_detail})",
                        initial_stop=stop_updates[0]['stop_price'] if stop_updates else initial_stop_loss,
                        stop_update_count=len(stop_updates),
                        entry_conditions=position.get('entry_conditions', '')
                    )
                    trades.append(trade)

                    print(f"📉 出场：{row_5m[0]} @ {initial_stop_loss:.2f} | 初始止损 ({stop_detail}) | 盈亏：{pnl:.2f} ({pnl_pct:.2f}%)")
                    position = None
                    initial_stop_loss = None
                    continue

                mobile_stop, stop_reason = strategy.get_mobile_stop(df_5m, i, green_stacks_5m, green_gaps_5m)

                if mobile_stop is not None:
                    if mobile_stop > initial_stop_loss:
                        stop_updates.append({
                            'time': row_5m[0],
                            'entry_price': position['entry_price'],
                            'stop_price': mobile_stop,
                            'type': '移动止损上移',
                            'reason': stop_reason,
                            'pnl': ''
                        })
                        initial_stop_loss = mobile_stop

                    if current_low <= initial_stop_loss:
                        price_diff = initial_stop_loss - position['entry_price']
                        pnl = price_diff * position['position_size'] * volume_multiple
                        pnl_pct = pnl / (position['entry_price'] * position['position_size'] * volume_multiple) * 100

                        stop_updates.append({
                            'time': row_5m[0],
                            'entry_price': position['entry_price'],
                            'stop_price': initial_stop_loss,
                            'type': '出场',
                            'reason': stop_reason,
                            'pnl': pnl
                        })

                        trade = Trade(
                            entry_time=position['entry_time'],
                            entry_price=position['entry_price'],
                            exit_time=row_5m[0],
                            exit_price=initial_stop_loss,
                            position_size=position['position_size'],
                            pnl=pnl,
                            pnl_pct=pnl_pct,
                            exit_reason=stop_reason,
                            initial_stop=stop_updates[0]['stop_price'] if stop_updates else 0.0,
                            stop_update_count=len(stop_updates),
                            entry_conditions=position.get('entry_conditions', '')
                        )
                        trades.append(trade)

                        print(f"📉 出场：{row_5m[0]} @ {initial_stop_loss:.2f} | {stop_reason} | 盈亏：{pnl:.2f} ({pnl_pct:.2f}%)")
                        position = None
                        initial_stop_loss = None

        if position is not None:
            last_row = df_5m[-1]
            price_diff = last_row[4] - position['entry_price']
            pnl = price_diff * position['position_size'] * volume_multiple
            pnl_pct = pnl / (position['entry_price'] * position['position_size'] * volume_multiple) * 100

            trade = Trade(
                entry_time=position['entry_time'],
                entry_price=position['entry_price'],
                exit_time=last_row[0],
                exit_price=last_row[4],
                position_size=position['position_size'],
                pnl=pnl,
                pnl_pct=pnl_pct,
                exit_reason="回测结束",
                entry_conditions=position.get('entry_conditions', '')
            )
            trades.append(trade)

        all_results.append({
            'symbol': symbol,
            'trades': trades,
            'symbol_info': symbol_info
        })

        print(f"  ✅ 完成：{len(trades)} 笔交易")

    print("\n" + "=" * 60)
    print("生成汇总报告...")
    print("=" * 60)

    output_path = Path("./trading") / "backtest_trades_0328.csv"
    output_path.parent.mkdir(exist_ok=True)

    with open(output_path, 'w') as f:
        f.write("symbol,entry_time,entry_price,exit_time,exit_price,initial_stop,stop_updates_count,pnl,pnl_pct,exit_reason,entry_conditions\n")
        for result in all_results:
            for t in result['trades']:
                cond = t.entry_conditions.replace('"', '""') if t.entry_conditions else ""
                if ',' in cond or '"' in cond:
                    cond = f'"{cond}"'
                f.write(f"{result['symbol']},{t.entry_time},{t.entry_price},{t.exit_time},{t.exit_price},{t.initial_stop:.2f},{t.stop_update_count},{t.pnl:.2f},{t.pnl_pct:.2f}%,{t.exit_reason},{cond}\n")

    print(f"\n📊 交易明细：{output_path}")

    total_trades = sum(len(r['trades']) for r in all_results)
    total_pnl = sum(sum(t.pnl for t in r['trades']) for r in all_results)
    winning = sum(1 for r in all_results for t in r['trades'] if t.pnl > 0)

    print(f"\n✅ 回测完成！")
    print(f"   总合约数：{len(all_results)}")
    print(f"   总交易数：{total_trades}")
    print(f"   盈利交易：{winning} ({winning / total_trades * 100:.1f}%)" if total_trades > 0 else "   无交易")
    print(f"   总盈亏：{total_pnl:,.2f}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='策略回测')
    parser.add_argument('--date', '-d', type=str, help='回测指定日期信号，格式 YYYY-MM-DD')
    parser.add_argument('symbols', nargs='*', help='回测指定合约')
    args = parser.parse_args()

    if args.date:
        backtest_date_signals(args.date)
    else:
        main()