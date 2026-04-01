#!/usr/bin/env python3
"""
比较 backtest_v7.py 和 run_live_strategy_backtest.py 的回测结果

分析差异原因
"""

import csv
import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict


def load_trades_from_csv(filepath: str) -> list:
    """加载 CSV 文件中的交易记录"""
    trades = []
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            trades.append({
                'symbol': row['symbol'],
                'entry_time': row['entry_time'][:19],  # 截取到秒
                'entry_price': float(row['entry_price']),
                'exit_time': row['exit_time'][:19],
                'exit_price': float(row['exit_price']),
                'pnl': float(row['pnl']),
                'pnl_pct': float(row['pnl_pct'].replace('%', '')),
                'exit_reason': row['exit_reason']
            })
    return trades


def normalize_time(time_str: str) -> str:
    """标准化时间字符串"""
    return time_str[:19].replace(' ', ' ')


def compare_trades(v7_trades: list, live_trades: list) -> dict:
    """比较两个策略的交易记录"""
    
    # 按合约分组
    v7_by_symbol = defaultdict(list)
    live_by_symbol = defaultdict(list)
    
    for t in v7_trades:
        v7_by_symbol[t['symbol']].append(t)
    
    for t in live_trades:
        live_by_symbol[t['symbol']].append(t)
    
    # 比较结果
    comparison = {
        'v7_total': len(v7_trades),
        'live_total': len(live_trades),
        'symbols_in_v7': len(v7_by_symbol),
        'symbols_in_live': len(live_by_symbol),
        'common_symbols': 0,
        'symbol_comparison': {}
    }
    
    # 找出共同合约
    common_symbols = set(v7_by_symbol.keys()) & set(live_by_symbol.keys())
    comparison['common_symbols'] = len(common_symbols)
    
    # 按合约比较
    for symbol in sorted(common_symbols):
        v7_count = len(v7_by_symbol[symbol])
        live_count = len(live_by_symbol[symbol])
        v7_pnl = sum(t['pnl'] for t in v7_by_symbol[symbol])
        live_pnl = sum(t['pnl'] for t in live_by_symbol[symbol])
        
        comparison['symbol_comparison'][symbol] = {
            'v7_count': v7_count,
            'live_count': live_count,
            'v7_pnl': v7_pnl,
            'live_pnl': live_pnl,
            'pnl_diff': live_pnl - v7_pnl
        }
    
    # 找出交易数量差异最大的合约
    diff_list = []
    for symbol, comp in comparison['symbol_comparison'].items():
        diff_list.append({
            'symbol': symbol,
            'v7_count': comp['v7_count'],
            'live_count': comp['live_count'],
            'count_diff': comp['live_count'] - comp['v7_count'],
            'v7_pnl': comp['v7_pnl'],
            'live_pnl': comp['live_pnl'],
            'pnl_diff': comp['pnl_diff']
        })
    
    # 按交易数量差异排序
    diff_list.sort(key=lambda x: abs(x['count_diff']), reverse=True)
    comparison['top_differences'] = diff_list[:20]
    
    # 统计差异类型
    more_trades_in_v7 = sum(1 for d in diff_list if d['count_diff'] < 0)
    more_trades_in_live = sum(1 for d in diff_list if d['count_diff'] > 0)
    same_trades = sum(1 for d in diff_list if d['count_diff'] == 0)
    
    comparison['stats'] = {
        'symbols_with_more_v7_trades': more_trades_in_v7,
        'symbols_with_more_live_trades': more_trades_in_live,
        'symbols_with_same_trades': same_trades
    }
    
    return comparison


def analyze_entry_times(v7_trades: list, live_trades: list) -> dict:
    """分析入场时间差异"""
    
    # 按入场时间分组统计
    v7_dates = defaultdict(int)
    live_dates = defaultdict(int)
    
    for t in v7_trades:
        date = t['entry_time'][:10]
        v7_dates[date] += 1
    
    for t in live_trades:
        date = t['entry_time'][:10]
        live_dates[date] += 1
    
    # 找出只有 V7 有交易的日期
    v7_only_dates = set(v7_dates.keys()) - set(live_dates.keys())
    live_only_dates = set(live_dates.keys()) - set(v7_dates.keys())
    
    return {
        'v7_trading_days': len(v7_dates),
        'live_trading_days': len(live_dates),
        'v7_only_dates': sorted(list(v7_only_dates))[:20],
        'live_only_dates': sorted(list(live_only_dates))[:20],
        'v7_dates_sample': dict(list(v7_dates.items())[:10]),
        'live_dates_sample': dict(list(live_dates.items())[:10])
    }


def analyze_exit_reasons(v7_trades: list, live_trades: list) -> dict:
    """分析出场原因差异"""
    
    v7_reasons = defaultdict(int)
    live_reasons = defaultdict(int)
    
    for t in v7_trades:
        # 简化出场原因
        reason = t['exit_reason'].split('(')[0].strip()
        v7_reasons[reason] += 1
    
    for t in live_trades:
        reason = t['exit_reason'].split('(')[0].strip()
        live_reasons[reason] += 1
    
    return {
        'v7_reasons': dict(v7_reasons),
        'live_reasons': dict(live_reasons)
    }


def main():
    """主函数"""
    print("="*70)
    print("回测结果差异分析")
    print("="*70)
    
    # 加载交易记录
    v7_path = "/home/ubuntu/trading/backtest_trades_v7.csv"
    live_path = "/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/strategies/live_strategy_backtest_trades.csv"
    
    print("\n加载交易记录...")
    v7_trades = load_trades_from_csv(v7_path)
    live_trades = load_trades_from_csv(live_path)
    
    print(f"  backtest_v7.py: {len(v7_trades)} 笔交易")
    print(f"  live_strategy: {len(live_trades)} 笔交易")
    print(f"  差异：{len(v7_trades) - len(live_trades)} 笔交易")
    
    # 比较交易记录
    print("\n" + "="*70)
    print("交易记录比较")
    print("="*70)
    
    comparison = compare_trades(v7_trades, live_trades)
    
    print(f"\n共同合约数：{comparison['common_symbols']}")
    print(f"V7 独有合约：{comparison['symbols_in_v7'] - comparison['common_symbols']}")
    print(f"Live 独有合约：{comparison['symbols_in_live'] - comparison['common_symbols']}")
    
    print(f"\n交易数量统计:")
    print(f"  V7 交易更多的合约数：{comparison['stats']['symbols_with_more_v7_trades']}")
    print(f"  Live 交易更多的合约数：{comparison['stats']['symbols_with_more_live_trades']}")
    print(f"  交易数量相同的合约数：{comparison['stats']['symbols_with_same_trades']}")
    
    # 显示差异最大的合约
    print("\n交易数量差异最大的合约（前 20）:")
    print(f"{'合约':<20} {'V7 数量':>10} {'Live 数量':>10} {'差异':>10} {'V7 盈亏':>12} {'Live 盈亏':>12} {'盈亏差异':>12}")
    print("-"*90)
    
    for item in comparison['top_differences']:
        print(f"{item['symbol']:<20} {item['v7_count']:>10} {item['live_count']:>10} "
              f"{item['count_diff']:>+10} {item['v7_pnl']:>+12.2f} {item['live_pnl']:>+12.2f} "
              f"{item['pnl_diff']:>+12.2f}")
    
    # 分析入场时间差异
    print("\n" + "="*70)
    print("入场时间分析")
    print("="*70)
    
    time_analysis = analyze_entry_times(v7_trades, live_trades)
    
    print(f"\nV7 交易天数：{time_analysis['v7_trading_days']}")
    print(f"Live 交易天数：{time_analysis['live_trading_days']}")
    
    if time_analysis['v7_only_dates']:
        print(f"\n只有 V7 有交易的日期（前 20）:")
        for date in time_analysis['v7_only_dates']:
            count = time_analysis['v7_dates_sample'].get(date, 0)
            print(f"  {date}: {count} 笔交易")
    
    # 分析出场原因差异
    print("\n" + "="*70)
    print("出场原因分析")
    print("="*70)
    
    reason_analysis = analyze_exit_reasons(v7_trades, live_trades)
    
    print("\nV7 出场原因:")
    for reason, count in sorted(reason_analysis['v7_reasons'].items(), key=lambda x: -x[1]):
        print(f"  {reason}: {count}")
    
    print("\nLive 出场原因:")
    for reason, count in sorted(reason_analysis['live_reasons'].items(), key=lambda x: -x[1]):
        print(f"  {reason}: {count}")
    
    # 保存详细报告
    report_path = Path(__file__).parent / "backtest_comparison_report.json"
    
    report = {
        'summary': {
            'v7_total': comparison['v7_total'],
            'live_total': comparison['live_total'],
            'difference': comparison['v7_total'] - comparison['live_total']
        },
        'symbol_comparison': comparison['symbol_comparison'],
        'top_differences': comparison['top_differences'],
        'stats': comparison['stats'],
        'time_analysis': time_analysis,
        'exit_reasons': reason_analysis
    }
    
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"\n详细报告已保存到：{report_path}")
    
    # 分析可能原因
    print("\n" + "="*70)
    print("差异原因分析")
    print("="*70)
    
    if comparison['stats']['symbols_with_more_v7_trades'] > comparison['stats']['symbols_with_more_live_trades']:
        print("""
可能原因：
1. 入场条件检查顺序不同：V7 可能更早触发入场条件
2. 冷却期计算差异：两个策略的冷却期计算可能不同
3. 信号队列处理差异：预检查信号队列的处理逻辑可能不同
4. 60 分钟 K 线索引映射差异：两个策略可能使用不同的 60 分钟 K 线索引

建议检查：
- 两个策略的入场条件检查顺序
- 冷却期的计算方式（是否包含微秒）
- 预检查信号队列的清理和触发逻辑
- 60 分钟 K 线的索引映射方法
""")
    
    return comparison


if __name__ == "__main__":
    main()