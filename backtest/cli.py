#!/usr/bin/env python3
"""
Indicator Evaluation System - 命令行入口

整合所有模块，提供统一的命令行接口：

使用方法：
    # 1. 批量分析单个合约的所有指标
    python backtest/cli.py analyze --symbol CU.SHF

    # 2. 指定开始日期
    python backtest/cli.py analyze --symbol CU.SHF --start-date 2026-01-01

    # 3. 生成可视化报告
    python backtest/cli.py visualize --symbol CU.SHF --results results.csv

    # 4. 应用过滤条件到策略
    python backtest/cli.py apply --conditions filters.json

    # 5. 完整流程：分析 + 可视化 + 应用
    python backtest/cli.py full --symbol CU.SHF
"""

import sys
import os
import argparse
import csv
from pathlib import Path

# 确保能找到模块
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from utils.strategy_config import Config
from backtest.batch_analyzer import BatchIndicatorAnalyzer
from backtest.visualization import CumulativeChartGenerator
from backtest.condition_overlay import (
    ConditionOverlay,
    generate_condition_config,
    load_condition_config
)


def cmd_analyze(args):
    """分析命令"""
    analyzer = BatchIndicatorAnalyzer()

    if args.symbol:
        # 分析指定合约
        print(f"\n分析合约: {args.symbol}")
        results = analyzer.batch_analyze(
            args.symbol,
            args.start_date,
            args.output
        )

        # 保存结果到 CSV
        if args.save_csv and results:
            csv_path = Path(args.output) / f"{args.symbol.replace('.', '_')}_indicators.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'name', 'total_trades', 'winning_rate', 'total_pnl',
                    'lower_bound', 'upper_bound', 'profit_zone', 'loss_zone', 'is_valid'
                ])

                for r in results:
                    writer.writerow([
                        r.name,
                        r.total_trades,
                        f"{r.winning_rate:.2f}",
                        f"{r.total_pnl:.2f}",
                        f"{r.lower_bound:.2f}",
                        f"{r.upper_bound:.2f}",
                        str(r.profit_zone),
                        str(r.loss_zone),
                        r.is_valid
                    ])

            print(f"\n结果已保存到: {csv_path}")

    else:
        # 分析所有主力合约
        contracts = analyzer.loader.load_main_contracts()
        print(f"共 {len(contracts)} 个主力合约")

        all_results = {}

        for product_id, contract in contracts.items():
            exchange = contract.get('ExchangeID', '')
            if exchange not in ['SHFE', 'DCE', 'CZCE', 'CFFEX']:
                continue

            symbol = f"{exchange}.{contract['MainContractID']}"

            try:
                results = analyzer.batch_analyze(symbol, args.start_date, args.output)
                if results:
                    valid = [r for r in results if r.is_valid]
                    if valid:
                        all_results[symbol] = valid[:10]  # 只保留 top 10
            except Exception as e:
                print(f"  ❌ {symbol}: {e}")

        print(f"\n{'=' * 60}")
        print(f"汇总: {len(all_results)} 个合约有有效指标")
        print(f"{'=' * 60}")

        for symbol, results in all_results.items():
            print(f"\n{symbol}: {len(results)} ��有效指标")
            for r in results[:3]:
                print(f"  - {r.name}: PnL={r.total_pnl:.2f}, 边界=[{r.lower_bound:.1f}, {r.upper_bound:.1f}]")


def cmd_visualize(args):
    """可视化命令"""
    if not args.results:
        print("错误: 请指定 --results 参数")
        return

    # 加载结果
    results = []
    with open(args.results, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            results.append(row)

    if not results:
        print("没有结果")
        return

    # 生成图表
    generator = CumulativeChartGenerator(args.output)

    for row in results[:20]:  # 只生成前 20 个
        name = row.get('name', '')
        if not name:
            continue

        # TODO: 需要从分析器获取原始数据
        # 这里简化处理
        print(f"生成图表: {name}")

    # 生成 HTML 报告
    html_path = generator.generate_report([])
    print(f"\n报告已生成: {html_path}")


def cmd_apply(args):
    """应用过滤条件命令"""
    if not args.conditions:
        print("错误: 请指定 --conditions 参数")
        return

    # 加载条件
    conditions = load_condition_config(args.conditions)

    print(f"\n加载了 {len(conditions)} 个过滤条件:")
    for cond in conditions:
        print(f"  {cond.indicator_name}: [{cond.lower_bound}, {cond.upper_bound}]")

    # 生成策略代码
    strategy_path = args.output / 'strategy_filters.py'

    with open(strategy_path, 'w', encoding='utf-8') as f:
        f.write("# Auto-generated strategy filters\n")
        f.write("# 条件过滤配置\n\n")

        f.write("FILTER_CONDITIONS = {\n")
        for cond in conditions:
            f.write(f"    '{cond.indicator_name}': ")
            f.write(f"({cond.lower_bound}, {cond.upper_bound}),\n")
        f.write("}\n")

    print(f"\n策略代码已生成: {strategy_path}")


def cmd_full(args):
    """完整流程命令"""
    analyzer = BatchIndicatorAnalyzer()

    print(f"\n{'=' * 60}")
    print("完整分析流程")
    print(f"{'=' * 60}")

    # 1. 运行回测
    symbols = [args.symbol] if args.symbol else []

    if not symbols:
        contracts = analyzer.loader.load_main_contracts()
        for product_id, contract in contracts.items():
            exchange = contract.get('ExchangeID', '')
            if exchange not in ['SHFE', 'DCE', 'CZCE', 'CFFEX']:
                continue
            symbols.append(f"{exchange}.{contract['MainContractID']}")

    all_valid_results = {}

    for symbol in symbols:
        try:
            print(f"\n分析 {symbol}...")
            results = analyzer.batch_analyze(symbol, args.start_date, args.output)

            valid = [r for r in results if r.is_valid]
            if valid:
                all_valid_results[symbol] = valid
        except Exception as e:
            print(f"  ❌ {e}")

    # 2. 生成汇总
    print(f"\n{'=' * 60}")
    print(f"汇总: {len(all_valid_results)} 个合约有有效指标")
    print(f"{'=' * 60}")

    # 3. 生成过滤条件配置
    if all_valid_results:
        # 取所有有效条件
        all_conditions = []
        for symbol, results in all_valid_results.items():
            for r in results[:3]:
                overlay = ConditionOverlay()
                cond = overlay.generate_condition_from_result(r)
                if cond:
                    all_conditions.append(cond)

        if all_conditions:
            config_path = Path(args.output) / 'filters.json'
            generate_condition_config(all_conditions, str(config_path))

    print("\n✅ 分析完成!")


def main():
    parser = argparse.ArgumentParser(
        description='Indicator Evaluation System',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 分析单个合约
  python backtest/cli.py analyze --symbol CU.SHF

  # 分析所有主力合约
  python backtest/cli.py analyze

  # 带日期参数
  python backtest/cli.py analyze --symbol CU.SHF --start-date 2026-01-01

  # 完整流程
  python backtest/cli.py full --symbol CU.SHF
        """
    )

    # 子命令
    subparsers = parser.add_subparsers(dest='cmd', help='可用命令')

    # analyze 命令
    analyze_parser = subparsers.add_parser('analyze', help='批量分析指标')
    analyze_parser.add_argument('--symbol', '-s', type=str, help='合约代码，如 CU.SHF')
    analyze_parser.add_argument('--start-date', type=str, help='开始日期')
    analyze_parser.add_argument('--output', '-o', type=str,
                       default='./backtest/reports', help='输出目录')
    analyze_parser.add_argument('--save-csv', action='store_true', help='保存到 CSV')

    # visualize 命令
    visualize_parser = subparsers.add_parser('visualize', help='生成可视化')
    visualize_parser.add_argument('--results', '-r', type=str, required=True,
                            help='结果文件路径')
    visualize_parser.add_argument('--output', '-o', type=str,
                            default='./backtest/reports/charts',
                            help='输出目录')

    # apply 命令
    apply_parser = subparsers.add_parser('apply', help='应用过滤条件')
    apply_parser.add_argument('--conditions', '-c', type=str, required=True,
                      help='条件配置文件')
    apply_parser.add_argument('--output', '-o', type=str,
                      default='./backtest/reports',
                      help='输出目录')

    # full 命令
    full_parser = subparsers.add_parser('full', help='完整分析流程')
    full_parser.add_argument('--symbol', '-s', type=str, help='合约代码')
    full_parser.add_argument('--start-date', type=str, help=' 시작日期')
    full_parser.add_argument('--output', '-o', type=str,
                        default='./backtest/reports', help='输出目录')

    args = parser.parse_args()

    if not args.cmd:
        parser.print_help()
        return

    # 执行命令
    if args.cmd == 'analyze':
        cmd_analyze(args)
    elif args.cmd == 'visualize':
        cmd_visualize(args)
    elif args.cmd == 'apply':
        cmd_apply(args)
    elif args.cmd == 'full':
        cmd_full(args)


if __name__ == '__main__':
    main()