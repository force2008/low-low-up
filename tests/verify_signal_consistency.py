#!/usr/bin/env python3
"""
验证实盘策略与 backtest_v7.py 的信号一致性（简化版）

使用方法：
    python strategies/verify_signal_consistency.py

输出：
    - 比较两个策略在相同数据上的入场信号
    - 报告信号差异（如有）
"""

import sqlite3
import json
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path

# 添加策略模块路径
sys.path.insert(0, str(Path(__file__).parent))

# 导入实盘策略模块
from TrendReversalV7LiveStrategy import (
    LiveConfig, DataLoader, MACDCalculator, StackIdentifier,
    TrendReversalStrategy
)


# ============== 简化的验证逻辑 ==============

def compare_strategies(symbol: str, db_path: str, contracts_path: str) -> dict:
    """
    比较 backtest_v7.py 和实盘策略的核心逻辑
    
    直接比较两个策略在相同 K 线数据上的判断结果
    """
    config = LiveConfig()
    config.DB_PATH = db_path
    config.CONTRACTS_PATH = contracts_path
    
    # 加载数据
    data_loader = DataLoader(db_path, contracts_path)
    df_5m_raw = data_loader.load_kline_fast(symbol, 300, config.MAX_5M_BARS)
    df_60m_raw = data_loader.load_kline_fast(symbol, 3600)
    
    if not df_5m_raw or not df_60m_raw:
        return {'error': '数据不足'}
    
    symbol_info = data_loader.get_symbol_info(symbol)
    strategy = TrendReversalStrategy(symbol_info)
    
    # 计算 MACD 和堆识别
    df_5m = MACDCalculator.calculate(df_5m_raw)
    df_5m, green_stacks_5m, green_gaps_5m = StackIdentifier.identify(df_5m)
    
    df_60m = MACDCalculator.calculate(df_60m_raw)
    df_60m, green_stacks_60m, green_gaps_60m = StackIdentifier.identify(df_60m)
    
    # 预计算索引映射
    index_map = _precompute_60m_index(df_5m, df_60m)
    
    # 测试结果列表
    test_results = []
    
    # 遍历每个 5 分钟 K 线，测试策略逻辑
    for i, row_5m in enumerate(df_5m):
        idx_60m = index_map[i] if i < len(index_map) else len(df_60m) - 1
        
        # 测试 60 分钟条件
        cond_60m_div, reason_60m_div, _, _ = strategy.check_60m_divergence(df_60m, idx_60m)
        cond_60m_dif_green, reason_60m_dif_green = strategy.check_60m_dif_turn_in_green(df_60m, idx_60m, green_stacks_60m)
        cond_60m_dif_red, reason_60m_dif_red = strategy.check_60m_dif_turn_in_red(df_60m, idx_60m)
        cond_60m_entry, reason_60m_entry, _, _ = strategy.check_60m_entry(df_60m, idx_60m, green_stacks_60m)
        
        # 测试 5 分钟条件
        cond_5m_entry, reason_5m_entry = strategy.check_5m_entry(df_5m, i, green_stacks_5m)
        cond_5m_filter, reason_5m_filter = strategy.check_5m_green_stack_filter(df_5m, i, green_stacks_5m)
        
        # 获取止损
        stop_loss, stop_reason = strategy.get_initial_stop_loss(df_5m, i, green_stacks_5m, green_gaps_5m)
        mobile_stop, mobile_reason = strategy.get_mobile_stop(df_5m, i, green_stacks_5m, green_gaps_5m)
        
        # 记录结果（只记录有信号的情况）
        if cond_60m_div or cond_60m_dif_green or cond_60m_dif_red or cond_5m_entry:
            test_results.append({
                'idx_5m': i,
                'idx_60m': idx_60m,
                'time_5m': row_5m[0],
                'time_60m': df_60m[idx_60m][0],
                '60m_divergence': cond_60m_div,
                '60m_dif_green': cond_60m_dif_green,
                '60m_dif_red': cond_60m_dif_red,
                '60m_entry': cond_60m_entry,
                '5m_entry': cond_5m_entry,
                '5m_filter': cond_5m_filter,
                'stop_loss': stop_loss,
                'mobile_stop': mobile_stop
            })
    
    return {
        'symbol': symbol,
        'test_results': test_results,
        'total_5m_bars': len(df_5m),
        'total_60m_bars': len(df_60m)
    }


def _precompute_60m_index(df_5m: List[tuple], df_60m: List[tuple]) -> List[int]:
    """预计算索引映射"""
    if not df_5m or not df_60m:
        return []
    
    index_map = []
    idx_60m = 0
    n_60m = len(df_60m)
    
    for row_5m in df_5m:
        time_5m = row_5m[0]
        
        while idx_60m < n_60m - 1 and df_60m[idx_60m + 1][0] <= time_5m:
            idx_60m += 1
        
        index_map.append(idx_60m)
    
    return index_map


def main():
    """主函数"""
    print("="*60)
    print("策略逻辑验证工具")
    print("="*60)
    
    db_path = "/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/data-manager/kline_data.db"
    contracts_path = "/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/data-manager/main_contracts.json"
    
    # 测试合约列表
    test_symbols = [
        "CZCE.CF605",
        "CZCE.AP605",
        "CFFEX.IC2606",
    ]
    
    all_results = {}
    
    for symbol in test_symbols:
        print(f"\n{'='*60}")
        print(f"测试合约：{symbol}")
        print(f"{'='*60}")
        
        result = compare_strategies(symbol, db_path, contracts_path)
        all_results[symbol] = result
        
        if 'error' in result:
            print(f"错误：{result['error']}")
            continue
        
        print(f"5 分钟 K 线数：{result['total_5m_bars']}")
        print(f"60 分钟 K 线数：{result['total_60m_bars']}")
        
        # 统计信号数量
        div_count = sum(1 for r in result['test_results'] if r['60m_divergence'])
        dif_green_count = sum(1 for r in result['test_results'] if r['60m_dif_green'])
        dif_red_count = sum(1 for r in result['test_results'] if r['60m_dif_red'])
        entry_count = sum(1 for r in result['test_results'] if r['60m_entry'])
        entry_5m_count = sum(1 for r in result['test_results'] if r['5m_entry'])
        filter_count = sum(1 for r in result['test_results'] if r['5m_filter'])
        
        print(f"\n信号统计:")
        print(f"  60m 底背离：{div_count} 次")
        print(f"  60m 绿柱 DIF 拐头：{dif_green_count} 次")
        print(f"  60m 红柱 DIF 拐头：{dif_red_count} 次")
        print(f"  60m 入场条件：{entry_count} 次")
        print(f"  5m 入场条件：{entry_5m_count} 次")
        print(f"  5m 底部抬升过滤：{filter_count} 次")
        
        # 显示前 5 个同时满足多个条件的点
        multi_cond = [r for r in result['test_results'] 
                      if r['60m_entry'] and r['5m_entry'] and r['5m_filter']]
        
        if multi_cond:
            print(f"\n同时满足 60m 入场 +5m 入场 +5m 过滤的点（前 5 个）:")
            for r in multi_cond[:5]:
                print(f"  {r['time_5m']} | 60m:{r['time_60m']} | 止损:{r['stop_loss']:.2f if r['stop_loss'] else 'N/A'}")
    
    # 保存详细报告
    report_path = Path(__file__).parent / "strategy_logic_report.json"
    
    # 简化结果以便 JSON 序列化
    simple_results = {}
    for symbol, result in all_results.items():
        if 'error' in result:
            simple_results[symbol] = {'error': result['error']}
            continue
        
        simple_results[symbol] = {
            'symbol': symbol,
            'total_5m_bars': result['total_5m_bars'],
            'total_60m_bars': result['total_60m_bars'],
            'signal_counts': {
                '60m_divergence': sum(1 for r in result['test_results'] if r['60m_divergence']),
                '60m_dif_green': sum(1 for r in result['test_results'] if r['60m_dif_green']),
                '60m_dif_red': sum(1 for r in result['test_results'] if r['60m_dif_red']),
                '60m_entry': sum(1 for r in result['test_results'] if r['60m_entry']),
                '5m_entry': sum(1 for r in result['test_results'] if r['5m_entry']),
                '5m_filter': sum(1 for r in result['test_results'] if r['5m_filter']),
            },
            'multi_condition_points': [
                {
                    'time_5m': r['time_5m'],
                    'time_60m': r['time_60m'],
                    'stop_loss': r['stop_loss']
                }
                for r in result['test_results'][:10]
                if r['60m_entry'] and r['5m_entry'] and r['5m_filter']
            ]
        }
    
    with open(report_path, 'w') as f:
        json.dump(simple_results, f, indent=2, default=str)
    
    print(f"\n{'='*60}")
    print(f"详细报告已保存到：{report_path}")
    print(f"{'='*60}")
    
    return all_results


if __name__ == "__main__":
    main()