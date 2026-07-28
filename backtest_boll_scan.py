#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BOLL 中轨策略盘后扫描

功能：
- 收盘后扫描所有主力合约的日 K 线和 60 分钟 K 线
- 检测最近一根 K 线是否触发 BOLL 中轨反弹/回踩信号
- 触发时发送飞书通知

用法：
    python backtest_boll_scan.py [test|online] [--date YYYY-MM-DD]

参数：
    test/online: 使用测试库或线上库，默认 test
    --date: 指定扫描日期，默认今天
"""

import json
import os
import sys
import argparse
from datetime import datetime, timedelta

# 项目根目录加入路径
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import sqlite3
from strategy.boll import BOLLCalculator
from strategies.boll_middle_band.StrategyBollMiddleBand import StrategyBollMiddleBand
from utils.feishu_notifier import send_feishu_strategy_signal


# 数据库路径
DB_PATHS = {
    "test": os.path.join(PROJECT_ROOT, "data", "db", "kline_data_test.db"),
    "online": os.path.join(PROJECT_ROOT, "data", "db", "kline_data.db"),
}

MAIN_CONTRACTS_PATH = os.path.join(PROJECT_ROOT, "data", "contracts", "main_contracts.json")


def load_main_contracts(json_file: str) -> list:
    """从 main_contracts.json 加载主力合约"""
    if not os.path.exists(json_file):
        print(f"[错误] 文件不存在：{json_file}")
        return []
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "main_contracts" in data:
        return data["main_contracts"]
    return []


def get_kline_data(conn, symbol: str, duration: int, limit: int = 100) -> list:
    """从数据库读取 K 线数据，返回正序列表"""
    cursor = conn.cursor()
    cursor.execute('''
        SELECT datetime, open, high, low, close, volume
        FROM kline_data
        WHERE symbol = ? AND duration = ?
        ORDER BY datetime DESC
        LIMIT ?
    ''', (symbol, duration, limit))
    rows = cursor.fetchall()
    # 反转为正序
    return [tuple(r) for r in reversed(rows)]


def scan_symbol(strategy, conn, symbol: str, scan_date: str) -> list:
    """扫描单个合约的日 K 和 60 分钟 K 线，返回信号列表"""
    signals = []

    # 日线扫描
    data_day = get_kline_data(conn, symbol, duration=86400, limit=50)
    if len(data_day) >= 20:
        # 过滤只保留 scan_date 及之前的 K 线
        data_day = [bar for bar in data_day if bar[0] <= f"{scan_date} 23:59:59"]
        if len(data_day) >= 20:
            data_day_with_boll = BOLLCalculator.calculate(data_day)
            sig = strategy.check_boll_middle_band_signal(
                data_day_with_boll, timeframe="day", symbol=symbol
            )
            if sig:
                signals.append(sig)

    # 60 分钟线扫描
    data_60m = get_kline_data(conn, symbol, duration=3600, limit=50)
    if len(data_60m) >= 20:
        # 过滤只保留 scan_date 及之前的 K 线
        data_60m = [bar for bar in data_60m if bar[0] <= f"{scan_date} 23:59:59"]
        if len(data_60m) >= 20:
            data_60m_with_boll = BOLLCalculator.calculate(data_60m)
            sig = strategy.check_boll_middle_band_signal(
                data_60m_with_boll, timeframe="60min", symbol=symbol
            )
            if sig:
                signals.append(sig)

    return signals


def send_signal(symbol: str, signal: dict, env_name: str):
    """发送飞书通知"""
    signal_data = {
        'signal_type': signal['signal_type'],
        'price': signal['current_close'],
        'stop_loss': 0,
        'position_size': 0,
        'strategy_name': 'BOLL中轨反弹回踩',
        'reason': f"盘后扫描 | 环境：{env_name} | {signal['reason']}",
        'time': signal.get('bar_time', datetime.now().isoformat()),
        'message': signal['message'],
    }

    try:
        send_feishu_strategy_signal(symbol, signal_data)
        print(f"  ✓ {symbol} 飞书通知已发送")
    except Exception as e:
        print(f"  ✗ {symbol} 飞书通知发送失败：{e}")


def main():
    parser = argparse.ArgumentParser(description="BOLL 中轨策略盘后扫描")
    parser.add_argument("env", nargs="?", default="test", choices=["test", "online"], help="数据库环境")
    parser.add_argument("--date", type=str, default=None, help="扫描日期，格式 YYYY-MM-DD，默认今天")
    args = parser.parse_args()

    scan_date = args.date or datetime.now().strftime('%Y-%m-%d')
    print(f"[扫描] 环境：{args.env} | 日期：{scan_date}")

    db_path = DB_PATHS.get(args.env)
    if not os.path.exists(db_path):
        print(f"[错误] 数据库不存在：{db_path}")
        return

    contracts = load_main_contracts(MAIN_CONTRACTS_PATH)
    if not contracts:
        print("[错误] 没有加载到合约列表")
        return

    conn = sqlite3.connect(db_path)
    strategy = StrategyBollMiddleBand({})

    total_signals = 0

    try:
        for inst in contracts:
            inst_id = inst.get("MainContractID") or inst.get("InstrumentID", "")
            exchange_id = inst.get("ExchangeID", "")
            if not inst_id:
                continue
            symbol = f"{exchange_id}.{inst_id}" if exchange_id else inst_id

            signals = scan_symbol(strategy, conn, symbol, scan_date)
            if signals:
                total_signals += len(signals)
                print(f"\n{symbol} 触发 {len(signals)} 个信号：")
                for sig in signals:
                    print(f"  - {sig['message']} | {sig['reason']}")
                    send_signal(symbol, sig, args.env)

        print(f"\n[完成] 共扫描 {len(contracts)} 个合约，触发 {total_signals} 个信号")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
