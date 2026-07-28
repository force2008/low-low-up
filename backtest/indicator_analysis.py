#!/usr/bin/env python3
"""Indicator quality analysis for low-low-up strategy.

本脚本使用 /home/ubuntu/low-low-up/data/db/kline_data.db 中的 K 线数据，
并基于现有的 low-low-up 策略信号逻辑，统计指标值与未来收益的关系。

示例：
    python backtest/indicator_analysis.py --symbol CU.SHF --indicator rsi --period 14 --future-bars 12
"""

import argparse
import csv
import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from strategy.macd import MACDCalculator, ATRCalculator
from strategy.stack import StackIdentifier
from strategy.index_map import IndexMapper
from strategies.low_low_up.StrategyLowLowUp import StrategyLowLowUp
from utils.strategy_config import Config, DataLoader


class IndicatorUtils:
    @staticmethod
    def sma(values: List[float], period: int) -> List[Optional[float]]:
        result = []
        window = []
        for v in values:
            window.append(v)
            if len(window) > period:
                window.pop(0)
            if len(window) == period:
                result.append(sum(window) / period)
            else:
                result.append(None)
        return result

    @staticmethod
    def ema(values: List[float], period: int) -> List[Optional[float]]:
        if not values:
            return []
        result: List[Optional[float]] = [None] * len(values)
        alpha = 2 / (period + 1)
        result[0] = values[0]
        for i in range(1, len(values)):
            result[i] = values[i] * alpha + result[i - 1] * (1 - alpha)
        return result

    @staticmethod
    def rsi(values: List[float], period: int) -> List[Optional[float]]:
        if len(values) < period + 1:
            return [None] * len(values)

        gains: List[float] = [0.0]
        losses: List[float] = [0.0]
        for i in range(1, len(values)):
            diff = values[i] - values[i - 1]
            gains.append(max(diff, 0.0))
            losses.append(max(-diff, 0.0))

        avg_gain = sum(gains[1: period + 1]) / period
        avg_loss = sum(losses[1: period + 1]) / period
        result: List[Optional[float]] = [None] * len(values)

        if avg_loss == 0:
            result[period] = 100.0
        else:
            rs = avg_gain / avg_loss
            result[period] = 100.0 - 100.0 / (1.0 + rs)

        for i in range(period + 1, len(values)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
            if avg_loss == 0:
                result[i] = 100.0
            else:
                rs = avg_gain / avg_loss
                result[i] = 100.0 - 100.0 / (1.0 + rs)

        return result

    @staticmethod
    def macd_hist(values: List[float], fast: int, slow: int, signal: int) -> List[Optional[float]]:
        if len(values) < slow:
            return [None] * len(values)
        ema_fast = IndicatorUtils.ema(values, fast)
        ema_slow = IndicatorUtils.ema(values, slow)
        dif = [None if ema_fast[i] is None or ema_slow[i] is None else ema_fast[i] - ema_slow[i] for i in range(len(values))]
        dea = IndicatorUtils.ema([v if v is not None else 0.0 for v in dif], signal)
        result: List[Optional[float]] = [None] * len(values)
        for i in range(len(values)):
            if dif[i] is not None and dea[i] is not None:
                result[i] = 2 * (dif[i] - dea[i])
            else:
                result[i] = None
        return result


class IndicatorAnalyzer:
    def __init__(self, db_path: str, contracts_path: str):
        self.config = Config()
        self.config.DB_PATH = db_path
        self.config.CONTRACTS_PATH = contracts_path
        self.loader = DataLoader(self.config.DB_PATH, self.config.CONTRACTS_PATH)

    def load_5m_data(self, symbol: str, limit: int = None) -> List[tuple]:
        return self.loader.load_kline_fast(symbol, self.config.DURATION_5M, limit)

    def load_60m_data(self, symbol: str, limit: int = None) -> List[tuple]:
        return self.loader.load_kline_fast(symbol, self.config.DURATION_60M, limit)

    def compute_indicator(self, indicator: str, values: List[float], period: int, extra: dict) -> List[Optional[float]]:
        indicator = indicator.lower()
        if indicator == 'rsi':
            return IndicatorUtils.rsi(values, period)
        if indicator == 'sma':
            return IndicatorUtils.sma(values, period)
        if indicator == 'ema':
            return IndicatorUtils.ema(values, period)
        if indicator == 'macd_hist':
            fast = extra.get('fast', 12)
            slow = extra.get('slow', 26)
            signal = extra.get('signal', 9)
            return IndicatorUtils.macd_hist(values, fast, slow, signal)
        raise ValueError(f"未知指标: {indicator}")

    def compute_future_return(self, close_prices: List[float], index: int, future_bars: int) -> Optional[float]:
        if index + future_bars >= len(close_prices):
            return None
        entry = close_prices[index]
        exit_price = close_prices[index + future_bars]
        if entry == 0:
            return None
        return (exit_price / entry) - 1.0

    def collect_signals(self,
                        symbol: str,
                        indicator: str,
                        period: int,
                        future_bars: int,
                        start_date: Optional[str] = None,
                        csv_out: Optional[str] = None) -> List[Dict]:
        df_5m_raw = self.load_5m_data(symbol)
        df_60m_raw = self.load_60m_data(symbol)

        if not df_5m_raw or not df_60m_raw:
            raise RuntimeError(f"{symbol} 的 5m 或 60m 数据不足")

        close_values = [row[4] for row in df_5m_raw]
        indicator_series = self.compute_indicator(indicator, close_values, period, {})

        df_5m = MACDCalculator.calculate(df_5m_raw)
        df_5m = ATRCalculator.calculate(df_5m, period=14)
        df_5m, green_stacks_5m, green_gaps_5m = StackIdentifier.identify(df_5m)

        df_60m = MACDCalculator.calculate(df_60m_raw)
        df_60m, green_stacks_60m, green_gaps_60m = StackIdentifier.identify(df_60m)

        index_map = self.loader.load_kline_fast(symbol, self.config.DURATION_5M, None)
        index_map = IndexMapper.precompute_60m_index(df_5m, df_60m)

        symbol_info = self.loader.get_symbol_info(symbol)
        strategy = StrategyLowLowUp(symbol_info or {})

        signals: List[Dict] = []

        for i, row in enumerate(df_5m):
            time_str = row[0][:19]
            if start_date and not time_str.startswith(start_date):
                continue
            idx_60m = index_map[i] if i < len(index_map) else len(df_60m) - 1
            if idx_60m < 4:
                continue

            hist_60m = df_60m[idx_60m][8]
            hist_60m_prev = df_60m[idx_60m - 1][8] if idx_60m > 0 else 0
            if hist_60m < 0:
                dif_turn, _ = strategy.check_60m_dif_turn_in_green(df_60m, idx_60m, green_stacks_60m)
                if dif_turn:
                    diver_ok, _, _, _ = strategy.check_60m_divergence(df_60m, idx_60m)
                    if diver_ok:
                        cond_5m, _ = strategy.check_5m_entry(df_5m, i, green_stacks_5m)
                        if cond_5m:
                            indicator_value = indicator_series[i]
                            future_return = self.compute_future_return(close_values, i, future_bars)
                            if indicator_value is not None and future_return is not None:
                                signals.append({
                                    'time': time_str,
                                    'type': '绿柱堆 DIF 拐头',
                                    'price': row[4],
                                    'indicator_value': indicator_value,
                                    'future_return': future_return,
                                })

            if hist_60m > 0 and hist_60m_prev < 0:
                diver_ok, _, _, _ = strategy.check_60m_divergence(df_60m, idx_60m)
                if diver_ok:
                    cond_5m, _ = strategy.check_5m_entry(df_5m, i, green_stacks_5m)
                    if cond_5m:
                        indicator_value = indicator_series[i]
                        future_return = self.compute_future_return(close_values, i, future_bars)
                        if indicator_value is not None and future_return is not None:
                            signals.append({
                                'time': time_str,
                                'type': '红转绿',
                                'price': row[4],
                                'indicator_value': indicator_value,
                                'future_return': future_return,
                            })

            if hist_60m > 0 and hist_60m_prev > 0:
                dif_turn_red, _ = strategy.check_60m_dif_turn_in_red(df_60m, idx_60m)
                if dif_turn_red:
                    diver_ok, _, _, _ = strategy.check_60m_bottom_rise_in_red(df_60m, idx_60m)
                    if diver_ok:
                        cond_5m, _ = strategy.check_5m_entry(df_5m, i, green_stacks_5m)
                        if cond_5m:
                            indicator_value = indicator_series[i]
                            future_return = self.compute_future_return(close_values, i, future_bars)
                            if indicator_value is not None and future_return is not None:
                                signals.append({
                                    'time': time_str,
                                    'type': '红柱堆 DIF 拐头',
                                    'price': row[4],
                                    'indicator_value': indicator_value,
                                    'future_return': future_return,
                                })

        if csv_out:
            with open(csv_out, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['time', 'type', 'price', 'indicator_value', 'future_return'])
                writer.writeheader()
                writer.writerows(signals)

        return signals

    def analyze(self, signals: List[Dict]) -> Dict:
        if not signals:
            return {}

        sorted_signals = sorted(signals, key=lambda x: x['indicator_value'])
        total_return = sum(item['future_return'] for item in sorted_signals)
        win_rate = sum(1 for item in sorted_signals if item['future_return'] > 0) / len(sorted_signals)
        avg_return = total_return / len(sorted_signals)
        positive_mean = sum(item['future_return'] for item in sorted_signals if item['future_return'] > 0) / max(1, sum(1 for item in sorted_signals if item['future_return'] > 0))
        negative_mean = sum(item['future_return'] for item in sorted_signals if item['future_return'] <= 0) / max(1, sum(1 for item in sorted_signals if item['future_return'] <= 0))

        cum = 0.0
        best_start = 0
        best_end = 0
        best_value = float('-inf')
        cum_values = [0.0]
        for item in sorted_signals:
            cum += item['future_return']
            cum_values.append(cum)
            if cum > best_value:
                best_value = cum
                best_end = len(cum_values) - 1

        best_start = 0
        if best_end > 0:
            best_start = next((i for i, x in enumerate(cum_values) if x == min(cum_values[:best_end + 1])), 0)

        return {
            'count': len(sorted_signals),
            'total_return': total_return,
            'avg_return': avg_return,
            'win_rate': win_rate,
            'positive_mean': positive_mean,
            'negative_mean': negative_mean,
            'best_range': (best_start, best_end),
            'best_value': best_value,
            'sorted_signals': sorted_signals,
        }


def parse_args():
    parser = argparse.ArgumentParser(description='回测指标质量分析')
    parser.add_argument('--symbol', required=True, help='合约代码，例如 CU.SHF')
    parser.add_argument('--indicator', default='rsi', choices=['rsi', 'sma', 'ema', 'macd_hist'], help='要评估的指标')
    parser.add_argument('--period', type=int, default=14, help='指标周期')
    parser.add_argument('--future-bars', type=int, default=12, help='计算未来收益时的K线数')
    parser.add_argument('--start-date', type=str, default=None, help='开始日期，例如 2026-01-01')
    parser.add_argument('--csv-out', type=str, default=None, help='将信号输出到CSV')
    parser.add_argument('--db-path', type=str, default=None, help='SQLite 数据库路径')
    parser.add_argument('--contracts-path', type=str, default=None, help='主力合约文件路径')
    return parser.parse_args()


def main():
    args = parse_args()
    db_path = args.db_path or Config.DB_PATH
    contracts_path = args.contracts_path or Config.CONTRACTS_PATH

    if not os.path.exists(db_path):
        raise FileNotFoundError(f"数据库未找到: {db_path}")
    if not os.path.exists(contracts_path):
        raise FileNotFoundError(f"主力合约文件未找到: {contracts_path}")

    analyzer = IndicatorAnalyzer(db_path, contracts_path)
    signals = analyzer.collect_signals(
        symbol=args.symbol,
        indicator=args.indicator,
        period=args.period,
        future_bars=args.future_bars,
        start_date=args.start_date,
        csv_out=args.csv_out,
    )
    stats = analyzer.analyze(signals)

    print(f"\nSymbol: {args.symbol}")
    print(f"Indicator: {args.indicator} period={args.period}")
    print(f"Samples: {stats.get('count', 0)}")
    if stats:
        print(f"Total future return: {stats['total_return']:.4f}")
        print(f"Average future return: {stats['avg_return']:.4f}")
        print(f"Win rate: {stats['win_rate']:.2%}")
        print(f"Positive mean: {stats['positive_mean']:.4f}")
        print(f"Negative mean: {stats['negative_mean']:.4f}")
        print(f"Best sorted range: {stats['best_range']} with cumulative {stats['best_value']:.4f}")

    if args.csv_out:
        print(f"CSV saved to {args.csv_out}")


if __name__ == '__main__':
    main()
