#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多周期波动率压缩策略扫描器
数据源：本地 SQLite 数据库
"""

import os
import sqlite3
import math
from datetime import datetime
from collections import defaultdict

# 获取项目根目录
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "db", "kline_data.db")

class VolatilityScanner:
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)
        self.conn.row_factory = sqlite3.Row
        
        # 策略参数
        self.params = {
            'ma60_pullback_depth': 0.03,      # 60 分钟回调深度 3%
            'vol_compression_threshold': 0.005,  # 5 分钟布林带宽阈值 0.5%
            'volume_multiplier': 1.5,          # 成交量放大倍数
            'adx_threshold': 25,               # ADX 趋势强度
            'avg_amplitude_threshold': 0.005,  # 平均振幅阈值 0.5%
        }
    
    def fetch_kline(self, symbol: str, duration: int, count: int = 100):
        """从数据库获取 K 线数据"""
        query = """
            SELECT datetime, open, high, low, close, volume, close_oi, vwap
            FROM kline_data
            WHERE symbol = ? AND duration = ?
            ORDER BY datetime DESC
            LIMIT ?
        """
        cursor = self.conn.execute(query, (symbol, duration, count))
        rows = cursor.fetchall()
        
        if not rows:
            return None
        
        # 反转成时间升序
        klines = []
        for row in reversed(rows):
            klines.append({
                'time': row['datetime'],
                'open': row['open'],
                'close': row['close'],
                'high': row['high'],
                'low': row['low'],
                'volume': row['volume'],
                'oi': row['close_oi'],
                'vwap': row['vwap']
            })
        
        return klines
    
    def calc_ma(self, data: list, period: int) -> list:
        """计算移动平均线"""
        result = []
        for i in range(len(data)):
            if i < period - 1:
                result.append(None)
            else:
                avg = sum(d['close'] for d in data[i-period+1:i+1]) / period
                result.append(avg)
        return result
    
    def calc_bb_width(self, data: list, period: int = 20) -> list:
        """计算布林带带宽"""
        mid = self.calc_ma(data, period)
        width = []
        
        for i in range(len(data)):
            if i < period - 1 or mid[i] is None:
                width.append(None)
            else:
                variance = sum((data[j]['close'] - mid[i])**2 
                              for j in range(i-period+1, i+1)) / period
                std = math.sqrt(variance)
                upper = mid[i] + 2 * std
                lower = mid[i] - 2 * std
                width.append((upper - lower) / mid[i])
        
        return width
    
    def calc_atr(self, data: list, period: int = 14) -> list:
        """计算 ATR"""
        atr = []
        tr = []
        
        for i in range(len(data)):
            if i == 0:
                tr.append(data[i]['high'] - data[i]['low'])
            else:
                hl = data[i]['high'] - data[i]['low']
                hpc = abs(data[i]['high'] - data[i-1]['close'])
                lpc = abs(data[i]['low'] - data[i-1]['close'])
                tr.append(max(hl, hpc, lpc))
        
        for i in range(len(data)):
            if i < period - 1:
                atr.append(None)
            elif i == period - 1:
                atr.append(sum(tr[:period]) / period)
            else:
                atr.append((atr[-1] * (period - 1) + tr[i]) / period)
        
        return atr
    
    def calc_atr_percentile(self, data: list, atr: list, period: int = 100) -> float:
        """计算当前 ATR 的历史百分位"""
        if len(atr) < period or atr[-1] is None:
            return None
        
        valid_atr = [a for a in atr[-period:] if a is not None]
        if len(valid_atr) < 10:
            return None
        
        current = atr[-1]
        rank = sum(1 for a in valid_atr if a <= current)
        return rank / len(valid_atr)
    
    def check_60min_trend(self, data_60: list) -> dict:
        """检查 60 分钟趋势条件"""
        if len(data_60) < 50:
            return None
        
        ma20 = self.calc_ma(data_60, 20)
        ma50 = self.calc_ma(data_60, 50)
        ma200 = self.calc_ma(data_60, 200)
        
        if not all([ma20[-1], ma50[-1], ma200[-1]]):
            return None
        
        # 趋势条件
        above_ma200 = data_60[-1]['close'] > ma200[-1]
        ma_bullish = ma20[-1] > ma50[-1]
        
        # 高点上移
        recent_highs = [d['high'] for d in data_60[-20:]]
        highs_rising = recent_highs[-1] > recent_highs[0] * 1.02
        
        trend_score = sum([above_ma200, ma_bullish, highs_rising])
        if trend_score < 2:
            return None
        
        # 回调条件
        recent_high = max(d['high'] for d in data_60[-20:-1])
        current = data_60[-1]['close']
        depth = (recent_high - current) / recent_high
        
        recent_closes = [d['close'] for d in data_60[-5:]]
        down_pressure = recent_closes[-1] < recent_closes[0]
        
        if not (depth >= self.params['ma60_pullback_depth'] and down_pressure):
            return None
        
        return {
            'trend_score': trend_score,
            'depth': depth,
            'above_ma200': above_ma200,
            'ma_bullish': ma_bullish
        }
    
    def check_5min_compression(self, data_5: list) -> dict:
        """检查 5 分钟波动率压缩"""
        if len(data_5) < 30:
            return None
        
        # 布林带带宽
        bb_width = self.calc_bb_width(data_5)
        if not bb_width[-1]:
            return None
        
        compressed = bb_width[-1] < self.params['vol_compression_threshold']
        
        # 平均振幅
        recent_ranges = [(data_5[i]['high'] - data_5[i]['low']) / data_5[i]['close'] 
                        for i in range(-10, 0)]
        avg_range = sum(recent_ranges) / 10
        range_ok = avg_range < self.params['avg_amplitude_threshold']
        
        # ATR 百分位
        atr = self.calc_atr(data_5)
        atr_pct = self.calc_atr_percentile(data_5, atr)
        atr_ok = atr_pct is not None and atr_pct < 0.2
        
        return {
            'compressed': compressed,
            'bb_width': bb_width[-1],
            'avg_range': avg_range,
            'range_ok': range_ok,
            'atr_percentile': atr_pct,
            'atr_ok': atr_ok
        }
    
    def check_breakout(self, data_5: list) -> dict:
        """检查突破信号"""
        if len(data_5) < 20:
            return None
        
        bb_width = self.calc_bb_width(data_5)
        if not bb_width[-1]:
            return None
        
        mid = self.calc_ma(data_5, 20)[-1]
        variance = sum((data_5[j]['close'] - mid)**2 
                      for j in range(-20, 0)) / 20
        std = math.sqrt(variance)
        upper = mid + 2 * std
        
        latest = data_5[-1]
        prev = data_5[-2]
        
        # 突破上轨
        breakout = latest['close'] > upper
        
        # 成交量放大
        vol_ma = sum(d['volume'] for d in data_5[-10:]) / 10
        volume_ok = latest['volume'] > vol_ma * self.params['volume_multiplier']
        volume_ratio = latest['volume'] / vol_ma if vol_ma > 0 else 0
        
        # 价格突破前高
        price_breakout = latest['close'] > prev['high']
        
        return {
            'breakout': breakout,
            'price_breakout': price_breakout,
            'volume_ok': volume_ok,
            'volume_ratio': volume_ratio,
            'bb_upper': upper,
            'latest_close': latest['close']
        }
    
    def scan_symbol(self, symbol: str) -> dict:
        """扫描单个合约"""
        # 获取数据
        data_60 = self.fetch_kline(symbol, 3600, 100)
        data_5 = self.fetch_kline(symbol, 300, 100)
        
        if not data_60 or not data_5:
            return None
        
        # 检查 60 分钟趋势
        trend = self.check_60min_trend(data_60)
        if not trend:
            return None
        
        # 检查 5 分钟压缩
        compression = self.check_5min_compression(data_5)
        if not compression:
            return None
        
        # 检查是否压缩完成
        if not (compression['compressed'] and compression['range_ok']):
            return None
        
        # 检查突破
        breakout = self.check_breakout(data_5)
        
        result = {
            'symbol': symbol,
            'price': data_5[-1]['close'],
            'trend_depth': trend['depth'],
            'bb_width': compression['bb_width'],
            'avg_range': compression['avg_range'],
            'atr_percentile': compression['atr_percentile'],
        }
        
        if breakout and breakout['price_breakout'] and breakout['volume_ok']:
            result['signal'] = 'BUY'
            result['volume_ratio'] = breakout['volume_ratio']
            result['breakout_price'] = breakout['bb_upper']
        else:
            result['signal'] = 'WATCH'
            result['volume_ratio'] = breakout['volume_ratio'] if breakout else 0
        
        return result
    
    def get_all_symbols(self) -> list:
        """获取所有可用合约"""
        query = "SELECT DISTINCT symbol FROM kline_data ORDER BY symbol"
        cursor = self.conn.execute(query)
        return [row['symbol'] for row in cursor.fetchall()]
    
    def scan_all(self, symbols: list = None):
        """扫描所有或部分合约"""
        if symbols is None:
            symbols = self.get_all_symbols()
        
        print(f"\n🔍 开始扫描 {len(symbols)} 个合约...\n")
        
        buy_signals = []
        watch_list = []
        
        for i, symbol in enumerate(symbols):
            print(f"[{i+1}/{len(symbols)}] {symbol}...", end=" ", flush=True)
            
            result = self.scan_symbol(symbol)
            
            if result:
                if result['signal'] == 'BUY':
                    buy_signals.append(result)
                    print(f"✅ BUY!")
                else:
                    watch_list.append(result)
                    print(f"👀 WATCH")
            else:
                print("❌")
        
        # 输出结果
        self.print_results(buy_signals, watch_list)
        
        return buy_signals, watch_list
    
    def print_results(self, buy_signals: list, watch_list: list):
        """打印扫描结果"""
        print(f"\n{'='*80}")
        print(f"📊 扫描完成 - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print(f"{'='*80}\n")
        
        if buy_signals:
            print(f"🚀 突破信号 ({len(buy_signals)}个):")
            print(f"{'合约':<20} {'价格':>12} {'回调':>8} {'带宽':>10} {'量比':>8} {'ATR%':>8}")
            print(f"{'-'*80}")
            for r in sorted(buy_signals, key=lambda x: x['volume_ratio'], reverse=True):
                atr_str = f"{r['atr_percentile']*100:>7.1f}%" if r['atr_percentile'] else "N/A"
                print(f"{r['symbol']:<20} {r['price']:>12.2f} {r['trend_depth']*100:>7.1f}% "
                      f"{r['bb_width']*100:>9.2f}% {r['volume_ratio']:>7.1f}x {atr_str}")
        else:
            print("🚀 突破信号：无\n")
        
        if watch_list:
            print(f"\n👀 观察列表 ({len(watch_list)}个):")
            print(f"{'合约':<20} {'价格':>12} {'回调':>8} {'带宽':>10} {'ATR%':>10}")
            print(f"{'-'*80}")
            for r in sorted(watch_list, key=lambda x: x['bb_width'])[:15]:
                atr_str = f"{r['atr_percentile']*100:>9.1f}%" if r['atr_percentile'] else "N/A"
                print(f"{r['symbol']:<20} {r['price']:>12.2f} {r['trend_depth']*100:>7.1f}% "
                      f"{r['bb_width']*100:>9.2f}% {atr_str}")
        
        print(f"\n{'='*80}\n")


def main():
    scanner = VolatilityScanner()
    
    # 获取所有合约
    symbols = scanner.get_all_symbols()
    print(f"数据库中共有 {len(symbols)} 个合约")
    
    # 扫描
    scanner.scan_all(symbols)


if __name__ == "__main__":
    main()
