# @Project: https://github.com/Jedore/ctp.examples
# @File:    BreakoutSignalDetector.py
# @Time:    2026/03/13
# @Author:  Assistant
# @Description: 波动率挤压突破信号检测器
# 
# 检测逻辑：
# 1. volatility_squeeze_breakout: 检测波动率挤压后的突破
# 2. candlestick_confirm: K 线形态确认
# 3. time_filter_breakout: 时间过滤器确认突破有效性

import pandas as pd
import numpy as np
from datetime import datetime, time
from typing import Dict, Optional, Tuple
import json
import os
import logging

# ==================== 配置参数 ====================
# 波动率挤压检测参数
BB_PERIOD = 20           # 布林带周期
BB_STD_DEV = 2.0         # 布林带标准差倍数
BB_WIDTH_PERCENTILE = 20 # 布林带宽度百分位阈值（低于 20% 分位认为是挤压）
ATR_PERIOD = 14          # ATR 周期
ATR_RATIO_THRESHOLD = 0.5 # ATR 比率阈值（当前 ATR/过去 N 根 K 线平均 ATR）

# 突破检测参数
BREAKOUT_LOOKBACK = 10   # 突破检测看多少根 K 线的高低点
MIN_BREAKOUT_STRENGTH = 0.01  # 最小突破强度（1%）

# K 线确认参数
MIN_BODY_RATIO = 0.5     # 最小实体比例（50%）
MAX_SHADOW_RATIO = 0.3   # 最大影线比例（30%）
MIN_VOLUME_RATIO = 1.2   # 最小成交量比率（突破时成交量/平均成交量）

# 时间过滤参数（夜盘时段）
NIGHT_TRADING_START = time(21, 0)   # 夜盘开始时间
NIGHT_TRADING_END = time(2, 30)     # 夜盘结束时间（次日）
DAY_TRADING_START = time(9, 0)      # 日盘开始时间
DAY_TRADING_END = time(15, 30)      # 日盘结束时间

# 信号配置
SIGNAL_FILE = "breakout_signals.json"
SIGNAL_COOLDOWN = 1800  # 信号冷却时间（秒）= 30 分钟

# 日志配置
logger = logging.getLogger(__name__)


def log(*args, **kwargs):
    """日志输出"""
    message = ' '.join(str(arg) for arg in args)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")


class BreakoutSignalDetector:
    """突破信号检测器"""
    
    def __init__(self, signal_file=SIGNAL_FILE):
        self.signal_file = signal_file
        self.signals = self._load_signals()
    
    def _load_signals(self) -> dict:
        """加载信号文件"""
        if os.path.exists(self.signal_file):
            try:
                with open(self.signal_file, 'r', encoding='utf-8') as f:
                    signals = json.load(f)
                return signals
            except Exception as e:
                log(f"加载信号文件失败：{e}")
                return {}
        return {}
    
    def _save_signals(self):
        """保存信号文件"""
        try:
            with open(self.signal_file, 'w', encoding='utf-8') as f:
                json.dump(self.signals, f, indent=2, ensure_ascii=False)
            log(f"保存信号文件：{self.signal_file}")
        except Exception as e:
            log(f"保存信号文件失败：{e}")
    
    def can_generate_signal(self, symbol: str) -> bool:
        """检查是否可以生成信号（冷却时间内不重复）"""
        if symbol not in self.signals:
            return True
        last_time = self.signals[symbol].get('last_signal_time')
        if not last_time:
            return True
        try:
            last_dt = datetime.fromisoformat(last_time)
            elapsed = (datetime.now() - last_dt).total_seconds()
            return elapsed >= SIGNAL_COOLDOWN
        except Exception:
            return True
    
    def add_signal(self, symbol: str, signal_type: str, direction: str, details: dict):
        """添加信号"""
        # 将 numpy 类型转换为 Python 原生类型以便 JSON 序列化
        def convert_to_native(obj):
            if isinstance(obj, (np.bool_, np.integer)):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, dict):
                return {k: convert_to_native(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_to_native(v) for v in obj]
            elif isinstance(obj, (bool, int, float, str, type(None))):
                return obj
            else:
                return str(obj)
        
        self.signals[symbol] = {
            'symbol': symbol,
            'signal_type': signal_type,
            'direction': direction,
            'last_signal_time': datetime.now().isoformat(),
            'details': convert_to_native(details),
            'trigger_count': self.signals.get(symbol, {}).get('trigger_count', 0) + 1
        }
        self._save_signals()
        log(f"🚀 {symbol} 突破信号：{direction} - {signal_type}")
    
    def detect(self, df: pd.DataFrame, symbol: str = None) -> Optional[Dict]:
        """
        执行完整的突破信号检测流程
        
        Args:
            df: K 线数据 DataFrame，包含 columns: datetime, open, high, low, close, volume
            symbol: 合约代码
        
        Returns:
            dict: 信号详情，如果没有信号则返回 None
        """
        if len(df) < BB_PERIOD + BREAKOUT_LOOKBACK + 20:
            return None
        
        # 1. 波动率挤压突破检测
        squeeze_result = self.volatility_squeeze_breakout(df)
        if not squeeze_result['is_squeeze_breakout']:
            return None
        
        # 2. K 线形态确认
        candle_result = self.candlestick_confirm(df)
        if not candle_result['is_confirmed']:
            return None
        
        # 3. 时间过滤器确认
        time_result = self.time_filter_breakout(df)
        if not time_result['is_valid_time']:
            return None
        
        # 所有检测通过，生成信号
        direction = squeeze_result.get('direction', 'unknown')
        signal_details = {
            'squeeze': squeeze_result,
            'candlestick': candle_result,
            'time_filter': time_result,
            'direction': direction,
            'timestamp': datetime.now().isoformat()
        }
        
        # 检查冷却时间并保存信号
        if self.can_generate_signal(symbol):
            self.add_signal(symbol, 'volatility_breakout', direction, signal_details)
            return signal_details
        
        return None
    
    def volatility_squeeze_breakout(self, df: pd.DataFrame, lookback: int = 20) -> Dict:
        """
        波动率压缩后突破 - 真突破概率更高
        
        逻辑：
        1. 计算 ATR，判断波动率是否处于 60 周期低位（压缩）
        2. 波动率扩张：突破时 ATR 开始上升
        3. 价格突破 N 日高低点
        4. 成交量确认：突破时成交量 > 1.5 * 20 周期均量
        
        Args:
            df: K 线数据 DataFrame
            lookback: 价格突破看多少周期的高低点，默认 20
        
        Returns:
            dict: {
                'is_squeeze': bool,  # 是否处于挤压状态
                'is_breakout': bool,  # 是否突破
                'is_squeeze_breakout': bool,  # 是否是挤压后的突破
                'direction': str,  # 突破方向：'long' 或 'short'
                'atr_percentile': float,  # ATR 百分位
                'atr_expanding': bool,  # ATR 是否扩张
                'breakout_strength': float,  # 突破强度
                'vol_confirmed': bool  # 成交量是否确认
            }
        """
        result = {
            'is_squeeze': False,
            'is_breakout': False,
            'is_squeeze_breakout': False,
            'direction': None,
            'atr_percentile': 0.0,
            'atr_expanding': False,
            'breakout_strength': 0.0,
            'vol_confirmed': False
        }
        
        if len(df) < 60:
            return result
        
        df = df.copy()
        
        # 1. 计算 ATR（使用 high - low 简化计算）
        df['atr'] = (df['high'] - df['low']).rolling(14).mean()
        
        # 2. 计算 ATR 百分位（60 周期内）
        def calc_percentile(x):
            if x.max() == x.min():
                return 0.5
            return (x.iloc[-1] - x.min()) / (x.max() - x.min())
        
        df['atr_percentile'] = df['atr'].rolling(60).apply(calc_percentile)
        
        # 3. 波动率压缩：ATR 处于底部 30%
        is_squeeze = df['atr_percentile'].iloc[-1] < 0.3
        result['is_squeeze'] = is_squeeze
        result['atr_percentile'] = float(df['atr_percentile'].iloc[-1]) if not pd.isna(df['atr_percentile'].iloc[-1]) else 0.0
        
        # 4. 波动率扩张：突破时 ATR 开始上升（当前 ATR > 5 周期前 ATR）
        atr_expanding = df['atr'].iloc[-1] > df['atr'].shift(5).iloc[-1]
        result['atr_expanding'] = bool(atr_expanding) if not pd.isna(atr_expanding) else False
        
        # 5. 价格突破 N 周期高低点
        df['price_high'] = df['high'].rolling(lookback).max().shift(1)
        df['price_low'] = df['low'].rolling(lookback).min().shift(1)
        
        current_close = df['close'].iloc[-1]
        price_high = df['price_high'].iloc[-1]
        price_low = df['price_low'].iloc[-1]
        
        # 6. 成交量确认
        df['vol_ma'] = df['volume'].rolling(20).mean()
        vol_confirmed = df['volume'].iloc[-1] > 1.5 * df['vol_ma'].iloc[-1]
        result['vol_confirmed'] = bool(vol_confirmed)
        
        # 7. 检测突破
        upside_breakout = current_close > price_high
        downside_breakout = current_close < price_low
        
        # 突破强度
        if upside_breakout and price_high > 0:
            result['breakout_strength'] = float((current_close - price_high) / price_high)
        elif downside_breakout and price_low > 0:
            result['breakout_strength'] = float((price_low - current_close) / price_low)
        
        # 8. 综合信号：压缩 + 突破 + ATR 扩张 + 放量
        if upside_breakout:
            result['is_breakout'] = True
            if is_squeeze and atr_expanding and vol_confirmed:
                result['is_squeeze_breakout'] = True
                result['direction'] = 'long'
        
        if downside_breakout:
            result['is_breakout'] = True
            if is_squeeze and atr_expanding and vol_confirmed:
                result['is_squeeze_breakout'] = True
                result['direction'] = 'short'
        
        return result
    
    def candlestick_confirm(self, df: pd.DataFrame) -> Dict:
        """
        突破时要求强势 K 线形态
        
        检测最后一根 K 线是否是强势 K 线（大阳线或大阴线）
        强势 K 线：实体占整根 K 线 70% 以上
        
        Args:
            df: K 线数据 DataFrame
        
        Returns:
            dict: {
                'is_confirmed': bool,  # 是否确认
                'candle_type': str,  # K 线类型：'yang' (阳线), 'yin' (阴线), 'neutral' (中性)
                'body_ratio': float,  # 实体比例
                'is_strong_candle': bool  # 是否是强势 K 线
            }
        """
        result = {
            'is_confirmed': False,
            'candle_type': 'neutral',
            'body_ratio': 0.0,
            'is_strong_candle': False
        }
        
        if len(df) < 2:
            return result
        
        df = df.copy()
        
        # 获取最后一根 K 线
        open_price = df['open'].iloc[-1]
        close_price = df['close'].iloc[-1]
        high_price = df['high'].iloc[-1]
        low_price = df['low'].iloc[-1]
        
        # 计算实体和 K 线范围
        body = abs(close_price - open_price)
        candle_range = high_price - low_price
        
        if candle_range <= 0:
            return result
        
        # 计算实体比例
        body_ratio = body / candle_range
        result['body_ratio'] = float(body_ratio)
        
        # 判断 K 线类型
        if close_price > open_price:
            result['candle_type'] = 'yang'
        elif close_price < open_price:
            result['candle_type'] = 'yin'
        else:
            result['candle_type'] = 'neutral'
        
        # 强势阳线/阴线：实体占整根 K 线 70% 以上
        is_strong_bull = (close_price > open_price) and (body_ratio > 0.7)
        is_strong_bear = (close_price < open_price) and (body_ratio > 0.7)
        is_strong_candle = is_strong_bull or is_strong_bear
        
        result['is_strong_candle'] = is_strong_candle
        result['is_confirmed'] = is_strong_candle
        
        return result
    
    def time_filter_breakout(self, df: pd.DataFrame) -> Dict:
        """
        只在高波动时段交易突破
        期货：开盘后 1 小时 + 夜盘开盘
        
        定义高波动时段（根据国内期货时间调整）：
        - 日盘：9:00-10:30, 13:30-15:00
        - 夜盘：21:00-23:00 (部分品种到 2:30)
        
        Args:
            df: K 线数据 DataFrame，需要包含 datetime 列
        
        Returns:
            dict: {
                'is_valid_time': bool,  # 是否是有效时间（高波动时段）
                'current_time': str,  # 当前时间
                'trading_session': str,  # 交易时段：'morning', 'afternoon', 'night', 'inactive'
                'is_active_session': bool  # 是否是活跃时段
            }
        """
        result = {
            'is_valid_time': False,
            'current_time': '',
            'trading_session': 'inactive',
            'is_active_session': False
        }
        
        if len(df) == 0 or 'datetime' not in df.columns:
            return result
        
        # 获取最后一根 K 线的时间
        last_datetime = df['datetime'].iloc[-1]
        
        # 如果是字符串，转换为 datetime
        if isinstance(last_datetime, str):
            last_datetime = pd.to_datetime(last_datetime)
        
        # 获取小时
        if hasattr(last_datetime, 'hour'):
            current_hour = last_datetime.hour
        else:
            current_hour = datetime.now().hour
        
        if hasattr(last_datetime, 'time'):
            current_time = last_datetime.time()
        else:
            current_time = datetime.now().time()
        
        result['current_time'] = current_time.strftime('%H:%M:%S')
        
        # 定义高波动时段（根据国内期货时间调整）
        # 日盘：9:00-10:30, 13:30-15:00
        # 夜盘：21:00-23:00 (部分品种到 2:30)
        is_morning_session = (current_hour == 9) or (current_hour == 10)  # 早盘 9:00-10:59
        is_afternoon_session = (current_hour == 13) or (current_hour == 14)  # 午盘 13:00-14:59
        is_night_session = (current_hour >= 21) and (current_hour <= 23)  # 夜盘 21:00-23:59
        
        is_active_session = is_morning_session or is_afternoon_session or is_night_session
        
        if is_morning_session:
            result['trading_session'] = 'morning'
        elif is_afternoon_session:
            result['trading_session'] = 'afternoon'
        elif is_night_session:
            result['trading_session'] = 'night'
        else:
            result['trading_session'] = 'inactive'
        
        result['is_active_session'] = is_active_session
        result['is_valid_time'] = is_active_session
        
        return result


def check_breakout_signal(df: pd.DataFrame, symbol: str = None, detector: BreakoutSignalDetector = None) -> Optional[Dict]:
    """
    检查突破信号的便捷函数
    
    Args:
        df: K 线数据 DataFrame
        symbol: 合约代码
        detector: 检测器实例，如果为 None 则创建新实例
    
    Returns:
        dict: 信号详情，如果没有信号则返回 None
    """
    if detector is None:
        detector = BreakoutSignalDetector()
    
    return detector.detect(df, symbol)


# 测试代码
if __name__ == '__main__':
    import sqlite3
    
    # 从数据库获取真实 K 线数据
    db_path = "kline_data.db"
    
    # 指定合约和周期
    test_symbol = "CZCE.CF605"
    test_duration = 300  # 5 分钟 K 线
    
    
    # 从数据库读取真实数据
    print(f"从数据库 {db_path} 读取数据...")
    print(f"合约：{test_symbol}, 周期：{test_duration}秒 (5 分钟)")
    conn = sqlite3.connect(db_path)
    
    # 获取指定合约的 5 分钟 K 线数据
    query = """
        SELECT datetime, open, high, low, close, volume, close_oi
        FROM kline_data 
        WHERE symbol = ? AND duration = ?
        ORDER BY datetime DESC 
        LIMIT 500
    """
    df = pd.read_sql_query(query, conn, params=(test_symbol, test_duration))
    conn.close()
    
    if len(df) > 0:
        print(f"读取到 {len(df)} 条 K 线数据")
        # 解析时间（使用 ISO8601 格式支持带微秒的时间）
        df['datetime'] = pd.to_datetime(df['datetime'], format='ISO8601')
        # 重命名 close_oi 为 open_interest（如果需要）
        if 'close_oi' in df.columns:
            df = df.rename(columns={'close_oi': 'open_interest'})
        # 按时间升序排列
        df = df.sort_values('datetime').reset_index(drop=True)
    else:
        print(f"数据库中没有 {test_symbol} 的 5 分钟 K 线数据")
        print("尝试获取任意合约数据...")
        query = """
            SELECT datetime, open, high, low, close, volume, close_oi, symbol
            FROM kline_data 
            WHERE duration = 300
            ORDER BY datetime DESC 
            LIMIT 1000
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        if len(df) > 0:
            print(f"读取到 {len(df)} 条 K 线数据")
            df['datetime'] = pd.to_datetime(df['datetime'], format='ISO8601')
            if 'close_oi' in df.columns:
                df = df.rename(columns={'close_oi': 'open_interest'})
            df = df.sort_values('datetime').reset_index(drop=True)
        else:
            print("数据库中没有数据，使用模拟数据测试...")
            np.random.seed(42)
            n = 200
            dates = pd.date_range(start='2026-01-01', periods=n, freq='5min')
            base_price = 100
            prices = [base_price]
            for i in range(1, n):
                if i < 100:
                    change = np.random.normal(0, 0.001)
                elif i < 120:
                    change = np.random.normal(0.005, 0.005)
                else:
                    change = np.random.normal(0, 0.003)
                prices.append(prices[-1] * (1 + change))
            prices = np.array(prices)
            df = pd.DataFrame({
                'datetime': dates,
                'open': prices * (1 + np.random.normal(0, 0.0005, n)),
                'high': prices * (1 + np.abs(np.random.normal(0, 0.002, n))),
                'low': prices * (1 - np.abs(np.random.normal(0, 0.002, n))),
                'close': prices,
                'volume': np.random.randint(100, 1000, n) * 10
            })
            df['high'] = df[['open', 'high', 'close']].max(axis=1)
            df['low'] = df[['open', 'low', 'close']].min(axis=1)
            df.loc[100:120, 'volume'] = df.loc[100:120, 'volume'] * 2
    
    # 创建检测器
    detector = BreakoutSignalDetector()
    
    print(f"\n开始遍历 {len(df)} 条 K 线检测信号...")
    print("=" * 60)
    
    # 遍历所有 K 线，模拟真实检测过程
    # 从第 65 根 K 线开始（需要足够的数据计算 ATR 百分位）
    signals_found = []
    condition_stats = {
        'squeeze': 0,
        'breakout': 0,
        'atr_expanding': 0,
        'vol_confirmed': 0,
        'strong_candle': 0,
        'active_time': 0
    }
    
    for i in range(65, len(df)):
        # 取前 i+1 根 K 线作为历史数据
        subset_df = df.iloc[:i+1].copy()
        
        # 分别检测各个条件
        squeeze_result = detector.volatility_squeeze_breakout(subset_df)
        candle_result = detector.candlestick_confirm(subset_df)
        time_result = detector.time_filter_breakout(subset_df)
        
        # 统计各条件满足次数
        if squeeze_result['is_squeeze']:
            condition_stats['squeeze'] += 1
        if squeeze_result['is_breakout']:
            condition_stats['breakout'] += 1
        if squeeze_result['atr_expanding']:
            condition_stats['atr_expanding'] += 1
        if squeeze_result['vol_confirmed']:
            condition_stats['vol_confirmed'] += 1
        if candle_result['is_strong_candle']:
            condition_stats['strong_candle'] += 1
        if time_result['is_active_session']:
            condition_stats['active_time'] += 1
        
        # 调用 detect 检测（只检测最后一根 K 线）
        result = detector.detect(subset_df, symbol='TEST_HISTORICAL')
        
        if result:
            current_time = subset_df['datetime'].iloc[-1]
            signals_found.append({
                'index': i,
                'datetime': current_time,
                'direction': result['direction'],
                'close': subset_df['close'].iloc[-1]
            })
            print(f"\n🚀 在 K 线 [{i}] {current_time} 检测到突破信号！")
            print(f"   方向：{result['direction']}")
            print(f"   收盘价：{subset_df['close'].iloc[-1]}")
            print(f"   ATR 百分位：{result['squeeze']['atr_percentile']:.2%}")
            print(f"   ATR 扩张：{result['squeeze']['atr_expanding']}")
            print(f"   成交量确认：{result['squeeze']['vol_confirmed']}")
            print(f"   K 线类型：{result['candlestick']['candle_type']}")
            print(f"   实体比例：{result['candlestick']['body_ratio']:.2%}")
            print(f"   交易时段：{result['time_filter']['trading_session']}")
    
    print("\n" + "=" * 60)
    print(f"检测完成！共找到 {len(signals_found)} 个突破信号")
    
    # 显示条件统计
    print(f"\n各条件满足次数统计 (共 {len(df) - 65} 根 K 线):")
    print(f"  波动率挤压 (ATR 百分位 < 30%): {condition_stats['squeeze']}")
    print(f"  价格突破：{condition_stats['breakout']}")
    print(f"  ATR 扩张：{condition_stats['atr_expanding']}")
    print(f"  成交量确认 (> 1.5 倍均量): {condition_stats['vol_confirmed']}")
    print(f"  强势 K 线 (实体 > 70%): {condition_stats['strong_candle']}")
    print(f"  活跃时段：{condition_stats['active_time']}")
    
    if signals_found:
        print("\n信号列表:")
        for sig in signals_found:
            print(f"  [{sig['index']}] {sig['datetime']} - {sig['direction']} @ {sig['close']}")
    else:
        print("\n未找到任何突破信号")
        print("\n可能原因：")
        print("  1. 市场处于震荡状态，没有明显的波动率挤压和突破")
        print("  2. 突破时没有满足成交量确认条件")
        print("  3. K 线形态不够强势（实体比例 < 70%）")
        print("  4. 突破发生在非活跃时段（不在 9-11 点、13-15 点、21-23 点）")
        print("\n综合条件（挤压 + 突破 + ATR 扩张 + 放量 + 强势 K 线 + 活跃时段）要求较高，")
        print("实际交易中可根据需要适当放宽条件。")
