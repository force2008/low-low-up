#!/usr/bin/env python3
"""
多时间框架实盘策略 v8（与 backtest_0328.py 信号完全一致）
MACD 多周期底背离策略

策略逻辑：与 backtest_0328.py 完全一致
- 60 分钟：MACD 转红 + 绿柱堆 K 线低点抬升（底背离确认）
- 5 分钟：DIF 二次拐头/绿柱堆萎缩 + 阳柱确认
- 入场时机：
  1. 信号队列（绿柱堆内 DIF 拐头）
  2. 信号队列（红柱堆内 DIF 拐头）
  3. 传统逻辑（绿柱堆结束转红）

风控参数：
- 冷却期：4 小时
- 初始止损：5 分钟前前绿柱堆间 K 线低点
- 移动止损：每次绿柱转红后移动止损

配置参数：
- 目标货值：10 万
- 5 分钟 K 线：最多 5000 根

创建日期：2026-03-28
"""

import sqlite3
import json
import sys
import os
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

# ============== 配置 ==============

class Config:
    DB_PATH = "/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/data-manager/kline_data.db"
    CONTRACTS_PATH = "/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/data-manager/main_contracts.json"

    DURATION_5M = 300
    DURATION_60M = 3600
    MAX_5M_BARS = 8000
    MAX_60M_BARS = 2000  # 60分钟最多加载2000根

    TARGET_NOTIONAL = 200000  # 20 万货值
    COOLDOWN_HOURS = 4  # 冷却期 4 小时


# ============== 信号类型枚举 ==============

class SignalType(Enum):
    ENTRY_LONG = "ENTRY_LONG"      # 做多入场
    EXIT_LONG = "EXIT_LONG"        # 平多出场


# ============== 数据结构 ==============

@dataclass
class Signal:
    """交易信号"""
    signal_type: SignalType
    symbol: str
    price: float
    time: str
    reason: str
    stop_loss: float = 0.0
    position_size: int = 1
    extra_data: dict = field(default_factory=dict)


@dataclass
class Position:
    """持仓信息"""
    symbol: str
    direction: str  # "long" or "short"
    entry_time: str
    entry_price: float
    position_size: int
    initial_stop: float  # 初始止损价
    current_stop: float  # 当前止损价
    stop_reason: str = ""


@dataclass
class Trade:
    """交易记录"""
    entry_time: str
    entry_price: float
    exit_time: str = None
    exit_price: float = None
    position_size: int = 0
    pnl: float = 0
    pnl_pct: float = 0
    exit_reason: str = ""
    initial_stop: float = 0.0
    stop_update_count: int = 0
    entry_conditions: str = ""


# ============== 数据加载 ==============

class DataLoader:
    def __init__(self, db_path: str, contracts_path: str):
        self.db_path = db_path
        self.contracts_path = contracts_path
        self._contracts_cache = None

    def load_main_contracts(self) -> Dict[str, dict]:
        if self._contracts_cache is not None:
            return self._contracts_cache

        with open(self.contracts_path, 'r') as f:
            contracts = json.load(f)
        self._contracts_cache = {c['ProductID']: c for c in contracts if c.get('IsTrading', 0) == 1}
        return self._contracts_cache

    def load_kline_fast(self, symbol: str, duration: int, limit: int = None) -> List[tuple]:
        """快速加载 K 线数据（加载最近的数据）"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        if limit:
            # 先获取最近的 limit 条记录，再按时间正序返回
            query = f"""SELECT datetime, open, high, low, close, volume
                       FROM kline_data
                       WHERE symbol = ? AND duration = ?
                       ORDER BY datetime DESC
                       LIMIT {limit}"""
        else:
            query = """SELECT datetime, open, high, low, close, volume
                       FROM kline_data WHERE symbol = ? AND duration = ?
                       ORDER BY datetime ASC"""

        cursor.execute(query, [symbol, duration])
        rows = cursor.fetchall()
        conn.close()

        result = [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows]
        if limit:
            result.reverse()  # 反转为正序
        return result

    def get_symbol_info(self, symbol: str) -> Optional[dict]:
        contracts = self.load_main_contracts()
        symbol_short = symbol.split('.')[-1] if '.' in symbol else symbol

        best_match = None
        best_match_len = 0

        for product_id, contract in contracts.items():
            if symbol_short.startswith(product_id) and len(product_id) > best_match_len:
                best_match = contract
                best_match_len = len(product_id)

        return best_match


# ============== MACD 计算 ==============

class MACDCalculator:
    @staticmethod
    def ema(values: List[float], span: int) -> List[float]:
        if len(values) == 0:
            return []

        multiplier = 2 / (span + 1)
        result = [values[0]]

        for i in range(1, len(values)):
            ema_val = values[i] * multiplier + result[-1] * (1 - multiplier)
            result.append(ema_val)

        return result

    @staticmethod
    def calculate(data: List[tuple]) -> List[tuple]:
        n = len(data)
        if n == 0:
            return []

        closes = [r[4] for r in data]

        ema12 = MACDCalculator.ema(closes, 12)
        ema26 = MACDCalculator.ema(closes, 26)

        dif = [ema12[i] - ema26[i] for i in range(n)]

        dea = MACDCalculator.ema(dif, 9)

        hist = [2 * (dif[i] - dea[i]) for i in range(n)]

        ma20 = []
        for i in range(n):
            if i < 2:
                ma20.append(0.0)
            else:
                ma20_val = sum(closes[i-1:i+1]) / 2
                ma20.append(ma20_val)

        return [(data[i][0], data[i][1], data[i][2], data[i][3], data[i][4], data[i][5],
                dif[i], dea[i], hist[i], ma20[i]) for i in range(n)]


# ============== 堆识别 ==============

class StackIdentifier:
    """
    MACD 红绿柱堆识别器

    注意：代码中使用的术语与标准术语对应关系：
    - hist > 0: 红柱（多头/上涨）
    - hist < 0: 绿柱（空头/下跌）
    - current_stack = 1: 红柱堆（hist > 0 的连续区域）
    - current_stack = -1: 绿柱堆（hist < 0 的连续区域）
    - green_stacks: 存储绿柱期间的 K 线数据（低点在底背离判断中使用）
    """
    @staticmethod
    def identify(data: List[tuple]) -> Tuple[List[tuple], Dict[int, dict], Dict[int, dict]]:
        n = len(data)
        if n == 0:
            return [], {}, {}

        result = []
        green_stacks = {}
        green_gaps = {}

        current_stack = 0
        stack_id = 0
        stack_high = 0.0
        stack_low = float('inf')
        stack_hist_sum = 0.0
        stack_start_idx = 0
        stack_end_idx = 0

        gap_id = 0
        gap_low = float('inf')
        gap_start_idx = -1
        in_gap = False

        last_green_complete = False
        last_green_hist_sum = 0.0
        last_green_low = float('inf')
        last_green_end_idx = -1

        for i in range(n):
            time, open, high, low, close, volume, dif, dea, hist, ma20 = data[i]
            prev_hist = data[i-1][8] if i > 0 else 0

            if prev_hist < 0 and hist > 0:
                last_green_complete = True
                last_green_hist_sum = stack_hist_sum
                last_green_low = stack_low
                last_green_end_idx = i - 1

            if hist > 0:  # 红柱（多头），开始或继续红柱堆
                if current_stack != 1:
                    if current_stack == -1:
                        gap_id = stack_id
                        gap_low = low
                        gap_start_idx = i
                        in_gap = True

                    stack_id += 1
                    current_stack = 1
                    stack_high = hist
                    stack_low = hist
                    stack_hist_sum = hist
                    stack_start_idx = i
                    stack_end_idx = i
                else:
                    stack_high = max(stack_high, hist)
                    stack_low = min(stack_low, hist)
                    stack_hist_sum += abs(hist)
                    stack_end_idx = i

                if in_gap:
                    gap_low = min(gap_low, low)

            elif hist < 0:  # 绿柱（空头），开始或继续绿柱堆
                if current_stack != -1:
                    if in_gap and gap_start_idx >= 0:
                        green_gaps[gap_id] = {
                            'low': gap_low,
                            'start_idx': gap_start_idx,
                            'end_idx': i - 1
                        }
                        in_gap = False

                    stack_id += 1
                    current_stack = -1
                    stack_high = hist
                    stack_low = low
                    stack_hist_sum = abs(hist)
                    stack_start_idx = i
                    stack_end_idx = i
                    last_green_complete = False
                else:
                    stack_high = max(stack_high, hist)
                    stack_low = min(stack_low, low)
                    stack_hist_sum += abs(hist)
                    stack_end_idx = i
            else:
                current_stack = 0
                if in_gap:
                    gap_low = min(gap_low, low)

            # 绿柱期间：持续更新绿柱堆的 K 线数据（用于底背离的低点判断）
            if current_stack == -1:
                if stack_id not in green_stacks:
                    green_stacks[stack_id] = {
                        'low': float('inf'),
                        'high': float('-inf'),
                        'start_idx': stack_start_idx,
                        'end_idx': stack_end_idx,
                        'hist_sum': 0.0
                    }
                green_stacks[stack_id]['low'] = min(green_stacks[stack_id]['low'], low)
                green_stacks[stack_id]['high'] = max(green_stacks[stack_id]['high'], high)
                green_stacks[stack_id]['hist_sum'] = stack_hist_sum
                green_stacks[stack_id]['end_idx'] = stack_end_idx

            result.append((time, open, high, low, close, volume, dif, dea, hist, ma20,
                          current_stack, stack_id,
                          1 if (prev_hist < 0 and hist > 0) else 0))

        if last_green_complete:
            temp_green_id = stack_id + 1
            green_stacks[temp_green_id] = {
                'low': last_green_low,
                'high': float('-inf'),
                'start_idx': -1,
                'end_idx': last_green_end_idx,
                'hist_sum': last_green_hist_sum,
                'immediate': True
            }

        return result, green_stacks, green_gaps


# ============== 索引映射 ==============

class IndexMapper:
    @staticmethod
    def precompute_60m_index(df_5m: List[tuple], df_60m: List[tuple]) -> List[int]:
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


# ============== 策略逻辑 ==============

class Strategy:
    def __init__(self, symbol_info: dict):
        self.symbol_info = symbol_info
        self.tick_size = symbol_info.get('PriceTick', 0.2) if symbol_info else 0.2

    def check_60m_dif_turn_in_green(self, df_60m: List[tuple], idx: int,
                                     green_stacks_60m: Dict[int, dict]) -> Tuple[bool, str]:
        if idx < 4:
            return False, "数据不足"

        hist_0 = df_60m[idx][8]

        if hist_0 >= 0:
            return False, "非绿柱堆"

        # 完整拐头逻辑：从高点跌到局部低点，再从低点反弹
        dif_4 = df_60m[idx-4][6]
        dif_3 = df_60m[idx-3][6]
        dif_2 = df_60m[idx-2][6]
        dif_1 = df_60m[idx-1][6]
        dif_0 = df_60m[idx][6]

        # 条件1：dif_3 > dif_2 或 dif_3 > dif_1（之前有下跌过程）
        # 条件2：dif_0 > dif_2 或 dif_0 > dif_1（当前在反弹）
        has_drop = (dif_3 > dif_2) or (dif_3 > dif_1)
        has_rise = (dif_0 > dif_2) or (dif_0 > dif_1)

        if has_drop and has_rise:
            return True, "60m 绿柱堆内 DIF 拐头"

        return False, "DIF 未拐头"

    def check_60m_divergence(self, df_60m: List[tuple], idx: int) -> Tuple[bool, str, float, float]:
        if idx < 4:
            return False, "数据不足", float('inf'), float('inf')

        current_hist = df_60m[idx][8]

        current_green_low = float('inf')
        current_stack_start = idx

        for j in range(idx, -1, -1):
            h = df_60m[j][8]
            if h >= 0:
                current_stack_start = j + 1
                break

        current_stack_end = idx
        for j in range(idx, len(df_60m)):
            h = df_60m[j][8]
            if h >= 0:
                break
            current_stack_end = j

        for j in range(current_stack_start, current_stack_end + 1):
            low = df_60m[j][3]
            current_green_low = min(current_green_low, low)

        start_search_idx = current_stack_start - 1
        while start_search_idx >= 0 and df_60m[start_search_idx][8] >= 0:
            start_search_idx -= 1

        stack_low = float('inf')
        in_green_stack = False
        prev_green_low = float('inf')

        for j in range(start_search_idx, -1, -1):
            h = df_60m[j][8]
            low = df_60m[j][3]

            if h < 0:
                in_green_stack = True
                stack_low = min(stack_low, low)
            else:
                if in_green_stack:
                    prev_green_low = stack_low
                    break
                stack_low = float('inf')
                in_green_stack = False

        if current_green_low == float('inf') or prev_green_low == float('inf'):
            return False, "绿柱堆数据不足", current_green_low, prev_green_low

        if current_green_low < prev_green_low:
            return False, f"绿柱堆低点未抬升 (当前:{current_green_low:.2f} < 前一个:{prev_green_low:.2f})", current_green_low, prev_green_low

        if current_green_low == prev_green_low:
            return True, f"60m 底背离确认 (低:{prev_green_low:.2f}→{current_green_low:.2f} 持平)", current_green_low, prev_green_low

        return True, f"60m 底背离确认 (低:{prev_green_low:.2f}→{current_green_low:.2f})", current_green_low, prev_green_low

    def check_60m_bottom_rise_in_red(self, df_60m: List[tuple], idx: int) -> Tuple[bool, str, float, float]:
        """检查 60 分钟红柱堆内 DIF 拐头时的 K 线低点是否高于前一个绿柱堆低点（底部抬升）"""
        if idx < 4:
            return False, "数据不足", float('inf'), float('inf')

        if df_60m[idx][8] <= 0:
            return False, "非红柱堆", float('inf'), float('inf')

        dif_3 = df_60m[idx-3][6]
        dif_2 = df_60m[idx-2][6]
        dif_1 = df_60m[idx-1][6]
        dif_0 = df_60m[idx][6]

        # 完整拐头逻辑：从高点跌到局部低点，再从低点反弹
        # 条件1：dif_3 > dif_2 或 dif_3 > dif_1（之前有下跌过程）
        # 条件2：dif_0 > dif_2 或 dif_0 > dif_1（当前在反弹）
        # 条件3：反弹幅度超过阈值（0.3%）
        has_drop = (dif_3 > dif_2) or (dif_3 > dif_1)
        has_rise = (dif_0 > dif_2) or (dif_0 > dif_1)

        if not (has_drop and has_rise):
            return False, "红柱堆内 DIF 未拐头", float('inf'), float('inf')

        # 找红柱堆拐头区域的最低价
        turn_low = float('inf')
        for j in range(max(0, idx - 3), idx + 1):
            turn_low = min(turn_low, df_60m[j][3])

        # 找当前红柱堆的起点（往前找第一个 hist <= 0）
        start_search_idx = idx
        while start_search_idx > 0 and df_60m[start_search_idx][8] > 0:
            start_search_idx -= 1

        # 找前一个绿柱堆的起点（再往前找第一个 hist > 0）
        prev_green_start = start_search_idx - 1
        while prev_green_start > 0 and df_60m[prev_green_start][8] <= 0:
            prev_green_start -= 1

        # 从红柱堆起点往前找前一个绿柱堆的最低价
        prev_green_low = float('inf')
        in_green = False
        for j in range(start_search_idx, prev_green_start - 1, -1):
            if j < 0:
                break
            h = df_60m[j][8]
            if h < 0:
                in_green = True
                prev_green_low = min(prev_green_low, df_60m[j][3])
            elif in_green:
                break

        if prev_green_low == float('inf'):
            return False, "前一个绿柱堆数据不足", float('inf'), float('inf')

        if turn_low > prev_green_low:
            return True, f"红柱堆底部抬升 (绿柱低:{prev_green_low:.2f}→红柱拐头低:{turn_low:.2f})", turn_low, prev_green_low

        return False, f"红柱堆底部未抬升 (绿柱低:{prev_green_low:.2f} <= 红柱拐头低:{turn_low:.2f})", turn_low, prev_green_low

    def check_60m_dif_turn_in_red(self, df_60m: List[tuple], idx: int) -> Tuple[bool, str]:
        if idx < 4:
            return False, "数据不足"

        hist_curr = df_60m[idx][8]
        hist_prev = df_60m[idx-1][8]

        if hist_curr <= 0 or hist_prev <= 0:
            return False, "非红柱堆或红柱堆未稳定形成"

        # 完整拐头逻辑：从高点跌到局部低点，再从低点反弹
        # dif_0 = dif_curr, dif_1 = dif_prev, dif_2 = dif_prev_prev, dif_3 = idx-3
        dif_0 = df_60m[idx][6]
        dif_1 = df_60m[idx-1][6]
        dif_2 = df_60m[idx-2][6]
        dif_3 = df_60m[idx-3][6]

        # 条件1：dif_3 > dif_2 或 dif_3 > dif_1（之前有下跌过程）
        # 条件2：dif_0 > dif_2 或 dif_0 > dif_1（当前在反弹）
        # 条件3：反弹幅度超过阈值（0.3%）
        has_drop = (dif_3 > dif_2) or (dif_3 > dif_1)
        has_rise = (dif_0 > dif_2) or (dif_0 > dif_1)

        if not (has_drop and has_rise):
            return False, "DIF 未拐头"

        return True, f"60m 红柱 DIF 拐头"

    def check_60m_entry(self, df_60m: List[tuple], idx: int,
                        green_stacks_60m: Dict[int, dict]) -> Tuple[bool, str, Optional[float], Optional[float]]:
        if idx < 4:
            return False, "数据不足", None, None

        hist_0 = df_60m[idx][8]
        hist_1 = df_60m[idx-1][8]

        if hist_0 <= 0:
            return False, "MACD 未转红", None, None

        if hist_1 >= 0:
            return False, "非刚结束绿柱堆", None, None

        diver_ok, diver_reason, last_green_low, prev_prev_green_low = self.check_60m_divergence(df_60m, idx)

        if not diver_ok:
            return False, diver_reason, last_green_low, prev_prev_green_low

        dif_5 = df_60m[idx-5][6] if idx >= 5 else df_60m[0][6]
        dif_4 = df_60m[idx-4][6]
        dif_3 = df_60m[idx-3][6]
        dif_2 = df_60m[idx-2][6]
        dif_1 = df_60m[idx-1][6]
        dif_0 = df_60m[idx][6]

        first_turn_down = dif_5 > dif_4 if idx >= 5 else True
        first_turn_up = dif_4 < dif_3
        second_turn_down = dif_3 > dif_2
        second_turn_up = dif_2 < dif_1 < dif_0

        dif_double_turn = first_turn_down and first_turn_up and second_turn_down and second_turn_up

        if dif_double_turn:
            return True, f"60m 底背离+DIF 二次拐头 (低:{prev_prev_green_low:.2f}→{last_green_low:.2f})", last_green_low, prev_prev_green_low
        else:
            return True, diver_reason, last_green_low, prev_prev_green_low

    def check_5m_green_stack_filter(self, df_5m: List[tuple], idx: int,
                                    green_stacks_5m: Dict[int, dict]) -> Tuple[bool, str]:
        """
        检查5分钟绿柱堆底部是否抬升
        使用绿柱堆的结束索引来判断，而非依赖 stack_id
        """
        # 找到当前时间点之前最近的两个已完成绿柱堆
        # 通过 green_stacks 中的 end_idx 来判断

        available_green_ids = []
        for sid, info in green_stacks_5m.items():
            end_idx = info.get('end_idx', -1)
            if end_idx >= 0 and end_idx < idx:
                available_green_ids.append(sid)

        if len(available_green_ids) < 2:
            return False, "绿柱堆数据不足"

        # 取最后两个绿柱堆
        available_green_ids.sort()
        current_green_id = available_green_ids[-1]
        prev_green_id = available_green_ids[-2]

        current_green_low = green_stacks_5m[current_green_id]['low']
        prev_green_low = green_stacks_5m[prev_green_id]['low']

        if current_green_low > prev_green_low:
            return True, f"5m 底部抬升 (前低:{prev_green_low:.2f}→当前低:{current_green_low:.2f})"

        return False, f"5m 底部未抬升 (前低:{prev_green_low:.2f} >= 当前低:{current_green_low:.2f})"

        if current_green_low > prev_green_low:
            return True, f"5m 底部抬升 (前低:{prev_green_low:.2f}→当前低:{current_green_low:.2f})"

        return False, f"5m 底部未抬升 (前低:{prev_green_low:.2f} >= 当前低:{current_green_low:.2f})"

    def check_5m_entry(self, df_5m: List[tuple], idx: int,
                       green_stacks_5m: Dict[int, dict]) -> Tuple[bool, str]:
        row = df_5m[idx]
        close = row[4]
        open = row[1]

        if close <= open:
            return False, "非阳柱"

        return True, "5m 红柱+阳柱确认"

    def get_initial_stop_loss(self, df_5m: List[tuple], idx: int,
                              green_stacks_5m: Dict[int, dict],
                              green_gaps_5m: Dict[int, dict]) -> Tuple[Optional[float], str]:
        """
        获取初始止损价：前前绿柱堆的K线低点
        使用绿柱堆的结束索引来判断
        """
        available_green_ids = []
        for sid, info in green_stacks_5m.items():
            end_idx = info.get('end_idx', -1)
            if end_idx >= 0 and end_idx < idx:
                available_green_ids.append(sid)

        if len(available_green_ids) >= 2:
            available_green_ids.sort()
            prev_prev_green_id = available_green_ids[-2]

            if prev_prev_green_id in green_stacks_5m:
                stop_loss = green_stacks_5m[prev_prev_green_id]['low']
                return stop_loss, f"前前绿柱堆 K 线低点:{stop_loss:.2f}"

        return None, "绿柱堆数据不足"

    def get_mobile_stop(self, df_5m: List[tuple], current_idx: int,
                        green_stacks_5m: Dict[int, dict],
                        green_gaps_5m: Dict[int, dict]) -> Tuple[Optional[float], str]:
        """
        获取移动止损：前前绿柱堆的K线低点
        使用绿柱堆的结束索引来判断
        """
        available_green_ids = []
        for sid, info in green_stacks_5m.items():
            end_idx = info.get('end_idx', -1)
            if end_idx >= 0 and end_idx < current_idx:
                available_green_ids.append(sid)

        if len(available_green_ids) >= 2:
            available_green_ids.sort()
            prev_prev_green_id = available_green_ids[-2]
            if prev_prev_green_id in green_stacks_5m:
                stop_price = green_stacks_5m[prev_prev_green_id]['low']
                return stop_price, f"移动止损 (前前绿柱堆 K 线低点:{stop_price:.2f})"

        return None, "绿柱堆数据不足"

    @staticmethod
    def get_60m_red_turn_low_static(df_60m: List[tuple]) -> Optional[float]:
        """获取 60 分钟红柱拐头期间的最低价（静态方法，供回测使用）

        遍历最近的 60 分钟红柱堆，找到红柱期间 DIF 拐头的最低价
        """
        if len(df_60m) < 5:
            return None

        # 找到最近的红柱堆
        for i in range(len(df_60m) - 1, -1, -1):
            hist = df_60m[i][8]
            if hist > 0:
                # 这是红柱堆，找红柱堆的起点
                red_start = i
                for j in range(i, -1, -1):
                    if df_60m[j][8] <= 0:
                        red_start = j + 1
                        break

                # 在红柱堆内找到 DIF 拐头的位置
                for j in range(max(3, red_start), i + 1):
                    if j >= 4:
                        dif_3 = df_60m[j-3][6]
                        dif_2 = df_60m[j-2][6]
                        dif_1 = df_60m[j-1][6]
                        dif_0 = df_60m[j][6]

                        # 检查是否在红柱堆内 DIF 拐头
                        has_drop = (dif_3 > dif_2) or (dif_3 > dif_1)
                        has_rise = (dif_0 > dif_2) or (dif_0 > dif_1)

                        if has_drop and has_rise:
                            # 找到拐点了，计算拐头区域的最低价
                            turn_low = float('inf')
                            for k in range(max(red_start, j - 3), j + 1):
                                low = df_60m[k][3]
                                turn_low = min(turn_low, low)
                            return turn_low
                return None
        return None


# ============== 实盘策略引擎 ==============

class LiveStrategyEngine:
    """实盘策略引擎（与 backtest_0328.py 信号完全一致）"""

    def __init__(self, symbol: str, config: Config = None):
        self.symbol = symbol
        self.config = config if config else Config()

        self.data_loader = DataLoader(self.config.DB_PATH, self.config.CONTRACTS_PATH)
        self.strategy = Strategy(self.data_loader.get_symbol_info(symbol))

        self.df_5m: List[tuple] = []
        self.df_5m_with_macd: List[tuple] = []
        self.df_60m: List[tuple] = []
        self.df_60m_with_macd: List[tuple] = []

        self.green_stacks_5m: Dict[int, dict] = {}
        self.green_gaps_5m: Dict[int, dict] = {}
        self.green_stacks_60m: Dict[int, dict] = {}
        self.green_gaps_60m: Dict[int, dict] = {}

        self.position: Optional[Position] = None
        self.signals: List[Signal] = []

        self.last_entry_time: Optional[datetime] = None
        self.precheck_signals_green: List[dict] = []
        self.precheck_signals_red: List[dict] = []

        self.last_60m_bar_time: Optional[str] = None

    def initialize(self):
        """初始化，加载历史数据"""
        self.df_5m = self.data_loader.load_kline_fast(
            self.symbol,
            self.config.DURATION_5M,
            self.config.MAX_5M_BARS
        )

        self.df_60m = self.data_loader.load_kline_fast(
            self.symbol,
            self.config.DURATION_60M,
            self.config.MAX_60M_BARS
        )

        self.df_5m_with_macd, self.green_stacks_5m, self.green_gaps_5m = StackIdentifier.identify(
            MACDCalculator.calculate(self.df_5m)
        )

        self.df_60m_with_macd, self.green_stacks_60m, self.green_gaps_60m = StackIdentifier.identify(
            MACDCalculator.calculate(self.df_60m)
        )

        if self.df_60m:
            self.last_60m_bar_time = self.df_60m[-1][0]

    def on_5m_bar(self, bar: tuple):
        """处理新的 5 分钟 K 线（实盘入口）"""
        self.df_5m.append(bar)
        if len(self.df_5m) > self.config.MAX_5M_BARS:
            self.df_5m.pop(0)

        self.df_5m_with_macd, self.green_stacks_5m, self.green_gaps_5m = StackIdentifier.identify(
            MACDCalculator.calculate(self.df_5m)
        )

        current_time = bar[0]
        idx_60m = self._find_60m_index(current_time)

        if idx_60m >= 0 and len(self.df_60m_with_macd) > 0:
            current_60m_time = self.df_60m_with_macd[idx_60m][0] if idx_60m < len(self.df_60m_with_macd) else None

            if current_60m_time != self.last_60m_bar_time:
                self.last_60m_bar_time = current_60m_time
                self._check_strategy_on_60m_complete()

        self._check_5m_entry()
        self._check_stop_loss()

    def _find_60m_index(self, time_5m: str) -> int:
        if not self.df_60m:
            return -1

        for i in range(len(self.df_60m) - 1, -1, -1):
            if self.df_60m[i][0] <= time_5m:
                return i

        return 0

    def _check_strategy_on_60m_complete(self):
        """60 分钟 K 线完成后检查策略条件"""
        if len(self.df_60m_with_macd) < 5:
            return

        idx_60m = len(self.df_60m_with_macd) - 1
        hist_60m = self.df_60m_with_macd[idx_60m][8]

        if hist_60m < 0:
            dif_turn, _ = self.strategy.check_60m_dif_turn_in_green(
                self.df_60m_with_macd, idx_60m
            )

            if dif_turn:
                diver_ok, diver_reason, current_green_low, prev_prev_green_low = \
                    self.strategy.check_60m_divergence(self.df_60m_with_macd, idx_60m)

                if diver_ok:
                    current_time = self.df_60m_with_macd[idx_60m][0]

                    existing_signal = next((s for s in self.precheck_signals_green
                                          if s['created_time'] == current_time), None)
                    if not existing_signal:
                        self.precheck_signals_green.append({
                            'type': 'green',
                            'created_time': current_time,
                            'expiry_time': current_time,
                            'current_green_low': current_green_low,
                            'prev_prev_green_low': prev_prev_green_low
                        })

        elif hist_60m > 0:
            dif_turn_red, reason = self.strategy.check_60m_dif_turn_in_red(
                self.df_60m_with_macd, idx_60m
            )

            if dif_turn_red:
                current_time = self.df_60m_with_macd[idx_60m][0]

                existing_signal = next((s for s in self.precheck_signals_red
                                      if s['created_time'] == current_time), None)
                if not existing_signal:
                    self.precheck_signals_red.append({
                        'type': 'red',
                        'created_time': current_time,
                        'expiry_time': current_time,
                        'current_green_low': None,
                        'prev_prev_green_low': None
                    })

    def _check_5m_entry(self):
        """检查 5 分钟入场条件"""
        if self.position is not None:
            return

        if len(self.df_5m_with_macd) < 5:
            return

        if self.last_entry_time:
            current_dt = datetime.strptime(self.df_5m_with_macd[-1][0][:19], '%Y-%m-%d %H:%M:%S')
            hours_passed = (current_dt - self.last_entry_time).total_seconds() / 3600
            if hours_passed < self.config.COOLDOWN_HOURS:
                return

        idx_5m = len(self.df_5m_with_macd) - 1
        idx_60m = len(self.df_60m_with_macd) - 1
        current_time = self.df_5m_with_macd[idx_5m][0][:19]

        # 检查预检查信号队列
        all_signals = self.precheck_signals_green + self.precheck_signals_red

        if all_signals:
            current_dt = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')

            def parse_time(time_str: str) -> datetime:
                time_str = time_str[:19]
                return datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')

            self.precheck_signals_green = [
                s for s in self.precheck_signals_green
                if parse_time(s['expiry_time']) + timedelta(minutes=30) > current_dt
            ]
            self.precheck_signals_red = [
                s for s in self.precheck_signals_red
                if parse_time(s['expiry_time']) + timedelta(minutes=30) > current_dt
            ]

            for signal in (self.precheck_signals_green + self.precheck_signals_red)[:]:
                signal_type = signal.get('type', 'unknown')

                diver_ok, diver_reason, _, _ = self.strategy.check_60m_divergence(
                    self.df_60m_with_macd, idx_60m
                )
                if not diver_ok:
                    if signal in self.precheck_signals_green:
                        self.precheck_signals_green.remove(signal)
                    if signal in self.precheck_signals_red:
                        self.precheck_signals_red.remove(signal)
                    continue

                if signal_type == 'green':
                    signal_source = "绿柱堆内 DIF 拐头"
                else:
                    signal_source = "红柱堆内 DIF 拐头"

                # 检查5分钟是否为阳柱（红K线）
                cond_5m, reason_5m = self.strategy.check_5m_entry(
                    self.df_5m_with_macd, idx_5m, self.green_stacks_5m
                )

                if cond_5m:
                    initial_stop_loss, stop_reason = self.strategy.get_initial_stop_loss(
                        self.df_5m_with_macd, idx_5m, self.green_stacks_5m, self.green_gaps_5m
                    )

                    if initial_stop_loss is None:
                        continue

                    entry_price = self.df_5m_with_macd[idx_5m][4]
                    self._create_entry_signal(entry_price, initial_stop_loss, stop_reason,
                                           f"{diver_reason} + {reason_5m}", current_time, signal_source)

                    if signal in self.precheck_signals_green:
                        self.precheck_signals_green.remove(signal)
                    if signal in self.precheck_signals_red:
                        self.precheck_signals_red.remove(signal)
                    return

        # 传统逻辑检查
        hist_60m = self.df_60m_with_macd[idx_60m][8]
        hist_60m_prev = self.df_60m_with_macd[idx_60m-1][8] if idx_60m > 0 else 0

        if hist_60m > 0 and hist_60m_prev < 0:
            diver_ok, diver_reason, curr_low, prev_prev_low = self.strategy.check_60m_divergence(
                self.df_60m_with_macd, idx_60m
            )

            if diver_ok:
                # 5分钟阳柱确认即可入场
                cond_5m, reason_5m = self.strategy.check_5m_entry(
                    self.df_5m_with_macd, idx_5m, self.green_stacks_5m
                )

                if cond_5m:
                        initial_stop_loss, stop_reason = self.strategy.get_initial_stop_loss(
                            self.df_5m_with_macd, idx_5m, self.green_stacks_5m, self.green_gaps_5m
                        )

                        if initial_stop_loss is None:
                            initial_stop_loss = prev_prev_low
                            stop_reason = f"60m 底背离低点:{initial_stop_loss:.2f}"

                        entry_price = self.df_5m_with_macd[idx_5m][4]
                        self._create_entry_signal(entry_price, initial_stop_loss, stop_reason,
                                                 f"{diver_reason} + {reason_5m}",
                                                 current_time, "绿柱堆结束转红")

    def _create_entry_signal(self, entry_price: float, stop_loss: float, stop_reason: str,
                            reason: str, current_time: str, source: str = None):
        """创建入场信号"""
        symbol_info = self.data_loader.get_symbol_info(self.symbol)
        volume_multiple = symbol_info.get('VolumeMultiple', 1) if symbol_info else 1

        contract_value = entry_price * volume_multiple
        if contract_value > self.config.TARGET_NOTIONAL:
            return

        position_size = max(1, int(self.config.TARGET_NOTIONAL / contract_value))

        # 检查止损价是否 >= 入场价，如果是则使用 60 分钟红柱拐头期间的最低价
        if stop_loss >= entry_price:
            # 获取 60 分钟红柱拐头期间的最低价
            turn_low = self._get_60m_red_turn_low()
            if turn_low is not None and turn_low < entry_price:
                stop_loss = turn_low
                stop_reason = f"60m红柱拐头最低价:{turn_low:.2f}"
            else:
                # 如果找不到有效的红柱拐头最低价，则不使用此信号
                return

        signal = Signal(
            signal_type=SignalType.ENTRY_LONG,
            symbol=self.symbol,
            price=entry_price,
            time=current_time,
            reason=reason,
            stop_loss=stop_loss,
            position_size=position_size,
            extra_data={'stop_reason': stop_reason, 'source': source}
        )

        self.signals.append(signal)
        self.position = Position(
            symbol=self.symbol,
            direction="long",
            entry_time=current_time,
            entry_price=entry_price,
            position_size=position_size,
            initial_stop=stop_loss,
            current_stop=stop_loss,
            stop_reason=stop_reason
        )
        self.last_entry_time = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')

    def _get_60m_red_turn_low(self) -> Optional[float]:
        """获取 60 分钟红柱拐头期间的最低价

        遍历最近的 60 分钟红柱堆，找到红柱期间 DIF 拐头的最低价
        """
        if len(self.df_60m_with_macd) < 5:
            return None

        # 找到最近的红柱堆
        for i in range(len(self.df_60m_with_macd) - 1, -1, -1):
            hist = self.df_60m_with_macd[i][8]
            if hist > 0:
                # 这是红柱堆，找红柱堆的起点
                red_start = i
                for j in range(i, -1, -1):
                    if self.df_60m_with_macd[j][8] <= 0:
                        red_start = j + 1
                        break

                # 在红柱堆内找到 DIF 拐头的位置
                for j in range(max(3, red_start), i + 1):
                    if j >= 4:
                        dif_3 = self.df_60m_with_macd[j-3][6]
                        dif_2 = self.df_60m_with_macd[j-2][6]
                        dif_1 = self.df_60m_with_macd[j-1][6]
                        dif_0 = self.df_60m_with_macd[j][6]

                        # 检查是否在红柱堆内 DIF 拐头
                        has_drop = (dif_3 > dif_2) or (dif_3 > dif_1)
                        has_rise = (dif_0 > dif_2) or (dif_0 > dif_1)

                        if has_drop and has_rise:
                            # 找到拐点了，计算拐头区域的最低价
                            turn_low = float('inf')
                            for k in range(max(red_start, j - 3), j + 1):
                                low = self.df_60m_with_macd[k][3]
                                turn_low = min(turn_low, low)
                            return turn_low
                return None
        return None

    def _check_stop_loss(self):
        """检查止损"""
        if self.position is None:
            return

        current_bar = self.df_5m_with_macd[-1]
        current_low = current_bar[2]
        current_time = current_bar[0]

        if current_low <= self.position.current_stop:
            signal = Signal(
                signal_type=SignalType.EXIT_LONG,
                symbol=self.position.symbol,
                price=self.position.current_stop,
                time=current_time,
                reason=f"止损触发 ({self.position.stop_reason})"
            )

            self.signals.append(signal)
            self.position = None
            self.last_entry_time = datetime.strptime(current_time[:19], '%Y-%m-%d %H:%M:%S')
            return

        mobile_stop, stop_reason = self.strategy.get_mobile_stop(
            self.df_5m_with_macd, len(self.df_5m_with_macd) - 1,
            self.green_stacks_5m, self.green_gaps_5m
        )

        if mobile_stop and mobile_stop > self.position.current_stop:
            self.position.current_stop = mobile_stop
            self.position.stop_reason = stop_reason

    def get_signals(self, clear: bool = False) -> List[Signal]:
        signals = self.signals.copy()
        if clear:
            self.signals.clear()
        return signals

    def get_position(self) -> Optional[Position]:
        return self.position


# ============== 飞书通知 ==============

def send_backtest_signal(symbol: str, signal_data: dict):
    """发送回测信号到飞书"""
    try:
        sys.path.insert(0, os.path.dirname(__file__))
        from feishu_notifier import FeishuNotifier
        notifier = FeishuNotifier()
        notifier.send_strategy_signal(symbol, signal_data)
    except Exception:
        pass


# ============== 回测某一天信号功能 ==============

def backtest_date_signals(date_str: str = None, db_path: str = None, contracts_path: str = None):
    """
    回测某一天的所有信号，并发送到飞书

    Args:
        date_str: 日期字符串，格式 'YYYY-MM-DD'，默认今天
        db_path: 数据库路径
        contracts_path: 合约配置路径
    """
    from datetime import date

    if date_str is None:
        date_str = date.today().strftime('%Y-%m-%d')

    print("="*60)
    print(f"回测信号分析 - {date_str}")
    print("="*60)

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
            hist_60m_prev = df_60m[idx_60m-1][8] if idx_60m > 0 else 0

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
                            initial_stop, stop_reason = strategy.get_initial_stop_loss(df_5m, i, green_stacks_5m, green_gaps_5m)
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
                        initial_stop, stop_reason = strategy.get_initial_stop_loss(df_5m, i, green_stacks_5m, green_gaps_5m)
                        if initial_stop:
                                day_signals.append({
                                    'time': time_str,
                                    'type': '绿柱堆转红柱信号',
                                    'price': row_5m[4],
                                    'stop_loss': initial_stop,
                                    'reason': f"{diver_reason} + {reason_filter} + {reason_5m}",
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
                            initial_stop, stop_reason = strategy.get_initial_stop_loss(df_5m, i, green_stacks_5m, green_gaps_5m)
                            if initial_stop:
                                day_signals.append({
                                    'time': time_str,
                                    'type': '红柱堆信号',
                                    'price': row_5m[4],
                                    'stop_loss': initial_stop,
                                    'reason': f"{diver_reason} + {reason_5m}",
                                    'source': '红柱堆内 DIF 拐头'
                                })

        if day_signals:
            print(f"  {symbol}: {len(day_signals)} 个信号")
            for sig in day_signals:
                print(f"    [{sig['time']}] {sig['type']} @ {sig['price']} | 止损:{sig['stop_loss']} | {sig['reason']}")
                # 发送到飞书
                try:
                    sys.path.insert(0, os.path.dirname(__file__))
                    from feishu_notifier import FeishuNotifier
                    notifier = FeishuNotifier()
                    notifier.send_strategy_signal(symbol, {
                        'signal_type': 'ENTRY_LONG',
                        'price': sig['price'],
                        'stop_loss': sig['stop_loss'],
                        'position_size': 1,
                        'reason': sig['reason'],
                        'time': sig['time'],
                        'extra_data': {'source': sig['source'], 'backtest_date': date_str}
                    })
                except Exception as e:
                    print(f"    ⚠️ 飞书发送失败: {e}")

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
    import sys

    print("="*60)
    print("多时间框架策略回测 v8（实盘版）")
    print("="*60)

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
        print(f"\n{'='*60}")
        print(f"回测合约：{symbol} ({idx+1}/{len(symbols_to_test)})")
        print(f"{'='*60}")

        symbol_info = loader.get_symbol_info(symbol)
        if symbol_info:
            print(f"合约信息：Tick={symbol_info.get('PriceTick')} | 乘数={symbol_info.get('VolumeMultiple')}")

        df_5m_raw = loader.load_kline_fast(symbol, 300, config.MAX_5M_BARS)
        df_60m_raw = loader.load_kline_fast(symbol, 3600, config.MAX_60M_BARS)

        if not df_5m_raw or not df_60m_raw:
            print(f"❌ {symbol} 数据不足，跳过")
            continue

        # 使用与 backtest_0328.py 相同的回测逻辑
        from pathlib import Path
        import csv

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
                print(f"  进度：{i+1}/{total_bars} ({(i+1)/total_bars*100:.1f}%)")

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
                hist_60m_prev = df_60m[idx_60m-1][8] if idx_60m > 0 else 0

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

    print("\n" + "="*60)
    print("生成汇总报告...")
    print("="*60)

    output_path = Path.home() / "trading" / "backtest_trades_0328.csv"
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
    print(f"   盈利交易：{winning} ({winning/total_trades*100:.1f}%)" if total_trades > 0 else "   无交易")
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