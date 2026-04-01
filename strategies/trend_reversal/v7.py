#!/usr/bin/env python3
"""
多时间框架实盘策略 v7（与 backtest_v7.py 信号完全一致）
MACD 多周期底背离策略（实盘版）

策略逻辑：
- 60 分钟：MACD 转红 + 绿柱堆 K 线低点抬升（底背离确认）
- 5 分钟：DIF 二次拐头/绿柱堆萎缩 + 阳柱确认
- 入场时机：
  1. 信号队列（绿柱堆内 DIF 拐头）：60 分钟绿柱堆内预检查 DIF 拐头 + 底背离，5 分钟 MACD 红柱直接入场
  2. 信号队列（红柱堆内 DIF 拐头）：60 分钟红柱堆内预检查 DIF 拐头，5 分钟 MACD 红柱直接入场
  3. 传统逻辑：60 分钟绿柱堆结束转红时直接入场（需要 5 分钟底部抬升过滤）

实盘特性：
- 从 5 分钟 K 线实时合成 60 分钟 K 线
- 60 分钟 K 线合成完成后执行策略检查
- 生成交易信号
- 支持移动止损

创建日期：2026-03-24
"""

import sqlite3
import json
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============== 枚举和常量 ==============

class SignalType(Enum):
    ENTRY_LONG = "ENTRY_LONG"      # 做多入场
    EXIT_LONG = "EXIT_LONG"        # 平多出场
    ENTRY_SHORT = "ENTRY_SHORT"    # 做空入场（预留）
    EXIT_SHORT = "EXIT_SHORT"      # 平空出场（预留）


class OrderStatus(Enum):
    PENDING = "PENDING"           # 待发送
    SENT = "SENT"                 # 已发送
    FILLED = "FILLED"             # 已成交
    CANCELLED = "CANCELLED"       # 已撤销
    REJECTED = "REJECTED"         # 被拒绝


# ============== 配置 ==============

class LiveConfig:
    """实盘配置"""
    # 数据库配置（默认值，可通过构造函数覆盖）
    DB_PATH = "/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/data-manager/kline_data.db"
    CONTRACTS_PATH = "/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/data-manager/main_contracts.json"
    
    # K 线配置
    DURATION_5M = 300   # 5 分钟
    DURATION_60M = 3600 # 60 分钟
    MAX_5M_BARS = 5000  # 最多保留 5000 根 5 分钟 K 线
    
    # 风控配置
    TARGET_NOTIONAL = 100000  # 目标货值（10 万）
    COOLDOWN_HOURS = 4        # 冷却期 4 小时
    MAX_POSITION = 1          # 最大持仓手数
    
    # 信号配置
    SIGNAL_EXPIRY_MINUTES = 30  # 信号有效期（分钟）


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
    take_profit: float = 0.0
    position_size: int = 1
    expiry_time: Optional[str] = None
    extra_data: dict = field(default_factory=dict)


@dataclass
class Order:
    """订单"""
    order_id: str
    signal: Signal
    status: OrderStatus = OrderStatus.PENDING
    fill_price: float = 0.0
    fill_time: Optional[str] = None
    ctp_order_id: str = ""
    error_msg: str = ""


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
    orders: List[Order] = field(default_factory=list)


# ============== MACD 计算器 ==============

class MACDCalculator:
    """MACD 计算器（与 backtest_v7.py 完全一致）"""
    
    @staticmethod
    def ema(values: List[float], span: int) -> List[float]:
        """计算 EMA"""
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
        """
        计算 MACD
        输入：[(time, open, high, low, close, volume), ...]
        输出：[(time, open, high, low, close, volume, dif, dea, hist), ...]
        """
        n = len(data)
        if n == 0:
            return []
        
        closes = [r[4] for r in data]
        
        ema12 = MACDCalculator.ema(closes, 12)
        ema26 = MACDCalculator.ema(closes, 26)
        dif = [ema12[i] - ema26[i] for i in range(n)]
        dea = MACDCalculator.ema(dif, 9)
        hist = [2 * (dif[i] - dea[i]) for i in range(n)]
        
        return [(data[i][0], data[i][1], data[i][2], data[i][3], data[i][4], data[i][5],
                dif[i], dea[i], hist[i]) for i in range(n)]


# ============== 堆识别 ==============

class StackIdentifier:
    """MACD 堆识别器（与 backtest_v7.py 完全一致）"""
    
    @staticmethod
    def identify(data: List[tuple]) -> Tuple[List[tuple], Dict[int, dict], Dict[int, dict]]:
        """
        识别 MACD 堆
        返回：
        1. 扩展后的数据（包含 stack_type, stack_id, green_stack_end）
        2. 绿柱堆信息字典
        3. 绿柱堆间区域信息字典
        """
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
            time, open, high, low, close, volume, dif, dea, hist = data[i]
            prev_hist = data[i-1][8] if i > 0 else 0
            
            if prev_hist < 0 and hist > 0:
                last_green_complete = True
                last_green_hist_sum = stack_hist_sum
                last_green_low = stack_low
                last_green_end_idx = i - 1
            
            if hist > 0:
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
            
            elif hist < 0:
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
            
            result.append((time, open, high, low, close, volume, dif, dea, hist,
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


# ============== 策略逻辑 ==============

class TrendReversalStrategy:
    """趋势反转策略（与 backtest_v7.py 完全一致）"""
    
    def __init__(self, symbol_info: dict = None):
        self.symbol_info = symbol_info
        self.tick_size = symbol_info.get('PriceTick', 0.2) if symbol_info else 0.2
    
    def check_60m_dif_turn_in_green(self, df_60m: List[tuple], idx: int,
                                     green_stacks_60m: Dict[int, dict] = None) -> Tuple[bool, str]:
        """
        预检查：60 分钟绿柱堆内的 DIF 二次拐头（提前确认信号）
        在绿柱堆还未结束时检查，为转红后的快速入场做准备
        """
        if idx < 4:
            return False, "数据不足"
        
        hist_0 = df_60m[idx][8]
        
        # 只在绿柱堆内检查（hist < 0）
        if hist_0 >= 0:
            return False, "非绿柱堆"
        
        dif_4 = df_60m[idx-4][6]
        dif_3 = df_60m[idx-3][6]
        dif_2 = df_60m[idx-2][6]
        dif_1 = df_60m[idx-1][6]
        dif_0 = df_60m[idx][6]
        
        # DIF 二次拐头
        if dif_3 > dif_2 < dif_1 < dif_0:
            return True, "60m 绿柱堆内 DIF 拐头"
        
        return False, "DIF 未拐头"
    
    def check_60m_dif_high_position(self, df_60m: List[tuple], idx: int,
                                     threshold: float = 0.5) -> Tuple[bool, str, float, float]:
        """
        检查 60 分钟 DIF 是否在高位
        条件：当前 DIF 值 / 最近 DIF 高点 > threshold（默认 0.5）则认为在高位，不开仓
        返回：(是否可开仓，原因，当前 DIF，最近 DIF 高点)
        """
        if idx < 20:  # 至少需要 20 根 K 线来找到可靠的高点
            return True, "数据不足，允许开仓", df_60m[idx][6], 0.0
        
        current_dif = df_60m[idx][6]
        
        # 向前查找最近的 DIF 高点（过去 20 根 K 线内的最大值）
        lookback = min(20, idx + 1)
        recent_dif_high = max(df_60m[idx-i][6] for i in range(lookback))
        
        # 如果当前 DIF 与高点的比值超过阈值，则认为在高位
        if recent_dif_high > 0 and current_dif > 0:
            ratio = current_dif / recent_dif_high
            if ratio > threshold:
                return False, f"60m DIF 在高位 (当前:{current_dif:.4f} / 高点:{recent_dif_high:.4f} = {ratio:.2%} > {threshold:.0%})", current_dif, recent_dif_high
        
        return True, f"60m DIF 位置正常 (当前:{current_dif:.4f} / 高点:{recent_dif_high:.4f})", current_dif, recent_dif_high
    
    def check_60m_divergence(self, df_60m: List[tuple], idx: int) -> Tuple[bool, str, float, float]:
        """
        检查 60 分钟底背离条件
        【优化】条件：当前绿柱堆 K 线低点 >= 前一个已结束的绿柱堆 K 线低点（允许持平）
        
        返回：(是否满足，原因，当前绿柱堆低点，前一个绿柱堆低点)
        """
        if idx < 4:
            return False, "数据不足", float('inf'), float('inf')
        
        # 直接从 df_60m 计算，避免字典污染
        current_green_low = float('inf')  # 当前绿柱堆的最低价
        prev_green_low = float('inf')     # 前一个已完成绿柱堆的最低价
        
        current_hist = df_60m[idx][8]
        
        # 第一步：计算当前绿柱堆的最低价（如果当前在绿柱堆内）
        if current_hist < 0:
            # 当前在绿柱堆内，向前找到绿柱堆的起始位置，并计算最低价
            for j in range(idx, -1, -1):
                h = df_60m[j][8]
                low = df_60m[j][3]
                if h < 0:
                    current_green_low = min(current_green_low, low)
                else:
                    break
        
        # 第二步：向前遍历找到前一个已完成的绿柱堆的低点
        # 从当前绿柱堆的起始位置之前开始
        start_search_idx = idx
        while start_search_idx >= 0 and df_60m[start_search_idx][8] < 0:
            start_search_idx -= 1
        
        # 现在从 start_search_idx 向前找前一个绿柱堆
        stack_low = float('inf')
        in_green_stack = False
        
        for j in range(start_search_idx, -1, -1):
            h = df_60m[j][8]
            low = df_60m[j][3]
            
            if h < 0:  # 绿柱
                in_green_stack = True
                stack_low = min(stack_low, low)
            else:  # 红柱或零柱
                if in_green_stack:
                    # 绿柱堆结束
                    if prev_green_low == float('inf'):
                        prev_green_low = stack_low
                        break
                    stack_low = float('inf')
                    in_green_stack = False
        
        # 第三步：检查底背离：当前绿柱堆最低价 >= 前一个绿柱堆最低价（允许持平）
        if current_green_low == float('inf') or prev_green_low == float('inf'):
            return False, "绿柱堆数据不足", current_green_low, prev_green_low
        
        if current_green_low < prev_green_low:
            return False, f"绿柱堆低点未抬升 (当前:{current_green_low:.2f} < 前一个:{prev_green_low:.2f})", current_green_low, prev_green_low
        
        if current_green_low == prev_green_low:
            return True, f"60m 底背离确认 (低:{prev_green_low:.2f}→{current_green_low:.2f} 持平)", current_green_low, prev_green_low
        
        return True, f"60m 底背离确认 (低:{prev_green_low:.2f}→{current_green_low:.2f})", current_green_low, prev_green_low
    
    def check_60m_dif_turn_in_red(self, df_60m: List[tuple], idx: int) -> Tuple[bool, str]:
        """
        检查 60 分钟 MACD 红柱期间的 DIF 拐头
        条件：dif_5 > dif_3 < dif_4（5,3,4 则 4 完成拐头）
        含义：DIF 在 5 的位置较高，下降到 3，然后拐头向上到 4
        形状：类似"V"型反转，3 是拐点
        """
        if idx < 5:
            return False, "数据不足"
        
        dif_5 = df_60m[idx-5][6]
        dif_3 = df_60m[idx-3][6]
        dif_4 = df_60m[idx-4][6]
        
        # DIF 拐头：5 > 3 < 4
        if dif_5 > dif_3 < dif_4:
            return True, f"60m 红柱 DIF 拐头 (5:{dif_5:.2f} > 3:{dif_3:.2f} < 4:{dif_4:.2f})"
        
        return False, "DIF 未拐头"
    
    def check_60m_entry(self, df_60m: List[tuple], idx: int, 
                        green_stacks_60m: Dict[int, dict] = None) -> Tuple[bool, str, Optional[float], Optional[float]]:
        """
        检查 60 分钟入场条件（传统逻辑）
        - 刚结束绿柱堆，当前 MACD 转红 (hist > 0)
        - 绿柱堆低点抬升（底背离）：当前绿柱堆 K 线低点 > 前前个绿柱堆 K 线低点
        - DIF 二次拐头（可选）
        返回：(是否满足，原因，当前绿柱堆低点，前前个绿柱堆低点)
        """
        if idx < 4:
            return False, "数据不足", None, None
        
        hist_0 = df_60m[idx][8]
        hist_1 = df_60m[idx-1][8]
        
        # 条件 1: MACD 转红 (hist > 0)
        if hist_0 <= 0:
            return False, "MACD 未转红", None, None
        
        # 条件 2: 刚结束绿柱堆（前一根是绿柱）
        if hist_1 >= 0:
            return False, "非刚结束绿柱堆", None, None
        
        # 条件 3: 绿柱堆低点抬升（底背离）
        diver_ok, diver_reason, last_green_low, prev_prev_green_low = self.check_60m_divergence(df_60m, idx)
        
        if not diver_ok:
            return False, diver_reason, last_green_low, prev_prev_green_low
        
        # 条件 4: DIF 二次拐头（可选，已包含在 diver_reason 中）
        dif_5 = df_60m[idx-5][6] if idx >= 5 else df_60m[0][6]
        dif_4 = df_60m[idx-4][6]
        dif_3 = df_60m[idx-3][6]
        dif_2 = df_60m[idx-2][6]
        dif_1 = df_60m[idx-1][6]
        dif_0 = df_60m[idx][6]
        
        # 二次拐头：先下降后上升，再下降再上升
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
        5 分钟绿柱堆底部抬升过滤
        条件：当前绿柱堆 K 线最低价 > 前一个绿柱堆 K 线最低价（底背离）
        类似 60 分钟底背离逻辑，但在 5 分钟级别应用
        """
        current_stack_id = df_5m[idx][10]
        green_ids = sorted([sid for sid in green_stacks_5m.keys() if sid <= current_stack_id])
        
        if len(green_ids) < 2:
            return False, "绿柱堆数据不足"
        
        # 当前绿柱堆和前一个绿柱堆
        current_green_id = green_ids[-1]
        prev_green_id = green_ids[-2]
        
        current_green_low = green_stacks_5m[current_green_id]['low']
        prev_green_low = green_stacks_5m[prev_green_id]['low']
        
        # 当前绿柱堆最低价 > 前一个绿柱堆最低价（底部抬升）
        if current_green_low > prev_green_low:
            return True, f"5m 底部抬升 (前低:{prev_green_low:.2f}→当前低:{current_green_low:.2f})"
        
        return False, f"5m 底部未抬升 (前低:{prev_green_low:.2f} >= 当前低:{current_green_low:.2f})"
    
    def check_5m_entry(self, df_5m: List[tuple], idx: int, 
                       green_stacks_5m: Dict[int, dict]) -> Tuple[bool, str]:
        """
        检查 5 分钟入场条件
        - MACD 红柱：当前 hist > 0
        - DIF 二次拐头：dif_3 > dif_2 < dif_1 < dif_0
        - 或 绿柱堆萎缩：当前绿柱堆绝对值和 < 前一个绿柱堆绝对值和的 30%
        - 阳柱确认：close > open
        """
        row = df_5m[idx]
        hist_0 = row[8]
        close = row[4]
        open = row[1]
        
        # 条件 1: MACD 红柱
        if hist_0 <= 0:
            return False, "非 MACD 红柱"
        
        # 条件 2: 阳柱确认
        if close <= open:
            return False, "非阳柱"
        
        # 条件 3: DIF 二次拐头 或 绿柱堆萎缩
        if idx >= 4:
            dif_3 = df_5m[idx-3][6]
            dif_2 = df_5m[idx-2][6]
            dif_1 = df_5m[idx-1][6]
            dif_0 = df_5m[idx][6]
            
            # DIF 二次拐头
            if dif_3 > dif_2 < dif_1 and dif_1 < dif_0:
                return True, "5m DIF 二次拐头"
        
        # 绿柱堆萎缩条件
        current_stack_id = df_5m[idx][10]
        green_ids = sorted([sid for sid in green_stacks_5m.keys() if sid < current_stack_id])
        
        if len(green_ids) >= 2:
            prev_green_id = green_ids[-1]
            prev_prev_green_id = green_ids[-2]
            
            prev_hist_sum = green_stacks_5m[prev_green_id]['hist_sum']
            prev_prev_hist_sum = green_stacks_5m[prev_prev_green_id]['hist_sum']
            
            if prev_hist_sum < prev_prev_hist_sum * 0.3:
                return True, "5m 绿柱堆萎缩"
        
        return False, "无 DIF 拐头或绿柱堆萎缩"
    
    def get_initial_stop_loss(self, df_5m: List[tuple], idx: int, 
                              green_stacks_5m: Dict[int, dict],
                              green_gaps_5m: Dict[int, dict]) -> Tuple[Optional[float], str]:
        """
        获取初始止损：5 分钟的前前次 MACD 的绿柱堆间的 K 线的最低价
        即前前个绿柱堆和前个绿柱堆之间的红柱区域的最低价
        返回：(止损价，原因)
        """
        current_stack_id = df_5m[idx][10]
        
        # 找到前一个绿柱堆的 ID
        green_ids = sorted([sid for sid in green_stacks_5m.keys() if sid < current_stack_id])
        
        # 前前次绿柱堆间的 K 线最低价 = 前前个绿柱堆 ID 对应的间隙
        # 例如：green_ids = [1, 2, 3], current=4
        # gap_id=2 表示第 2 个绿柱堆和第 3 个绿柱堆之间的区域
        # 我们需要 gap_id=1（第 1 个和第 2 个绿柱堆之间的区域）作为"前前次"
        if len(green_ids) >= 2:
            prev_prev_green_id = green_ids[-2]  # 前前个绿柱堆
            # 前前个绿柱堆后的间隙就是前前次绿柱堆间的区域
            if prev_prev_green_id in green_gaps_5m:
                stop_loss = green_gaps_5m[prev_prev_green_id]['low']
                return stop_loss, f"前前绿柱堆间 K 线低点:{stop_loss:.2f}"
        
        return None, "绿柱堆间数据不足"
    
    def get_mobile_stop(self, df_5m: List[tuple], current_idx: int,
                        green_stacks_5m: Dict[int, dict],
                        green_gaps_5m: Dict[int, dict]) -> Tuple[Optional[float], str]:
        """
        移动止损：每一次绿柱转红后，移动止损到前前次的最低价
        前前次 = 前前个绿柱堆间的 K 线最低价
        """
        current_stack_id = df_5m[current_idx][10]
        
        # 找到当前绿柱堆之前的绿柱堆
        green_ids = sorted([sid for sid in green_stacks_5m.keys() if sid < current_stack_id])
        
        if len(green_ids) < 2:
            return None, "绿柱堆数据不足"
        
        # 前前个绿柱堆后的间隙（前前次绿柱堆间的区域）
        prev_prev_green_id = green_ids[-2]
        if prev_prev_green_id in green_gaps_5m:
            stop_price = green_gaps_5m[prev_prev_green_id]['low']
            return stop_price, f"移动止损 (前前绿柱堆间 K 线低点:{stop_price:.2f})"
        
        return None, "绿柱堆间数据不足"


# ============== K 线合成器 ==============

class KlineSynthesizer:
    """K 线合成器 - 从 5 分钟合成 60 分钟"""
    
    @staticmethod
    def synthesize_from_12bars(bars_5m: List[tuple]) -> Optional[tuple]:
        """从 12 根 5 分钟 K 线合成 1 根 60 分钟 K 线"""
        if len(bars_5m) < 12:
            return None
        
        syn_time = bars_5m[-1][0]
        syn_open = bars_5m[0][1]
        syn_high = max(k[2] for k in bars_5m)
        syn_low = min(k[3] for k in bars_5m)
        syn_close = bars_5m[-1][4]
        syn_volume = sum(k[5] for k in bars_5m)
        
        return (syn_time, syn_open, syn_high, syn_low, syn_close, syn_volume)


# ============== 数据加载器 ==============

class DataLoader:
    """数据加载器"""
    
    def __init__(self, db_path: str, contracts_path: str):
        self.db_path = db_path
        self.contracts_path = contracts_path
        self._contracts_cache = None
        self._conn = None
    
    def _get_connection(self):
        """获取数据库连接（单例）"""
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        return self._conn
    
    def close(self):
        """关闭数据库连接"""
        if self._conn:
            self._conn.close()
            self._conn = None
    
    def load_main_contracts(self) -> Dict[str, dict]:
        if self._contracts_cache is not None:
            return self._contracts_cache
        
        with open(self.contracts_path, 'r') as f:
            contracts = json.load(f)
        self._contracts_cache = {c['ProductID']: c for c in contracts if c.get('IsTrading', 0) == 1}
        return self._contracts_cache
    
    def load_kline_fast(self, symbol: str, duration: int, limit: int = None) -> List[tuple]:
        """快速加载 K 线数据"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        query = """SELECT datetime, open, high, low, close, volume 
                   FROM kline_data WHERE symbol = ? AND duration = ?
                   ORDER BY datetime ASC"""
        
        if limit:
            query += f" LIMIT {limit}"
        
        cursor.execute(query, [symbol, duration])
        rows = cursor.fetchall()
        
        return [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows]
    
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
    
    def get_latest_5m_bar(self, symbol: str) -> Optional[tuple]:
        """获取最新的 5 分钟 K 线"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        query = """SELECT datetime, open, high, low, close, volume 
                   FROM kline_data WHERE symbol = ? AND duration = 300
                   ORDER BY datetime DESC LIMIT 1"""
        
        cursor.execute(query, [symbol])
        row = cursor.fetchone()
        
        if row:
            return (row[0], row[1], row[2], row[3], row[4], row[5])
        return None


# ============== 实盘策略引擎 ==============

class LiveStrategyEngine:
    """实盘策略引擎（与 backtest_v7.py 信号完全一致）"""
    
    def __init__(self, symbol: str, db_path: str = None, contracts_path: str = None, config: LiveConfig = None):
        self.symbol = symbol
        
        # 保存或创建配置对象
        self.config = config if config else LiveConfig()
        
        # 支持外部传入数据库路径
        if db_path is not None:
            self.db_path = db_path
        elif self.config.DB_PATH:
            self.db_path = self.config.DB_PATH
        else:
            self.db_path = LiveConfig.DB_PATH
        
        if contracts_path is not None:
            self.contracts_path = contracts_path
        elif self.config.CONTRACTS_PATH:
            self.contracts_path = self.config.CONTRACTS_PATH
        else:
            self.contracts_path = LiveConfig.CONTRACTS_PATH
        
        # 创建数据加载器
        self.data_loader = DataLoader(self.db_path, self.contracts_path)
        self.strategy = TrendReversalStrategy(self.data_loader.get_symbol_info(symbol))
        
        # 状态变量
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
        self.orders: List[Order] = []
        
        self.last_entry_time: Optional[datetime] = None
        self.precheck_signals_green: List[dict] = []
        self.precheck_signals_red: List[dict] = []
        
        self.last_60m_bar_time: Optional[str] = None
        
        logger.info(f"策略引擎初始化完成：{symbol}")
    
    def initialize(self):
        """初始化，加载历史数据"""
        logger.info("初始化历史数据...")
        
        # 加载 5 分钟 K 线
        self.df_5m = self.data_loader.load_kline_fast(
            self.symbol, 
            self.config.DURATION_5M, 
            self.config.MAX_5M_BARS
        )
        
        # 加载 60 分钟 K 线
        self.df_60m = self.data_loader.load_kline_fast(
            self.symbol, 
            self.config.DURATION_60M
        )
        
        # 计算 MACD
        self.df_5m_with_macd, self.green_stacks_5m, self.green_gaps_5m = StackIdentifier.identify(
            MACDCalculator.calculate(self.df_5m)
        )
        
        self.df_60m_with_macd, self.green_stacks_60m, self.green_gaps_60m = StackIdentifier.identify(
            MACDCalculator.calculate(self.df_60m)
        )
        
        if self.df_60m:
            self.last_60m_bar_time = self.df_60m[-1][0]
        
        logger.info(f"加载完成：5 分钟{len(self.df_5m)}根，60 分钟{len(self.df_60m)}根")
    
    def on_5m_bar(self, bar: tuple):
        """
        处理新的 5 分钟 K 线
        这是实盘时的主要入口，由行情推送触发
        
        【修改】使用 5 分钟 K 线索引映射到 60 分钟 K 线，而不是合成 60 分钟 K 线
        这样可以确保与 backtest_v7.py 的信号完全一致
        """
        # 添加到 5 分钟 K 线列表
        self.df_5m.append(bar)
        if len(self.df_5m) > self.config.MAX_5M_BARS:
            self.df_5m.pop(0)
        
        # 重新计算 5 分钟 MACD 和堆
        self.df_5m_with_macd, self.green_stacks_5m, self.green_gaps_5m = StackIdentifier.identify(
            MACDCalculator.calculate(self.df_5m)
        )
        
        # 【修改】使用 5 分钟 K 线的时间找到对应的 60 分钟 K 线索引
        # 这样确保与 backtest_v7.py 使用相同的 60 分钟 K 线数据
        current_time = bar[0]
        idx_60m = self._find_60m_index(current_time)
        
        if idx_60m >= 0 and len(self.df_60m_with_macd) > 0:
            # 检查 60 分钟 K 线是否更新
            current_60m_time = self.df_60m_with_macd[idx_60m][0] if idx_60m < len(self.df_60m_with_macd) else None
            
            if current_60m_time != self.last_60m_bar_time:
                self.last_60m_bar_time = current_60m_time
                logger.info(f"60 分钟 K 线更新：{current_60m_time}")
                
                # 60 分钟 K 线完成后执行策略检查
                self._check_strategy_on_60m_complete()
        
        # 检查 5 分钟入场条件（如果 60 分钟条件已满足）
        self._check_5m_entry()
        
        # 检查持仓止损
        self._check_stop_loss()
    
    def _find_60m_index(self, time_5m: str) -> int:
        """
        根据 5 分钟 K 线时间找到对应的 60 分钟 K 线索引
        
        原理：找到时间上最接近但不超过 5 分钟 K 线时间的 60 分钟 K 线
        """
        if not self.df_60m:
            return -1
        
        # 从后向前查找，提高效率
        for i in range(len(self.df_60m) - 1, -1, -1):
            if self.df_60m[i][0] <= time_5m:
                return i
        
        return 0  # 如果 5 分钟 K 线时间早于所有 60 分钟 K 线，返回 0
    
    def _check_strategy_on_60m_complete(self):
        """60 分钟 K 线完成后检查策略条件（与 backtest_v7.py 完全一致）"""
        if len(self.df_60m_with_macd) < 5:
            return
        
        idx_60m = len(self.df_60m_with_macd) - 1
        hist_60m = self.df_60m_with_macd[idx_60m][8]
        
        # 检查绿柱堆内 DIF 拐头
        if hist_60m < 0:
            dif_turn, _ = self.strategy.check_60m_dif_turn_in_green(
                self.df_60m_with_macd, idx_60m
            )
            
            if dif_turn:
                diver_ok, diver_reason, current_green_low, prev_prev_green_low = \
                    self.strategy.check_60m_divergence(self.df_60m_with_macd, idx_60m)
                
                if diver_ok:
                    current_time = self.df_60m_with_macd[idx_60m][0]
                    expiry_time = current_time
                    self.precheck_signals_green.append({
                        'type': 'green',
                        'created_time': current_time,
                        'expiry_time': expiry_time,
                        'current_green_low': current_green_low,
                        'prev_prev_green_low': prev_prev_green_low
                    })
                    logger.info(f"绿柱堆内 DIF 拐头信号：{diver_reason}")
        
        # 检查红柱堆内 DIF 拐头
        elif hist_60m > 0:
            dif_turn_red, reason = self.strategy.check_60m_dif_turn_in_red(
                self.df_60m_with_macd, idx_60m
            )
            
            if dif_turn_red:
                current_time = self.df_60m_with_macd[idx_60m][0]
                self.precheck_signals_red.append({
                    'type': 'red',
                    'created_time': current_time,
                    'expiry_time': current_time,
                    'current_green_low': None,
                    'prev_prev_green_low': None
                })
                logger.info(f"红柱堆内 DIF 拐头信号：{reason}")
    
    def _check_5m_entry(self):
        """检查 5 分钟入场条件（与 backtest_v7.py 完全一致）"""
        if self.position is not None:
            return  # 已有持仓
        
        if len(self.df_5m_with_macd) < 5:
            return
        
        # 检查冷却期（与 backtest_v7.py 一致：冷却期内跳过所有入场逻辑）
        if self.last_entry_time:
            current_dt = datetime.strptime(self.df_5m_with_macd[-1][0][:19], '%Y-%m-%d %H:%M:%S')
            hours_passed = (current_dt - self.last_entry_time).total_seconds() / 3600
            if hours_passed < self.config.COOLDOWN_HOURS:
                # 冷却期内，不处理预检查信号，不添加新信号
                return
        
        idx_5m = len(self.df_5m_with_macd) - 1
        idx_60m = len(self.df_60m_with_macd) - 1
        current_time = self.df_5m_with_macd[idx_5m][0][:19]
        
        # ========== 路径 1&2: 检查预检查信号队列（绿柱堆内/红柱堆内 DIF 拐头）==========
        all_signals = self.precheck_signals_green + self.precheck_signals_red
        
        if all_signals:
            current_dt = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')
            
            # 清理过期信号（处理带微秒的时间格式）
            def parse_time(time_str: str) -> datetime:
                """解析时间字符串，处理带微秒和不带微秒的格式"""
                time_str = time_str[:19]  # 截取到秒
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
                
                # 绿柱堆内信号需要再次检查底背离
                if signal_type == 'green':
                    diver_ok, diver_reason, _, _ = self.strategy.check_60m_divergence(
                        self.df_60m_with_macd, idx_60m
                    )
                    if not diver_ok:
                        if signal in self.precheck_signals_green:
                            self.precheck_signals_green.remove(signal)
                        continue
                else:
                    diver_reason = "红柱堆内 DIF 拐头（趋势向上）"
                
                # 检查 5 分钟入场条件
                cond_5m, reason_5m = self.strategy.check_5m_entry(
                    self.df_5m_with_macd, idx_5m, self.green_stacks_5m
                )
                
                if cond_5m:
                    # 获取止损价
                    initial_stop_loss, stop_reason = self.strategy.get_initial_stop_loss(
                        self.df_5m_with_macd, idx_5m, self.green_stacks_5m, self.green_gaps_5m
                    )
                    
                    if initial_stop_loss is None:
                        continue
                    
                    entry_price = self.df_5m_with_macd[idx_5m][4]
                    self._create_entry_signal(entry_price, initial_stop_loss, stop_reason, 
                                           f"{diver_reason} + {reason_5m}", current_time)
                    
                    # 清除信号队列
                    self.precheck_signals_green.clear()
                    self.precheck_signals_red.clear()
                    return
        
        # ========== 路径 3: 60 分钟红柱 DIF 拐头直接入场 ==========
        hist_60m = self.df_60m_with_macd[idx_60m][8]
        
        if hist_60m > 0:  # 红柱期间
            dif_turn_red, reason_dif_turn = self.strategy.check_60m_dif_turn_in_red(
                self.df_60m_with_macd, idx_60m
            )
            
            if dif_turn_red:
                # 检查底背离
                diver_ok, diver_reason, curr_low, prev_prev_low = self.strategy.check_60m_divergence(
                    self.df_60m_with_macd, idx_60m
                )
                
                if diver_ok:
                    # 检查 5 分钟小绿柱过滤
                    cond_filter, reason_filter = self.strategy.check_5m_green_stack_filter(
                        self.df_5m_with_macd, idx_5m, self.green_stacks_5m
                    )
                    
                    if cond_filter:
                        # 检查 5 分钟入场条件
                        cond_5m, reason_5m = self.strategy.check_5m_entry(
                            self.df_5m_with_macd, idx_5m, self.green_stacks_5m
                        )
                        
                        if cond_5m:
                            initial_stop_loss, stop_reason = self.strategy.get_initial_stop_loss(
                                self.df_5m_with_macd, idx_5m, self.green_stacks_5m, self.green_gaps_5m
                            )
                            
                            if initial_stop_loss is None:
                                # 使用 60 分钟底背离的低点作为止损
                                initial_stop_loss = prev_prev_low
                                stop_reason = f"60m 底背离低点:{initial_stop_loss:.2f}"
                            
                            entry_price = self.df_5m_with_macd[idx_5m][4]
                            self._create_entry_signal(entry_price, initial_stop_loss, stop_reason,
                                                     f"{reason_dif_turn} + {diver_reason} + {reason_filter} + {reason_5m}",
                                                     current_time, source="60m 红柱 DIF 拐头")
                            return
        
        # ========== 路径 4: 传统逻辑（绿柱堆结束转红）==========
        hist_60m_prev = self.df_60m_with_macd[idx_60m-1][8] if idx_60m > 0 else 0
        
        if hist_60m > 0 and hist_60m_prev < 0:  # 刚转红
            diver_ok, diver_reason, curr_low, prev_prev_low = self.strategy.check_60m_divergence(
                self.df_60m_with_macd, idx_60m
            )
            
            if diver_ok:
                # 检查 5 分钟底部抬升过滤
                cond_filter, reason_filter = self.strategy.check_5m_green_stack_filter(
                    self.df_5m_with_macd, idx_5m, self.green_stacks_5m
                )
                
                if cond_filter:
                    # 检查 5 分钟入场条件
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
                                                 f"{diver_reason} + {reason_filter} + {reason_5m}",
                                                 current_time, source="绿柱堆结束转红")
                        return
    
    def _create_entry_signal(self, entry_price: float, stop_loss: float, stop_reason: str,
                            reason: str, current_time: str, source: str = None):
        """创建入场信号"""
        symbol_info = self.data_loader.get_symbol_info(self.symbol)
        volume_multiple = symbol_info.get('VolumeMultiple', 1) if symbol_info else 1
        
        contract_value = entry_price * volume_multiple
        if contract_value > self.config.TARGET_NOTIONAL:
            logger.warning(f"合约价值超过目标货值，跳过：{contract_value:.2f} > {self.config.TARGET_NOTIONAL}")
            return
        
        position_size = max(1, int(self.config.TARGET_NOTIONAL / contract_value))
        
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
        source_str = f" [{source}]" if source else ""
        logger.info(f"📈 入场信号：{current_time} @ {entry_price:.2f} | 止损:{stop_loss:.2f} | {reason}{source_str}")
    
    def _check_stop_loss(self):
        """检查止损"""
        if self.position is None:
            return
        
        current_bar = self.df_5m_with_macd[-1]
        current_low = current_bar[2]
        current_time = current_bar[0]
        
        # 检查初始止损
        if current_low <= self.position.current_stop:
            # 生成出场信号
            signal = Signal(
                signal_type=SignalType.EXIT_LONG,
                symbol=self.position.symbol,
                price=self.position.current_stop,
                time=current_time,
                reason=f"止损触发 ({self.position.stop_reason})"
            )
            
            self.signals.append(signal)
            
            logger.info(f"📉 止损出场：{current_time} @ {self.position.current_stop:.2f} | {self.position.stop_reason}")
            
            self.position = None
            return
        
        # 检查移动止损
        mobile_stop, stop_reason = self.strategy.get_mobile_stop(
            self.df_5m_with_macd, len(self.df_5m_with_macd) - 1,
            self.green_stacks_5m, self.green_gaps_5m
        )
        
        if mobile_stop and mobile_stop > self.position.current_stop:
            self.position.current_stop = mobile_stop
            self.position.stop_reason = stop_reason
            logger.info(f"移动止损上移：{mobile_stop:.2f} | {stop_reason}")
    
    def get_signals(self, clear: bool = False) -> List[Signal]:
        """获取信号列表"""
        signals = self.signals.copy()
        if clear:
            self.signals.clear()
        return signals
    
    def get_position(self) -> Optional[Position]:
        """获取当前持仓"""
        return self.position
    
    def get_status(self) -> dict:
        """获取策略状态"""
        return {
            'symbol': self.symbol,
            'has_position': self.position is not None,
            'position': self.position.__dict__ if self.position else None,
            'signals_count': len(self.signals),
            '5m_bars': len(self.df_5m),
            '60m_bars': len(self.df_60m),
            'precheck_green': len(self.precheck_signals_green),
            'precheck_red': len(self.precheck_signals_red)
        }


# ============== 回测模式（用于验证信号一致性） ==============

def run_backtest_comparison(symbol: str, db_path: str, contracts_path: str, 
                            config: LiveConfig = None) -> Tuple[List[dict], List[dict]]:
    """
    使用回测模式验证实盘策略与 backtest_v7.py 的信号一致性
    
    返回：(backtest_v7_trades, live_strategy_signals)
    """
    from backtest_v7 import run_backtest as run_backtest_v7, DataLoader as BacktestDataLoader, Config as BacktestConfig
    
    # 加载数据
    data_loader = DataLoader(db_path, contracts_path)
    
    df_5m_raw = data_loader.load_kline_fast(symbol, 300, 5000)
    df_60m_raw = data_loader.load_kline_fast(symbol, 3600)
    
    if not df_5m_raw or not df_60m_raw:
        print(f"数据不足")
        return [], []
    
    symbol_info = data_loader.get_symbol_info(symbol)
    
    # 运行 backtest_v7.py 回测
    backtest_config = BacktestConfig()
    backtest_config.DB_PATH = db_path
    backtest_config.CONTRACTS_PATH = contracts_path
    
    v7_trades, _ = run_backtest_v7(symbol, df_5m_raw, df_60m_raw, symbol_info, backtest_config)
    
    # 运行实盘策略回测
    live_config = config if config else LiveConfig()
    live_config.DB_PATH = db_path
    live_config.CONTRACTS_PATH = contracts_path
    
    engine = LiveStrategyEngine(symbol, db_path, contracts_path, live_config)
    engine.initialize()
    
    # 模拟 5 分钟 K 线推送
    for bar in df_5m_raw:
        engine.on_5m_bar(bar)
    
    # 获取信号
    live_signals = engine.get_signals()
    
    # 转换为统一格式
    v7_results = []
    for t in v7_trades:
        v7_results.append({
            'entry_time': t.entry_time,
            'entry_price': t.entry_price,
            'exit_time': t.exit_time,
            'exit_price': t.exit_price,
            'pnl': t.pnl,
            'pnl_pct': t.pnl_pct,
            'exit_reason': t.exit_reason
        })
    
    live_results = []
    for s in live_signals:
        live_results.append({
            'time': s.time,
            'price': s.price,
            'type': s.signal_type.value,
            'reason': s.reason,
            'stop_loss': s.stop_loss,
            'position_size': s.position_size
        })
    
    return v7_results, live_results


# ============== 主函数（测试用） ==============

def main():
    """主函数 - 测试用"""
    print("="*60)
    print("多时间框架实盘策略 v7 - 测试模式")
    print("="*60)
    
    # 测试合约
    symbol = "CZCE.CF605"
    
    # 创建策略引擎
    engine = LiveStrategyEngine(symbol)
    
    # 初始化
    engine.initialize()
    
    # 获取策略状态
    status = engine.get_status()
    print(f"\n策略状态：")
    for key, value in status.items():
        print(f"  {key}: {value}")
    
    # 模拟接收 5 分钟 K 线推送
    print("\n模拟接收 K 线推送...")
    
    # 获取最新 K 线
    latest_bar = engine.data_loader.get_latest_5m_bar(symbol)
    if latest_bar:
        print(f"最新 K 线：{latest_bar}")
        engine.on_5m_bar(latest_bar)
    
    # 获取信号
    signals = engine.get_signals(clear=True)
    if signals:
        print(f"\n生成信号：")
        for sig in signals:
            print(f"  {sig.signal_type.value} | {sig.time} | {sig.price:.2f} | {sig.reason}")
    else:
        print("\n暂无信号")
    
    print("\n" + "="*60)
    print("测试完成")
    print("="*60)


if __name__ == "__main__":
    main()