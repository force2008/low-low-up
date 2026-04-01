#!/usr/bin/env python3
"""
多时间框架策略回测框架 v7（最终版）
MACD 多周期底背离策略

策略逻辑：
- 60 分钟：MACD 转红 + 绿柱堆 K 线低点抬升（底背离确认）
- 5 分钟：DIF 二次拐头/绿柱堆萎缩 + 阳柱确认
- 入场时机：
  1. 信号队列（绿柱堆内 DIF 拐头）：60 分钟绿柱堆内预检查 DIF 拐头 + 底背离，5 分钟 MACD 红柱直接入场（无需底部抬升过滤）
  2. 信号队列（红柱堆内 DIF 拐头）：60 分钟红柱堆内预检查 DIF 拐头（dif_5 > dif_3 < dif_4），5 分钟 MACD 红柱直接入场（无需底背离，无需底部抬升过滤）
  3. 传统逻辑：60 分钟绿柱堆结束转红时直接入场（需要 5 分钟底部抬升过滤）
  
说明：
- 条件 1 和条件 2 是并列的"或"关系，60 分钟条件满足时 5 分钟直接入场
- 条件 1 针对绿柱堆内的 DIF 拐头（下跌过程中的拐点），需要底背离确认
- 条件 2 针对红柱堆内的 DIF 拐头（上涨过程中的回调拐点），无需底背离（趋势已向上）
- 条件 1 和 2 入场时只需 5 分钟 MACD 红柱 + 阳柱确认，无需底部抬升过滤
- 条件 3（传统逻辑）仍需要 5 分钟底部抬升过滤

风控参数：
- 冷却期：4 小时（不重复开单）
- 初始止损：5 分钟前前绿柱堆间 K 线低点
- 移动止损：每次绿柱转红后，移动止损到前前绿柱堆间 K 线低点

配置参数：
- 目标货值：50 万
- 5 分钟 K 线：最多 5000 根
- 绿柱堆萎缩阈值：30%（5 分钟入场备选条件）

创建日期：2026-03-20
最后更新：2026-03-21（新增 60m 红柱 DIF 拐头入场条件）
策略核心：60 分钟底背离趋势确认 + 5 分钟精确入场 + 多入场时机覆盖
"""

import sqlite3
import json
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timedelta

# ============== 配置 ==============

class Config:
    DB_PATH = "/home/ubuntu/low-low-up/data/db/kline_data.db"
    CONTRACTS_PATH = "/home/ubuntu/low-low-up/data/contracts/main_contracts.json"
    
    DURATION_5M = 300
    DURATION_60M = 3600
    MAX_5M_BARS = 5000
    
    TARGET_NOTIONAL = 100000  # 50 万货值
    COOLDOWN_HOURS = 4  # 冷却期 4 小时

# ============== 数据结构 ==============

@dataclass
class Trade:
    entry_time: str
    entry_price: float
    exit_time: Optional[str] = None
    exit_price: Optional[float] = None
    position_size: int = 0
    pnl: float = 0
    pnl_pct: float = 0
    exit_reason: str = ""
    initial_stop: float = 0.0  # 初始止损价
    stop_update_count: int = 0  # 止损调整次数
    entry_conditions: str = ""  # 入场条件

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
        """快速加载 K 线数据"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = """SELECT datetime, open, high, low, close, volume 
                   FROM kline_data WHERE symbol = ? AND duration = ?
                   ORDER BY datetime ASC"""
        
        if limit:
            query += f" LIMIT {limit}"
        
        cursor.execute(query, [symbol, duration])
        rows = cursor.fetchall()
        conn.close()
        
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

# ============== MACD 计算（统一版本） ==============

class MACDCalculator:
    """统一的 MACD 计算器"""
    
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
        计算 MACD 和 MA20
        输入：[(time, open, high, low, close, volume), ...]
        输出：[(time, open, high, low, close, volume, dif, dea, hist, ma20), ...]
        """
        n = len(data)
        if n == 0:
            return []
        
        closes = [r[4] for r in data]
        
        # 计算 EMA
        ema12 = MACDCalculator.ema(closes, 12)
        ema26 = MACDCalculator.ema(closes, 26)
        
        # 计算 DIF
        dif = [ema12[i] - ema26[i] for i in range(n)]
        
        # 计算 DEA
        dea = MACDCalculator.ema(dif, 9)
        
        # 计算 MACD 柱
        hist = [2 * (dif[i] - dea[i]) for i in range(n)]
        
        # 计算 MA20（简单移动平均线）
        ma20 = []
        for i in range(n):
            if i < 2:
                ma20.append(0.0)  # 数据不足时返回 0
            else:
                ma20_val = sum(closes[i-1:i+1]) / 2
                ma20.append(ma20_val)
        
        # 返回扩展后的元组
        return [(data[i][0], data[i][1], data[i][2], data[i][3], data[i][4], data[i][5],
                dif[i], dea[i], hist[i], ma20[i]) for i in range(n)]

# ============== 堆识别 ==============

class StackIdentifier:
    @staticmethod
    def identify(data: List[tuple]) -> Tuple[List[tuple], Dict[int, dict], Dict[int, dict]]:
        """
        识别 MACD 堆
        返回：
        1. 扩展后的数据（包含 stack_type, stack_id, green_stack_end）
        2. 绿柱堆信息字典 {stack_id: {'low': low_price, 'high': high_price, 'start_idx': start, 'end_idx': end, 'hist_sum': sum}}
        3. 绿柱堆间区域信息字典 {gap_id: {'low': low_price, 'start_idx': start, 'end_idx': end}}
           gap_id 表示第几个绿柱堆后的间隙，gap_id=1 表示第一个绿柱堆和第二个绿柱堆之间的区域
        """
        n = len(data)
        if n == 0:
            return [], {}, {}
        
        result = []
        green_stacks = {}
        green_gaps = {}  # 绿柱堆间的区域（红柱区域）
        
        current_stack = 0
        stack_id = 0
        stack_high = 0.0
        stack_low = float('inf')
        stack_hist_sum = 0.0
        stack_start_idx = 0
        stack_end_idx = 0
        
        # 绿柱堆间区域跟踪
        gap_id = 0
        gap_low = float('inf')
        gap_start_idx = -1
        in_gap = False
        
        # 新增：绿柱堆结束立即确认
        last_green_complete = False
        last_green_hist_sum = 0.0
        last_green_low = float('inf')
        last_green_end_idx = -1
        
        for i in range(n):
            time, open, high, low, close, volume, dif, dea, hist, ma20 = data[i]
            prev_hist = data[i-1][8] if i > 0 else 0
            
            # 检测绿柱堆结束（前一根是绿柱，当前是红柱）→ 立即确认
            if prev_hist < 0 and hist > 0:
                # 绿柱堆结束，立即确认最后一个绿柱堆的信息
                last_green_complete = True
                last_green_hist_sum = stack_hist_sum
                last_green_low = stack_low
                last_green_end_idx = i - 1
            
            if hist > 0:
                # 红柱区域 - 可能是绿柱堆间的间隙
                if current_stack != 1:
                    # 从绿柱堆切换到红柱
                    if current_stack == -1:
                        # 绿柱堆结束，开始记录间隙
                        gap_id = stack_id  # 用前一个绿柱堆的 ID 作为间隙 ID
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
                
                # 更新间隙最低价
                if in_gap:
                    gap_low = min(gap_low, low)
            
            elif hist < 0:
                # 绿柱区域
                if current_stack != -1:
                    # 从红柱切换到绿柱 - 结束间隙记录
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
                    stack_low = low  # 绿柱堆的 low 是 K 线的最低价
                    stack_hist_sum = abs(hist)
                    stack_start_idx = i
                    stack_end_idx = i
                    # 重置绿柱堆结束标记
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
            
            # 记录绿柱堆信息（绿柱堆是 hist < 0，即 stack_type = -1）
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
            
            # 扩展元组（新增绿柱堆结束标记）
            result.append((time, open, high, low, close, volume, dif, dea, hist, ma20,
                          current_stack, stack_id, 
                          1 if (prev_hist < 0 and hist > 0) else 0))  # 绿柱堆结束标记
        
        # 保存最后一个绿柱堆信息到 green_stacks（如果绿柱堆已结束）
        if last_green_complete:
            # 使用 stack_id+1 作为已结束绿柱堆的临时 ID
            temp_green_id = stack_id + 1
            green_stacks[temp_green_id] = {
                'low': last_green_low,
                'high': float('-inf'),
                'start_idx': -1,
                'end_idx': last_green_end_idx,
                'hist_sum': last_green_hist_sum,
                'immediate': True  # 标记为立即确认的绿柱堆
            }
        
        return result, green_stacks, green_gaps

# ============== 索引映射 ==============

class IndexMapper:
    @staticmethod
    def precompute_60m_index(df_5m: List[tuple], df_60m: List[tuple]) -> List[int]:
        """预计算每个 5 分钟 K 线对应的 60 分钟索引"""
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
    
    @staticmethod
    def synthesize_60m_from_5m(df_5m: List[tuple], idx_5m: int) -> Optional[tuple]:
        """
        从 5 分钟 K 线实时合成 60 分钟 K 线
        返回：(datetime, open, high, low, close, volume) 或 None
        """
        if idx_5m < 11:  # 至少需要 12 根 5 分钟 K 线
            return None
        
        # 获取最近 12 根 5 分钟 K 线（60 分钟 = 12 × 5 分钟）
        klines_5m = df_5m[idx_5m-11:idx_5m+1]
        
        if len(klines_5m) < 12:
            return None
        
        # 合成 60 分钟 K 线
        syn_time = klines_5m[-1][0]  # 使用最后一根的时间
        syn_open = klines_5m[0][1]   # 第一根的开盘
        syn_high = max(k[2] for k in klines_5m)  # 最高价
        syn_low = min(k[3] for k in klines_5m)   # 最低价
        syn_close = klines_5m[-1][4]  # 最后一根的收盘
        syn_volume = sum(k[5] for k in klines_5m)  # 成交量累加
        
        return (syn_time, syn_open, syn_high, syn_low, syn_close, syn_volume)

# ============== 策略逻辑 ==============

class Strategy:
    def __init__(self, symbol_info: dict):
        self.symbol_info = symbol_info
        self.tick_size = symbol_info.get('PriceTick', 0.2) if symbol_info else 0.2
    
    def check_60m_dif_turn_in_green(self, df_60m: List[tuple], idx: int, 
                                     green_stacks_60m: Dict[int, dict]) -> Tuple[bool, str]:
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
        
        # DIF 一次拐头
        # 总合约数：89
        # 总交易数：751
        # 盈利交易：234 (31.2%)
        # 总盈亏：164,510.00
        # if dif_2 > dif_1 < dif_0:
        #     return True, "60m 绿柱堆内 DIF 拐头"

        # DIF 二次拐头
        # 总合约数：89
        # 总交易数：721
        # 盈利交易：229 (31.8%)
        # 总盈亏：162,495.00
        if dif_3 > dif_2 > dif_1 < dif_0:
            return True, "60m 绿柱堆内 DIF 拐头"
        
        return False, "DIF 未拐头"
    
    def check_60m_divergence(self, df_60m: List[tuple], idx: int) -> Tuple[bool, str, float, float]:
        """
        检查 60 分钟底背离条件
        条件：当前绿柱堆 K 线低点 >= 前一个已结束的绿柱堆 K 线低点（允许持平）
        
        返回：(是否满足，原因，当前绿柱堆低点，前一个绿柱堆低点)
        """
        if idx < 4:
            return False, "数据不足", float('inf'), float('inf')
        
        # 第一步：确定当前是否在绿柱堆内
        current_hist = df_60m[idx][8]
        
        # 第二步：找到当前绿柱堆的完整范围（包括未来的绿柱）
        current_green_low = float('inf')
        current_stack_start = idx
        
        # 向前找到绿柱堆的开始
        for j in range(idx, -1, -1):
            h = df_60m[j][8]
            if h >= 0:
                current_stack_start = j + 1
                break
        
        # 向后找到绿柱堆的结束（包括所有连续的绿柱）
        current_stack_end = idx
        for j in range(idx, len(df_60m)):
            h = df_60m[j][8]
            if h >= 0:
                break
            current_stack_end = j
        
        # 计算当前绿柱堆的最低价
        for j in range(current_stack_start, current_stack_end + 1):
            low = df_60m[j][3]
            current_green_low = min(current_green_low, low)
        
        # 第三步：找到前一个已完成的绿柱堆
        # 从当前绿柱堆开始之前查找
        start_search_idx = current_stack_start - 1
        while start_search_idx >= 0 and df_60m[start_search_idx][8] >= 0:
            start_search_idx -= 1
        
        # 找到前一个完整的绿柱堆
        stack_low = float('inf')
        in_green_stack = False
        prev_green_low = float('inf')
        
        for j in range(start_search_idx, -1, -1):
            h = df_60m[j][8]
            low = df_60m[j][3]
            
            if h < 0:  # 绿柱
                in_green_stack = True
                stack_low = min(stack_low, low)
            else:  # 红柱或零柱
                if in_green_stack:
                    # 绿柱堆结束
                    prev_green_low = stack_low
                    break
                stack_low = float('inf')
                in_green_stack = False
        
        # 第四步：检查底背离
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
        条件：
        1. 至少有2根红柱（红柱堆稳定形成）
        2. 红柱堆内的DIF形成V型反转：dif_prev_prev > dif_prev < dif_curr
        """
        if idx < 2:
            return False, "数据不足"
        
        # 检查是否处于红柱堆内
        hist_curr = df_60m[idx][8]
        hist_prev = df_60m[idx-1][8]
        
        if hist_curr <= 0 or hist_prev <= 0:
            return False, "非红柱堆或红柱堆未稳定形成"
        
        # 检查红柱堆内的DIF拐头
        dif_prev_prev = df_60m[idx-2][6]
        dif_prev = df_60m[idx-1][6]
        dif_curr = df_60m[idx][6]
        
        # DIF 拐头：前前 > 前 < 当前
        if dif_prev_prev > dif_prev < dif_curr:
            return True, f"60m 红柱 DIF 拐头 ({dif_prev_prev:.2f} > {dif_prev:.2f} < {dif_curr:.2f})"
        
        return False, "DIF 未拐头"
    
    def check_60m_entry(self, df_60m: List[tuple], idx: int, 
                        green_stacks_60m: Dict[int, dict]) -> Tuple[bool, str, Optional[float], Optional[float]]:
        """
        检查 60 分钟入场条件
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
        
        # 条件 4: DIF 二次拐头
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
        - 阳柱确认：close > open
        注：60分钟DIF二次拐头满足后，5分钟只需满足红柱+阳柱即可入场
        """
        row = df_5m[idx]
        hist_0 = row[8]
        close = row[4]
        open = row[1]
        
        # 条件 1: MACD 红柱
        # if hist_0 <= 0:
        #     return False, "非 MACD 红柱"
        
        # 条件 2: 阳柱确认
        if close <= open:
            return False, "非阳柱"
        
        return True, "5m 红柱+阳柱确认"
    
    def get_initial_stop_loss(self, df_5m: List[tuple], idx: int, 
                              green_stacks_5m: Dict[int, dict],
                              green_gaps_5m: Dict[int, dict]) -> Tuple[Optional[float], str]:
        """
        获取初始止损：5 分钟的前前绿柱堆里的最低价
        返回：(止损价，原因)
        """
        current_stack_id = df_5m[idx][11]
        
        # 找到前一个绿柱堆的 ID
        green_ids = sorted([sid for sid in green_stacks_5m.keys() if sid < current_stack_id])
        
        # 需要至少两个绿柱堆
        if len(green_ids) >= 2:
            # 再往前一个绿柱堆里的最低价
            prev_prev_green_id = green_ids[-2]
            
            if prev_prev_green_id in green_stacks_5m:
                stop_loss = green_stacks_5m[prev_prev_green_id]['low']
                return stop_loss, f"前前绿柱堆 K 线低点:{stop_loss:.2f}"
        
        return None, "绿柱堆数据不足"
    
    def get_mobile_stop(self, df_5m: List[tuple], current_idx: int,
                        green_stacks_5m: Dict[int, dict],
                        green_gaps_5m: Dict[int, dict]) -> Tuple[Optional[float], str]:
        """
        移动止损：每一次绿柱转红后，移动止损到前前绿柱堆里的最低价
        """
        current_stack_id = df_5m[current_idx][11]
        
        # 找到当前绿柱堆之前的绿柱堆
        green_ids = sorted([sid for sid in green_stacks_5m.keys() if sid < current_stack_id])
        
        if len(green_ids) < 2:
            return None, "绿柱堆数据不足"
        
        # 前前个绿柱堆里的最低价
        prev_prev_green_id = green_ids[-2]
        if prev_prev_green_id in green_stacks_5m:
            stop_price = green_stacks_5m[prev_prev_green_id]['low']
            return stop_price, f"移动止损 (前前绿柱堆 K 线低点:{stop_price:.2f})"
        
        return None, "绿柱堆数据不足"

# ============== 回测主逻辑 ==============

def run_backtest(symbol: str, df_5m_raw: List[tuple], df_60m_raw: List[tuple],
                 symbol_info: dict, config: Config) -> List[Trade]:
    """运行回测"""
    
    # 计算 MACD（统一版本）
    df_5m = MACDCalculator.calculate(df_5m_raw)
    df_5m, green_stacks_5m, green_gaps_5m = StackIdentifier.identify(df_5m)
    
    df_60m = MACDCalculator.calculate(df_60m_raw)
    df_60m, green_stacks_60m, green_gaps_60m = StackIdentifier.identify(df_60m)
    
    # 预计算索引映射
    index_map = IndexMapper.precompute_60m_index(df_5m, df_60m)
    
    # 初始化策略
    strategy = Strategy(symbol_info)
    trades = []
    
    # 状态变量
    position = None
    last_entry_time = None  # 冷却期跟踪
    initial_stop_loss = None  # 初始止损
    entry_stack_id = None  # 入场时的绿柱堆 ID
    stop_updates = []  # 止损变化记录
    
    # 60 分钟预检查信号队列（方案 C：使用队列保持状态）
    # 绿柱堆内 DIF 拐头信号队列
    precheck_signals_green = []
    # 红柱堆内 DIF 拐头信号队列（新增）
    precheck_signals_red = []
    
    volume_multiple = symbol_info.get('VolumeMultiple', 1) if symbol_info else 1
    
    total_bars = len(df_5m)
    
    for i, row_5m in enumerate(df_5m):
        # 进度显示
        if (i + 1) % 1000 == 0:
            print(f"  进度：{i+1}/{total_bars} ({(i+1)/total_bars*100:.1f}%)")
        
        idx_60m = index_map[i] if i < len(index_map) else len(df_60m) - 1
        
        # 没有持仓，检查入场
        if position is None:
            # 检查冷却期
            if last_entry_time is not None:
                entry_dt = datetime.strptime(last_entry_time[:19], '%Y-%m-%d %H:%M:%S')
                current_dt = datetime.strptime(row_5m[0][:19], '%Y-%m-%d %H:%M:%S')
                hours_passed = (current_dt - entry_dt).total_seconds() / 3600
                
                if hours_passed < config.COOLDOWN_HOURS:
                    continue  # 冷却期内，跳过
            
            # 【优化 1】60 分钟绿柱堆内预检查 DIF 拐头 + 底部抬升（方案 C：信号队列）
            if idx_60m >= 4:
                hist_60m = df_60m[idx_60m][8]
                
                # 绿柱堆内 DIF 拐头预检查
                if hist_60m < 0:  # 绿柱堆内
                    dif_turn, _ = strategy.check_60m_dif_turn_in_green(df_60m, idx_60m, green_stacks_60m)
                    
                    if dif_turn:
                        # 检查底背离，如果满足则添加信号到队列
                        diver_ok, diver_reason, current_green_low, prev_prev_green_low = strategy.check_60m_divergence(df_60m, idx_60m)
                        
                        if diver_ok:
                            # 信号有效期：30 分钟（6 根 5 分钟 K 线）
                            expiry_time = row_5m[0][:19]
                            precheck_signals_green.append({
                                'type': 'green',
                                'created_time': row_5m[0][:19],
                                'expiry_time': expiry_time,
                                'current_green_low': current_green_low,
                                'prev_prev_green_low': prev_prev_green_low
                            })
                
                # 红柱堆内 DIF 拐头预检查（新增）
                # 说明：红柱堆内 DIF 拐头时，价格已经处于上升趋势中，底部已经抬升，无需再检查底背离
                elif hist_60m > 0:  # 红柱堆内
                    dif_turn_red, _ = strategy.check_60m_dif_turn_in_red(df_60m, idx_60m)
                    
                    if dif_turn_red:
                        # 红柱堆内 DIF 拐头，无需检查底背离（趋势已向上）
                        # 信号有效期：30 分钟（6 根 5 分钟 K 线）
                        expiry_time = row_5m[0][:19]
                        precheck_signals_red.append({
                            'type': 'red',
                            'created_time': row_5m[0][:19],
                            'expiry_time': expiry_time,
                            'current_green_low': None,  # 红柱堆内不需要底背离
                            'prev_prev_green_low': None
                        })
            
            # 【优化 2】检查预检查信号队列，满足 5 分钟条件则入场
            precheck_entry_done = False  # 标志变量，避免重复入场
            
            # 合并两个信号队列进行处理
            all_signals = precheck_signals_green + precheck_signals_red
            
            if all_signals:
                current_time = row_5m[0][:19]
                
                # 清理过期信号（超过 30 分钟）
                current_dt = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S.%f') if '.' in current_time else datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')
                
                # 清理绿柱堆内信号
                precheck_signals_green = [
                    s for s in precheck_signals_green 
                    if datetime.strptime(s['expiry_time'], '%Y-%m-%d %H:%M:%S') + timedelta(minutes=30) > current_dt
                ]
                # 清理红柱堆内信号
                precheck_signals_red = [
                    s for s in precheck_signals_red 
                    if datetime.strptime(s['expiry_time'], '%Y-%m-%d %H:%M:%S') + timedelta(minutes=30) > current_dt
                ]
                
                # 检查每个信号是否满足 5 分钟条件
                for signal in (precheck_signals_green + precheck_signals_red)[:]:
                    signal_type = signal.get('type', 'unknown')
                    
                    # 绿柱堆和红柱堆内信号都需要检查底背离
                    diver_ok, diver_reason, curr_low_60m, prev_prev_low_60m = strategy.check_60m_divergence(df_60m, idx_60m)
                    
                    if not diver_ok:
                        # 底背离条件不满足，跳过此信号
                        if signal in precheck_signals_green:
                            precheck_signals_green.remove(signal)
                        if signal in precheck_signals_red:
                            precheck_signals_red.remove(signal)
                        continue
                    
                    # 设置信号来源
                    if signal_type == 'green':
                        signal_source = "绿柱堆内 DIF 拐头"
                    else:
                        signal_source = "红柱堆内 DIF 拐头"
                    
                    # 【修改】60 分钟条件满足时，5 分钟直接入场（跳过底部抬升过滤）
                    # 检查 5 分钟入场条件（MACD 红柱 + 阳柱 + DIF 拐头/绿柱堆萎缩）
                    cond_5m, reason_5m = strategy.check_5m_entry(df_5m, i, green_stacks_5m)
                    
                    if cond_5m:
                        # 获取初始止损（前前次绿柱堆间的 K 线最低价）
                        initial_stop_loss, stop_reason = strategy.get_initial_stop_loss(df_5m, i, green_stacks_5m, green_gaps_5m)
                        
                        if initial_stop_loss is None:
                            continue
                        
                        entry_price = row_5m[4]
                        contract_value = entry_price * volume_multiple
                        
                        # 单手合约价值超过目标货值，跳过不交易
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
                        entry_stack_id = df_5m[i][10]
                        last_entry_time = row_5m[0]
                        
                        # 记录初始止损
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
                        
                        # 清除信号队列
                        precheck_signals_green.clear()
                        precheck_signals_red.clear()
                        precheck_entry_done = True
                        
                        # 记录入场条件到 position
                        position['entry_conditions'] = entry_conditions
                        
                        break  # 跳出信号队列循环
            
            # 如果预检查已入场，跳过传统逻辑
            if precheck_entry_done:
                continue
            
            # 【新增】检查 60 分钟 MACD 红柱期间的 DIF 拐头入场
            hist_60m = df_60m[idx_60m][8]
            if hist_60m > 0:  # MACD 红柱期间
                dif_turn_red, reason_dif_turn = strategy.check_60m_dif_turn_in_red(df_60m, idx_60m)
                
                if dif_turn_red:
                    # 检查底背离条件（确保趋势向上）
                    diver_ok, diver_reason, curr_low_60m, prev_prev_low_60m = strategy.check_60m_divergence(df_60m, idx_60m)
                    
                    if diver_ok:
                        # 检查 5 分钟小绿柱过滤
                        cond_filter, reason_filter = strategy.check_5m_green_stack_filter(df_5m, i, green_stacks_5m)
                        
                        if cond_filter:
                            # 检查 5 分钟入场条件
                            cond_5m, reason_5m = strategy.check_5m_entry(df_5m, i, green_stacks_5m)
                            
                            if cond_5m:
                                # 获取初始止损
                                initial_stop_loss, stop_reason = strategy.get_initial_stop_loss(df_5m, i, green_stacks_5m, green_gaps_5m)
                                
                                if initial_stop_loss is None:
                                    initial_stop_loss, stop_reason = strategy.get_initial_stop_loss(df_5m, i, green_stacks_5m, green_gaps_5m)
                                    if initial_stop_loss is None:
                                        # 使用 60 分钟底背离的低点作为止损
                                        initial_stop_loss = prev_prev_low_60m
                                        stop_reason = f"60m 底背离低点:{initial_stop_loss:.2f}"
                                
                                entry_price = row_5m[4]
                                contract_value = entry_price * volume_multiple
                                
                                if contract_value <= config.TARGET_NOTIONAL:
                                    position_size = max(1, int(config.TARGET_NOTIONAL / contract_value))
                                    
                                    position = {
                                        'entry_idx': i,
                                        'entry_time': row_5m[0],
                                        'entry_price': entry_price,
                                        'position_size': position_size,
                                        'stop_reason': stop_reason
                                    }
                                    entry_stack_id = df_5m[i][10]
                                    last_entry_time = row_5m[0]
                                    
                                    stop_updates = [{
                                        'time': row_5m[0],
                                        'entry_price': entry_price,
                                        'stop_price': initial_stop_loss,
                                        'type': '初始止损',
                                        'reason': stop_reason,
                                        'pnl': ''
                                    }]
                                    
                                    entry_conditions = f"{reason_dif_turn} + {diver_reason} + {reason_filter} + {reason_5m} [60m 红柱 DIF 拐头]"
                                    print(f"📈 入场：{row_5m[0]} @ {entry_price:.2f} | 止损:{initial_stop_loss:.2f} ({stop_reason}) | {entry_conditions}")
                                    precheck_entry_done = True
                                    
                                    # 记录入场条件到 position
                                    position['entry_conditions'] = entry_conditions
                                    
                                    continue
            
            # 检查 60 分钟条件（含底背离）- 传统逻辑（等转红）
            cond_60m, reason_60m, curr_low_60m, prev_low_60m = strategy.check_60m_entry(df_60m, idx_60m, green_stacks_60m)
            
            if cond_60m:
                # 检查 5 分钟小绿柱过滤
                cond_filter, reason_filter = strategy.check_5m_green_stack_filter(df_5m, i, green_stacks_5m)
                
                if cond_filter:
                    # 检查 5 分钟入场条件
                    cond_5m, reason_5m = strategy.check_5m_entry(df_5m, i, green_stacks_5m)
                    
                    if cond_5m:
                        # 获取初始止损（前前次绿柱堆间的 K 线最低价）
                        initial_stop_loss, stop_reason = strategy.get_initial_stop_loss(df_5m, i, green_stacks_5m, green_gaps_5m)
                        
                        if initial_stop_loss is None:
                            continue
                        
                        entry_price = row_5m[4]
                        contract_value = entry_price * volume_multiple
                        
                        # 单手合约价值超过目标货值，跳过不交易
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
                        entry_stack_id = df_5m[i][10]
                        last_entry_time = row_5m[0]
                        
                        # 记录初始止损
                        stop_updates = [{
                            'time': row_5m[0],
                            'entry_price': entry_price,
                            'stop_price': initial_stop_loss,
                            'type': '初始止损',
                            'reason': stop_reason,
                            'pnl': ''
                        }]
                        
                        green_ended = df_5m[i][11] if len(df_5m[i]) > 11 else 0
                        entry_conditions = f"{reason_60m} + {reason_filter} + {reason_5m}"
                        if green_ended == 1:
                            entry_conditions += " [绿柱堆结束立即入场]"
                            print(f"📈 入场：{row_5m[0]} @ {entry_price:.2f} | 止损:{initial_stop_loss:.2f} ({stop_reason}) | {entry_conditions}")
                        else:
                            print(f"📈 入场：{row_5m[0]} @ {entry_price:.2f} | 止损:{initial_stop_loss:.2f} ({stop_reason}) | {entry_conditions}")
                        
                        # 记录入场条件到 position
                        position['entry_conditions'] = entry_conditions
        
        # 有持仓，检查出场
        else:
            current_low = row_5m[3]
            
            # 检查初始止损
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
                entry_stack_id = None
                continue
            
            # 检查移动止损（每一次绿柱转红后，移动止损到前前次的最低价）
            mobile_stop, stop_reason = strategy.get_mobile_stop(df_5m, i, green_stacks_5m, green_gaps_5m)
            
            if mobile_stop is not None:
                # 如果移动止损上移，记录变化
                if mobile_stop > initial_stop_loss:
                    stop_updates.append({
                        'time': row_5m[0],
                        'entry_price': position['entry_price'],
                        'stop_price': mobile_stop,
                        'type': '移动止损上移',
                        'reason': stop_reason,
                        'pnl': ''
                    })
                    initial_stop_loss = mobile_stop  # 更新当前止损价
                
                # 检查是否触发止损
                if current_low <= initial_stop_loss:
                    price_diff = initial_stop_loss - position['entry_price']
                    pnl = price_diff * position['position_size'] * volume_multiple
                    pnl_pct = pnl / (position['entry_price'] * position['position_size'] * volume_multiple) * 100
                    
                    # 记录最终出场
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
                    entry_stack_id = None
    
    # 处理未平仓
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
    
    return trades, stop_updates

# ============== 主函数 ==============

def main():
    import sys
    
    print("="*60)
    print("多时间框架策略回测 v7（最终版：60m 底背离 +5m 入场）")
    print("="*60)
    
    config = Config()
    loader = DataLoader(config.DB_PATH, config.CONTRACTS_PATH)
    
    # 检查命令行参数
    symbols_to_test = []
    if len(sys.argv) > 1:
        # 使用命令行参数指定的合约
        symbols_to_test = sys.argv[1:]
        print(f"\n回测指定合约：{symbols_to_test}")
    else:
        # 自动构建合约列表
        print("\n加载主力合约列表...")
        contracts = loader.load_main_contracts()
        print(f"找到 {len(contracts)} 个活跃合约")
        
        for product_id, contract in contracts.items():
            exchange = contract.get('ExchangeID', '')
            symbol = f"{exchange}.{contract['MainContractID']}"
            symbols_to_test.append(symbol)
    
    # 批量回测
    all_results = []
    skipped = []
    
    for idx, symbol in enumerate(symbols_to_test):
        print(f"\n{'='*60}")
        print(f"回测合约：{symbol} ({idx+1}/{len(symbols_to_test)})")
        print(f"{'='*60}")
        
        symbol_info = loader.get_symbol_info(symbol)
        if symbol_info:
            print(f"合约信息：Tick={symbol_info.get('PriceTick')} | 乘数={symbol_info.get('VolumeMultiple')}")
        
        # 快速加载数据
        df_5m_raw = loader.load_kline_fast(symbol, 300, config.MAX_5M_BARS)
        df_60m_raw = loader.load_kline_fast(symbol, 3600)
        
        if not df_5m_raw or not df_60m_raw:
            print(f"❌ {symbol} 数据不足，跳过")
            skipped.append(symbol)
            continue
        
        # 运行回测
        trades, stop_updates = run_backtest(symbol, df_5m_raw, df_60m_raw, symbol_info, config)
        
        all_results.append({
            'symbol': symbol,
            'trades': trades,
            'symbol_info': symbol_info
        })
        
        print(f"  ✅ 完成：{len(trades)} 笔交易")
    
    # 生成汇总报告
    print("\n" + "="*60)
    print("生成汇总报告...")
    print("="*60)
    
    # 保存交易明细 CSV（扩展字段：初始止损价、止损调整次数、入场条件）
    output_path = Path.home() / "trading" / "backtest_trades_0325.csv"
    output_path.parent.mkdir(exist_ok=True)
    
    with open(output_path, 'w') as f:
        f.write("symbol,entry_time,entry_price,exit_time,exit_price,initial_stop,stop_updates_count,pnl,pnl_pct,exit_reason,entry_conditions\n")
        for result in all_results:
            for t in result['trades']:
                # 转义 entry_conditions 中的逗号和引号
                cond = t.entry_conditions.replace('"', '""') if t.entry_conditions else ""
                if ',' in cond or '"' in cond:
                    cond = f'"{cond}"'
                f.write(f"{result['symbol']},{t.entry_time},{t.entry_price},{t.exit_time},{t.exit_price},{t.initial_stop:.2f},{t.stop_update_count},{t.pnl:.2f},{t.pnl_pct:.2f}%,{t.exit_reason},{cond}\n")
    
    print(f"\n📊 交易明细（含止损变化）：{output_path}")
    
    # 打印汇总
    total_trades = sum(len(r['trades']) for r in all_results)
    total_pnl = sum(sum(t.pnl for t in r['trades']) for r in all_results)
    winning = sum(1 for r in all_results for t in r['trades'] if t.pnl > 0)
    
    print(f"\n✅ 回测完成！")
    print(f"   总合约数：{len(all_results)}")
    print(f"   总交易数：{total_trades}")
    print(f"   盈利交易：{winning} ({winning/total_trades*100:.1f}%)" if total_trades > 0 else "   无交易")
    print(f"   总盈亏：{total_pnl:,.2f}")
    print(f"   交易明细：{output_path}")

if __name__ == "__main__":
    main()
