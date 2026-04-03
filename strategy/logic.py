#!/usr/bin/env python3
"""
Strategy: 包含所有 60 分钟和 5 分钟的信号判断逻辑
"""

from typing import Dict, List, Optional, Tuple


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

        # 检查 DIF 拐头：dif_3 > dif_2 > dif_1 < dif_0（先跌后涨的完整拐头）
        has_drop = dif_3 > dif_2 > dif_1
        has_rise = dif_1 < dif_0

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
        """检查 60 分钟红柱堆内 DIF 拐头"""
        if idx < 4:
            return False, "数据不足"

        hist_curr = df_60m[idx][8]
        hist_prev = df_60m[idx-1][8]

        if hist_curr <= 0 or hist_prev <= 0:
            return False, "非红柱堆或红柱堆未稳定形成"

        dif_3 = df_60m[idx-3][6]
        dif_2 = df_60m[idx-2][6]
        dif_1 = df_60m[idx-1][6]
        dif_0 = df_60m[idx][6]

        # 检查 DIF 拐头：dif_3 > dif_2 > dif_1 < dif_0（先跌后涨的完整拐头）
        has_drop = dif_3 > dif_2 > dif_1
        has_rise = dif_1 < dif_0

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
        """检查5分钟绿柱堆底部是否抬升"""
        current_stack_id = df_5m[idx][11]  # stack_id, not current_stack

        # 找可用的绿柱堆
        # 1. 当前及之前的绿柱堆
        green_ids = sorted([sid for sid in green_stacks_5m.keys() if sid <= current_stack_id])

        # 2. 立即溢出绿柱堆（当前在红柱堆时，id = current_stack_id + 1）
        if current_stack_id % 2 == 1:  # 红柱堆（id 为奇数）
            immediate_overflow = current_stack_id + 1
            if immediate_overflow in green_stacks_5m:
                green_ids.append(immediate_overflow)
                green_ids.sort()

        # 3. 找最近一个已完成绿柱堆
        if len(green_ids) == 1:
            sorted_ids = sorted(green_stacks_5m.keys())
            next_green_id = current_stack_id + 1
            while next_green_id > 0 and next_green_id not in green_stacks_5m:
                next_green_id += 2
            if next_green_id > 0 and next_green_id in green_stacks_5m:
                green_ids.append(next_green_id)
                green_ids.sort()

        if len(green_ids) < 2:
            return False, "绿柱堆数据不足"

        current_green_id = green_ids[-1]
        prev_green_id = green_ids[-2]

        current_green_low = green_stacks_5m[current_green_id]['low']
        prev_green_low = green_stacks_5m[prev_green_id]['low']

        if current_green_low > prev_green_low:
            return True, f"5m 底部抬升 (前低:{prev_green_low:.2f}→当前低:{current_green_low:.2f})"

        return False, f"5m 底部未抬升 (前低:{prev_green_low:.2f} >= 当前低:{current_green_low:.2f})"

    def check_5m_entry(self, df_5m: List[tuple], idx: int,
                       green_stacks_5m: Dict[int, dict]) -> Tuple[bool, str]:
        """检查 5 分钟入场条件（阳柱确认）"""
        row = df_5m[idx]
        close = row[4]
        open = row[1]

        if close <= open:
            return False, "非阳柱"

        return True, "5m 红柱+阳柱确认"

    def get_initial_stop_loss(self, df_5m: List[tuple], idx: int,
                              green_stacks_5m: Dict[int, dict],
                              green_gaps_5m: Dict[int, dict],
                              df_60m: List[tuple] = None,
                              green_stacks_60m: Dict[int, dict] = None) -> Tuple[Optional[float], str]:
        """
        获取初始止损价

        止损逻辑：
        1. 如果 ATR 处于历史较低位置（波动率低），使用60分钟绿柱堆低点作为止损
        2. 否则使用5分钟前前绿柱堆低点作为止损

        Args:
            df_5m: 5分钟K线数据
            idx: 当前索引
            green_stacks_5m: 5分钟绿柱堆信息
            green_gaps_5m: 5分钟绿柱间隙信息
            df_60m: 60分钟K线数据（可选）
            green_stacks_60m: 60分钟绿柱堆信息（可选）

        Returns:
            (止损价，原因)
        """
        # 获取当前K线的收盘价（开仓参考价）
        current_close = df_5m[idx][4]

        # ========== 判断是否使用60分钟绿柱堆低点作为止损 ==========
        # 条件：ATR处于历史较低位置（波动率低）
        use_60m_stop = False

        # 检查是否有ATR数据（ATR在第11列，索引10）
        if len(df_5m) > idx and len(df_5m[idx]) > 10:
            current_atr = df_5m[idx][10]  # 当前ATR值

            if current_atr > 0:
                # 计算ATR百分位（当前ATR在历史ATR中的位置）
                lookback = min(200, idx)  # 回看周期
                atr_values = [df_5m[i][10] for i in range(max(0, idx - lookback), idx + 1)
                              if len(df_5m[i]) > 10 and df_5m[i][10] > 0]

                if len(atr_values) >= 20:
                    count_below = sum(1 for v in atr_values if v < current_atr)
                    atr_percentile = count_below / len(atr_values)

                    # 如果ATR百分位 < 0.3（当前波动率较低）
                    # 使用60分钟绿柱堆低点作为止损
                    if atr_percentile < 0.3:
                        use_60m_stop = True

        # ========== 根据条件选择止损方式 ==========
        if use_60m_stop and df_60m is not None and green_stacks_60m is not None:
            # 方式1：使用60分钟绿柱堆低点作为止损
            # 找到当前60分钟K线对应的索引
            current_time = df_5m[idx][0]

            # 在60分钟数据中找到对应的索引
            idx_60m = 0
            for i, row in enumerate(df_60m):
                if row[0] >= current_time:
                    idx_60m = i
                    break

            # 判断60分钟是否在拐头
            dif_turn_60m, _ = self.check_60m_dif_turn_in_green(df_60m, idx_60m, green_stacks_60m)

            # 获取60分钟绿柱堆低点
            current_stack_id_60m = df_60m[idx_60m][11] if len(df_60m[idx_60m]) > 11 else 0

            available_green_ids_60m = []
            for sid, info in green_stacks_60m.items():
                end_idx = info.get('end_idx', -1)
                if end_idx >= 0 and end_idx < idx_60m:
                    available_green_ids_60m.append(sid)

            # 60分钟拐头时用前一个绿柱堆，不拐头时用当前绿柱堆
            if dif_turn_60m and len(available_green_ids_60m) >= 2:
                available_green_ids_60m.sort()
                prev_green_id_60m = available_green_ids_60m[-2]  # 前一个绿柱堆
            elif len(available_green_ids_60m) >= 1:
                available_green_ids_60m.sort()
                prev_green_id_60m = available_green_ids_60m[-1]  # 当前绿柱堆
            else:
                prev_green_id_60m = None

            if prev_green_id_60m is not None and prev_green_id_60m in green_stacks_60m:
                stop_loss = green_stacks_60m[prev_green_id_60m]['low']

                # 同样检查：止损价不能 >= 开仓价
                if stop_loss >= current_close:
                    return None, f"60分钟绿柱堆低点({stop_loss:.2f}) >= 当前价({current_close:.2f})，下降趋势不开仓"

                return stop_loss, f"60分钟{'前' if dif_turn_60m else '当'}绿柱堆 K 线低点({atr_percentile:.2%})，止损:{stop_loss:.2f}"

        # 方式2：使用5分钟前前绿柱堆低点作为止损（默认）
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

                # 如果前前绿柱堆最低价 >= 当前收盘价，说明当前是下降趋势
                # 此时止损价会高于开仓价，刚开仓就会触发止损，不应该开仓
                if stop_loss >= current_close:
                    return None, f"5分钟前前绿柱堆低点({stop_loss:.2f}) >= 当前价({current_close:.2f})，下降趋势不开仓"

                return stop_loss, f"5分钟前前绿柱堆 K 线低点:{stop_loss:.2f}"

        return None, "绿柱堆数据不足"

    def get_mobile_stop(self, df_5m: List[tuple], current_idx: int,
                        green_stacks_5m: Dict[int, dict],
                        green_gaps_5m: Dict[int, dict]) -> Tuple[Optional[float], str]:
        """获取移动止损：前前绿柱堆的K线低点"""
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
