#!/usr/bin/env python3
"""
StrategyLowLowUp: 包含所有 60 分钟和 5 分钟的信号判断逻辑
"""

from typing import Dict, List, Optional, Tuple
import pandas as pd


class StrategyLowLowUp:
    def __init__(self, symbol_info: dict):
        self.name = "60分钟底抬"
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

            # 波动率低时用60分钟绿柱堆低点作为止损
            # 始终使用前一个绿柱堆（不是前前绿柱堆）
            if len(available_green_ids_60m) >= 2:
                available_green_ids_60m.sort()
                prev_green_id_60m = available_green_ids_60m[-1]  # 前一个绿柱堆
            elif len(available_green_ids_60m) >= 1:
                available_green_ids_60m.sort()
                prev_green_id_60m = available_green_ids_60m[-1]  # 只有一个时用它
            else:
                prev_green_id_60m = None

            if prev_green_id_60m is not None and prev_green_id_60m in green_stacks_60m:
                stop_loss = green_stacks_60m[prev_green_id_60m]['low']

                # 同样检查：止损价不能 >= 开仓价
                if stop_loss >= current_close:
                    return None, f"60分钟绿柱堆低点({stop_loss:.2f}) >= 当前价({current_close:.2f})，下降趋势不开仓"

                return stop_loss, f"60分钟前绿柱堆 K 线低点({atr_percentile:.2%})，止损:{stop_loss:.2f}"

        # 方式2：使用5分钟前前绿柱堆低点作为止损（默认）
        # 但如果前一个绿柱堆的最低价比前前绿柱堆的最低价更低，则用前一个绿柱堆的最低价
        available_green_ids = []
        for sid, info in green_stacks_5m.items():
            end_idx = info.get('end_idx', -1)
            if end_idx >= 0 and end_idx < idx:
                available_green_ids.append(sid)

        if len(available_green_ids) >= 2:
            available_green_ids.sort()
            prev_green_id = available_green_ids[-1]      # 前一个绿柱堆
            prev_prev_green_id = available_green_ids[-2]  # 前前绿柱堆

            if prev_prev_green_id in green_stacks_5m:
                prev_prev_low = green_stacks_5m[prev_prev_green_id]['low']

                # 获取前一个绿柱堆的最低价
                prev_low = green_stacks_5m[prev_green_id]['low']

                # 取较低的那个作为止损价（更保守）
                stop_loss = min(prev_prev_low, prev_low)

                # 如果止损价 >= 当前收盘价，说明当前是下降趋势
                # 此时止损价会高于开仓价，刚开仓就会触发止损，不应该开仓
                if stop_loss >= current_close:
                    return None, f"5分钟绿柱堆低点({stop_loss:.2f}) >= 当前价({current_close:.2f})，下降趋势不开仓"

                # 记录使用的止损来源
                if stop_loss < prev_prev_low:
                    stop_reason = f"前一个绿柱堆低点:{stop_loss:.2f} (低于前前:{prev_prev_low:.2f})"
                else:
                    stop_reason = f"前前绿柱堆 K 线低点:{stop_loss:.2f}"

                return stop_loss, stop_reason

        return None, "绿柱堆数据不足"

    def get_mobile_stop(self, df_5m: List[tuple], current_idx: int,
                        green_stacks_5m: Dict[int, dict],
                        green_gaps_5m: Dict[int, dict]) -> Tuple[Optional[float], str]:
        """
        获取移动止损：前前绿柱堆的K线低点

        优化：如果前一个绿柱堆的最低价比前前绿柱堆的最低价更低，则用前一个绿柱堆的最低价
        这样可以让价格更顺利地移动起来，避免轻易触发止损
        """
        available_green_ids = []
        for sid, info in green_stacks_5m.items():
            end_idx = info.get('end_idx', -1)
            if end_idx >= 0 and end_idx < current_idx:
                available_green_ids.append(sid)

        if len(available_green_ids) >= 2:
            available_green_ids.sort()
            prev_green_id = available_green_ids[-1]      # 前一个绿柱堆
            prev_prev_green_id = available_green_ids[-2]  # 前前绿柱堆

            if prev_prev_green_id in green_stacks_5m and prev_green_id in green_stacks_5m:
                prev_prev_low = green_stacks_5m[prev_prev_green_id]['low']
                prev_low = green_stacks_5m[prev_green_id]['low']

                # 取较低的那个作为移动止损价（更保守）
                stop_price = min(prev_prev_low, prev_low)

                if stop_price < prev_prev_low:
                    return stop_price, f"移动止损 (前一个绿柱堆低点:{stop_price:.2f} 低于前前:{prev_prev_low:.2f})"
                else:
                    return stop_price, f"移动止损 (前前绿柱堆 K 线低点:{stop_price:.2f})"

    # ========== 过滤方法 ==========

    def check_60m_all_limits(self, df_60m: list, idx_60m: int) -> bool:
        """检查60分钟K线是否都是一字板（涨跌停）

        Args:
            df_60m: 60分钟K线数据
            idx_60m: 当前60分钟索引

        Returns:
            True 表示都是一字板（应该过滤信号），False 表示正常
        """
        if idx_60m < 1:
            return False

        def is_limit_kline(kline):
            # 如果开高低收都相同，说明价格没有波动，可能是涨跌停
            open_price = kline[1]
            close_price = kline[4]
            high_price = kline[2]
            low_price = kline[3]
            return (abs(open_price - close_price) < 0.01 and
                    abs(open_price - high_price) < 0.01 and
                    abs(open_price - low_price) < 0.01)

        current_kline = df_60m[idx_60m]
        prev_kline = df_60m[idx_60m - 1]

        current_is_limit = is_limit_kline(current_kline)
        prev_is_limit = is_limit_kline(prev_kline)

        if current_is_limit and prev_is_limit:
            return True

        return False

    def is_large_60m_drop(self, df_60m: list, current_price: float, data_5m: list = None, lookback: int = 40) -> Tuple[bool, str]:
        """判断当前60分钟跌幅是否较大（超过过去40根K线跌幅的80分位值）

        Args:
            df_60m: 60分钟K线数据（带MACD）
            current_price: 当前价格（5分钟开盘价）
            data_5m: 5分钟K线数据（用于获取60分钟周期的开盘价）
            lookback: 回看K线数量

        Returns:
            (是否过滤, 原因)

        说明：
            只用跌幅数据（负数）来计算80分位值
            如果当前跌幅比80分位值更负，说明跌幅较大
        """
        try:
            if len(df_60m) < lookback or current_price <= 0:
                return False, ""

            # 计算过去每根K线相对于前一根K线的跌幅
            drops = []
            for i in range(1, min(lookback + 1, len(df_60m))):
                prev_close = df_60m[i - 1][4]
                curr_close = df_60m[i][4]
                if prev_close > 0:
                    drop = (curr_close - prev_close) / prev_close
                    drops.append(drop)

            if len(drops) < 20:
                return False, ""

            # 只保留跌幅（负数），去掉涨幅（正数）
            drops_negative = [d for d in drops if d < 0]
            if len(drops_negative) < 10:
                return False, "跌幅数据不足"

            # 计算跌幅的80分位值
            drops_negative_series = pd.Series(drops_negative)
            percentile_80 = drops_negative_series.quantile(0.8)

            # 获取当前5分钟所属的60分钟周期的第一个5分钟K线开盘价
            if data_5m is None or len(data_5m) < 1:
                return False, ""

            from datetime import datetime
            current_5m_time_str = data_5m[-1][0][:19]
            current_dt = datetime.strptime(current_5m_time_str, '%Y-%m-%d %H:%M:%S')
            period_minute = (current_dt.minute // 60) * 60
            period_start = current_dt.replace(minute=period_minute, second=0, microsecond=0)
            period_start_str = period_start.strftime('%Y-%m-%d %H:%M:%S')

            first_5m_in_period = None
            for row in data_5m:
                if row[0][:19] == period_start_str:
                    first_5m_in_period = row
                    break

            if first_5m_in_period is None:
                return False, ""

            open_60m = first_5m_in_period[1]
            if open_60m <= 0:
                return False, ""

            # 计算从60分钟周期开盘到当前的跌幅
            current_drop = (current_price - open_60m) / open_60m

            # 如果当前跌幅比80分位值更负（跌得更多），说明跌幅较大
            if current_drop < percentile_80:
                return True, f"跌幅过大({current_drop:.2%}) < 80分位值({percentile_80:.2%})"

            return False, ""
        except Exception as e:
            return False, f"计算失败: {e}"

        return None, "绿柱堆数据不足"

    # ========== 统一信号检查方法 ==========

    def check_60m_precheck(self, df_60m: List[tuple], idx_60m: int,
                          green_stacks_60m: Dict[int, dict]) -> Tuple[Optional[dict], str]:
        """
        检查60分钟是否产生预检测信号

        Args:
            df_60m: 60分钟K线数据（带MACD）
            idx_60m: 当前60分钟索引
            green_stacks_60m: 60分钟绿柱堆信息

        Returns:
            (预检测信号dict, 原因) 或 (None, 原因)
            信号dict: {'type': 'green'/'red', 'sub_type': 'dif_turn'/'green_to_red', 'created_time': ...}
        """
        if idx_60m < 4 or len(df_60m) < 5:
            return None, "60分钟数据不足"

        # 检查连续一字板
        if self.check_60m_all_limits(df_60m, idx_60m):
            return None, "连续一字板，跳过"

        hist_60m = df_60m[idx_60m][8]
        hist_60m_prev = df_60m[idx_60m - 1][8] if idx_60m > 0 else 0
        current_60m_time = df_60m[idx_60m][0]

        # 绿柱堆内 DIF 拐头 + 底背离
        if hist_60m < 0:
            dif_turn, _ = self.check_60m_dif_turn_in_green(df_60m, idx_60m, green_stacks_60m)
            if dif_turn:
                diver_ok, diver_reason, _, _ = self.check_60m_divergence(df_60m, idx_60m)
                if diver_ok:
                    return {
                        'type': 'green',
                        'sub_type': 'dif_turn',
                        'created_time': current_60m_time,
                    }, f"60分钟绿柱堆内DIF拐头+底背离"

        # 绿柱堆转红柱堆 + 底背离
        elif hist_60m > 0 and hist_60m_prev < 0:
            diver_ok, diver_reason, _, _ = self.check_60m_divergence(df_60m, idx_60m)
            if diver_ok:
                return {
                    'type': 'green',
                    'sub_type': 'green_to_red',
                    'created_time': current_60m_time,
                }, f"60分钟绿柱堆转红柱堆+底背离"

        # 红柱堆内 DIF 拐头 + 底部抬升
        elif hist_60m > 0:
            dif_turn_red, _ = self.check_60m_dif_turn_in_red(df_60m, idx_60m)
            if dif_turn_red:
                diver_ok, diver_reason, curr_low, prev_low = self.check_60m_bottom_rise_in_red(df_60m, idx_60m)
                if diver_ok:
                    return {
                        'type': 'red',
                        'sub_type': 'dif_turn',
                        'created_time': current_60m_time,
                    }, f"60分钟红柱堆内DIF拐头+底部抬升"

        return None, "未满足60分钟预检测条件"

    def check_5m_entry_signal(self, df_5m: List[tuple], idx_5m: int,
                             df_60m: List[tuple], idx_60m: int,
                             green_stacks_5m: Dict[int, dict],
                             green_stacks_60m: Dict[int, dict],
                             precheck_signals: List[dict],
                             position_info: dict = None,
                             last_entry_time: str = None,
                             cooldown_hours: int = 4) -> Tuple[Optional[dict], Optional[float], str]:
        """
        检查5分钟是否满足入场信号

        Args:
            df_5m: 5分钟K线数据（带MACD）
            idx_5m: 当前5分钟索引
            df_60m: 60分钟K线数据（带MACD）
            idx_60m: 当前60分钟索引
            green_stacks_5m: 5分钟绿柱堆信息
            green_stacks_60m: 60分钟绿柱堆信息
            precheck_signals: 有效的预检测信号列表
            position_info: 当前持仓信息（如果有）
            last_entry_time: 上次入场时间
            cooldown_hours: 冷却时间（小时）

        Returns:
            (入场信号dict, 止损价, 原因) 或 (None, None, 原因)
            入场信号: {'entry_price': float, 'entry_time': str, 'stop_loss': float, 'reason': str}
        """
        from datetime import datetime, timedelta

        current_time = df_5m[idx_5m][0][:19]
        current_dt = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S.%f') if '.' in current_time else datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')

        # 检查冷却时间
        if position_info is None and last_entry_time:
            try:
                entry_dt = datetime.strptime(last_entry_time[:19], '%Y-%m-%d %H:%M:%S')
                hours_passed = (current_dt - entry_dt).total_seconds() / 3600
                if hours_passed < cooldown_hours:
                    return None, None, f"冷却期内({hours_passed:.1f}小时)"
            except:
                pass

        # 过滤过期预检测信号（超过8小时）
        valid_precheck = []
        for sig in precheck_signals:
            try:
                sig_time = datetime.strptime(sig['created_time'][:19], '%Y-%m-%d %H:%M:%S')
                hours_old = (current_dt - sig_time).total_seconds() / 3600
                if hours_old < 8:
                    valid_precheck.append(sig)
            except:
                pass

        if not valid_precheck:
            return None, None, "无有效预检测信号"

        current_price = df_5m[idx_5m][4]  # 收盘价
        current_open = df_5m[idx_5m][1]
        current_low = df_5m[idx_5m][3]

        # 检查入场条件：阳柱（收盘价 > 开盘价）
        if current_price <= current_open:
            return None, None, "5分钟非阳柱"

        # 遍历有效预检测信号
        for sig in valid_precheck:
            sig_type = sig.get('type', 'unknown')
            sub_type = sig.get('sub_type', 'dif_turn')

            # 绿柱堆信号：检查底背离
            if sig_type == 'green':
                diver_ok, diver_reason, current_green_low, _ = self.check_60m_divergence(df_60m, idx_60m)
                if not diver_ok:
                    continue

                # 绿柱堆内DIF拐头：需要检查当前5分钟是否在绿柱堆中
                if sub_type == 'dif_turn':
                    in_green = False
                    for stack_id, stack in green_stacks_5m.items():
                        if stack['start_idx'] <= idx_5m <= stack['end_idx']:
                            in_green = True
                            break
                    if not in_green:
                        continue
                # 绿柱堆转红柱堆：不需要检查绿柱堆限制

            # 红柱堆信号：检查底部抬升
            elif sig_type == 'red':
                diver_ok, diver_reason, _, _ = self.check_60m_bottom_rise_in_red(df_60m, idx_60m)
                if not diver_ok:
                    continue

            # 计算止损价
            stop_loss, stop_reason = self.get_initial_stop_loss(
                df_5m, idx_5m,
                green_stacks_5m, {},
                df_60m, green_stacks_60m
            )

            if stop_loss is None:
                continue

            # 检查止损价是否 >= 入场价
            if stop_loss >= current_price:
                continue

            # 计算入场价（突破绿柱堆高点或当前价）
            if sig_type == 'green' and sub_type == 'dif_turn':
                # 获取当前绿柱堆的高点作为突破价
                entry_price = current_price  # 简化：直接用当前价
            else:
                entry_price = current_price

            entry_signal = {
                'entry_price': entry_price,
                'entry_time': current_time,
                'stop_loss': stop_loss,
                'reason': f"{diver_reason} + 5分钟阳柱确认"
            }

            return entry_signal, stop_loss, f"入场信号: {entry_signal['reason']}"

        return None, None, "未满足5分钟入场条件"
