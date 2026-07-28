# -*- coding: utf-8 -*-
"""
BOLL 中轨反弹/回踩策略

纯通知型策略：在日线和 60 分钟周期上检测价格接近 BOLL 中轨的信号。
"""

from typing import Optional, Dict


class StrategyBollMiddleBand:
    """BOLL 中轨反弹/回踩策略"""

    def __init__(self, symbol_info: dict):
        self.name = "BOLL中轨反弹回踩"
        self.symbol_info = symbol_info or {}
        self.tick_size = self.symbol_info.get('PriceTick', 0.2)

        # 按 symbol_timeframe_direction 记录最后发送信号的 K 线时间，防止同一根 K 线重复发
        self._last_sent_signals: Dict[str, str] = {}

    def check_boll_middle_band_signal(
        self,
        data: list,
        timeframe: str,
        min_consecutive: int = 3,
        proximity_threshold_pct: float = 0.005,
        symbol: str = "",
    ) -> Optional[dict]:
        """
        检测 BOLL 中轨附近的反弹/回踩信号。

        Args:
            data: K 线元组列表，需已附加 BOLL 列：
                  (datetime, open, high, low, close, volume, ma, upper, middle, lower)
            timeframe: "day" 或 "60min"
            min_consecutive: 要求连续收于中轨上方/下方的最少根数
            proximity_threshold_pct: 当前收盘价与中轨的接近阈值，默认 0.5%
            symbol: 合约代码，用于去重 key

        Returns:
            信号字典或 None
        """
        if len(data) < max(min_consecutive, 5) + 2:
            return None

        idx = len(data) - 1
        current_close = float(data[idx][4])
        current_middle = float(data[idx][8])

        if current_middle == 0:
            return None

        # 当前 K 线是否接近中轨
        proximity = abs(current_close - current_middle) / current_middle
        if proximity > proximity_threshold_pct:
            return None

        # 前一根 K 线应明显不接近中轨，避免连续重复触发
        prev_close = float(data[idx - 1][4])
        prev_middle = float(data[idx - 1][8])
        if prev_middle == 0:
            return None
        prev_proximity = abs(prev_close - prev_middle) / prev_middle
        if prev_proximity <= proximity_threshold_pct:
            return None

        # 先尝试：前一根及之前连续收于中轨下方
        count_below = 0
        for i in range(idx - 1, -1, -1):
            close_i = float(data[i][4])
            middle_i = float(data[i][8])
            if close_i < middle_i:
                count_below += 1
            else:
                break

        bar_time = str(data[idx][0])

        if count_below >= min_consecutive:
            signal_key = f"{symbol}_{timeframe}_short"
            if self._last_sent_signals.get(signal_key) == bar_time:
                return None
            self._last_sent_signals[signal_key] = bar_time

            label = "日" if timeframe == "day" else "60分钟"
            return {
                'signal_type': f"BOLL_REBOUND_{timeframe.upper()}",
                'timeframe': timeframe,
                'direction': 'short',
                'current_close': current_close,
                'middle_band': current_middle,
                'consecutive_count': count_below,
                'message': f"价格反弹，接近{label}K线中轨附近",
                'reason': (
                    f"连续{count_below}根收盘价低于BOLL中轨后反弹接近中轨，"
                    f"当前价{current_close:.2f}，中轨{current_middle:.2f}"
                ),
                'bar_time': bar_time,
            }

        # 再尝试：前一根及之前连续收于中轨上方
        count_above = 0
        for i in range(idx - 1, -1, -1):
            close_i = float(data[i][4])
            middle_i = float(data[i][8])
            if close_i > middle_i:
                count_above += 1
            else:
                break

        if count_above >= min_consecutive:
            signal_key = f"{symbol}_{timeframe}_long"
            if self._last_sent_signals.get(signal_key) == bar_time:
                return None
            self._last_sent_signals[signal_key] = bar_time

            label = "日" if timeframe == "day" else "60分钟"
            return {
                'signal_type': f"BOLL_PULLBACK_{timeframe.upper()}",
                'timeframe': timeframe,
                'direction': 'long',
                'current_close': current_close,
                'middle_band': current_middle,
                'consecutive_count': count_above,
                'message': f"价格回踩，接近{label}K线中轨附近",
                'reason': (
                    f"连续{count_above}根收盘价高于BOLL中轨后回踩接近中轨，"
                    f"当前价{current_close:.2f}，中轨{current_middle:.2f}"
                ),
                'bar_time': bar_time,
            }

        return None
