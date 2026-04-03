#!/usr/bin/env python3
"""
IndexMapper 计算和映射工具类
"""
from typing import List, Tuple, Dict
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