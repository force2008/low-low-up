#!/usr/bin/env python3
"""
边界条件叠加模块 - 将有效的 Indicator 条件应用到策略

功能：
1. 根据分析结果生成过滤条件
2. 将条件叠加到回测策略中
3. 比较叠加前后的效果差异
"""

import sys
import os
import re
from typing import List, Dict, Tuple, Optional, Callable
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from utils.strategy_config import Config


@dataclass
class FilterCondition:
    """过滤条件"""
    indicator_name: str           # 指标名称
    lower_bound: float = 0.0   # 下界，包含
    upper_bound: float = 100.0  # 上界，包含
    enabled: bool = True        # 是否启用


@dataclass
class CompareResult:
    """对比结果"""
    condition_name: str
    before_trades: int = 0
    before_pnl: float = 0.0
    before_win_rate: float = 0.0
    after_trades: int = 0
    after_pnl: float = 0.0
    after_win_rate: float = 0.0
    improvement: float = 0.0    # 改善程度
    filtered_trades: int = 0    # 被过滤的交易数


class ConditionOverlay:
    """边界条件叠加器"""

    def __init__(self, config: Config = None):
        self.config = config or Config()
        self.conditions: List[FilterCondition] = []

    def add_condition(self, indicator_name: str, lower: float, upper: float):
        """添加过滤条件"""
        condition = FilterCondition(
            indicator_name=indicator_name,
            lower_bound=lower,
            upper_bound=upper,
            enabled=True
        )
        self.conditions.append(condition)
        return condition

    def generate_condition_from_result(self, result) -> Optional[FilterCondition]:
        """从分析结果生成过滤条件

        Args:
            result: BatchIndicatorResult 对象

        Returns:
            FilterCondition 或 None
        """
        if not hasattr(result, 'is_valid') or not result.is_valid:
            return None

        if not result.profit_zone:
            return None

        lower, upper = result.profit_zone
        if lower >= upper:
            return None

        return FilterCondition(
            indicator_name=result.name,
            lower_bound=lower,
            upper_bound=upper,
            enabled=True
        )

    def check_entry(self, indicator_value: float, indicator_name: str) -> bool:
        """检查是否满足入场条件

        Args:
            indicator_value: 当前 indicator 值
            indicator_name: 指标名称

        Returns:
            True 表示满足条件，可以入场；False 表示被过滤
        """
        # 如果没有条件，返回True（不做过滤）
        if not self.conditions:
            return True

        # 检查所有启用的条件
        for condition in self.conditions:
            if not condition.enabled:
                continue

            if condition.indicator_name != indicator_name:
                continue

            # 检查是否在范围内
            if condition.lower_bound <= indicator_value <= condition.upper_bound:
                return True
            else:
                return False

        # 没找到匹配的指标，不做过滤
        return True

    def get_active_conditions(self) -> List[FilterCondition]:
        """获取所有启用的条件"""
        return [c for c in self.conditions if c.enabled]

    def generate_filter_function(self, indicator_name: str) -> Callable:
        """生成过滤函数

        Returns:
            一个函数，输入 indicator 值，返回是否通过
        """
        active = self.get_active_conditions()
        matching = [c for c in active if c.indicator_name == indicator_name]

        if not matching:
            # 无条件，返回始终 True 的函数
            return lambda x: True

        condition = matching[0]
        return lambda x: condition.lower_bound <= x <= condition.upper_bound


class BacktestComparator:
    """回测对比器"""

    def __init__(self, analyzer):
        self.analyzer = analyzer

    def run_comparison(self, symbol: str,
                   base_conditions: List[FilterCondition],
                   indicator_name: str) -> CompareResult:
        """运行对比回测

        Args:
            symbol: 合约代码
            base_conditions: 基础过滤条件（已有条件）
            indicator_name: 要测试的指标名

        Returns:
            CompareResult
        """
        # 先运行不含新条件的回测
        trades_before, _ = self.analyzer.run_backtest_with_indicators(
            symbol, [],  # 不记录任何指标
            base_conditions
        )

        # 再运行含新条件的回测
        overlay = ConditionOverlay(self.config)
        for cond in base_conditions:
            overlay.add_condition(cond.indicator_name, cond.lower_bound, cond.upper_bound)

        trades_after, indicator_values = self.analyzer.run_backtest_with_indicators(
            symbol, [indicator_name], base_conditions
        )

        # 比较
        before_trades = len(trades_before)
        before_pnl = sum(t.pnl for t in trades_before)
        before_winning = sum(1 for t in trades_before if t.pnl > 0)
        before_win_rate = before_winning / before_trades * 100 if before_trades else 0

        after_trades = len(trades_after)
        after_pnl = sum(t.pnl for t in trades_after)
        after_winning = sum(1 for t in trades_after if t.pnl > 0)
        after_win_rate = after_winning / after_trades * 100 if after_trades else 0

        improvement = after_pnl - before_pnl

        return CompareResult(
            condition_name=indicator_name,
            before_trades=before_trades,
            before_pnl=before_pnl,
            before_win_rate=before_win_rate,
            after_trades=after_trades,
            after_pnl=after_pnl,
            after_win_rate=after_win_rate,
            improvement=improvement,
            filtered_trades=before_trades - after_trades
        )


def apply_conditions_to_strategy(strategy_class, conditions: List[FilterCondition]):
    """应用条件到策略类

    这个函数返回一个修改后的策略类，包含额外的入场过滤条件
    """
    # 获取原始的 check_entry 方法
    original_check_entry = strategy_class.check_5m_entry

    def modified_check_5m_entry(cls_self, df_5m, idx, green_stacks_5m):
        """修改后的入场检查方法"""
        # 首先调用原始逻辑
        result, reason = original_check_entry(cls_self, df_5m, idx, green_stacks_5m)
        if not result:
            return result, reason

        # 然后检查过滤条件
        for cond in conditions:
            if not cond.enabled:
                continue

            # 获取当前指标值
            indicator_value = cls_self.get_current_indicator(
                df_5m, idx, cond.indicator_name
            )

            if indicator_value is not None:
                if not (cond.lower_bound <= indicator_value <= cond.upper_bound):
                    return False, f"Indicator {cond.indicator_name} 不在范围内: {indicator_value}"

        return True, reason

    return modified_check_5m_entry


def generate_condition_config(conditions: List[FilterCondition],
                         output_path: str = './strategy/filters.json'):
    """生成条件配置文件（供运行时加载）"""
    import json

    config_data = {}
    for cond in conditions:
        config_data[cond.indicator_name] = {
            'lower': cond.lower_bound,
            'upper': cond.upper_bound,
            'enabled': cond.enabled
        }

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(config_data, f, indent=2)

    print(f"条件配置已保存到: {output_path}")
    return output_path


def load_condition_config(input_path: str) -> List[FilterCondition]:
    """从配置文件加载条件"""
    import json

    with open(input_path, 'r', encoding='utf-8') as f:
        config_data = json.load(f)

    conditions = []
    for name, config in config_data.items():
        cond = FilterCondition(
            indicator_name=name,
            lower_bound=config.get('lower', 0),
            upper_bound=config.get('upper', 100),
            enabled=config.get('enabled', True)
        )
        conditions.append(cond)

    return conditions


def test_condition():
    """测试条件功能"""
    import random

    # 创建模拟的条件
    conditions = []

    # 假设 RSI 在 25-40 之间盈利
    cond1 = FilterCondition(
        indicator_name='RSI_close_14',
        lower_bound=25,
        upper_bound=40
    )
    conditions.append(cond1)

    # 创建一个叠加器
    overlay = ConditionOverlay()

    # 添加条件
    for c in conditions:
        overlay.add_condition(c.indicator_name, c.lower_bound, c.upper_bound)

    # 测试
    print("=== 测试过滤 ===")

    test_values = [
        (20, False, "RSI 20 应该被过滤"),
        (30, True, "RSI 30 在范围内"),
        (45, False, "RSI 45 应该被过滤"),
    ]

    for val, expected, desc in test_values:
        result = overlay.check_entry(val, 'RSI_close_14')
        status = "✅" if result == expected else "❌"
        print(f"{status} {desc}: {val} -> {result}")

    print("\n=== 测试结果生成 ===")

    # 模拟 BatchIndicatorResult
    class MockResult:
        def __init__(self):
            self.name = 'RSI_close_14'
            self.is_valid = True
            self.profit_zone = (25.0, 40.0)

    result = MockResult()
    new_cond = overlay.generate_condition_from_result(result)

    if new_cond:
        print(f"从结果生成条件: {new_cond.indicator_name}")
        print(f"  范围: [{new_cond.lower_bound}, {new_cond.upper_bound}]")


if __name__ == '__main__':
    test_condition()