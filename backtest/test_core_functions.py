#!/usr/bin/env python3
"""
策略核心功能单元测试 - 绿柱堆拐头+底抬、红柱堆内拐头
"""

import sys
import os
import unittest
sys.path.insert(0, os.path.dirname(__file__))

from strategy_indicators import MACDCalculator, StackIdentifier
from strategy_logic import Strategy


def create_kline_with_macd(closes: list, dif_values: list, hist_values: list) -> list:
    """创建测试用K线数据，直接指定DIF和hist值"""
    import random
    data = []
    for i, close in enumerate(closes):
        random.seed(i * 100)
        open_price = close + random.uniform(-3, 3)
        high = max(close, open_price) + random.uniform(0, 5)
        low = min(close, open_price) - random.uniform(0, 5)
        volume = random.randint(1000, 10000)
        time = f"2025-01-0{(i // 10) + 1} {9 + i % 10:02d}:00:00"

        dif = dif_values[i]
        # hist = 2 * (dif - dea) => dea = dif - hist / 2
        dea = dif - hist_values[i] / 2
        hist = hist_values[i]
        ma20 = close

        data.append((time, open_price, high, low, close, volume, dif, dea, hist, ma20))
    return data


def create_kline_data(closes: list, opens: list = None) -> list:
    """创建测试用K线数据"""
    import random
    data = []
    if opens is None:
        opens = [c + random.uniform(-3, 3) for c in closes]

    for i, (close, open_price) in enumerate(zip(closes, opens)):
        random.seed(i * 100)
        high = max(close, open_price) + random.uniform(0, 5)
        low = min(close, open_price) - random.uniform(0, 5)
        volume = random.randint(1000, 10000)
        time = f"2025-01-0{i%9+1} 09:00:00"
        data.append((time, open_price, high, low, close, volume))
    return data


class TestGreenStackDifTurnAndBottomRise(unittest.TestCase):
    """测试绿柱堆拐头 + 底抬 (底背离)"""

    def setUp(self):
        self.strategy = Strategy({'PriceTick': 0.2})

    def test_green_stack_dif_turn_success(self):
        """测试：绿柱堆内 DIF 拐头 - 成功场景"""
        # 创建绿柱堆数据：DIF 先跌后涨（完整拐头）
        # 模拟：dif 从 -10 跌到 -15，再反弹到 -12
        closes = [100, 97, 95, 96, 98, 99, 100, 101, 102, 103]

        # DIF 序列：-10, -13, -15, -14, -12, -11, -10, -9, -8, -7
        # 在 idx=4 时检查：
        #   dif_3 (idx-1) = -14, dif_2 (idx-2) = -15, dif_1 (idx-3) = -13
        #   has_drop: dif_3(-14) > dif_2(-15) = True（有下跌）
        #   dif_0 (idx) = -12, dif_2 (idx-2) = -15
        #   has_rise: dif_0(-12) > dif_2(-15) = True（有反弹）
        dif_values = [-10, -13, -15, -14, -12, -11, -10, -9, -8, -7]
        hist_values = [-4, -4, -4, -3, -3, -2, -2, -1, -1, -1]

        test_data = create_kline_with_macd(closes, dif_values, hist_values)

        # 堆识别
        df_result, green_stacks, green_gaps = StackIdentifier.identify(test_data)

        # 测试第4根（绿柱堆内，DIF拐头）
        result, reason = self.strategy.check_60m_dif_turn_in_green(test_data, 4, green_stacks)

        self.assertTrue(result, f"预期检测到绿柱堆拐头，实际: {reason}")
        print(f"✓ 绿柱堆拐头成功: {reason}")

    def test_green_stack_dif_no_turn(self):
        """测试：绿柱堆内 DIF 未拐头 - 失败场景"""
        # 创建绿柱堆数据：DIF 持续下跌，无拐头
        closes = [100, 99, 98, 97, 96, 95, 94, 93, 92, 91]
        data = create_kline_data(closes)

        # DIF 持续下跌
        test_data = []
        dif_values = [-10, -11, -12, -13, -14, -15, -16, -17, -18, -19]

        for i, row in enumerate(data):
            time, open, high, low, close, volume = row
            dif = dif_values[i]
            dea = dif - 3  # 保持绿柱
            hist = dif - dea
            ma20 = close
            test_data.append((time, open, high, low, close, volume, dif, dea, hist, ma20))

        df_result, green_stacks, green_gaps = StackIdentifier.identify(test_data)

        # 测试第5根
        result, reason = self.strategy.check_60m_dif_turn_in_green(test_data, 5, green_stacks)

        self.assertFalse(result, "DIF未拐头应该返回False")
        print(f"✓ 绿柱堆未拐头正确: {reason}")

    def test_green_stack_bottom_rise_success(self):
        """测试：绿柱堆底抬（底背离）- 成功场景"""
        # 创建三个绿柱堆，用红柱分隔
        # 第一个绿柱堆 -> 红柱 -> 第二个绿柱堆 -> 红柱 -> 第三个绿柱堆
        # 检测第三个绿柱堆和第二个绿柱堆的底背离
        n = 25
        closes = [100 + i for i in range(n)]

        # 低点序列：
        # 绿柱堆1 (i=0-3): 低点 90
        # 红柱 (i=4): hist > 0
        # 绿柱堆2 (i=5-9): 低点 88
        # 红柱 (i=10): hist > 0
        # 绿柱堆3 (i=11-24): 低点 92（比88高，作为当前堆）
        lows = [100, 92, 90, 91, 95, 92, 90, 89, 88, 89, 95, 95, 92, 94, 96, 98, 100, 102, 104, 106, 108, 110, 112, 114, 116]

        # DIF/Hist:
        # 绿柱堆1 (i=0-3): hist < 0
        # 红柱 (i=4): hist > 0
        # 绿柱堆2 (i=5-9): hist < 0
        # 红柱 (i=10): hist > 0
        # 绿柱堆3 (i=11-24): hist < 0
        dif_values = [-10 + i * 0.5 for i in range(4)]  # 绿柱堆1
        dif_values += [5]  # 红柱
        dif_values += [-8 + i * 0.3 for i in range(5)]  # 绿柱堆2
        dif_values += [6]  # 红柱
        dif_values += [-3 + i * 0.2 for i in range(14)]  # 绿柱堆3

        hist_values = [-4 + i * 0.5 for i in range(4)]  # 绿柱堆1
        hist_values += [2]  # 红柱
        hist_values = [-3 + i * 0.5 for i in range(5)]  # 绿柱堆2
        hist_values += [3]  # 红柱
        hist_values += [-1 - i * 0.1 for i in range(14)]  # 绿柱堆3

        # 重新构建完整的 hist_values
        hist_values = []
        # 绿柱堆1 (i=0-3)
        hist_values.extend([-4 + i * 0.5 for i in range(4)])
        # 红柱 (i=4)
        hist_values.append(2)
        # 绿柱堆2 (i=5-9)
        hist_values.extend([-3 + i * 0.3 for i in range(5)])
        # 红柱 (i=10)
        hist_values.append(3)
        # 绿柱堆3 (i=11-24)
        hist_values.extend([-1 - i * 0.08 for i in range(14)])

        test_data = create_kline_with_macd(closes, dif_values, hist_values)

        # 修改 low 值
        for i in range(len(test_data)):
            row = list(test_data[i])
            row[3] = lows[i]  # low
            test_data[i] = tuple(row)

        # 打印所有 hist 值用于调试
        print("  Hist 序列:")
        for i, row in enumerate(test_data):
            print(f"    i={i}: hist={row[8]:.2f}, low={row[3]:.1f}")

        # 堆识别
        df_result, green_stacks, green_gaps = StackIdentifier.identify(test_data)

        # 打印调试信息
        print(f"  识别到的绿柱堆数量: {len(green_stacks)}")
        for sid, info in green_stacks.items():
            print(f"    堆 {sid}: start={info.get('start_idx')}, end={info.get('end_idx')}, low={info.get('low')}")

        # 在第三个绿柱堆上检测底背离（索引20）
        result, reason, curr_low, prev_low = self.strategy.check_60m_divergence(test_data, 20)

        self.assertTrue(result, f"预期检测到底背离，实际: {reason}, curr_low={curr_low}, prev_low={prev_low}")
        self.assertGreaterEqual(curr_low, prev_low, "当前低点应该 >= 前一个低点")
        print(f"✓ 绿柱堆底背离成功: {reason}, curr_low={curr_low:.2f}, prev_low={prev_low:.2f}")

    def test_green_stack_bottom_not_rise(self):
        """测试：绿柱堆底未抬升 - 失败场景"""
        # 创建两个绿柱堆，第二个低点低于第一个（未底背离）
        n = 15
        closes = [100 + i for i in range(n)]

        # 低点序列：
        # 绿柱堆1 (i=0-3): 低点 90
        # 红柱 (i=4): hist > 0
        # 绿柱堆2 (i=5-14): 低点 85（低于90）
        lows = [100, 92, 90, 91, 95, 90, 88, 87, 86, 85, 87, 89, 91, 93, 95]

        # DIF/Hist:
        # 绿柱堆1 (i=0-3): hist < 0
        # 红柱 (i=4): hist > 0
        # 绿柱堆2 (i=5-14): hist < 0
        hist_values = [-4, -3.5, -3, -2.5, 2, -2, -1.8, -1.6, -1.4, -1.2, -1, -0.8, -0.6, -0.4, -0.2]
        dif_values = [-10 + i * 0.5 for i in range(5)]
        dif_values += [-5 + i * 0.3 for i in range(10)]

        test_data = create_kline_with_macd(closes, dif_values, hist_values)

        # 修改 low 值
        for i in range(len(test_data)):
            row = list(test_data[i])
            row[3] = lows[i]
            test_data[i] = tuple(row)

        # 检测底背离
        result, reason, curr_low, prev_low = self.strategy.check_60m_divergence(test_data, 12)

        self.assertFalse(result, "底未抬升应该返回False")
        print(f"✓ 绿柱堆底未抬升正确: {reason}")


class TestRedStackDifTurn(unittest.TestCase):
    """测试红柱堆内 DIF 拐头"""

    def setUp(self):
        self.strategy = Strategy({'PriceTick': 0.2})

    def test_red_stack_dif_turn_success(self):
        """测试：红柱堆内 DIF 拐头 - 成功场景"""
        # 创建红柱堆数据：DIF 先跌后涨（完整拐头）
        # 模拟：红柱堆内 dif 从 10 跌到 5，再反弹到 6
        # 关键：需要连续红柱 (hist > 0)
        closes = [100, 101, 102, 103, 104, 105, 106, 107, 108, 109]

        # DIF 序列：10, 9, 8, 6, 5, 5, 6, 7, 8, 9
        # 在 idx=6 检查时：
        #   dif_3 = dif[3] = 6, dif_2 = dif[2] = 8, dif_1 = dif[1] = 5
        #   has_drop: 6 > 8 > 5 = False ❌ 错误
        # 重新设计：
        # DIF: idx=6 时，dif[3]=8, dif[2]=6, dif[1]=5, dif[0]=6
        # has_drop: 8 > 6 > 5 = True (8 > 6 且 6 > 5)
        # has_rise: 5 < 6 = True
        dif_values = [10, 9, 8, 7, 6, 5, 6, 7, 8, 9]  # 正确的拐头序列
        hist_values = [8, 7, 6, 5, 4, 3, 4, 5, 6, 7]  # 红柱

        test_data = create_kline_with_macd(closes, dif_values, hist_values)

        # 测试第6根（红柱堆内 DIF 拐头）
        result, reason = self.strategy.check_60m_dif_turn_in_red(test_data, 6)

        self.assertTrue(result, f"预期检测到红柱堆内拐头，实际: {reason}")
        print(f"✓ 红柱堆内 DIF 拐头成功: {reason}")

    def test_red_stack_dif_no_turn(self):
        """测试：红柱堆内 DIF 未拐头 - 失败场景"""
        # 创建红柱堆数据：DIF 持续上涨，无拐头
        closes = [100, 101, 102, 103, 104, 105, 106, 107, 108, 109]
        data = create_kline_data(closes)

        # DIF 持续上涨
        test_data = []
        dif_values = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]

        for i, row in enumerate(data):
            time, open, high, low, close, volume = row
            dif = dif_values[i]
            dea = 5
            hist = 2 * (dif - dea)  # 红柱
            ma20 = close
            test_data.append((time, open, high, low, close, volume, dif, dea, hist, ma20))

        result, reason = self.strategy.check_60m_dif_turn_in_red(test_data, 6)

        self.assertFalse(result, "DIF未拐头应该返回False")
        print(f"✓ 红柱堆未拐头正确: {reason}")

    def test_red_stack_not_stable(self):
        """测试：红柱堆未稳定形成 - 失败场景"""
        # 当前是红柱但前一根不是（刚转红），不算稳定红柱堆
        closes = [100, 99, 98, 97, 98, 99, 100, 101, 102, 103]
        data = create_kline_data(closes)

        test_data = []
        # hist 从负转正（刚转红）
        hist_values = [-2, -1, 0, 1, 2, 3, 4, 5, 6, 7]
        dif_values = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17]

        for i, row in enumerate(data):
            time, open, high, low, close, volume = row
            dif = dif_values[i]
            dea = 10
            hist = hist_values[i]
            ma20 = close
            test_data.append((time, open, high, low, close, volume, dif, dea, hist, ma20))

        # 第3根是刚转红，检查应该失败
        result, reason = self.strategy.check_60m_dif_turn_in_red(test_data, 3)

        self.assertFalse(result, "红柱堆未稳定应该返回False")
        print(f"✓ 红柱堆未稳定正确: {reason}")

    def test_red_stack_bottom_rise_success(self):
        """测试：红柱堆底部抬升 - 成功场景"""
        # 红柱堆内 DIF 拐头，且红柱堆低点高于前一个绿柱堆低点
        # 前一个绿柱堆低点：90，红柱堆拐头区域低点：92
        n = 16
        closes = [100 + i for i in range(n)]

        # 低点：前一个绿柱堆最低90，红柱堆拐头区域最低92
        lows = [100, 90, 91, 92, 93, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102]

        # DIF/Hist 序列
        # 绿柱堆 (i=0-2): hist < 0
        # 红柱堆 (i=3-15): dif 先跌后涨 (拐头)
        #   在 idx=7 检查：dif[4]=12, dif[5]=10, dif[6]=8, dif[7]=9
        #   has_drop: 12 > 10 > 8 = True ✓
        #   has_rise: 8 < 9 = True ✓
        dif_values = [-10 + i * 0.5 for i in range(3)]  # 绿柱
        dif_values += [14, 12, 10, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]  # 红柱：先跌后涨

        hist_values = [-4 + i * 0.5 for i in range(3)]  # 绿柱
        hist_values += [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]  # 红柱

        test_data = create_kline_with_macd(closes, dif_values, hist_values)

        # 修改 low 值
        for i in range(len(test_data)):
            row = list(test_data[i])
            row[3] = lows[i]  # low
            test_data[i] = tuple(row)

        # 打印调试信息
        print("  DIF 序列:")
        for i, row in enumerate(test_data):
            print(f"    i={i}: dif={row[6]:.1f}, hist={row[8]:.1f}, low={row[3]:.1f}")

        # 测试第7根（红柱堆内 DIF 拐头，检查底部抬升）
        result, reason, turn_low, prev_green_low = self.strategy.check_60m_bottom_rise_in_red(test_data, 7)

        self.assertTrue(result, f"预期检测到红柱堆底部抬升，实际: {reason}, turn_low={turn_low}, prev_green_low={prev_green_low}")
        print(f"✓ 红柱堆底部抬升成功: {reason}, turn_low={turn_low:.2f}, prev_green_low={prev_green_low:.2f}")
        print(f"✓ 红柱堆底部抬升成功: {reason}, turn_low={turn_low:.2f}, prev_green_low={prev_green_low:.2f}")

        # 测试第7根（红柱堆内 DIF 拐头，检查底部抬升）
        result, reason, turn_low, prev_green_low = self.strategy.check_60m_bottom_rise_in_red(test_data, 7)

        self.assertTrue(result, f"预期检测到红柱堆底部抬升，实际: {reason}")
        print(f"✓ 红柱堆底部抬升成功: {reason}, turn_low={turn_low:.2f}, prev_green_low={prev_green_low:.2f}")

    def test_red_stack_bottom_not_rise(self):
        """测试：红柱堆底部未抬升 - 失败场景"""
        # 红柱堆内 DIF 拐头，但红柱堆低点低于前一个绿柱堆低点
        closes = [100, 98, 96, 94, 92, 90, 88, 89, 90, 91, 92, 93]
        data = create_kline_data(closes)

        test_data = []
        lows = [100, 98, 96, 94, 92, 90, 88, 87, 88, 89, 90, 91]

        for i, row in enumerate(data):
            time, open, high, low, volume = row[:5]
            low = lows[i]

            if i < 2:
                dif = -8 + i
                dea = -10
                hist = -4
            else:
                if i < 5:
                    dif = -6 + i * 0.5
                else:
                    dif = -3.5 + (i - 5)
                dea = -2
                hist = 2 * (dif - dea)

            ma20 = lows[i]
            test_data.append((time, open, high, low, lows[i], volume, dif, dea, hist, ma20))

        result, reason, turn_low, prev_green_low = self.strategy.check_60m_bottom_rise_in_red(test_data, 7)

        self.assertFalse(result, "底部未抬升应该返回False")
        print(f"✓ 红柱堆底部未抬升正确: {reason}")


class TestEdgeCases(unittest.TestCase):
    """边界情况测试"""

    def setUp(self):
        self.strategy = Strategy({'PriceTick': 0.2})

    def test_insufficient_data(self):
        """测试：数据不足场景"""
        closes = [100, 101, 102]
        data = create_kline_data(closes)

        test_data = []
        for i, row in enumerate(data):
            time, open, high, low, close, volume = row
            test_data.append((time, open, high, low, close, volume, 10, 8, 4, close))

        # idx=3 但数据只有3根
        result, reason = self.strategy.check_60m_dif_turn_in_green(test_data, 3, {})

        self.assertFalse(result)
        self.assertIn("数据不足", reason)
        print(f"✓ 数据不足正确处理: {reason}")

    def test_non_green_stack(self):
        """测试：非绿柱堆场景"""
        closes = [100, 101, 102, 103, 104, 105]
        data = create_kline_data(closes)

        # 设置为红柱
        test_data = []
        for i, row in enumerate(data):
            time, open, high, low, close, volume = row
            dif = 10 + i
            dea = 8
            hist = 4  # 红柱
            test_data.append((time, open, high, low, close, volume, dif, dea, hist, close))

        result, reason = self.strategy.check_60m_dif_turn_in_green(test_data, 5, {})

        self.assertFalse(result)
        self.assertIn("非绿柱堆", reason)
        print(f"✓ 非绿柱堆正确处理: {reason}")


if __name__ == "__main__":
    print("=" * 60)
    print("策略核心功能单元测试")
    print("=" * 60)

    # 创建测试套件
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # 添加测试类
    suite.addTests(loader.loadTestsFromTestCase(TestGreenStackDifTurnAndBottomRise))
    suite.addTests(loader.loadTestsFromTestCase(TestRedStackDifTurn))
    suite.addTests(loader.loadTestsFromTestCase(TestEdgeCases))

    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 60)
    if result.wasSuccessful():
        print("✅ 所有测试通过")
    else:
        print(f"❌ {len(result.failures)} 个测试失败, {len(result.errors)} 个错误")
    print("=" * 60)