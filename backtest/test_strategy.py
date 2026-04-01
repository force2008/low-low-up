#!/usr/bin/env python3
"""
策略单元测试 - 测试关键策略逻辑
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from strategy_indicators import MACDCalculator, StackIdentifier, IndexMapper
from strategy_logic import Strategy
from strategy_utils import DataLoader, Config


class TestStrategy:
    """策略测试类"""

    @staticmethod
    def create_test_data(closes: list) -> list:
        """创建测试数据"""
        # 生成模拟数据：每根K线包含 (time, open, high, low, close, volume)
        data = []
        for i, close in enumerate(closes):
            # 随机生成一些波动
            import random
            random.seed(i)
            open_price = close + random.uniform(-5, 5)
            high_price = max(close, open_price) + random.uniform(0, 5)
            low_price = min(close, open_price) - random.uniform(0, 5)
            volume = random.randint(1000, 10000)

            time = f"2025-01-01 {i:02d}:00:00"
            data.append((time, open_price, high_price, low_price, close, volume))

        return data

    @staticmethod
    def test_60m_dif_turn_in_green():
        """测试 60 分钟绿柱堆内 DIF 拐头"""
        print("\n=== 测试：60分钟绿柱堆内 DIF 拐头 ===")

        # 创建测试数据：绿柱堆，DIF 先跌后涨
        # DIF 序列：-10, -12, -15, -14, -13 (先跌后涨)
        closes = [100, 99, 98, 97, 98, 99, 100, 101, 100, 99]  # 价格先跌后涨
        data = TestStrategy.create_test_data(closes)

        # 计算 MACD
        df_with_macd = MACDCalculator.calculate(data)

        # 手动设置 DIF 值，模拟绿柱堆内拐头
        # 在第8根设置绿柱，DIF从低点上拐
        test_data = []
        for i, row in enumerate(df_with_macd):
            time, open, high, low, close, volume, dif, dea, hist, ma20 = row

            # 设置 DIF 模拟绿柱堆内拐头：-10, -12, -15, -14, -13, -11
            if i == 0:
                dif = -10
                hist = -5
            elif i == 1:
                dif = -12
                hist = -6
            elif i == 2:
                dif = -15
                hist = -8
            elif i == 3:
                dif = -14  # 反弹
                hist = -5
            elif i == 4:
                dif = -13  # 继续反弹
                hist = -4
            elif i >= 5:
                dif = -11
                hist = -3

            test_data.append((time, open, high, low, close, volume, dif, dea, hist, ma20))

        # 堆识别
        df_result, green_stacks, green_gaps = StackIdentifier.identify(test_data)

        # 测试策略方法
        strategy = Strategy({'PriceTick': 0.2})

        # 检查第5根（绿柱堆内 DIF 拐头）
        result, reason = strategy.check_60m_dif_turn_in_green(test_data, 5, green_stacks)
        print(f"第5根 - 绿柱堆内 DIF 拐头: {result}, {reason}")

        # 预期：应该检测到拐头
        assert result == True, f"预期检测到拐头，实际: {result}"
        print("✓ 测试通过")

    @staticmethod
    def test_60m_divergence():
        """测试 60 分钟底背离（绿柱堆低点抬升）"""
        print("\n=== 测试：60分钟底背离 ===")

        # 创建测试数据：两个绿柱堆，第二个低点高于第一个
        closes = [100, 99, 98, 97, 96, 95, 94, 95, 96, 97, 98, 99]  # 下跌后反弹
        data = TestStrategy.create_test_data(closes)

        # 设置 MACD 值模拟底背离
        test_data = []
        for i, row in enumerate(data):
            time, open, high, low, close, volume = row
            dif = -10 + i * 0.5  # DIF 上升
            dea = -8 + i * 0.4
            hist = 2 * (dif - dea)
            ma20 = close

            test_data.append((time, open, high, low, close, volume, dif, dea, hist, ma20))

        # 堆识别
        df_result, green_stacks, green_gaps = StackIdentifier.identify(test_data)

        print(f"识别到的绿柱堆数量: {len(green_stacks)}")
        for sid, info in green_stacks.items():
            print(f"  绿柱堆 {sid}: low={info['low']:.2f}")

        # 测试策略方法 - 检查第10根
        strategy = Strategy({'PriceTick': 0.2})
        result, reason, curr_low, prev_low = strategy.check_60m_divergence(test_data, 10)
        print(f"第10根 - 底背离: {result}, {reason}")
        print(f"  当前绿柱堆低点: {curr_low:.2f}, 前一个绿柱堆低点: {prev_low:.2f}")

        # 预期：应该检测到底背离（低点抬升或持平）
        assert result == True, f"预期检测到底背离，实际: {result}"
        print("✓ 测试通过")

    @staticmethod
    def test_60m_dif_turn_in_red():
        """测试 60 分钟红柱堆内 DIF 拐头"""
        print("\n=== 测试：60分钟红柱堆内 DIF 拐头 ===")

        # 创建测试数据：红柱堆，DIF 先跌后涨
        closes = [100, 101, 102, 101, 100, 101, 102, 103, 104, 105]
        data = TestStrategy.create_test_data(closes)

        # 设置 MACD 值模拟红柱堆内拐头
        test_data = []
        for i, row in enumerate(data):
            time, open, high, low, close, volume = row

            # 模拟红柱堆内 DIF 拐头：dif 先跌后涨
            if i < 3:
                dif = 10 - i * 2  # 10, 8, 6 (下跌)
            else:
                dif = 6 + i  # 反弹上涨

            dea = 5
            hist = 2 * (dif - dea)  # 红柱
            ma20 = close

            test_data.append((time, open, high, low, close, volume, dif, dea, hist, ma20))

        # 测试策略方法 - 检查第6根
        strategy = Strategy({'PriceTick': 0.2})
        result, reason = strategy.check_60m_dif_turn_in_red(test_data, 6)
        print(f"第6根 - 红柱堆内 DIF 拐头: {result}, {reason}")

        # 预期：应该检测到红柱堆内拐头
        assert result == True, f"预期检测到红柱堆内拐头，实际: {result}"
        print("✓ 测试通过")

    @staticmethod
    def test_60m_bottom_rise_in_red():
        """测试 60 分钟红柱堆底部抬升"""
        print("\n=== 测试：60分钟红柱堆底部抬升 ===")

        # 创建测试数据：红柱堆内 DIF 拐头，且红柱堆低点高于前一个绿柱堆低点
        closes = [95, 94, 93, 92, 91, 92, 93, 94, 95, 96]  # 先跌后涨
        data = TestStrategy.create_test_data(closes)

        # 设置 MACD 值
        test_data = []
        for i, row in enumerate(data):
            time, open, high, low, close, volume = row

            # 模拟：前两根是绿柱，后面是红柱
            if i < 2:
                dif = -10 + i
                dea = -8
                hist = -4  # 绿柱
            else:
                # 红柱堆内 DIF 拐头
                if i < 5:
                    dif = -8 + i * 0.5  # DIF 上升
                else:
                    dif = -5 + (i - 5)  # DIF 继续上升（拐头）
                dea = -2
                hist = 2 * (dif - dea)  # 红柱

            ma20 = close
            test_data.append((time, open, high, low, close, volume, dif, dea, hist, ma20))

        # 测试策略方法 - 检查第7根
        strategy = Strategy({'PriceTick': 0.2})
        result, reason, turn_low, prev_green_low = strategy.check_60m_bottom_rise_in_red(test_data, 7)
        print(f"第7根 - 红柱堆底部抬升: {result}, {reason}")
        print(f"  红柱拐头区域最低价: {turn_low:.2f}, 前一个绿柱堆最低价: {prev_green_low:.2f}")

        # 预期：应该检测到红柱堆底部抬升
        assert result == True, f"预期检测到红柱堆底部抬升，实际: {result}"
        print("✓ 测试通过")

    @staticmethod
    def test_real_data():
        """测试真实数据 - CZCE.AP605 2025-11-12"""
        print("\n=== 测试：真实数据 CZCE.AP605 2025-11-12 ===")

        config = Config()
        loader = DataLoader(config.DB_PATH, config.CONTRACTS_PATH)

        symbol = 'CZCE.AP605'

        # 加载 60 分钟数据
        df_60m_raw = loader.load_kline_fast(symbol, 3600, 2000)
        df_60m = MACDCalculator.calculate(df_60m_raw)
        df_60m, green_stacks_60m, green_gaps_60m = StackIdentifier.identify(df_60m)

        strategy = Strategy(None)

        # 找到 2025-11-12 的数据并测试
        found_signal = False
        for i in range(len(df_60m)):
            time_str = df_60m[i][0][:10]
            if time_str == '2025-11-12':
                hist = df_60m[i][8]

                # 检查红柱堆内 DIF 拐头
                if hist > 0 and i >= 4:
                    dif_turn_red, reason = strategy.check_60m_dif_turn_in_red(df_60m, i)
                    if dif_turn_red:
                        diver_ok, diver_reason, curr_low, prev_low = strategy.check_60m_bottom_rise_in_red(df_60m, i)
                        print(f"时间: {df_60m[i][0]}")
                        print(f"  红柱堆内 DIF 拐头: {dif_turn_red}, {reason}")
                        print(f"  底部抬升: {diver_ok}, {diver_reason}")
                        print(f"  红柱拐头低: {curr_low:.2f}, 绿柱堆低: {prev_low:.2f}")
                        found_signal = True

        if found_signal:
            print("✓ 找到红柱堆信号")
        else:
            print("⚠ 未找到红柱堆信号")

    @staticmethod
    def test_5m_green_stack_filter():
        """测试 5 分钟绿柱堆过滤"""
        print("\n=== 测试：5分钟绿柱堆底部抬升过滤 ===")

        config = Config()
        loader = DataLoader(config.DB_PATH, config.CONTRACTS_PATH)

        symbol = 'CZCE.AP605'

        # 加载 5 分钟数据
        df_5m_raw = loader.load_kline_fast(symbol, 300, 8000)
        df_5m = MACDCalculator.calculate(df_5m_raw)
        df_5m, green_stacks_5m, green_gaps_5m = StackIdentifier.identify(df_5m)

        strategy = Strategy({'PriceTick': 0.2})

        # 检查 5 分钟绿柱堆底部抬升
        # 取最后一个绿柱堆区域测试
        if len(green_stacks_5m) >= 2:
            # 找到最近一个有足够历史的索引
            test_idx = len(df_5m) - 100  # 取靠后的位置

            if test_idx >= 0:
                result, reason = strategy.check_5m_green_stack_filter(df_5m, test_idx, green_stacks_5m)
                print(f"第 {test_idx} 根 - 5分钟绿柱堆底部抬升: {result}, {reason}")

        print("✓ 测试通过")


def run_all_tests():
    """运行所有测试"""
    print("=" * 60)
    print("策略单元测试")
    print("=" * 60)

    test = TestStrategy()

    # 模拟数据测试
    test.test_60m_dif_turn_in_green()
    test.test_60m_divergence()
    test.test_60m_dif_turn_in_red()
    test.test_60m_bottom_rise_in_red()
    test.test_5m_green_stack_filter()

    # 真实数据测试
    test.test_real_data()

    print("\n" + "=" * 60)
    print("所有测试完成")
    print("=" * 60)


if __name__ == "__main__":
    run_all_tests()