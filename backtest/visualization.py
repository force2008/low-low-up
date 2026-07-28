#!/usr/bin/env python3
"""
Visualization Module - 生成 Cumulative Sum Curve 可视化

生成类似课件中的图表：
1. Cumulative Sum 曲线
2. Indicator 随时间变化图
3. 边界条件可视化
"""

import sys
import os
import math
from typing import List, Dict, Tuple, Optional
from pathlib import Path
from dataclasses import dataclass

try:
    import matplotlib
    matplotlib.use('Agg')  # 非交互式后端
    import matplotlib.pyplot as plt
    import matplotlib.cm as cm
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    print("警告: matplotlib 未安装，将跳过图表生成")


@dataclass
class ChartConfig:
    """图表配置"""
    width: int = 12
    height: int = 8
    dpi: int = 100
    title_fontsize: int = 12
    label_fontsize: int = 10
    line_width: float = 1.5
    colors: Tuple[str, str] = ('red', 'green')  # (profit, loss)


class CumulativeChartGenerator:
    """Cumulative Sum 曲线图表生成器"""

    def __init__(self, output_dir: str = './backtest/reports/charts'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.config = ChartConfig()

    def generate_single_chart(self,
                    indicator_name: str,
                    sorted_indicators: List[float],
                    pnl_sorted: List[float],
                    title: str = None) -> str:
        """生成单个指标的累积和曲线图

        Args:
            indicator_name: 指标名称
            sorted_indicators: 排序后的指标值
            pnl_sorted: 排序后的 PnL
            title: 图表标题

        Returns:
            保存的文件路径
        """
        if not HAS_MATPLOTLIB:
            return ""

        if not sorted_indicators or not pnl_sorted:
            return ""

        # 计算累积和
        cumsum = []
        total = 0.0
        for pnl in pnl_sorted:
            total += pnl
            cumsum.append(total)

        # 创建图表 - 3个子图
        fig, axes = plt.subplots(3, 1, figsize=(self.config.width, self.config.height * 1.5))
        fig.suptitle(title or f"Cumulative Sum Analysis: {indicator_name}", fontsize=self.config.title_fontsize)

        # 子图1: Cumulative Sum 曲线
        ax1 = axes[0]
        x = range(len(cumsum))
        ax1.plot(x, cumsum, color=self.config.colors[0], linewidth=self.config.line_width)
        ax1.fill_between(x, cumsum, 0, where=[c > 0 for c in cumsum],
                      interpolate=True, alpha=0.3, color='green')
        ax1.fill_between(x, cumsum, 0, where=[c <= 0 for c in cumsum],
                      interpolate=True, alpha=0.3, color='red')
        ax1.axhline(y=0, color='gray', linestyle='--', linewidth=0.5)
        ax1.set_xlabel('Sorted Index', fontsize=self.config.label_fontsize)
        ax1.set_ylabel('Cumulative PnL', fontsize=self.config.label_fontsize)
        ax1.set_title('Cumulative Sum Curve', fontsize=self.config.label_fontsize)
        ax1.grid(True, alpha=0.3)

        # 标注最大值和最小值
        if cumsum:
            max_idx = cumsum.index(max(cumsum))
            min_idx = cumsum.index(min(cumsum))
            ax1.annotate(f'Max: {cumsum[max_idx]:.2f}', xy=(max_idx, cumsum[max_idx]),
                       xytext=(max_idx, cumsum[max_idx] + max(cumsum) * 0.1),
                       fontsize=8)
            ax1.annotate(f'Min: {cumsum[min_idx]:.2f}', xy=(min_idx, cumsum[min_idx]),
                       xytext=(min_idx, cumsum[min_idx] - max(cumsum) * 0.1),
                       fontsize=8)

        # ���图2: Indicator 值分布
        ax2 = axes[1]
        ax2.scatter(range(len(sorted_indicators)), sorted_indicators,
                  c=pnl_sorted, cmap='RdYlGn', s=10, alpha=0.6)
        ax2.set_xlabel('Sorted Index', fontsize=self.config.label_fontsize)
        ax2.set_ylabel(indicator_name, fontsize=self.config.label_fontsize)
        ax2.set_title(f'{indicator_name} Distribution (colored by PnL)', fontsize=self.config.label_fontsize)
        ax2.grid(True, alpha=0.3)

        # 子图3: 每个 indicator 值对应的 PnL（散点图）
        ax3 = axes[2]
        scatter = ax3.scatter(sorted_indicators, pnl_sorted,
                          c=cumsum, cmap='RdYlGn', s=15, alpha=0.6)
        ax3.axhline(y=0, color='gray', linestyle='--', linewidth=0.5)
        ax3.set_xlabel(indicator_name, fontsize=self.config.label_fontsize)
        ax3.set_ylabel('PnL', fontsize=self.config.label_fontsize)
        ax3.set_title(f'PnL vs {indicator_name}', fontsize=self.config.label_fontsize)
        ax3.grid(True, alpha=0.3)

        # 添加颜色条
        plt.colorbar(scatter, ax=ax3, label='Cumulative PnL')

        plt.tight_layout()

        # 保存
        filename = f"{indicator_name.replace(' ', '_').replace('/', '_')}.png"
        filepath = self.output_dir / filename
        plt.savefig(filepath, dpi=self.config.dpi)
        plt.close()

        return str(filepath)

    def generate_report(self,
                     results: List,
                     output_filename: str = 'indicator_report.html') -> str:
        """生成 HTML 报告

        Args:
            results: 分析结果列表
            output_filename: 输出文件名

        Returns:
            报告文件路径
        """
        if not results:
            return ""

        html_path = self.output_dir / output_filename

        # 过滤有效结果
        valid_results = [r for r in results if hasattr(r, 'is_valid') and r.is_valid]

        # 生成 HTML
        html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Indicator Analysis Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        h1 {{ color: #333; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #4CAF50; color: white; }}
        tr:nth-child(even) {{ background-color: #f2f2f2; }}
        .valid {{ color: green; font-weight: bold; }}
        .invalid {{ color: gray; }}
        .chart-cell {{ text-align: center; }}
        img {{ max-width: 100%; height: auto; }}
        .summary {{ background: #f0f0f0; padding: 15px; margin: 15px 0; }}
    </style>
</head>
<body>
    <h1>Indicator Analysis Report</h1>
    <div class="summary">
        <strong>Total Indicators:</strong> {len(results)}<br>
        <strong>Valid Indicators:</strong> {len(valid_results)}
    </div>

    <h2>Top Valid Indicators</h2>
    <table>
        <tr>
            <th>#</th>
            <th>Indicator</th>
            <th>Trades</th>
            <th>Win Rate</th>
            <th>Total PnL</th>
            <th>Lower Bound</th>
            <th>Upper Bound</th>
            <th>Profit Zone</th>
            <th>Loss Zone</th>
        </tr>
"""

        for i, r in enumerate(valid_results[:50]):
            profit_zone = r.profit_zone if r.profit_zone else "-"
            loss_zone = r.loss_zone if r.loss_zone else "-"

            html += f"""
        <tr>
            <td>{i + 1}</td>
            <td>{r.name}</td>
            <td>{r.total_trades}</td>
            <td>{r.winning_rate:.1f}%</td>
            <td class="{'valid' if r.total_pnl > 0 else 'invalid'}">{r.total_pnl:.2f}</td>
            <td>{r.lower_bound:.2f}</td>
            <td>{r.upper_bound:.2f}</td>
            <td>{profit_zone}</td>
            <td>{loss_zone}</td>
        </tr>
"""

        html += """
    </table>
</body>
</html>
"""

        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html)

        return str(html_path)

    def generate_summary_stats(self, results: List) -> Dict:
        """生成汇总统计"""
        if not results:
            return {}

        total = len(results)
        valid = sum(1 for r in results if hasattr(r, 'is_valid') and r.is_valid)
        profitable = sum(1 for r in results if hasattr(r, 'total_pnl') and r.total_pnl > 0)

        avg_pnl = 0
        if results:
            avg_pnl = sum(r.total_pnl for r in results) / len(results)

        avg_winning_rate = 0
        if results:
            avg_winning_rate = sum(r.winning_rate for r in results) / len(results)

        return {
            'total_indicators': total,
            'valid_indicators': valid,
            'profitable_indicators': profitable,
            'avg_pnl': avg_pnl,
            'avg_winning_rate': avg_winning_rate,
        }


def test_chart():
    """测试图表生成"""
    if not HAS_MATPLOTLIB:
        print("matplotlib 未安装，跳过测试")
        return

    # 生成测试数据
    import random
    random.seed(42)

    indicator_values = list(range(20, 70))
    pnl_list = [random.uniform(-50, 50) for _ in range(len(indicator_values))]

    # 添加偏向：低 RSI 时倾向于赚钱
    for i, iv in enumerate(indicator_values):
        if iv < 30:
            pnl_list[i] += 30  # 低 RSI 多赚
        elif iv > 50:
            pnl_list[i] -= 30  # 高 RSI 多亏

    generator = CumulativeChartGenerator()

    # 添加偏向：
    # 创建有明显盈利模式的 indicator 值
    indicator_values = list(range(20, 70))
    pnl_list = []
    for iv in indicator_values:
        if iv < 30:
            pnl = random.uniform(20, 50)
        elif iv < 40:
            pnl = random.uniform(-10, 30)
        else:
            pnl = random.uniform(-40, -10)
        pnl_list.append(pnl)

    filepath = generator.generate_single_chart(
        'Test_RSI14',
        indicator_values,
        pnl_list,
        'Test RSI Analysis'
    )

    print(f"图表已保存到: {filepath}")


if __name__ == '__main__':
    test_chart()