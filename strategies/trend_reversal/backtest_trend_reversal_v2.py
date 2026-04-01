#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TrendReversalV2 回测脚本 - 使用优化版图表
"""

import sqlite3
import json
import math
import os
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from TrendReversalV2Strategy import TrendReversalV2Strategy, Signal, Contract

@dataclass
class BacktestResult:
    symbol: str; contract_name: str; start_date: str; end_date: str
    initial_capital: float; final_equity: float; total_return: float
    total_trades: int; wins: int; losses: int; win_rate: float
    avg_win: float; avg_loss: float; profit_factor: float
    max_drawdown: float; sharpe_ratio: float
    trades: List[Dict]; equity_curve: List[Dict]


class BacktestEngine:
    def __init__(self, db_path: str, contracts_path: str, config: Dict = None):
        self.db_path = db_path
        self.contracts_path = contracts_path
        self.strategy = TrendReversalV2Strategy(config)
        self.strategy.load_contracts(contracts_path)
        self.base_output_dir = "/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/backtest"
        
    def get_kline_data(self, symbol: str, duration: int, limit: int) -> List[Dict]:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT datetime, open, high, low, close, volume FROM kline_data WHERE symbol = ? AND duration = ? ORDER BY datetime DESC LIMIT ?", (symbol, duration, limit))
        rows = cursor.fetchall()
        conn.close()
        return [{'time': row[0], 'open': row[1], 'high': row[2], 'low': row[3], 'close': row[4], 'volume': row[5]} for row in reversed(rows)]
    
    def get_available_symbols(self, limit: int = 50) -> List[str]:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT symbol FROM kline_data WHERE duration = 300 ORDER BY symbol LIMIT ?", (limit,))
        symbols = [row[0] for row in cursor.fetchall()]
        conn.close()
        return symbols
    
    def _get_output_dir(self, symbol: str) -> str:
        symbol_safe = symbol.replace('.', '_').replace('/', '_')
        output_dir = os.path.join(self.base_output_dir, symbol_safe)
        os.makedirs(output_dir, exist_ok=True)
        return output_dir
    
    def run_backtest(self, symbol: str) -> Optional[BacktestResult]:
        print(f"\n{'='*60}\n📊 回测：{symbol}\n{'='*60}")
        
        contract = self.strategy._get_contract(symbol)
        if not contract:
            for c in self.strategy.contracts.values():
                if c.MainContractID == symbol.split('.')[-1]:
                    contract = c
                    break
        
        if not contract:
            print(f"⚠️ 合约 {symbol} 不在配置中")
            return None
        
        print(f"✅ 合约：{contract.ProductID} | 乘数：{contract.VolumeMultiple}")
        
        data_5min = self.get_kline_data(symbol, 300, 500)
        print(f"✅ 5 分钟：{len(data_5min)} 条")
        
        data_60min = self.get_kline_data(symbol, 3600, 200)
        print(f"✅ 60 分钟：{len(data_60min)} 条")
        
        if len(data_5min) < 200 or len(data_60min) < 50:
            print("⚠️ 数据不足")
            return None
        
        print(f"📅 时间：{data_5min[0]['time']} ~ {data_5min[-1]['time']}")
        
        signals = self.strategy.run_backtest(symbol, data_5min, data_60min)
        if not signals:
            print("⚠️ 无交易信号")
            return None
        
        print(f"📈 {len(signals)} 个交易信号")
        
        initial_capital = 100000
        equity = initial_capital
        equity_curve = []
        wins, losses = 0, 0
        
        for signal in signals:
            equity += signal.pnl_amount
            equity_curve.append({'time': signal.exit_time, 'equity': equity})
            wins += 1 if signal.pnl_amount > 0 else 0
            losses += 1 if signal.pnl_amount < 0 else 0
        
        total_trades = wins + losses
        win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
        total_return = ((equity - initial_capital) / initial_capital) * 100
        avg_win = sum(s.pnl_amount for s in signals if s.pnl_amount > 0) / wins if wins > 0 else 0
        avg_loss = abs(sum(s.pnl_amount for s in signals if s.pnl_amount < 0) / losses) if losses > 0 else 0
        profit_factor = abs(avg_win / avg_loss) if avg_loss > 0 else 0
        
        max_equity = initial_capital
        max_drawdown = 0
        for point in equity_curve:
            if point['equity'] > max_equity:
                max_equity = point['equity']
            drawdown = (max_equity - point['equity']) / max_equity
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        
        sharpe_ratio = 0
        if len(equity_curve) > 1:
            returns = [(equity_curve[i]['equity'] - equity_curve[i-1]['equity']) / equity_curve[i-1]['equity'] for i in range(1, len(equity_curve))]
            if returns:
                avg_ret = sum(returns) / len(returns)
                std_ret = math.sqrt(sum((r - avg_ret) ** 2 for r in returns) / len(returns))
                sharpe_ratio = (avg_ret / std_ret * math.sqrt(252)) if std_ret > 0 else 0
        
        result = BacktestResult(symbol=symbol, contract_name=contract.ProductID, start_date=data_5min[0]['time'], end_date=data_5min[-1]['time'], initial_capital=initial_capital, final_equity=equity, total_return=total_return, total_trades=total_trades, wins=wins, losses=losses, win_rate=win_rate, avg_win=avg_win, avg_loss=avg_loss, profit_factor=profit_factor, max_drawdown=max_drawdown*100, sharpe_ratio=sharpe_ratio, trades=[asdict(s) for s in signals], equity_curve=equity_curve)
        
        print(f"\n{'='*60}\n📈 结果\n{'='*60}")
        print(f"初始：¥{initial_capital:,.0f} | 最终：¥{equity:,.0f} | 收益：{total_return:.2f}%")
        print(f"交易：{total_trades} | 胜率：{win_rate:.1f}% | 盈亏比：{profit_factor:.2f}")
        print(f"回撤：{max_drawdown*100:.2f}% | 夏普：{sharpe_ratio:.2f}\n")
        
        return result
    
    def generate_html_report(self, result: BacktestResult, output_dir: str = None):
        if not output_dir:
            output_dir = self._get_output_dir(result.symbol)
        output_path = os.path.join(output_dir, "backtest_report.html")
        
        trades_html = ""
        for i, t in enumerate(result.trades[-20:], 1):
            cls = "profit" if t['pnl_amount'] > 0 else "loss"
            trades_html += f"<tr><td>{i}</td><td>{t['entry_time']}</td><td>{t['entry_price']:.2f}</td><td>{t['exit_time']}</td><td>{t['exit_price']:.2f}</td><td class='{cls}'>{t['pnl_pct']*100:+.2f}%</td><td class='{cls}'>¥{t['pnl_amount']:+,.0f}</td><td>{t['exit_reason']}</td></tr>"
        
        equity_data = [f"['{p['time']}', {p['equity']:.2f}]" for p in result.equity_curve]
        
        html = f"""<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"><title>回测报告 - {result.symbol}</title><script src="https://cdn.jsdelivr.net/npm/chart.js"></script><style>*{{margin:0;padding:0;box-sizing:border-box}}body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#f5f5f5;color:#333;line-height:1.6}}.container{{max-width:1200px;margin:0 auto;padding:20px}}.header{{background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;padding:30px;border-radius:10px;margin-bottom:20px}}.card{{background:#fff;border-radius:10px;padding:20px;margin-bottom:20px;box-shadow:0 2px 10px rgba(0,0,0,0.1)}}.card h2{{color:#667eea;border-bottom:2px solid #667eea;padding-bottom:10px;margin-bottom:15px}}.stats-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:15px}}.stat-item{{background:#f8f9fa;padding:15px;border-radius:8px;text-align:center}}.stat-value{{font-size:1.6em;font-weight:bold;color:#667eea}}.stat-label{{color:#666;font-size:0.85em;margin-top:5px}}.positive{{color:#10b981}}.negative{{color:#ef4444}}table{{width:100%;border-collapse:collapse}}th,td{{padding:10px;text-align:left;border-bottom:1px solid #eee}}th{{background:#f8f9fa;color:#667eea}}.profit{{color:#10b981;font-weight:600}}.loss{{color:#ef4444;font-weight:600}}.chart-container{{height:400px}}.badge{{display:inline-block;padding:4px 12px;border-radius:20px;font-size:0.85em}}.badge-success{{background:#d1fae5;color:#065f46}}.badge-warning{{background:#fef3c7;color:#92400e}}</style></head><body><div class="container"><div class="header"><h1>📊 TrendReversalV2 回测报告</h1><p>{result.symbol} | {result.start_date} ~ {result.end_date}</p></div><div class="card"><h2>📈 核心指标</h2><div class="stats-grid"><div class="stat-item"><div class="stat-value {'positive' if result.total_return>0 else 'negative'}">{result.total_return:+.2f}%</div><div class="stat-label">收益率</div></div><div class="stat-item"><div class="stat-value">¥{result.final_equity:,.0f}</div><div class="stat-label">最终权益</div></div><div class="stat-item"><div class="stat-value">{result.total_trades}</div><div class="stat-label">交易次数</div></div><div class="stat-item"><div class="stat-value">{result.win_rate:.1f}%</div><div class="stat-label">胜率</div></div><div class="stat-item"><div class="stat-value">{result.profit_factor:.2f}</div><div class="stat-label">盈亏比</div></div><div class="stat-item"><div class="stat-value"><span class="badge {'badge-success' if result.wins>result.losses else 'badge-warning'}">{result.wins}赢/{result.losses}亏</span></div><div class="stat-label">分布</div></div></div></div><div class="card"><h2>📊 权益曲线</h2><div class="chart-container"><canvas id="equityChart"></canvas></div></div><div class="card"><h2>⚠️ 风险</h2><div class="stats-grid"><div class="stat-item"><div class="stat-value {'negative' if result.max_drawdown>10 else 'positive'}">{result.max_drawdown:.2f}%</div><div class="stat-label">最大回撤</div></div><div class="stat-item"><div class="stat-value {'positive' if result.sharpe_ratio>1 else 'negative'}">{result.sharpe_ratio:.2f}</div><div class="stat-label">夏普比率</div></div></div></div><div class="card"><h2>💰 交易记录</h2><table><thead><tr><th>#</th><th>入场</th><th>价格</th><th>出场</th><th>价格</th><th>涨跌</th><th>盈亏</th><th>原因</th></tr></thead><tbody>{trades_html}</tbody></table></div></div><script>const data=[{', '.join(equity_data)}];new Chart(document.getElementById('equityChart'),{{type:'line',data:{{labels:data.map(d=>d[0]),datasets:[{{label:'权益',data:data.map(d=>d[1]),borderColor:'#667eea',backgroundColor:'rgba(102,126,234,0.1)',fill:true,tension:0.4}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}},tooltip:{{callbacks:{{label:c=>'¥'+c.parsed.y.toLocaleString()}}}}}},scales:{{x:{{ticks:{{maxTicksLimit:10}}}},y:{{ticks:{{callback:v=>'¥'+v.toLocaleString()}}}}}}}});</script></body></html>"""
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f"✅ HTML: {output_path}")
        return output_path
    
    def generate_signal_chart(self, symbol: str, trades: List[Dict], output_dir: str = None):
        """使用优化版图表生成"""
        try:
            print("📈 生成信号图表 (优化版)...")
            
            if not output_dir:
                output_dir = self._get_output_dir(symbol)
            
            # 导入绘图函数
            sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
            from plot_trend_reversal_signals import plot_symbol_with_signals, load_kline_data
            
            # 加载 K 线数据
            kline_data = load_kline_data(self.db_path, symbol, 300, 500)
            
            # 生成图表
            output_path = os.path.join(output_dir, "signal_chart.png")
            plot_symbol_with_signals(symbol, kline_data, trades, output_path)
            
            return output_path
        except Exception as e:
            print(f"⚠️ 图表生成失败：{e}")
            import traceback
            traceback.print_exc()
            return None
    
    def save_json(self, result: BacktestResult, output_dir: str = None):
        if not output_dir:
            output_dir = self._get_output_dir(result.symbol)
        output_path = os.path.join(output_dir, "backtest_results.json")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(asdict(result), f, ensure_ascii=False, indent=2)
        print(f"✅ JSON: {output_path}")
        return output_path


def main():
    db_path = "/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/kline_data.db"
    contracts_path = "/home/ubuntu/quant/ctp.examples/openctp-ctp2tts/main_contracts.json"
    
    print("\n🎯 TrendReversalV2 回测系统\n")
    print("📁 输出：backtest/{SYMBOL}/\n")
    
    engine = BacktestEngine(db_path, contracts_path)
    
    print("📋 获取合约列表...")
    symbols = engine.get_available_symbols(50)
    print(f"✅ {len(symbols)} 个合约\n")
    
    if len(sys.argv) > 1:
        target_symbols = sys.argv[1:]
    else:
        print("可用合约 (前 20):")
        for i, s in enumerate(symbols[:20], 1): print(f"  {i}. {s}")
        choice = input("\n选择 (序号/代码，空格分隔，留空=前 5 个): ").strip()
        if not choice:
            target_symbols = symbols[:5]
        else:
            target_symbols = []
            for item in choice.split():
                if item.isdigit():
                    idx = int(item)-1
                    if 0<=idx<len(symbols): target_symbols.append(symbols[idx])
                else: target_symbols.append(item)
    
    print(f"\n🚀 回测：{target_symbols}\n")
    
    all_results = []
    for symbol in target_symbols:
        result = engine.run_backtest(symbol)
        if result:
            all_results.append(result)
            output_dir = engine._get_output_dir(symbol)
            engine.save_json(result, output_dir)
            engine.generate_html_report(result, output_dir)
            engine.generate_signal_chart(symbol, result.trades, output_dir)
    
    if len(all_results) > 1:
        print(f"\n{'='*60}\n📊 汇总 ({len(all_results)} 个合约)\n{'='*60}")
        avg_ret = sum(r.total_return for r in all_results)/len(all_results)
        total_trades = sum(r.total_trades for r in all_results)
        total_wins = sum(r.wins for r in all_results)
        total_losses = sum(r.losses for r in all_results)
        win_rate = (total_wins/(total_wins+total_losses)*100) if (total_wins+total_losses)>0 else 0
        best = max(all_results, key=lambda r: r.total_return)
        worst = min(all_results, key=lambda r: r.total_return)
        print(f"平均收益：{avg_ret:.2f}%")
        print(f"总交易：{total_trades} | 胜率：{win_rate:.1f}%")
        print(f"最佳：{best.symbol} ({best.total_return:+.2f}%)")
        print(f"最差：{worst.symbol} ({worst.total_return:+.2f}%)")
        print(f"{'='*60}\n")
    
    print("✅ 完成!\n")


if __name__ == "__main__":
    main()
