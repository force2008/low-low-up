#!/usr/bin/env python3
"""
使用 TrendReversalV7LiveStrategy 进行回测（与 backtest_v7.py 逻辑完全一致）

使用方法：
    python strategies/run_live_strategy_backtest.py

输出：
    - 回测交易明细 CSV
    - 汇总统计报告
"""

import sqlite3
import json
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

# 添加策略模块路径
sys.path.insert(0, str(Path(__file__).parent))

from TrendReversalV7LiveStrategy import (
    LiveConfig, DataLoader, MACDCalculator, StackIdentifier,
    TrendReversalStrategy, Signal, SignalType, Position
)


# ============== 配置 ==============

class BacktestConfig:
    """回测配置"""
    DB_PATH = "/home/ubuntu/low-low-up/data/db/kline_data.db"
    CONTRACTS_PATH = "/home/ubuntu/low-low-up/data/contracts/main_contracts.json"
    
    DURATION_5M = 300
    DURATION_60M = 3600
    MAX_5M_BARS = 5000
    
    TARGET_NOTIONAL = 100000  # 目标货值
    COOLDOWN_HOURS = 4  # 冷却期 4 小时


@dataclass
class BacktestTrade:
    """回测交易记录"""
    symbol: str
    entry_time: str
    entry_price: float
    exit_time: str
    exit_price: float
    position_size: int
    pnl: float
    pnl_pct: float
    exit_reason: str
    initial_stop: float
    stop_update_count: int = 0


# ============== 回测引擎 ==============

class BacktestEngine:
    """回测引擎 - 与 backtest_v7.py 逻辑完全一致"""
    
    def __init__(self, symbol: str, db_path: str, contracts_path: str, config: BacktestConfig = None):
        self.symbol = symbol
        self.db_path = db_path
        self.contracts_path = contracts_path
        self.config = config if config else BacktestConfig()
        
        # 创建数据加载器
        self.data_loader = DataLoader(self.db_path, self.contracts_path)
        self.strategy = TrendReversalStrategy(self.data_loader.get_symbol_info(symbol))
        
        # 状态变量
        self.df_5m: List[tuple] = []
        self.df_5m_with_macd: List[tuple] = []
        self.df_60m: List[tuple] = []
        self.df_60m_with_macd: List[tuple] = []
        
        self.green_stacks_5m: Dict[int, dict] = {}
        self.green_gaps_5m: Dict[int, dict] = {}
        self.green_stacks_60m: Dict[int, dict] = {}
        self.green_gaps_60m: Dict[int, dict] = {}
        
        self.trades: List[BacktestTrade] = []
        
        # 持仓状态
        self.position = None
        self.last_entry_time = None
        self.initial_stop_loss = None
        self.stop_updates = []
        self.entry_stack_id = None
        
        # 60 分钟预检查信号队列
        self.precheck_signals_green = []
        self.precheck_signals_red = []
        
        # 索引映射
        self.index_map: List[int] = []
    
    def initialize(self):
        """初始化，加载历史数据"""
        # 加载 5 分钟 K 线
        self.df_5m = self.data_loader.load_kline_fast(
            self.symbol, 
            self.config.DURATION_5M, 
            self.config.MAX_5M_BARS
        )
        
        # 加载 60 分钟 K 线
        self.df_60m = self.data_loader.load_kline_fast(
            self.symbol, 
            self.config.DURATION_60M
        )
        
        # 计算 MACD
        self.df_5m_with_macd, self.green_stacks_5m, self.green_gaps_5m = StackIdentifier.identify(
            MACDCalculator.calculate(self.df_5m)
        )
        
        self.df_60m_with_macd, self.green_stacks_60m, self.green_gaps_60m = StackIdentifier.identify(
            MACDCalculator.calculate(self.df_60m)
        )
        
        # 预计算索引映射
        self.index_map = self._precompute_60m_index()
    
    def _precompute_60m_index(self) -> List[int]:
        """预计算索引映射"""
        if not self.df_5m or not self.df_60m:
            return []
        
        index_map = []
        idx_60m = 0
        n_60m = len(self.df_60m)
        
        for row_5m in self.df_5m:
            time_5m = row_5m[0]
            
            while idx_60m < n_60m - 1 and self.df_60m[idx_60m + 1][0] <= time_5m:
                idx_60m += 1
            
            index_map.append(idx_60m)
        
        return index_map
    
    def run_backtest(self) -> List[BacktestTrade]:
        """运行回测（与 backtest_v7.py 完全一致的逻辑）"""
        print(f"开始回测：{self.symbol}")
        print(f"  5 分钟 K 线：{len(self.df_5m)}根")
        print(f"  60 分钟 K 线：{len(self.df_60m)}根")
        
        symbol_info = self.data_loader.get_symbol_info(self.symbol)
        volume_multiple = symbol_info.get('VolumeMultiple', 1) if symbol_info else 1
        
        total_bars = len(self.df_5m)
        
        for i, row_5m in enumerate(self.df_5m):
            # 进度显示
            if (i + 1) % 1000 == 0:
                print(f"  进度：{i+1}/{total_bars} ({(i+1)/total_bars*100:.1f}%)")
            
            idx_60m = self.index_map[i] if i < len(self.index_map) else len(self.df_60m) - 1
            
            # 没有持仓，检查入场
            if self.position is None:
                # 检查冷却期
                if self.last_entry_time is not None:
                    entry_dt = datetime.strptime(self.last_entry_time[:19], '%Y-%m-%d %H:%M:%S')
                    current_dt = datetime.strptime(row_5m[0][:19], '%Y-%m-%d %H:%M:%S')
                    hours_passed = (current_dt - entry_dt).total_seconds() / 3600
                    
                    if hours_passed < self.config.COOLDOWN_HOURS:
                        continue  # 冷却期内，跳过
                
                # 【新增】检查 60 分钟 DIF 是否在高位（高位过滤）
                dif_high_ok, dif_high_reason, current_dif, recent_dif_high = self.strategy.check_60m_dif_high_position(self.df_60m_with_macd, idx_60m)
                
                if not dif_high_ok:
                    # DIF 在高位，跳过所有入场检查
                    continue
                
                # 【优化 1】60 分钟绿柱堆内预检查 DIF 拐头 + 底部抬升
                if idx_60m >= 4:
                    hist_60m = self.df_60m_with_macd[idx_60m][8]
                    
                    # 绿柱堆内 DIF 拐头预检查
                    if hist_60m < 0:  # 绿柱堆内
                        dif_turn, _ = self.strategy.check_60m_dif_turn_in_green(
                            self.df_60m_with_macd, idx_60m
                        )
                        
                        if dif_turn:
                            # 检查底背离，如果满足则添加信号到队列
                            diver_ok, diver_reason, current_green_low, prev_prev_green_low = \
                                self.strategy.check_60m_divergence(self.df_60m_with_macd, idx_60m)
                            
                            if diver_ok:
                                # 信号有效期：30 分钟（6 根 5 分钟 K 线）
                                expiry_time = row_5m[0][:19]
                                self.precheck_signals_green.append({
                                    'type': 'green',
                                    'created_time': row_5m[0][:19],
                                    'expiry_time': expiry_time,
                                    'current_green_low': current_green_low,
                                    'prev_prev_green_low': prev_prev_green_low
                                })
                    
                    # 红柱堆内 DIF 拐头预检查
                    elif hist_60m > 0:  # 红柱堆内
                        dif_turn_red, _ = self.strategy.check_60m_dif_turn_in_red(
                            self.df_60m_with_macd, idx_60m
                        )
                        
                        if dif_turn_red:
                            # 红柱堆内 DIF 拐头，无需检查底背离（趋势已向上）
                            expiry_time = row_5m[0][:19]
                            self.precheck_signals_red.append({
                                'type': 'red',
                                'created_time': row_5m[0][:19],
                                'expiry_time': expiry_time,
                                'current_green_low': None,
                                'prev_prev_green_low': None
                            })
                
                # 【优化 2】检查预检查信号队列，满足 5 分钟条件则入场
                precheck_entry_done = False
                
                all_signals = self.precheck_signals_green + self.precheck_signals_red
                
                if all_signals:
                    current_time = row_5m[0][:19]
                    
                    # 清理过期信号（超过 30 分钟）
                    current_dt = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')
                    
                    self.precheck_signals_green = [
                        s for s in self.precheck_signals_green 
                        if datetime.strptime(s['expiry_time'], '%Y-%m-%d %H:%M:%S') + timedelta(minutes=30) > current_dt
                    ]
                    self.precheck_signals_red = [
                        s for s in self.precheck_signals_red 
                        if datetime.strptime(s['expiry_time'], '%Y-%m-%d %H:%M:%S') + timedelta(minutes=30) > current_dt
                    ]
                    
                    # 检查每个信号是否满足 5 分钟条件
                    for signal in (self.precheck_signals_green + self.precheck_signals_red)[:]:
                        signal_type = signal.get('type', 'unknown')
                        
                        # 绿柱堆内信号需要检查底背离，红柱堆内信号不需要（趋势已向上）
                        diver_ok = True
                        diver_reason = "红柱堆内 DIF 拐头（趋势向上）"
                        
                        if signal_type == 'green':
                            diver_ok, diver_reason, curr_low_60m, prev_prev_low_60m = \
                                self.strategy.check_60m_divergence(self.df_60m_with_macd, idx_60m)
                            
                            if not diver_ok:
                                if signal in self.precheck_signals_green:
                                    self.precheck_signals_green.remove(signal)
                                continue
                        
                        # 检查 5 分钟入场条件
                        cond_5m, reason_5m = self.strategy.check_5m_entry(
                            self.df_5m_with_macd, i, self.green_stacks_5m
                        )
                        
                        if cond_5m:
                            # 获取初始止损
                            initial_stop_loss, stop_reason = self.strategy.get_initial_stop_loss(
                                self.df_5m_with_macd, i, self.green_stacks_5m, self.green_gaps_5m
                            )
                            
                            if initial_stop_loss is None:
                                continue
                            
                            entry_price = row_5m[4]
                            contract_value = entry_price * volume_multiple
                            
                            if contract_value > self.config.TARGET_NOTIONAL:
                                continue
                            
                            position_size = max(1, int(self.config.TARGET_NOTIONAL / contract_value))
                            
                            self.position = {
                                'entry_idx': i,
                                'entry_time': row_5m[0],
                                'entry_price': entry_price,
                                'position_size': position_size,
                                'stop_reason': stop_reason
                            }
                            self.entry_stack_id = self.df_5m_with_macd[i][10]
                            self.last_entry_time = row_5m[0]
                            self.initial_stop_loss = initial_stop_loss
                            
                            self.stop_updates = [{
                                'time': row_5m[0],
                                'entry_price': entry_price,
                                'stop_price': initial_stop_loss,
                                'type': '初始止损',
                                'reason': stop_reason,
                                'pnl': ''
                            }]
                            
                            signal_source = "绿柱堆内 DIF 拐头" if signal_type == 'green' else "红柱堆内 DIF 拐头"
                            print(f"📈 入场：{row_5m[0]} @ {entry_price:.2f} | 止损:{initial_stop_loss:.2f} ({stop_reason}) | {diver_reason} + {reason_5m} [{signal_source}]")
                            
                            # 清除信号队列
                            self.precheck_signals_green.clear()
                            self.precheck_signals_red.clear()
                            precheck_entry_done = True
                            break
                
                # 如果预检查已入场，跳过传统逻辑
                if precheck_entry_done:
                    continue
                
                # 【新增】检查 60 分钟 MACD 红柱期间的 DIF 拐头入场
                hist_60m = self.df_60m_with_macd[idx_60m][8]
                if hist_60m > 0:  # MACD 红柱期间
                    dif_turn_red, reason_dif_turn = self.strategy.check_60m_dif_turn_in_red(
                        self.df_60m_with_macd, idx_60m
                    )
                    
                    if dif_turn_red:
                        # 检查底背离条件
                        diver_ok, diver_reason, curr_low_60m, prev_prev_low_60m = \
                            self.strategy.check_60m_divergence(self.df_60m_with_macd, idx_60m)
                        
                        if diver_ok:
                            # 检查 5 分钟小绿柱过滤
                            cond_filter, reason_filter = self.strategy.check_5m_green_stack_filter(
                                self.df_5m_with_macd, i, self.green_stacks_5m
                            )
                            
                            if cond_filter:
                                # 检查 5 分钟入场条件
                                cond_5m, reason_5m = self.strategy.check_5m_entry(
                                    self.df_5m_with_macd, i, self.green_stacks_5m
                                )
                                
                                if cond_5m:
                                    # 获取初始止损
                                    initial_stop_loss, stop_reason = self.strategy.get_initial_stop_loss(
                                        self.df_5m_with_macd, i, self.green_stacks_5m, self.green_gaps_5m
                                    )
                                    
                                    if initial_stop_loss is None:
                                        initial_stop_loss = prev_prev_low_60m
                                        stop_reason = f"60m 底背离低点:{initial_stop_loss:.2f}"
                                    
                                    entry_price = row_5m[4]
                                    contract_value = entry_price * volume_multiple
                                    
                                    if contract_value <= self.config.TARGET_NOTIONAL:
                                        position_size = max(1, int(self.config.TARGET_NOTIONAL / contract_value))
                                        
                                        self.position = {
                                            'entry_idx': i,
                                            'entry_time': row_5m[0],
                                            'entry_price': entry_price,
                                            'position_size': position_size,
                                            'stop_reason': stop_reason
                                        }
                                        self.entry_stack_id = self.df_5m_with_macd[i][10]
                                        self.last_entry_time = row_5m[0]
                                        self.initial_stop_loss = initial_stop_loss
                                        
                                        self.stop_updates = [{
                                            'time': row_5m[0],
                                            'entry_price': entry_price,
                                            'stop_price': initial_stop_loss,
                                            'type': '初始止损',
                                            'reason': stop_reason,
                                            'pnl': ''
                                        }]
                                        
                                        print(f"📈 入场：{row_5m[0]} @ {entry_price:.2f} | 止损:{initial_stop_loss:.2f} ({stop_reason}) | {reason_dif_turn} + {diver_reason} + {reason_filter} + {reason_5m} [60m 红柱 DIF 拐头]")
                                        continue
                
                # 检查 60 分钟条件（含底背离）- 传统逻辑
                cond_60m, reason_60m, curr_low_60m, prev_low_60m = self.strategy.check_60m_entry(
                    self.df_60m_with_macd, idx_60m, self.green_stacks_60m
                )
                
                if cond_60m:
                    # 检查 5 分钟小绿柱过滤
                    cond_filter, reason_filter = self.strategy.check_5m_green_stack_filter(
                        self.df_5m_with_macd, i, self.green_stacks_5m
                    )
                    
                    if cond_filter:
                        # 检查 5 分钟入场条件
                        cond_5m, reason_5m = self.strategy.check_5m_entry(
                            self.df_5m_with_macd, i, self.green_stacks_5m
                        )
                        
                        if cond_5m:
                            # 获取初始止损
                            initial_stop_loss, stop_reason = self.strategy.get_initial_stop_loss(
                                self.df_5m_with_macd, i, self.green_stacks_5m, self.green_gaps_5m
                            )
                            
                            if initial_stop_loss is None:
                                continue
                            
                            entry_price = row_5m[4]
                            contract_value = entry_price * volume_multiple
                            
                            if contract_value > self.config.TARGET_NOTIONAL:
                                continue
                            
                            position_size = max(1, int(self.config.TARGET_NOTIONAL / contract_value))
                            
                            self.position = {
                                'entry_idx': i,
                                'entry_time': row_5m[0],
                                'entry_price': entry_price,
                                'position_size': position_size,
                                'stop_reason': stop_reason
                            }
                            self.entry_stack_id = self.df_5m_with_macd[i][10]
                            self.last_entry_time = row_5m[0]
                            self.initial_stop_loss = initial_stop_loss
                            
                            self.stop_updates = [{
                                'time': row_5m[0],
                                'entry_price': entry_price,
                                'stop_price': initial_stop_loss,
                                'type': '初始止损',
                                'reason': stop_reason,
                                'pnl': ''
                            }]
                            
                            green_ended = self.df_5m_with_macd[i][11] if len(self.df_5m_with_macd[i]) > 11 else 0
                            if green_ended == 1:
                                print(f"📈 入场：{row_5m[0]} @ {entry_price:.2f} | 止损:{initial_stop_loss:.2f} ({stop_reason}) | {reason_60m} + {reason_filter} + {reason_5m} [绿柱堆结束立即入场]")
                            else:
                                print(f"📈 入场：{row_5m[0]} @ {entry_price:.2f} | 止损:{initial_stop_loss:.2f} ({stop_reason}) | {reason_60m} + {reason_filter} + {reason_5m}")
            
            # 有持仓，检查出场
            else:
                current_low = row_5m[3]  # 使用最低价检查止损（正确逻辑）
                
                # 检查初始止损
                if self.initial_stop_loss is not None and current_low <= self.initial_stop_loss:
                    price_diff = self.initial_stop_loss - self.position['entry_price']
                    pnl = price_diff * self.position['position_size'] * volume_multiple
                    pnl_pct = pnl / (self.position['entry_price'] * self.position['position_size'] * volume_multiple) * 100
                    
                    stop_detail = self.position.get('stop_reason', '初始止损')
                    
                    trade = BacktestTrade(
                        symbol=self.symbol,
                        entry_time=self.position['entry_time'],
                        entry_price=self.position['entry_price'],
                        exit_time=row_5m[0],
                        exit_price=self.initial_stop_loss,
                        position_size=self.position['position_size'],
                        pnl=pnl,
                        pnl_pct=pnl_pct,
                        exit_reason=f"初始止损 ({stop_detail})",
                        initial_stop=self.stop_updates[0]['stop_price'] if self.stop_updates else self.initial_stop_loss,
                        stop_update_count=len(self.stop_updates)
                    )
                    self.trades.append(trade)
                    
                    print(f"📉 出场：{row_5m[0]} @ {self.initial_stop_loss:.2f} | 初始止损 ({stop_detail}) | 盈亏：{pnl:.2f} ({pnl_pct:.2f}%)")
                    self.position = None
                    self.initial_stop_loss = None
                    self.entry_stack_id = None
                    continue
                
                # 检查移动止损
                mobile_stop, stop_reason = self.strategy.get_mobile_stop(
                    self.df_5m_with_macd, i, self.green_stacks_5m, self.green_gaps_5m
                )
                
                if mobile_stop is not None:
                    # 如果移动止损上移，记录变化
                    if mobile_stop > self.initial_stop_loss:
                        self.stop_updates.append({
                            'time': row_5m[0],
                            'entry_price': self.position['entry_price'],
                            'stop_price': mobile_stop,
                            'type': '移动止损上移',
                            'reason': stop_reason,
                            'pnl': ''
                        })
                        self.initial_stop_loss = mobile_stop
                    
                    # 检查是否触发止损
                    if current_low <= self.initial_stop_loss:
                        price_diff = self.initial_stop_loss - self.position['entry_price']
                        pnl = price_diff * self.position['position_size'] * volume_multiple
                        pnl_pct = pnl / (self.position['entry_price'] * self.position['position_size'] * volume_multiple) * 100
                        
                        self.stop_updates.append({
                            'time': row_5m[0],
                            'entry_price': self.position['entry_price'],
                            'stop_price': self.initial_stop_loss,
                            'type': '出场',
                            'reason': stop_reason,
                            'pnl': pnl
                        })
                        
                        trade = BacktestTrade(
                            symbol=self.symbol,
                            entry_time=self.position['entry_time'],
                            entry_price=self.position['entry_price'],
                            exit_time=row_5m[0],
                            exit_price=self.initial_stop_loss,
                            position_size=self.position['position_size'],
                            pnl=pnl,
                            pnl_pct=pnl_pct,
                            exit_reason=stop_reason,
                            initial_stop=self.stop_updates[0]['stop_price'] if self.stop_updates else 0.0,
                            stop_update_count=len(self.stop_updates)
                        )
                        self.trades.append(trade)
                        
                        print(f"📉 出场：{row_5m[0]} @ {self.initial_stop_loss:.2f} | {stop_reason} | 盈亏：{pnl:.2f} ({pnl_pct:.2f}%)")
                        self.position = None
                        self.entry_stack_id = None
                        self.initial_stop_loss = None
        
        # 处理未平仓
        if self.position is not None:
            last_row = self.df_5m[-1]
            price_diff = last_row[4] - self.position['entry_price']
            pnl = price_diff * self.position['position_size'] * volume_multiple
            pnl_pct = pnl / (self.position['entry_price'] * self.position['position_size'] * volume_multiple) * 100
            
            trade = BacktestTrade(
                symbol=self.symbol,
                entry_time=self.position['entry_time'],
                entry_price=self.position['entry_price'],
                exit_time=last_row[0],
                exit_price=last_row[4],
                position_size=self.position['position_size'],
                pnl=pnl,
                pnl_pct=pnl_pct,
                exit_reason="回测结束",
                initial_stop=self.initial_stop_loss if self.initial_stop_loss else 0,
                stop_update_count=len(self.stop_updates)
            )
            self.trades.append(trade)
        
        return self.trades


# ============== 主函数 ==============

def main():
    """主函数"""
    print("="*60)
    print("TrendReversalV7LiveStrategy 回测（与 backtest_v7.py 逻辑一致）")
    print("="*60)
    
    config = BacktestConfig()
    
    # 加载主力合约列表
    print("\n加载主力合约列表...")
    with open(config.CONTRACTS_PATH, 'r') as f:
        contracts = json.load(f)
    
    # 过滤活跃合约
    active_contracts = [c for c in contracts if c.get('IsTrading', 0) == 1]
    print(f"找到 {len(active_contracts)} 个活跃合约")
    
    # 构建合约列表
    symbols_to_test = []
    for contract in active_contracts:
        exchange = contract.get('ExchangeID', '')
        symbol = f"{exchange}.{contract['MainContractID']}"
        symbols_to_test.append(symbol)
    
    # 批量回测
    all_trades = []
    skipped = []
    
    for idx, symbol in enumerate(symbols_to_test):
        print(f"\n{'='*60}")
        print(f"回测合约：{symbol} ({idx+1}/{len(symbols_to_test)})")
        print(f"{'='*60}")
        
        try:
            # 创建回测引擎
            engine = BacktestEngine(symbol, config.DB_PATH, config.CONTRACTS_PATH, config)
            
            # 初始化
            engine.initialize()
            
            # 运行回测
            trades = engine.run_backtest()
            
            all_trades.extend(trades)
            
            print(f"  ✅ 完成：{len(trades)} 笔交易")
        
        except Exception as e:
            print(f"  ❌ 错误：{e}")
            import traceback
            traceback.print_exc()
            skipped.append(symbol)
    
    # 生成汇总报告
    print("\n" + "="*60)
    print("生成汇总报告...")
    print("="*60)
    
    if not all_trades:
        print("没有交易记录")
        return
    
    # 保存交易明细 CSV
    output_path = Path(__file__).parent / "live_strategy_backtest_trades.csv"
    
    with open(output_path, 'w') as f:
        f.write("symbol,entry_time,entry_price,exit_time,exit_price,initial_stop,stop_updates_count,pnl,pnl_pct,exit_reason\n")
        for t in all_trades:
            f.write(f"{t.symbol},{t.entry_time},{t.entry_price},{t.exit_time},{t.exit_price},{t.initial_stop:.2f},{t.stop_update_count},{t.pnl:.2f},{t.pnl_pct:.2f}%,{t.exit_reason}\n")
    
    print(f"\n📊 交易明细：{output_path}")
    
    # 打印汇总
    total_trades = len(all_trades)
    total_pnl = sum(t.pnl for t in all_trades)
    winning = sum(1 for t in all_trades if t.pnl > 0)
    losing = sum(1 for t in all_trades if t.pnl <= 0)
    
    # 计算盈亏比
    total_profit = sum(t.pnl for t in all_trades if t.pnl > 0)
    total_loss = abs(sum(t.pnl for t in all_trades if t.pnl < 0))
    profit_loss_ratio = total_profit / total_loss if total_loss > 0 else float('inf')
    
    # 计算胜率
    win_rate = winning / total_trades * 100 if total_trades > 0 else 0
    
    # 计算平均盈利和平均亏损
    avg_profit = total_profit / winning if winning > 0 else 0
    avg_loss = total_loss / losing if losing > 0 else 0
    
    print(f"\n✅ 回测完成！")
    print(f"   总合约数：{len(symbols_to_test) - len(skipped)}")
    print(f"   跳过合约：{len(skipped)}")
    print(f"   总交易数：{total_trades}")
    print(f"   盈利交易：{winning} ({win_rate:.1f}%)")
    print(f"   亏损交易：{losing} ({100-win_rate:.1f}%)")
    print(f"   总盈亏：{total_pnl:,.2f}")
    print(f"   总盈利：{total_profit:,.2f}")
    print(f"   总亏损：{total_loss:,.2f}")
    print(f"   盈亏比：{profit_loss_ratio:.2f}")
    print(f"   平均盈利：{avg_profit:,.2f}")
    print(f"   平均亏损：{avg_loss:,.2f}")
    print(f"   交易明细：{output_path}")
    
    # 保存汇总报告
    report_path = Path(__file__).parent / "live_strategy_backtest_report.json"
    
    report = {
        'total_symbols': len(symbols_to_test) - len(skipped),
        'skipped_symbols': len(skipped),
        'total_trades': total_trades,
        'winning_trades': winning,
        'losing_trades': losing,
        'win_rate': win_rate,
        'total_pnl': total_pnl,
        'total_profit': total_profit,
        'total_loss': total_loss,
        'profit_loss_ratio': profit_loss_ratio,
        'avg_profit': avg_profit,
        'avg_loss': avg_loss,
        'skipped': skipped
    }
    
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"   汇总报告：{report_path}")


if __name__ == "__main__":
    main()