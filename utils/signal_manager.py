#!/usr/bin/env python3
"""
策略信号管理模块
"""

import json
from datetime import datetime


class StrategySignalManager:
    """策略信号管理器"""

    def __init__(self, signal_file="strategy_signals_v2.json"):
        self.signal_file = signal_file
        self.signals = self._load_signals()

    def _load_signals(self) -> list:
        """加载信号文件"""
        import os
        if os.path.exists(self.signal_file):
            try:
                with open(self.signal_file, 'r', encoding='utf-8') as f:
                    signals = json.load(f)
                print(f"加载策略信号文件：{self.signal_file}, 共 {len(signals)} 条信号")
                return signals
            except Exception as e:
                print(f"加载策略信号文件失败：{e}")
                return []
        return []

    def _save_signals(self):
        """保存信号文件"""
        try:
            with open(self.signal_file, 'w', encoding='utf-8') as f:
                json.dump(self.signals, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"保存策略信号文件失败：{e}")

    def add_signal(self, symbol: str, signal_data: dict):
        """添加策略信号"""
        signal_record = {
            'symbol': symbol,
            'signal_type': signal_data.get('signal_type', ''),
            'price': signal_data.get('price', 0),
            'stop_loss': signal_data.get('stop_loss', 0),
            'position_size': signal_data.get('position_size', 0),
            'reason': signal_data.get('reason', ''),
            'time': signal_data.get('time', datetime.now().isoformat()),
            'created_at': datetime.now().isoformat()
        }
        self.signals.append(signal_record)
        self._save_signals()
        print(f"📝 {symbol} 保存策略信号：{signal_record['signal_type']} @ {signal_record['price']}")