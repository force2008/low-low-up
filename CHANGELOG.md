# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- 止损空间检查：止损空间小于0.3%时不开仓，避免被正常波动触发止损
  - 修改文件: `strategies/low_low_up/StrategyLowLowUp.py`
  - 修改位置: `get_initial_stop_loss` 方法
  - 修改内容: 在60分钟和5分钟止损计算后增加止损空间检查，返回 None 表示不开仓

### Fixed
- 平仓后清空预检测信号：避免用旧信号立即开仓
  - 修改文件: `KlineCollector_v2.py`
  - 修改位置: `_check_stop_loss_v2` 方法
  - 修改内容: 平仓后清空 `precheck_signals_green` 和 `precheck_signals_red`

### Fixed
- 集合竞价K线时间归属修复：08:55和20:55的集合竞价数据归到正确时段
  - 修改文件: `KlineCollector_v2.py`
  - 修改位置: `_get_kline_time` 方法
  - 修改内容: 
    - 08:55 集合竞价归到 09:00
    - 20:55 集合竞价归到 21:00
  - 修改位置2: `add_tick` 方法
  - 修改内容2: 跳过保存08:00和20:00的虚假K线（直接从09:00和21:00开始）

### Changed
- 策略重构：统一策略逻辑到 StrategyLowLowUp.py
  - 修改文件: `strategies/low_low_up/StrategyLowLowUp.py`, `KlineCollector_v2.py`, `backtest/strategy_backtest.py`
  - 新增方法:
    - `check_60m_precheck`: 检查60分钟预检测信号
    - `check_5m_entry_signal`: 检查5分钟入场信号
  - 止损计算统一使用 `strategy.get_initial_stop_loss()`

### Removed
- 排除胜率过低的品种: rr, wr, pk
  - 修改文件: `utils/strategy_config.py`, `KlineCollector_v2.py`, `backtest/strategy_backtest.py`
  - 在 `Config.EXCLUDED_PRODUCTS` 中配置

---

## [v1.0.0] - 2026-04-08

### Added
- 初始版本
- 多时间框架策略 (60分钟+5分钟)
- MACD指标计算
- 绿柱堆识别
- 底背离/底抬升信号检测
- 止损计算（使用绿柱堆低点）
- 飞书通知