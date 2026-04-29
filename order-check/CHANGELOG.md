# Changelog

## [2026-04-28]

### Added
- 新增 `initial_positions.json` 配置，支持首次运行时从固定配置建仓
- `PositionSyncManager` 新增 `execute_orders()` 方法，支持从 `signal.json` 读取并执行委托
- `PositionSyncManager` 新增 `_place_order()` 通用下单方法，支持指定开平标志
- `PositionSyncManager` 新增 `_send_position_mismatch_alert()` 持仓不一致飞书告警
- `PositionSyncManager` 新增 `_extract_field()` 字段自动映射工具

### Changed
- **启动顺序修正**：`run_pipeline.py` 启动时先执行 `automate_export` 导出 CSV，再生成 `hold-std.json`，最后持仓同步
- **持仓同步逻辑重构**：去掉"一致则加仓"逻辑，改为：
  - 账户空仓 + 标准持仓有数据 → 首次建仓
  - 持仓不一致 → 飞书告警，不自动交易
- `run_pipeline.py` 主循环增加持仓对比 + 委托执行步骤
- `compare_orders.py` 的 `generate_hold_std()` 空 CSV 时返回 True（生成空的 `hold-std.json`）

### Fixed
- 修复首次运行时 CSV 未导出就跳过持仓同步的问题
- 修复空 CSV 导致 `generate_hold_std()` 返回 False 中断流程的问题

## [2026-04-27]

### Added
- 新增 `PositionSyncManager` 类，实现 CTP 持仓查询、对比、限价单交易
- 新增 `QueryPositions.py` 独立持仓查询脚本
- 新增 `PositionManagerUI.py` tkinter 持仓管理界面（查看持仓、平仓）
- `run_pipeline.py` 增加 CTP 环境检测（`sys.argv[1]` / `CTP_ENV` 环境变量）
- `run_pipeline.py` 增加持仓同步配置块（`ENABLE_POSITION_SYNC_AT_STARTUP` 等）
- 飞书通知：报单提交、成交回报均发送飞书消息

### Changed
- 限价单价格改为严格使用 BidPrice1 / AskPrice1，不加减 tick
- 交易时段判断：TTS 模拟环境（`openctp.cn`）跳过时段检查
- `OnRtnTrade` 回调增加飞书成交通知

### Fixed
- 修复 `generate_hold_std()` 首次运行失败导致程序退出的问题
- 修复缺少 DLL（`RohonBaseV64.dll`、`WinDataCollect.dll`）导致的导入错误
- 修复 `main_contracts.json` 缺失时程序崩溃的问题
