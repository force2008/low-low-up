# -*- coding: utf-8 -*-
"""
持仓同步管理器模块

该模块将 PositionSyncManager 拆分为多个小文件，便于维护和理解：
- constants.py: 交易时段配置常量
- base.py: CTP基类 + 初始化，包含CTP回调
- data.py: 数据加载（合约信息、持仓标准文件、交易时段判断）
- market.py: 行情持仓查询
- order_ops.py: 订单操作（在途委托处理）
- sync.py: 同步逻辑（主方法、委托执行）
- position_sync_manager.py: 主入口，重新组装所有功能
"""

from .position_sync_manager import (
    PositionSyncManager,
    PositionSyncManagerBase,
    PositionSyncManagerData,
    PositionSyncManagerMarket,
    PositionSyncManagerOrderOps,
    PositionSyncManagerSync,
    run_position_sync,
)

__all__ = [
    'PositionSyncManager',
    'PositionSyncManagerBase',
    'PositionSyncManagerData',
    'PositionSyncManagerMarket',
    'PositionSyncManagerOrderOps',
    'PositionSyncManagerSync',
    'run_position_sync',
]