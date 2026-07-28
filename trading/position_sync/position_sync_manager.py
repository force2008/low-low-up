# -*- coding: utf-8 -*-
"""
持仓同步管理器

功能:
- 首次建仓：账户空仓时，按 hold-std.json / initial_positions.json 买入建仓（含30秒超时撤单重发）
- 持仓对比：对比账户实际持仓与 hold-std.json，不一致时自动补单/平仓（默认30分钟冷却，防TTS/线上来回切）
- 委托执行：从 signal.json 读取新增委托，在 CTP 上执行限价单（不自动撤单）
- 支持通过 main_contracts.json 自动查找合约所属交易所及 PriceTick

用法:
    from trading.position_sync.position_sync_manager import PositionSyncManager
    mgr = PositionSyncManager(hold_std_path, main_contracts_path)
    mgr.sync_and_trade()          # 建仓 + 持仓对比（首次建仓后每30分钟执行一次）
    mgr.execute_orders(signal_path)  # 执行委托（仅提交，不撤单）
    del mgr  # 释放CTP连接

模块结构:
- constants.py: 交易时段配置常量
- base.py: CTP基类 + 初始化，包含CTP回调
- data.py: 数据加载（合约信息、持仓标准文件、交易时段判断）
- market.py: 行情持仓查询
- order_ops.py: 订单操作（在途委托处理）
- sync.py: 同步逻辑（主方法、委托执行）
- position_sync_manager.py: 主入口，重新组装所有功能
"""

import os
import sys

# 把项目根目录加入路径
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# 导入各个模块的类
from .base import PositionSyncManagerBase
from .data import PositionSyncManagerData
from .market import PositionSyncManagerMarket
from .order_ops import PositionSyncManagerOrderOps
from .sync import PositionSyncManagerSync


class PositionSyncManager(
    PositionSyncManagerData,
    PositionSyncManagerMarket,
    PositionSyncManagerOrderOps,
    PositionSyncManagerSync,
    PositionSyncManagerBase,
):
    """持仓同步管理器：通过多继承组合各个功能模块

    继承顺序（按优先级，从左到右）：
    1. PositionSyncManagerData - 数据加载
    2. PositionSyncManagerMarket - 行情持仓查询
    3. PositionSyncManagerOrderOps - 订单操作
    4. PositionSyncManagerSync - 同步逻辑
    5. PositionSyncManagerBase - CTP基类、初始化、CTP回调
    """
    pass


# ----------------------------------------------------------------------
# 便捷函数
# ----------------------------------------------------------------------
def run_position_sync(
    hold_std_path: str,
    main_contracts_path: str,
    trade_volume: int = 1,
    timeout: int = 30,
    conf=None,
    env_name: str = None,
    logger=None,
    position_ratio: float = 1.0,
) -> bool:
    """便捷函数：单次运行持仓同步（同步方式）"""
    mgr = None
    try:
        print(f"[run_position_sync] 创建 PositionSyncManager...")
        print(f"  hold_std_path={hold_std_path}")
        print(f"  main_contracts_path={main_contracts_path}")
        print(f"  position_ratio={position_ratio}")
        mgr = PositionSyncManager(
            hold_std_path=hold_std_path,
            main_contracts_path=main_contracts_path,
            conf=conf,
            env_name=env_name,
            position_ratio=position_ratio,
        )
        if logger:
            mgr.set_logger(logger)
        print(f"[run_position_sync] PositionSyncManager 创建成功，调用 sync_and_trade...")
        result = mgr.sync_and_trade(
            trade_volume=trade_volume, timeout=timeout, position_ratio=position_ratio
        )
        print(f"[run_position_sync] sync_and_trade 返回: {result}")
        return result
    except Exception as e:
        import traceback
        print(f"[异常] 持仓同步过程中出错: {e}")
        traceback.print_exc()
        return False
    finally:
        if mgr is not None:
            try:
                mgr.shutdown()
            except Exception:
                pass
            try:
                del mgr
            except Exception:
                pass


def run_position_sync_loop(
    hold_std_path: str,
    main_contracts_path: str,
    trade_volume: int = 1,
    conf=None,
    env_name: str = None,
    logger=None,
    stop_event=None,
    position_ratio: float = 1.0,
) -> bool:
    """持续运行持仓同步循环（保持 CTP 连接，持续接收成交回报）

    逻辑：
    1. 首次同步：对比 hold-std.json 与实际持仓，提交差异委托
    2. 持续监控：持续对比，发现差异立即处理
    3. 无固定间隔：不需要每N秒同步一次，有差异就处理

    Args:
        hold_std_path: 标准持仓文件路径
        main_contracts_path: 主力合约配置路径
        trade_volume: 交易手数
        conf: CTP 配置
        env_name: 环境名称
        logger: 日志记录器
        stop_event: 停止事件（threading.Event），设为 None 则一直运行
        position_ratio: 持仓同步比例

    Returns:
        bool: 是否正常结束
    """
    import threading
    import os
    mgr = None
    last_hold_std_mtime = 0

    def _log(msg):
        if logger:
            logger.info(msg)
        print(msg)

    try:
        _log(f"[同步] 创建 PositionSyncManager...")
        _log(f"  hold_std_path={hold_std_path}")
        _log(f"  main_contracts_path={main_contracts_path}")
        _log(f"  position_ratio={position_ratio}")

        mgr = PositionSyncManager(
            hold_std_path=hold_std_path,
            main_contracts_path=main_contracts_path,
            conf=conf,
            env_name=env_name,
            position_ratio=position_ratio,
        )
        if logger:
            mgr.set_logger(logger)

        # 获取初始文件的修改时间
        if os.path.exists(hold_std_path):
            last_hold_std_mtime = os.path.getmtime(hold_std_path)

        _log(f"[同步] PositionSyncManager 创建成功，开始监控...")

        # 首次同步
        _log("[同步] 首次同步...")
        mgr.sync_and_trade(trade_volume=trade_volume, position_ratio=position_ratio)

        # 持续监控循环
        while True:
            # 检查停止信号
            if stop_event is not None and stop_event.is_set():
                _log("[同步] 收到停止信号，退出")
                break

            # 检查 hold-std.json 是否更新
            if os.path.exists(hold_std_path):
                current_mtime = os.path.getmtime(hold_std_path)
                if current_mtime > last_hold_std_mtime:
                    _log(f"[同步] 检测到 hold-std.json 更新，执行同步...")
                    last_hold_std_mtime = current_mtime
                    mgr.sync_and_trade(trade_volume=trade_volume, position_ratio=position_ratio)
                else:
                    # 文件没变，短暂等待后继续监控
                    threading.Event().wait(timeout=2)
            else:
                # 文件不存在，短暂等待
                threading.Event().wait(timeout=2)

    except Exception as e:
        import traceback
        _log(f"[异常] 同步循环出错: {e}")
        traceback.print_exc()
        return False
    finally:
        if mgr is not None:
            _log("[同步] 关闭 PositionSyncManager...")
            try:
                mgr.shutdown()
            except Exception:
                pass
            try:
                del mgr
            except Exception:
                pass
        _log("[同步] 同步循环已退出")
        return True


# 向后兼容：直接从 position_sync_manager 导入 PositionSyncManager
__all__ = [
    'PositionSyncManager',
    'PositionSyncManagerBase',
    'PositionSyncManagerData',
    'PositionSyncManagerMarket',
    'PositionSyncManagerOrderOps',
    'PositionSyncManagerSync',
    'run_position_sync',
    'run_position_sync_loop',
]


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    hold_std = os.path.join(base_dir, "order-check", "hold-std.json")
    main_contracts = os.path.join(base_dir, "data", "contracts", "main_contracts.json")
    run_position_sync(hold_std, main_contracts, trade_volume=1, timeout=30)