# -*- coding: utf-8 -*-
"""
独立持仓查询脚本

用法:
    cd /c/projects/low-low-up
    python trading/QueryPositions.py [env]

env 可选: 7x24 (默认) / online
"""

import json
import sys
import os
import time
import threading

# 项目根目录
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from ctp.base_tdapi import CTdSpiBase, tdapi
from config import config


class QueryPositionSpi(CTdSpiBase):
    """只查询持仓的 SPI"""

    def __init__(self, conf=None):
        self._positions = []
        self._done = threading.Event()
        super().__init__(conf=conf)

    def wait_login(self, timeout: int = 30):
        for _ in range(timeout):
            time.sleep(1)
            if self.is_login:
                return
        raise TimeoutError(f"CTP 登录超时（{timeout}秒）")

    def query(self, timeout: int = 10) -> list:
        self._done.clear()
        self._positions = []
        req = tdapi.CThostFtdcQryInvestorPositionField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user_id
        self._api.ReqQryInvestorPosition(req, 0)
        ok = self._done.wait(timeout=timeout)
        if not ok:
            print("[警告] 持仓查询超时")
        return list(self._positions)

    def OnRspQryInvestorPosition(
        self,
        pInvestorPosition: tdapi.CThostFtdcInvestorPositionField,
        pRspInfo: tdapi.CThostFtdcRspInfoField,
        nRequestID: int,
        bIsLast: bool,
    ):
        if pInvestorPosition:
            self._positions.append({
                "合约": (pInvestorPosition.InstrumentID or "").strip().upper(),
                "方向": self._direction_name(pInvestorPosition.PosiDirection),
                "总持仓": pInvestorPosition.Position,
                "今仓": pInvestorPosition.TodayPosition,
                "昨仓": pInvestorPosition.YdPosition,
                "持仓成本": getattr(pInvestorPosition, "OpenCost", 0.0),
            })
        if bIsLast:
            self._done.set()

    @staticmethod
    def _direction_name(d):
        return {tdapi.THOST_FTDC_PD_Long: "多", tdapi.THOST_FTDC_PD_Short: "空"}.get(d, "未知")


def print_positions(positions: list):
    if not positions:
        print("\n当前账户无持仓\n")
        return

    print(f"\n{'='*70}")
    print(f"{'合约':<12}{'方向':<8}{'总持仓':<10}{'今仓':<10}{'昨仓':<10}")
    print("-" * 70)

    # 聚合（同合约同方向可能有多条）
    aggregated = {}
    for p in positions:
        key = (p["合约"], p["方向"])
        if key not in aggregated:
            aggregated[key] = {"总持仓": 0, "今仓": 0, "昨仓": 0}
        aggregated[key]["总持仓"] += p["总持仓"]
        aggregated[key]["今仓"] += p["今仓"]
        aggregated[key]["昨仓"] += p["昨仓"]

    for (inst, direction), vols in aggregated.items():
        print(f"{inst:<12}{direction:<8}{vols['总持仓']:<10}{vols['今仓']:<10}{vols['昨仓']:<10}")

    print("=" * 70)
    print(f"共 {len(aggregated)} 个持仓合约\n")


def main():
    env_name = sys.argv[1].lower() if len(sys.argv) > 1 else "7x24"
    conf = config.envs.get(env_name)
    if not conf:
        print(f"未知环境: {env_name}，可用: {list(config.envs.keys())}")
        return

    spi = None
    try:
        print(f"正在连接 CTP ({env_name}) ...")
        spi = QueryPositionSpi(conf=conf)
        spi.wait_login()
        print("登录成功，查询持仓中...")

        positions = spi.query(timeout=10)
        print_positions(positions)

        # 同时输出 JSON 方便下游使用
        if positions:
            aggregated = {}
            for p in positions:
                key = (p["合约"], p["方向"])
                if key not in aggregated:
                    aggregated[key] = {"合约": p["合约"], "买/卖": p["方向"], "手数": 0}
                aggregated[key]["手数"] += p["总持仓"]

            hold_std = list(aggregated.values())
            print(json.dumps(hold_std, ensure_ascii=False, indent=2))
    except Exception as e:
        print(f"查询失败: {e}")
    finally:
        if spi is not None:
            try:
                del spi
            except Exception:
                pass


if __name__ == "__main__":
    main()
