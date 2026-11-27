# -*- coding: utf-8 -*-
"""只读探针：查询 CTP 中可交易的「组合合约」（郑商所 SPD 跨期套利等）。

用途：验证原生组合单可行性——确认组合合约代码命名格式、腿序、可用字段。
不报单、不撤单，仅 ReqQryInstrument。

用法（在仓库根目录执行）：
    python utils/probe_combo.py 7x24
    python utils/probe_combo.py 7x24 MA701 MA702     # 指定要关心的两腿
"""
import sys
import time

from ctp.base_tdapi import CTdSpiBase, tdapi


class ComboProbe(CTdSpiBase):
    def __init__(self, conf=None, want_legs=("MA701", "MA702")):
        self._want = {str(x).strip().upper() for x in want_legs}
        self._combo_rows = []
        self._sample_dumped = False
        self._done = False
        super().__init__(conf)          # 连接 + 认证 + 登录（阻塞到登录完成）
        self._query_all()

    def _query_all(self):
        self.print("请求查询全部合约（客户端筛选组合合约）")
        req = tdapi.CThostFtdcQryInstrumentField()
        try:
            ret = self._api.ReqQryInstrument(req, 0)
        except Exception as e:
            print("ReqQryInstrument 异常:", e)
            self._done = True
            return
        if ret != 0:
            print("ReqQryInstrument 发送失败 ret=", ret)
            self._done = True

    def OnRspQryInstrument(self, pInstrument, pRspInfo, nRequestID, bIsLast):
        if pInstrument:
            try:
                iid = str(getattr(pInstrument, "InstrumentID", "") or "").strip()
                up = iid.upper()
                pc = str(getattr(pInstrument, "ProductClass", "")).strip()
                is_pc_comb = (pc == str(getattr(tdapi, "THOST_FTDC_PC_Combination", "")))
                if ("&" in iid) or is_pc_comb or up.startswith("SPD"):
                    self._combo_rows.append((
                        iid,
                        str(getattr(pInstrument, "ExchangeID", "") or "").strip(),
                        pc,
                        str(getattr(pInstrument, "CombinationType", "") or "").strip(),
                        str(getattr(pInstrument, "ExchangeInstID", "") or "").strip(),
                    ))
                    if not self._sample_dumped:
                        self._sample_dumped = True
                        names = [n for n in dir(pInstrument) if n and n[0].isupper()]
                        print("\n===== 组合合约样例字段（首个命中）=====")
                        for n in names:
                            try:
                                print(f"  {n} = {getattr(pInstrument, n)!r}")
                            except Exception:
                                pass
                        print("=====================================\n")
            except Exception:
                pass
        if bIsLast:
            self._done = True
            self._dump()

    def _dump(self):
        print("\n===== 组合合约扫描结果 =====")
        print(f"命中组合合约数: {len(self._combo_rows)}")
        for row in self._combo_rows[:200]:
            print("  ", row)
        print("\n----- 与关心两腿相关的组合 -----")
        hit = 0
        for iid, eid, pc, ct, eii in self._combo_rows:
            up = iid.upper()
            if all(leg in up for leg in self._want):
                hit += 1
                print(f"  [{eid}] {iid}  (ExchangeInstID={eii}, ProductClass={pc}, CombType={ct})")
        if hit == 0:
            print(f"  未找到同时包含 {sorted(self._want)} 的组合（可能该环境无此组合或命名不同）")

    def wait_done(self, timeout=60):
        t0 = time.time()
        while time.time() - t0 < timeout:
            if self._done:
                break
            time.sleep(0.5)


if __name__ == "__main__":
    argv = sys.argv[1:]
    env = argv[0] if argv and argv[0] in ("7x24", "online", "simu", "simu-7x24", "simu-vip") else "7x24"
    legs = tuple(argv[1:]) if len(argv) > 1 else ("MA701", "MA702")
    # 让 CTdSpiBase 走我们指定的 env：直接传 conf
    from config import config as _cfg
    conf = _cfg.envs.get(env)
    print(f"环境={env} 关心两腿={legs}")
    probe = ComboProbe(conf=conf, want_legs=legs)
    probe.wait_done(timeout=60)
    try:
        probe._api.Release()
    except Exception:
        pass
