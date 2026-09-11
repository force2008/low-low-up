# -*- coding: utf-8 -*-
"""
数据加载模块

包含：
- _load_contract_info, _get_contract_info
- _standardize_contract, _get_exact_instrument_id
- _load_hold_std, _positions_to_hold_std, _save_hold_std
- _parse_hold_std, _aggregate_actual_positions, _get_position_detail
- _get_actual_position_volume, compare_positions
- is_contract_in_trading_time, _is_time_in_sessions, _is_simulation_env
- _guess_exchange
"""

import json
import os
import re
from datetime import datetime, time as dt_time
from typing import Dict, List, Optional, Tuple

from .constants import PRODUCT_TRADING_SESSIONS, DAY_3SEG

# 把项目根目录加入路径，以便导入 ctp 模块
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class PositionSyncManagerData:
    """持仓同步管理器 - 数据加载部分"""

    # ------------------------------------------------------------------
    # 排除品种工具：由 Base.__init__ 写入 self._exclude_products（全大写 set）
    # ------------------------------------------------------------------
    def _is_contract_excluded(self, instrument_id_upper: str) -> bool:
        """合约代码前缀命中任一排除品种即返回 True。

        例：排除 {"SC", "FG"} → 合约 SC2501 / SC2503 / FG501 / FG509 均命中并跳过；
        未配置排除时 self._exclude_products 为空 set，全部返回 False。
        """
        excludes = getattr(self, '_exclude_products', None)
        if not excludes:
            return False
        s = (instrument_id_upper or '').upper()
        if not s:
            return False
        for prefix in excludes:
            if s.startswith(prefix):
                return True
        return False

    def _load_contract_info(self) -> bool:
        """从 main_contracts.json / instruments.json 加载合约信息（交易所、PriceTick、ProductID）"""
        self._contract_info: Dict[str, dict] = {}
        self._instrument_exact_case: Dict[str, str] = {}
        self._product_exchange_map: Dict[str, str] = {}

        # 1) 主力合约配置
        if not os.path.exists(self.main_contracts_path):
            self.print(f"[错误] 找不到 main_contracts.json: {self.main_contracts_path}")
            return False
        try:
            with open(self.main_contracts_path, "r", encoding="utf-8") as f:
                contracts = json.load(f)
            for c in contracts:
                exact = c.get("MainContractID", "").strip()
                cid = exact.upper()
                product_id = c.get("ProductID", "").strip().upper()
                exchange = c.get("ExchangeID", "").strip()
                if cid:
                    self._contract_info[cid] = {
                        "ExchangeID": exchange,
                        "PriceTick": float(c.get("PriceTick", 1)),
                        "ProductID": product_id,
                        "InstrumentName": c.get("InstrumentName", "").strip(),
                    }
                    if exact:
                        self._instrument_exact_case[cid] = exact
                if product_id and exchange:
                    self._product_exchange_map[product_id] = exchange
            self.print(f"[信息] 已加载 {len(contracts)} 个主力合约信息")
        except Exception as e:
            self.print(f"[错误] 加载 main_contracts.json 失败: {e}")
            return False

        # 2) 全部合约（补充大小写映射，特别是 GFEX lowercase 合约）
        inst_path = os.path.join(PROJECT_ROOT, "data", "contracts", "instruments.json")
        if os.path.exists(inst_path):
            try:
                with open(inst_path, "r", encoding="utf-8") as f:
                    instruments = json.load(f)
                added = 0
                for ins in instruments:
                    exact = ins.get("InstrumentID", "").strip()
                    cid = exact.upper()
                    product_id = ins.get("ProductID", "").strip().upper()
                    exchange = ins.get("ExchangeID", "").strip()
                    if cid:
                        self._instrument_exact_case[cid] = exact
                        if cid not in self._contract_info:
                            self._contract_info[cid] = {
                                "ExchangeID": exchange,
                                "PriceTick": float(ins.get("PriceTick", 1)),
                                "ProductID": product_id,
                                "InstrumentName": ins.get("InstrumentName", "").strip(),
                            }
                            added += 1
                    if product_id and exchange:
                        self._product_exchange_map[product_id] = exchange
                if added:
                    self.print(f"[信息] 从 instruments.json 补充加载 {added} 个合约信息")
            except Exception as e:
                self.print(f"[警告] 加载 instruments.json 失败: {e}")
        return True

    def _get_contract_info(self, instrument_id: str) -> dict:
        """获取合约信息，优先从 main_contracts.json，找不到时尝试推断"""
        inst = self._standardize_contract(instrument_id)
        info = self._contract_info.get(inst.upper())
        if info:
            return info
        # 推断 ProductID（去掉尾部数字）
        product_id = inst.rstrip("0123456789")
        return {
            "ExchangeID": self._guess_exchange(inst),
            "PriceTick": 1.0,
            "ProductID": product_id,
            "InstrumentName": inst,
        }

    def _standardize_contract(self, instrument_id: str) -> str:
        """标准化合约代码：
        - 优先从映射恢复原始大小写（GFEX小写、SHFE小写、DCE小写等）
        - CZCE 4位年月转3位年月（SA2405→SA405），保持与融航等柜台一致的3位格式
        """
        inst = instrument_id.strip().upper()

        # 1. 映射中有精确匹配，恢复原始大小写（这是 CTP 实际使用的格式）
        exact = self._instrument_exact_case.get(inst)
        if exact:
            return exact

        # 2. CZCE 合约：4位年月 → 3位年月
        m = re.match(r'^([A-Z]{1,3})(\d{2})(\d{2})$', inst)
        if m:
            product = m.group(1)
            year_digit = m.group(2)[-1]
            month = m.group(3)
            czce_fmt = f"{product}{year_digit}{month}"

            czce_products = {
                "CF", "RM", "MA", "SR", "TA", "OI", "FG", "SA", "AP",
                "SM", "SF", "PX", "PR", "PF", "PK", "PL", "SH", "UR",
                "CJ", "CY", "JR", "PM", "RS", "WH", "ZC",
            }
            if product in czce_products:
                # 优先从映射恢复大小写
                exact = self._instrument_exact_case.get(czce_fmt)
                if exact:
                    return exact
                # 如果 3 位格式存在于配置中，直接使用
                if czce_fmt in self._contract_info:
                    return czce_fmt
                # 4 位格式本身不在配置中， fallback 到 3 位格式
                if inst not in self._contract_info:
                    return czce_fmt

        # 3. 通过 ProductID 确定交易所：DCE/GFEX/SHFE/INE 统一小写
        product_id = inst.rstrip("0123456789")
        exchange = self._product_exchange_map.get(product_id)
        if exchange in ("DCE", "GFEX", "SHFE", "INE"):
            lower_inst = inst.lower()
            exact = self._instrument_exact_case.get(lower_inst.upper())
            if exact:
                return exact
            return lower_inst

        # 未知交易所（如其他需要小写的品种），保留原始大小写
        return instrument_id.strip()

    def _get_exact_instrument_id(self, instrument_id: str) -> str:
        """返回原始大小写的合约代码（GFEX 等需要小写）"""
        inst = instrument_id.strip().upper()
        return self._instrument_exact_case.get(inst, instrument_id.strip())

    def _load_hold_std(self) -> bool:
        if not os.path.exists(self.hold_std_path):
            self.print(f"[错误] 找不到 hold-std.json: {self.hold_std_path}")
            return False
        try:
            with open(self.hold_std_path, "r", encoding="utf-8") as f:
                self._hold_std = json.load(f)
            total_vol = 0
            for item in self._hold_std:
                vol = item.get("持仓量") or item.get("手数") or item.get("数量") or "0"
                try:
                    total_vol += int(float(str(vol).strip()))
                except (ValueError, TypeError):
                    pass
            self.print(f"[信息] 已加载标准持仓 {len(self._hold_std)} 个合约，共 {total_vol} 手")
            return True
        except Exception as e:
            self.print(f"[错误] 加载 hold-std.json 失败: {e}")
            return False

    def _positions_to_hold_std(self, positions: List[dict]) -> List[dict]:
        """将 CTP 原始持仓聚合转换为 hold-std.json 格式"""
        aggregated: Dict[Tuple[str, int], int] = {}
        for pos in positions:
            contract = self._standardize_contract(pos["InstrumentID"])
            direction = pos["PosiDirection"]
            volume = pos["Position"]
            if not contract or direction not in (2, 3):
                continue
            key = (contract, direction)
            aggregated[key] = aggregated.get(key, 0) + volume

        result: List[dict] = []
        for (contract, direction), volume in aggregated.items():
            result.append({
                "合约": contract,
                "买/卖": "买" if direction == 2 else "卖",
                "手数": volume,
            })
        return result

    def _save_hold_std(self) -> bool:
        """将 self._hold_std 保存到 hold-std.json"""
        try:
            with open(self.hold_std_path, "w", encoding="utf-8") as f:
                json.dump(self._hold_std, f, ensure_ascii=False, indent=2)
            self.print(f"[信息] 已保存标准持仓到 {self.hold_std_path}")
            return True
        except Exception as e:
            self.print(f"[错误] 保存 hold-std.json 失败: {e}")
            return False

    # ------------------------------------------------------------------
    # 交易时段判断
    # ------------------------------------------------------------------
    @staticmethod
    def _is_time_in_sessions(now_time: dt_time, sessions: List[Tuple[dt_time, dt_time]]) -> bool:
        """判断当前时间是否在任一交易时段内（支持跨午夜，如 21:00-01:00）"""
        for start, end in sessions:
            if start <= end:
                if start <= now_time <= end:
                    return True
            else:
                # 跨午夜
                if now_time >= start or now_time <= end:
                    return True
        return False

    def _is_simulation_env(self) -> bool:
        """判断是否为 TTS/模拟环境（7x24 运行，无需检查交易时段）"""
        return "openctp.cn" in (self._front or "")

    def is_contract_in_trading_time(self, instrument_id: str) -> bool:
        """检查指定合约当前是否处于可交易时段"""
        # TTS 模拟环境 7x24 运行，跳过时段检查
        if self._is_simulation_env():
            return True
        info = self._get_contract_info(instrument_id)
        product_id = info.get("ProductID", "")
        sessions = PRODUCT_TRADING_SESSIONS.get(product_id)
        if not sessions:
            # DCE 标准化后 ProductID 是大写，但配置里是小写，做兼容
            sessions = PRODUCT_TRADING_SESSIONS.get(product_id.lower())
        if not sessions:
            # 未配置的品种，默认走三段日盘
            sessions = DAY_3SEG
        now_time = datetime.now().time()
        return self._is_time_in_sessions(now_time, sessions)

    # ------------------------------------------------------------------
    # 持仓对比
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_direction(row: dict) -> str:
        """从字典中提取方向字段，兼容多种列名"""
        for key in ("买/卖", "多空", "方向", "持仓方向", "Direction", "direction", "买卖"):
            val = (row.get(key) or "").strip()
            if val:
                return val
        return ""

    @staticmethod
    def _extract_contract(row: dict) -> str:
        # 优先取代码类字段，避免"合约名"在某些柜台是中文描述导致对不齐
        for key in ("合约ID", "合约代码", "InstrumentID", "instrument_id", "合约", "合约名", "合约名称"):
            val = (row.get(key) or "").strip()
            if val:
                return val.upper()
        return ""

    @staticmethod
    def _extract_volume(row: dict) -> int:
        for key in ("持仓量", "手数", "数量", "总持仓", "Volume", "volume"):
            val = row.get(key)
            if val is not None:
                try:
                    return int(float(str(val).strip()))
                except (ValueError, TypeError):
                    continue
        return 0

    def _load_main_by_product(self) -> bool:
        """加载 main_contracts_by_product.json，构建三个便捷缓存：
        self._main_by_product          : {ProductID: {main,main2,main3,volume_multiple,price_tick,...}}
        self._allowed_contracts_set    : {具体合约代码 UPPER}（仅在 allow_level 中的那些级别的具体合约）
        self._product_volume_multiple  : {ProductID: volume_multiple}（算 notional 用）

        懒加载：首次 _parse_hold_std 调一次；之后 allow_level 变更时把 self._main_by_product_loaded=False
        下一次 _parse_hold_std 再重算。
        """
        path = getattr(self, '_main_by_product_path', None)
        if not path:
            path = os.path.join(PROJECT_ROOT, "data", "contracts", "main_contracts_by_product.json")
            self._main_by_product_path = path
        if not os.path.exists(path):
            self.print(f"[错误] 找不到 main_contracts_by_product.json: {path}，将回退为「全部合约允许」")
            self._main_by_product = {}
            self._allowed_contracts_set = set()
            self._product_volume_multiple = {}
            self._main_by_product_loaded = True  # 避免每次 _parse_hold_std 都重试
            return False
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            self.print(f"[错误] 读取 main_contracts_by_product.json 失败: {e}，将回退为「全部合约允许」")
            self._main_by_product = {}
            self._allowed_contracts_set = set()
            self._product_volume_multiple = {}
            self._main_by_product_loaded = True
            return False
        if not isinstance(data, dict):
            self.print(f"[错误] main_contracts_by_product.json 根节点类型错误 {type(data)}，应为 dict")
            self._main_by_product_loaded = True
            return False
        allow_level = getattr(self, '_allow_contract_level', None) or {"main", "main2"}
        allowed_set = set()
        vm_map = {}
        for product_id, info in data.items():
            if not isinstance(info, dict):
                continue
            # 1. 存储完整 info
            self._main_by_product[product_id.upper()] = info
            # 2. 缓存合约乘数
            try:
                vm = float(info.get("volume_multiple", 1))
                if vm > 0:
                    vm_map[product_id.upper()] = vm
            except (TypeError, ValueError):
                pass
            # 3. 构建具体合约允许集合（按 allow_level 取具体 key）
            for level_key in allow_level:
                contract_id = info.get(level_key)
                if contract_id:
                    cid = str(contract_id).strip().upper()
                    if cid:
                        allowed_set.add(cid)
        self._allowed_contracts_set = allowed_set
        self._product_volume_multiple = vm_map
        self._main_by_product_loaded = True
        self.print(
            f"[信息] main_by_product 加载完成: 共 {len(data)} 品种, "
            f"允许级别={sorted(allow_level)}, 允许具体合约={len(allowed_set)} 个, "
            f"乘数缓存={len(vm_map)} 品种"
        )
        return True

    def _contract_is_mainline(self, instrument_id_upper: str) -> Optional[bool]:
        """判断具体合约代码是否为 allow_level 指定的主/次主/次次主力。

        返回 True/False/None：
          True  = 明确是主流通合约
          False = 明确是非主流通（远月/冷门月）
          None  = 文件没加载成功（或未知品种），应视为"放行"避免误杀。
        """
        if not getattr(self, '_main_by_product_loaded', False):
            self._load_main_by_product()
        allowed = getattr(self, '_allowed_contracts_set', None)
        if allowed is None:
            return None
        if len(allowed) == 0:
            # allow_level 对应合约集合为空（= 文件加载失败），不拦避免误杀
            return None
        inst = (instrument_id_upper or '').upper()
        if not inst:
            return None
        return inst in allowed

    def _estimate_volume_multiple(self, instrument_id_upper: str) -> float:
        """估算合约乘数（用于算 notional = qty × price × vm），失败回退 1.0。"""
        if not getattr(self, '_main_by_product_loaded', False):
            self._load_main_by_product()
        product_id = (instrument_id_upper or '').strip().upper().rstrip("0123456789")
        if product_id:
            vm = (getattr(self, '_product_volume_multiple', None) or {}).get(product_id)
            if vm and vm > 0:
                return float(vm)
        # 回退：查 _contract_info（旧 main_contracts.json 加载的）有没有 VolumeMultiple 字段
        info = self._contract_info.get((instrument_id_upper or '').upper()) or {}
        vm_raw = info.get("VolumeMultiple") if isinstance(info, dict) else None
        try:
            vm2 = float(vm_raw)
            if vm2 > 0:
                return vm2
        except (TypeError, ValueError):
            pass
        return 1.0

    def _estimate_notional(self, instrument_id_upper: str, qty_hand: int) -> float:
        """估算单合约目标持仓的名义成交额（元）：qty_hand × LastPrice_or_PreClose × VolumeMultiple。

        数据源优先级：
          1) 若 manager 能取到最新行情 -> 用 LatestPrice × vm；
          2) 否则按 main_by_product.json 查不到时回退：vm * 1000 * qty（保守估值，偏低不会误杀真实大单）
        """
        vm = self._estimate_volume_multiple(instrument_id_upper)
        price = 0.0
        # 尝试1：get_market_data 最新价
        try:
            md_getter = getattr(self, 'get_market_data', None)
            if callable(md_getter):
                md = md_getter(instrument_id_upper) or {}
                if isinstance(md, dict):
                    for k in ('LastPrice', 'LatestPrice', 'ClosePrice', 'PreClosePrice', 'SettlementPrice'):
                        try:
                            p = float(md.get(k, 0))
                            if p > 0:
                                price = p
                                break
                        except (TypeError, ValueError):
                            continue
        except Exception:
            pass
        # 尝试2：_main_by_product 有没有昨收/最新（没有的字段就 0）
        if price <= 0:
            product_id = (instrument_id_upper or '').strip().upper().rstrip("0123456789")
            if product_id:
                if not getattr(self, '_main_by_product_loaded', False):
                    self._load_main_by_product()
                info = (getattr(self, '_main_by_product', None) or {}).get(product_id, None)
                if isinstance(info, dict):
                    for k in ('main_volume',):  # 无昨收字段，跳过
                        pass
        # 尝试3：_contract_info 里有没有默认昨收 （一般没有，占位）
        if price <= 0:
            # 回退：保守估算——按 1000 元/手（这个估计偏低，尽量放行不误杀）
            #   但对于真实品种的乘数，比如 FG 乘数 20 → 20 × 1500 = 3 万 → 1 手 3 万
            #   所以直接取 1000 × vm 就能保证 1 手 FG 是 2 万 ≈ 真实 3 万的量级。
            price = 1000.0
        try:
            q = int(qty_hand)
        except (TypeError, ValueError):
            q = 0
        return float(max(q, 0)) * float(price) * float(vm)

    def _parse_hold_std(self) -> Dict[Tuple[str, int], int]:
        """目标持仓构造器，支持 ratio/ration 的三种模式 + 反探单过滤。

        (A) ratio > 0  → 正常跟单：hold-std 方向不变，手数 = round(原始手数 × ratio)，单合约最小 1 手（若原始>0）。
        (B) ratio == 0 → 清仓模式：目标持仓强制返回空 dict，相当于所有品种目标为 0；
                         后续比对会判定为"实际持仓超额"，走 _submit_excess_orders 平仓分支，
                         平仓价格按 sync.py 既定逻辑：多平挂卖一(BidPrice1)、空平挂买一(AskPrice1)。
        (C) ratio < 0  → 对冲模式：以 hold-std 里的 source_account 原始持仓为基准，方向反转后再乘 abs(ratio)。
                         例：source wangk0402 持 FG 多 1 手，ratio=-1 → target 持 FG 空 1 手；ratio=-2 → 空 2 手。
                         等价于"跟单账户作为 signal_account 的对手方"。
        三种模式下 exclude 品种过滤、非标准化跳过、volume<=0 跳过、
        追加 4 层「反探单过滤」：deny_products → 主流通合约检查 + 大单豁免 → min_qty_hand → min_notional。
        """
        # 清仓模式：直接空 dict，不做反探单过滤（清仓是要把所有持仓平掉，包括非主流通/黑名单品种）
        ratio = getattr(self, '_position_ratio', 1.0)
        if ratio == 0:
            self.print("[模式-清仓] ration=0，目标持仓全部置 0（将以限价挂卖一/买一平掉所有实际持仓）")
            return {}

        # 1) 先构建原始缩放后的 result（按 ratio/对冲 正常算）：代码复用原先已有实现
        raw_result: Dict[Tuple[str, int], int] = {}
        total_original = 0
        excluded_original = 0
        hedged_total = 0
        abs_ratio = abs(ratio)

        for i, row in enumerate(self._hold_std):
            raw_contract = self._extract_contract(row)
            contract = self._standardize_contract(raw_contract)
            direction_str = self._extract_direction(row)
            volume = self._extract_volume(row)
            if not contract:
                self.print(f"[调试-hold] 第{i}条 contract为空 raw='{raw_contract}' keys={list(row.keys())}")
                continue
            if volume <= 0:
                self.print(f"[调试-hold] 第{i}条 {contract} volume={volume}")
                continue
            if self._is_contract_excluded(contract):
                excluded_original += volume
                self.print(f"[exclude] 目标持仓跳过（{contract} 命中排除品种）: {direction_str} {volume}手")
                continue
            if direction_str in ("买", "多头", "多", "Buy", "BUY", "buy", "B"):
                src_direction = 2
            elif direction_str in ("卖", "空头", "空", "Sell", "SELL", "sell", "S"):
                src_direction = 3
            else:
                self.print(f"[调试-hold] 第{i}条 {contract} 方向无法解析 '{direction_str}'")
                continue
            total_original += volume

            if ratio > 0:
                direction = src_direction
                scaled_volume = max(1, int(round(volume * ratio))) if volume > 0 else 0
            else:
                direction = 3 if src_direction == 2 else 2
                hedged_after = int(round(volume * abs_ratio)) if volume > 0 else 0
                scaled_volume = hedged_after
                hedged_total += hedged_after

            if scaled_volume > 0:
                key = (contract, direction)
                raw_result[key] = raw_result.get(key, 0) + scaled_volume

        # 2) 追加 4 层「反探单过滤」（仅对 ration != 0 生效）
        deny_products = getattr(self, '_deny_products', None) or set()
        min_qty_hand = getattr(self, '_min_qty_hand', None)
        min_notional = getattr(self, '_min_notional', None)
        big_notional_exemption = getattr(self, '_big_notional_exemption', 1_000_000.0)

        if not getattr(self, '_main_by_product_loaded', False):
            self._load_main_by_product()

        final_result: Dict[Tuple[str, int], int] = {}
        filtered_counts = {"deny": 0, "non_main": 0, "min_qty": 0, "min_notional": 0}
        exempt_non_main = 0

        for (contract, direction), qty in raw_result.items():
            if qty <= 0:
                continue
            cu = (contract or '').strip().upper()
            product_id = cu.rstrip("0123456789")

            # 层1：黑名单 deny（全球冷门 + 账号追加）→ 强剔除
            if deny_products and product_id and product_id in deny_products:
                self.print(
                    f"[deny] 跳过（品种 {product_id} 命中黑名单合约）: {contract} "
                    f"{'买' if direction == 2 else '卖'} {qty}手"
                )
                filtered_counts["deny"] += 1
                continue

            # 层2：非主流通合约 → 先估算 notional，再判断是否满足大豁免
            mainline_flag = self._contract_is_mainline(cu)
            if mainline_flag is False:
                notional = self._estimate_notional(cu, qty)
                if notional >= big_notional_exemption:
                    exempt_non_main += 1
                    self.print(
                        f"[非主流通-大单豁免] {contract} {'买' if direction == 2 else '卖'} {qty}手, "
                        f"估算成交额约 {notional:,.0f} 元 >= {big_notional_exemption:,.0f} 豁免阈值，放行"
                    )
                else:
                    self.print(
                        f"[非主流通] 跳过（{contract} 不属于 allow_level={sorted(getattr(self, '_allow_contract_level', None))}），"
                        f"且成交额 {notional:,.0f} 元 < {big_notional_exemption:,.0f} 豁免阈值: "
                        f"{'买' if direction == 2 else '卖'} {qty}手"
                    )
                    filtered_counts["non_main"] += 1
                    continue

            # 层3：min_qty_hand（单合约手数太小视为探单）
            if min_qty_hand is not None and min_qty_hand > 0 and qty < min_qty_hand:
                self.print(
                    f"[min_qty] 跳过（{contract} 单合约 {qty} 手 < {min_qty_hand} 手门槛，视为探单）"
                )
                filtered_counts["min_qty"] += 1
                continue

            # 层4：min_notional（单合约成交额太小视为探单）
            if min_notional is not None and min_notional > 0:
                notional = self._estimate_notional(cu, qty)
                if notional < min_notional:
                    self.print(
                        f"[min_notional] 跳过（{contract} 成交额 {notional:,.0f} 元 < {min_notional:,.0f} 元门槛，视为探单）："
                        f"{qty} 手"
                    )
                    filtered_counts["min_notional"] += 1
                    continue

            # 全部通过
            final_result[(contract, direction)] = qty

        # 日志：先打印原模式（缩放/对冲）汇总，再追加过滤统计
        total_scaled_raw = sum(raw_result.values())
        total_final = sum(final_result.values())
        if ratio < 0:
            base_mode_msg = (
                f"[模式-对冲(×{abs_ratio})] 原始source持仓: {total_original} 手（已排除品种占 {excluded_original} 手）, "
                f"对冲后未过滤={total_scaled_raw} 手"
            )
        else:
            base_mode_msg = (
                f"[比例] 原始目标持仓: {total_original} 手（已排除品种占 {excluded_original} 手）, "
                f"缩放后未过滤={total_scaled_raw} 手 (ratio={ratio})"
            )
        self.print(base_mode_msg)
        filter_applied = any(v > 0 for v in filtered_counts.values()) or exempt_non_main > 0
        if filter_applied:
            parts = []
            if filtered_counts["deny"]:
                parts.append(f"黑名单剔除 {filtered_counts['deny']} 个合约")
            if filtered_counts["non_main"]:
                parts.append(f"非主流通剔除 {filtered_counts['non_main']} 个合约")
            if exempt_non_main:
                parts.append(f"非主流通大单豁免 {exempt_non_main} 个合约")
            if filtered_counts["min_qty"]:
                parts.append(f"手数门槛剔除 {filtered_counts['min_qty']} 个合约")
            if filtered_counts["min_notional"]:
                parts.append(f"成交额门槛剔除 {filtered_counts['min_notional']} 个合约")
            self.print(f"[反探单过滤] {'、'.join(parts)}；最终通过合约 {len(final_result)} 个，目标手数合计: {total_final}")
        elif len(raw_result) != len(final_result) or total_scaled_raw != total_final:
            self.print(
                f"[反探单过滤] 无剔除；最终通过合约 {len(final_result)} 个，目标手数合计: {total_final}"
            )
        return final_result

    def _aggregate_actual_positions(self) -> Dict[Tuple[str, int], int]:
        result: Dict[Tuple[str, int], int] = {}
        excluded_actual = 0
        for pos in self._actual_positions:
            contract = self._standardize_contract(pos["InstrumentID"])
            # 排除品种过滤：实际持仓侧直接跳过，避免当成"超额"触发平仓
            if self._is_contract_excluded(contract):
                excluded_actual += int(pos.get("Position", 0) or 0)
                continue
            key = (contract, pos["PosiDirection"])
            result[key] = result.get(key, 0) + pos["Position"]
        if excluded_actual > 0:
            self.print(f"[exclude] 实际持仓跳过 {excluded_actual} 手（命中排除品种，不参与对齐比较）")
        return result

    def _get_position_detail(self, contract: str, direction: int) -> dict:
        """获取指定合约+方向的持仓详情（总持仓、今仓、昨仓、交易所）"""
        pos_total = 0
        today_total = 0
        yd_total = 0
        exchange_id = ""
        contract_upper = contract.upper()
        for pos in self._actual_positions:
            if pos["InstrumentID"].upper() == contract_upper and pos["PosiDirection"] == direction:
                pos_total += pos["Position"]
                today_total += pos["TodayPosition"]
                yd_total += pos["YdPosition"]
                if not exchange_id:
                    exchange_id = pos.get("ExchangeID", "")
        return {
            "Position": pos_total,
            "TodayPosition": today_total,
            "YdPosition": yd_total,
            "ExchangeID": exchange_id,
        }

    def _get_actual_position_volume(self, contract: str, direction: int) -> int:
        """获取指定合约+方向的实际持仓总量（直接从 CTP 原始数据汇总）"""
        return self._get_position_detail(contract, direction)["Position"]

    def compare_positions(self) -> Tuple[bool, List[dict]]:
        target = self._parse_hold_std()
        actual = self._aggregate_actual_positions()

        self.print(f"[信息] 标准持仓: {len(target)} 个, 实际持仓: {len(actual)} 个")

        if set(target.keys()) != set(actual.keys()):
            self.print("[信息] 持仓合约/方向不一致")
            for k in set(target.keys()) - set(actual.keys()):
                self.print(f"  标准有但账户无: {k[0]} 方向={k[1]}")
            for k in set(actual.keys()) - set(target.keys()):
                self.print(f"  账户有但标准无: {k[0]} 方向={k[1]}")
            return False, []

        for key, t_vol in target.items():
            a_vol = actual.get(key, 0)
            if t_vol != a_vol:
                self.print(
                    f"[信息] 手数不一致: {key[0]} 方向={key[1]} "
                    f"标准={t_vol} 实际={a_vol}"
                )
                return False, []

        orders = []
        for (contract, direction), vol in target.items():
            info = self._get_contract_info(contract)
            orders.append({
                "instrument_id": contract,
                "direction": "buy" if direction == 2 else "sell",
                "volume": vol,
                "exchange_id": info["ExchangeID"],
                "product_id": info["ProductID"],
                "price_tick": info["PriceTick"],
            })
        return True, orders

    def _guess_exchange(self, instrument_id: str) -> str:
        # 优先从 main_contracts.json / instruments.json 的 ProductID 映射中查找
        product_id = instrument_id.rstrip("0123456789").upper()
        exchange = self._product_exchange_map.get(product_id)
        if exchange:
            return exchange
        # fallback 到硬编码前缀表
        prefix = instrument_id[:2].upper()
        mapping = {
            "IF": "CFFEX", "IC": "CFFEX", "IH": "CFFEX", "IM": "CFFEX",
            "TS": "CFFEX", "TF": "CFFEX", "T": "CFFEX", "TL": "CFFEX",
            "AU": "SHFE", "AG": "SHFE", "CU": "SHFE", "AL": "SHFE",
            "ZN": "SHFE", "PB": "SHFE", "NI": "SHFE", "SN": "SHFE",
            "RB": "SHFE", "HC": "SHFE", "FU": "SHFE", "BU": "SHFE",
            "RU": "SHFE", "SP": "SHFE", "AO": "SHFE", "BR": "SHFE",
            "NR": "SHFE", "SC": "INE", "LU": "INE", "BC": "INE",
            "EC": "INE", "AP": "CZCE", "CF": "CZCE", "CY": "CZCE",
            "FG": "CZCE", "MA": "CZCE", "OI": "CZCE", "RM": "CZCE",
            "SA": "CZCE", "SF": "CZCE", "SM": "CZCE", "SR": "CZCE",
            "TA": "CZCE", "UR": "CZCE", "PX": "CZCE", "PF": "CZCE",
            "PK": "CZCE", "PR": "CZCE", "PL": "CZCE", "SH": "CZCE",
            "A": "DCE", "B": "DCE", "C": "DCE", "CS": "DCE",
            "EB": "DCE", "EG": "DCE", "I": "DCE", "J": "DCE",
            "JD": "DCE", "JM": "DCE", "L": "DCE", "LH": "DCE",
            "M": "DCE", "P": "DCE", "PG": "DCE", "PP": "DCE",
            "RR": "DCE", "V": "DCE", "Y": "DCE", "FB": "DCE",
            "BB": "DCE", "LG": "DCE", "LC": "GFEX", "SI": "GFEX",
            "PS": "GFEX", "PT": "GFEX", "PD": "GFEX",
        }
        return mapping.get(prefix, "SHFE")