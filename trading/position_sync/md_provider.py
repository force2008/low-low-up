# -*- coding: utf-8 -*-
"""行情 API 订阅提供者

通过独立的行情前置（MdApi）订阅合约并缓存最新 tick，
供 PositionSyncManager 统一获取行情（无论 simu 还是 online 柜台）。
"""

import os
import shutil
import threading
import time
from typing import Dict, Optional

import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config import config
from ctp.base_mdapi import CMdSpiBase, mdapi


class MdQuoteProvider(CMdSpiBase):
    """通过 CTP 行情 API 订阅并缓存最新行情快照"""

    def __init__(self, conf=None):
        # 必须先初始化锁和缓存，因为父类初始化会触发回调
        self._quotes_lock = threading.Lock()
        self._quotes: Dict[str, dict] = {}
        self._subscribed: set = set()
        self._login_event = threading.Event()
        self._login_ok = False
        self._login_error = ""

        # 不调用 CMdSpiBase.__init__，自己控制流文件目录，避免多实例冲突
        mdapi.CThostFtdcMdSpi.__init__(self)

        if conf is None:
            conf = config.get_env_config()

        self._front = conf.get("md")
        self._user_id = conf.get("user_id")
        self._password = conf.get("password")
        self._authcode = conf.get("authcode")
        self._appid = conf.get("appid")
        self._broker_id = conf.get("broker_id")
        self._user_product_info = conf.get("user_product_info")

        self._is_login = False
        self._is_last = False
        self._trading_day = ""
        self._front_id = None
        self._session_id = None

        self.print("启动行情Api")
        self.print(f" 行情前置地址: {self._front}")
        self.print(f" 用户: {self._user_id}")

        # 使用带进程 ID 的唯一流文件目录，避免同用户多实例或上次未正常退出时文件锁冲突
        pid = os.getpid()
        flat_dir = f"{self._user_id}_md_{pid}"
        if os.path.exists(flat_dir):
            try:
                shutil.rmtree(flat_dir)
            except Exception as e:
                self.print(f"[MdQuoteProvider] 清理旧流目录失败: {e}")
        os.makedirs(flat_dir, exist_ok=True)
        flat_path = os.path.join(flat_dir, f"{self._user_id}_{pid}")

        if not self._front:
            self._login_error = "配置中无行情前置地址"
            self.print(f"[MdQuoteProvider] {self._login_error}")
            return

        self._api: mdapi.CThostFtdcMdApi = mdapi.CThostFtdcMdApi.CreateFtdcMdApi(flat_path)
        self.print(f" API版本: {self._api.GetApiVersion()}")

        self._api.RegisterFront(self._front)
        self._api.RegisterSpi(self)
        self._api.Init()
        self.print(" 初始化完成")

        self.wait_login()

        if not self._is_login:
            # 登录失败时立即释放 API，避免 CTP 在后台持续重连刷屏
            self.print(f"[MdQuoteProvider] 行情登录未成功，释放 API: {self._login_error}")
            self.shutdown()

    def wait_login(self, timeout: int = 15):
        """覆盖父类的阻塞式 wait_login，增加超时保护"""
        for _ in range(timeout):
            if self._is_login:
                return
            time.sleep(1)
        self._login_error = f"行情登录超时（{timeout}秒）"
        self.print(f"[MdQuoteProvider] {self._login_error}")

    def OnFrontConnected(self):
        """行情前置连接成功：发送登录请求"""
        self.print("行情前置连接成功")
        if not getattr(self, "_api", None):
            return
        req = mdapi.CThostFtdcReqUserLoginField()
        req.BrokerID = self._broker_id
        req.UserID = self._user_id
        req.Password = self._password
        req.UserProductInfo = self._user_product_info
        self._check_req(req, self._api.ReqUserLogin(req, 0))

    def OnFrontDisconnected(self, nReason: int):
        """行情前置连接断开"""
        reason_map = {
            0: "网络读失败/连接被对端关闭",
            4097: "网络读失败",
            4098: "网络写失败",
            8193: "读心跳超时",
            8194: "写心跳超时",
            8195: "收到错误包",
        }
        reason_text = reason_map.get(nReason, f"未知原因({nReason})")
        self.print(f"行情前置连接断开: nReason={nReason} {reason_text}")

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        ok = self._check_rsp(pRspInfo, pRspUserLogin, is_last=bIsLast)
        if not ok:
            if pRspInfo:
                self._login_error = f"ErrorID={pRspInfo.ErrorID} {pRspInfo.ErrorMsg}"
                self.print(f"[MdQuoteProvider] 行情登录失败: {self._login_error}")
            self._login_event.set()
            return

        self._trading_day = pRspUserLogin.TradingDay
        self._front_id = pRspUserLogin.FrontID
        self._session_id = pRspUserLogin.SessionID
        self._is_login = True
        self._login_ok = True
        self._login_event.set()
        self.print("[MdQuoteProvider] 行情登录成功")

    def OnRspError(self, pRspInfo, nRequestID, bIsLast):
        """通用错误回报"""
        if pRspInfo:
            self.print(
                f"[MdQuoteProvider] 收到错误回报: "
                f"ErrorID={pRspInfo.ErrorID}, ErrorMsg={pRspInfo.ErrorMsg}, "
                f"nRequestID={nRequestID}"
            )

    def OnRtnDepthMarketData(self, pDepthMarketData: mdapi.CThostFtdcDepthMarketDataField):
        """行情推送：缓存最新买卖价和最新价"""
        if not pDepthMarketData:
            return
        inst = (pDepthMarketData.InstrumentID or "").strip()
        if not inst:
            return
        data = {
            "InstrumentID": inst,
            "LastPrice": pDepthMarketData.LastPrice,
            "BidPrice1": pDepthMarketData.BidPrice1,
            "BidVolume1": pDepthMarketData.BidVolume1,
            "AskPrice1": pDepthMarketData.AskPrice1,
            "AskVolume1": pDepthMarketData.AskVolume1,
            "UpperLimitPrice": pDepthMarketData.UpperLimitPrice,
            "LowerLimitPrice": pDepthMarketData.LowerLimitPrice,
        }
        with self._quotes_lock:
            self._quotes[inst.upper()] = data

    def subscribe(self, instrument_id: str):
        """订阅指定合约行情（幂等）"""
        exact_id = instrument_id.strip()
        key = exact_id.upper()
        with self._quotes_lock:
            if key in self._subscribed:
                return
            self._subscribed.add(key)

        if not self._is_login:
            # 等待登录完成（最多 10 秒）
            self._login_event.wait(timeout=10)
        if not self._is_login:
            self.print(f"[MdQuoteProvider] 行情未登录，无法订阅 {exact_id}")
            return

        try:
            encoded = [exact_id.encode("utf-8")]
            ret = self._api.SubscribeMarketData(encoded, 1)
            self.print(f"[MdQuoteProvider] 订阅 {exact_id} ret={ret}")
        except Exception as e:
            self.print(f"[MdQuoteProvider] 订阅 {exact_id} 异常: {e}")

    def subscribe_many(self, instrument_ids: list):
        """批量订阅多个合约行情（幂等，线程安全）"""
        if not instrument_ids:
            return

        # 过滤未订阅的合约
        to_subscribe = []
        with self._quotes_lock:
            for inst in instrument_ids:
                exact_id = inst.strip()
                key = exact_id.upper()
                if key not in self._subscribed:
                    self._subscribed.add(key)
                    to_subscribe.append(exact_id)

        if not to_subscribe:
            return

        if not self._is_login:
            self._login_event.wait(timeout=10)
        if not self._is_login:
            self.print(f"[MdQuoteProvider] 行情未登录，无法订阅 {to_subscribe}")
            return

        try:
            encoded = [i.encode("utf-8") for i in to_subscribe]
            ret = self._api.SubscribeMarketData(encoded, len(encoded))
            self.print(f"[MdQuoteProvider] 批量订阅 {len(to_subscribe)} 个合约 ret={ret}")
        except Exception as e:
            self.print(f"[MdQuoteProvider] 批量订阅异常: {e}")

    def get_quote(self, instrument_id: str, timeout: float = 3.0, auto_subscribe: bool = True) -> Optional[dict]:
        """获取指定合约的最新行情快照

        Args:
            instrument_id: 合约代码（大小写不敏感，内部统一按 CTP 返回处理）
            timeout: 等待首次 tick 的最大秒数
            auto_subscribe: 是否在获取前自动订阅（批量查询时可设为 False）

        Returns:
            dict 或 None
        """
        if not self._is_login:
            self._login_event.wait(timeout=10)
        if not self._is_login:
            return None

        if auto_subscribe:
            self.subscribe(instrument_id)

        key = instrument_id.strip().upper()
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._quotes_lock:
                data = self._quotes.get(key)
                if data:
                    return dict(data)
            time.sleep(0.1)

        # 超时后再查一次缓存
        with self._quotes_lock:
            data = self._quotes.get(key)
            if data:
                return dict(data)
        return None

    def shutdown(self):
        """释放行情 API"""
        try:
            if hasattr(self, "_api") and self._api:
                self._api.Release()
                self._api = None
        except Exception as e:
            self.print(f"[MdQuoteProvider] shutdown 异常: {e}")
