# @Project: https://github.com/Jedore/ctp.examples
# @File:    ArbitrageTrading.py
# @Time:    21/02/2026
# @Author:  Assistant
# @Description: 套利交易：支持多个套利对
#   套利对1: lc2605 - lc2607 < -1000 时开仓，>400 时平仓
#   套利对2: lc2605 - lc2609 < -1500 时开仓，>400 时平仓

import sys
import os
import atexit
import logging
from datetime import datetime
from ctp.base_tdapi import CTdSpiBase, tdapi
from ctp.base_mdapi import CMdSpiBase, mdapi
from config import config


# 配置日志
log_filename = datetime.now().strftime("ArbitrageTrading_%Y%m%d_%H%M%S.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def print_log(*args, **kwargs):
    """ 日志输出函数 """
    message = ' '.join(str(arg) for arg in args)
    logger.info(message)


# 全局变量
td_spi_instance = None
md_spi_instance = None


def cleanup():
    """ 退出时清理资源 """
    global md_spi_instance, td_spi_instance
    
    print_log("\n清理资源...")
    
    if md_spi_instance:
        try:
            md_spi_instance.release()
        except Exception as e:
            print_log(f"清理行情 API 失败: {e}")
    
    if td_spi_instance:
        try:
            td_spi_instance.release()
        except Exception as e:
            print_log(f"清理交易 API 失败: {e}")
    
    print_log("资源清理完成")


# 注册退出处理函数
atexit.register(cleanup)


class ArbitragePair:
    """ 套利对类，管理单个套利对的状态和交易逻辑 """
    
    def __init__(self, name, instrument_a, instrument_b, open_threshold, close_threshold, volume, td_spi):
        self.name = name
        self.instrument_a = instrument_a
        self.instrument_b = instrument_b
        self.open_threshold = open_threshold
        self.close_threshold = close_threshold
        self.volume = volume
        self.td_spi = td_spi
        
        self.price_a = 0.0
        self.price_b = 0.0
        self.has_opened = False
        self.has_closed = False
        self.open_price_a = 0.0
        self.open_price_b = 0.0
        self.continue_after_close = True
    
    def update_price(self, instrument_id, price):
        """ 更新合约价格 """
        if instrument_id == self.instrument_a:
            self.price_a = price
        elif instrument_id == self.instrument_b:
            self.price_b = price
    
    def check_and_execute(self):
        """ 检查并执行交易 """
        if self.price_a > 0 and self.price_b > 0:
            spread = self.price_a - self.price_b
            print_log(f"{self.name} 价差: {self.instrument_a} - {self.instrument_b} = {spread}")
            
            if not self.has_opened:
                if spread < self.open_threshold:
                    print_log(f"{self.name} 触发开仓条件！价差 {spread} < {self.open_threshold}")
                    self.execute_open()
            elif self.has_opened and not self.has_closed:
                if spread > self.close_threshold:
                    print_log(f"{self.name} 触发平仓条件！价差 {spread} > {self.close_threshold}")
                    self.execute_close()
    
    def has_existing_position(self):
        """ 检查是否已有持仓
        
        Returns:
            bool: True表示已有持仓，False表示没有持仓
        """
        has_long_a = self.td_spi.has_position(self.instrument_a, tdapi.THOST_FTDC_D_Buy)
        has_short_b = self.td_spi.has_position(self.instrument_b, tdapi.THOST_FTDC_D_Sell)
        
        if has_long_a or has_short_b:
            print_log(f"{self.name} 检测到已有持仓: {self.instrument_a}多头={has_long_a}, {self.instrument_b}空头={has_short_b}")
            return True
        return False
    
    def execute_open(self):
        """ 执行开仓交易 """
        if self.has_opened:
            print_log(f"{self.name} 已经开仓过，不再重复执行")
            return
        
        if self.has_existing_position():
            print_log(f"{self.name} 已有持仓，跳过开仓")
            return
        
        print_log("=" * 70)
        print_log(f"{self.name} 开始执行开仓交易")
        print_log("=" * 70)
        
        self.open_price_a = self.price_a
        self.open_price_b = self.price_b
        
        self.td_spi.insert_order(
            instrument_id=self.instrument_a,
            direction=tdapi.THOST_FTDC_D_Buy,
            offset_flag=tdapi.THOST_FTDC_OF_Open,
            price=self.price_a,
            volume=self.volume
        )
        
        self.td_spi.insert_order(
            instrument_id=self.instrument_b,
            direction=tdapi.THOST_FTDC_D_Sell,
            offset_flag=tdapi.THOST_FTDC_OF_Open,
            price=self.price_b,
            volume=self.volume
        )
        
        self.has_opened = True
        print_log(f"{self.name} 开仓交易执行完成")
        print_log(f"{self.name} 开仓价差: {self.price_a} - {self.price_b} = {self.price_a - self.price_b}")
        print_log("=" * 70)
    
    def execute_close(self):
        """ 执行平仓交易 """
        if not self.has_opened or self.has_closed:
            print_log(f"{self.name} 未开仓或已平仓，不再执行平仓")
            return
        
        print_log("=" * 70)
        print_log(f"{self.name} 开始执行平仓交易")
        print_log("=" * 70)
        
        self.td_spi.insert_order(
            instrument_id=self.instrument_a,
            direction=tdapi.THOST_FTDC_D_Sell,
            offset_flag=tdapi.THOST_FTDC_OF_Close,
            price=self.price_a,
            volume=self.volume
        )
        
        self.td_spi.insert_order(
            instrument_id=self.instrument_b,
            direction=tdapi.THOST_FTDC_D_Buy,
            offset_flag=tdapi.THOST_FTDC_OF_Close,
            price=self.price_b,
            volume=self.volume
        )
        
        self.has_closed = True
        print_log(f"{self.name} 平仓交易执行完成")
        print_log(f"{self.name} 开仓价差: {self.open_price_a} - {self.open_price_b} = {self.open_price_a - self.open_price_b}")
        print_log(f"{self.name} 平仓价差: {self.price_a} - {self.price_b} = {self.price_a - self.price_b}")
        profit = (self.price_a - self.open_price_a) * self.volume + (self.open_price_b - self.price_b) * self.volume
        print_log(f"{self.name} 套利盈亏: {profit}")
        print_log("=" * 70)
        
        if self.continue_after_close:
            print_log(f"{self.name} 重置标志，继续监控价差...")
            self.has_opened = False
            self.has_closed = False
            self.open_price_a = 0.0
            self.open_price_b = 0.0


class CMdSpi(CMdSpiBase):
    
    def __init__(self, td_spi):
        super().__init__()
        self.td_spi = td_spi
        self.arbitrage_pairs = []
        self._init_arbitrage_pairs()
    
    def _init_arbitrage_pairs(self):
        """ 初始化套利对 """
        self.arbitrage_pairs = [
            ArbitragePair(
                name="套利对1",
                instrument_a='lc2605',
                instrument_b='lc2607',
                open_threshold=-1000,
                close_threshold=200,
                volume=5,
                td_spi=self.td_spi
            ),
            # ArbitragePair(
            #     name="套利对2",
            #     instrument_a='lc2605',
            #     instrument_b='lc2609',
            #     open_threshold=-1500,
            #     close_threshold=400,
            #     volume=5,
            #     td_spi=self.td_spi
            # )
        ]
    
    def subscribe_market_data(self):
        """ 订阅行情数据 """
        instruments = set()
        for pair in self.arbitrage_pairs:
            instruments.add(pair.instrument_a)
            instruments.add(pair.instrument_b)
        
        instruments = sorted(instruments)
        encode_instruments = [inst.encode('utf-8') for inst in instruments]
        
        print_log(f"订阅行情: {', '.join(instruments)}")
        self._check_req(instruments, self._api.SubscribeMarketData(encode_instruments, len(instruments)))
    
    def OnRtnDepthMarketData(self, pDepthMarketData: mdapi.CThostFtdcDepthMarketDataField):
        """ 行情数据推送 """
        if pDepthMarketData:
            instrument_id = pDepthMarketData.InstrumentID if hasattr(pDepthMarketData, 'InstrumentID') else ""
            last_price = pDepthMarketData.LastPrice if hasattr(pDepthMarketData, 'LastPrice') else 0.0
            
            print_log(f"{instrument_id} 最新价: {last_price}")
            
            for pair in self.arbitrage_pairs:
                pair.update_price(instrument_id, last_price)
                pair.check_and_execute()
    
    def release(self):
        """ 释放资源 """
        try:
            self._api.Release()
            print_log("行情 API 已释放")
        except Exception as e:
            print_log(f"释放行情 API 失败: {e}")


class CTdSpi(CTdSpiBase):
    
    def __init__(self, conf=config.envs["7x24"]):
        super().__init__(conf)
        self.order_ref = 0
        self.positions = {}
        self._position_query_done = False
        self._query_investor_position()
        self._wait_position_query()
    
    def _wait_position_query(self):
        """ 等待持仓查询完成 """
        import time
        timeout = 10
        elapsed = 0
        while not self._position_query_done and elapsed < timeout:
            time.sleep(0.5)
            elapsed += 0.5
        if self._position_query_done:
            print_log("持仓查询完成")
        else:
            print_log("警告：持仓查询超时")
    
    def _query_investor_position(self):
        """ 查询投资者持仓 """
        req = tdapi.CThostFtdcQryInvestorPositionField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user_id
        ret = self._api.ReqQryInvestorPosition(req, 0)
        self._check_req(req, ret)
    
    def has_position(self, instrument_id, direction=None):
        """ 检查是否有持仓
        
        Args:
            instrument_id: 合约代码
            direction: 方向 (可选)，None表示任意方向
                      tdapi.THOST_FTDC_D_Buy 表示多头持仓
                      tdapi.THOST_FTDC_D_Sell 表示空头持仓
        
        Returns:
            bool: 是否有持仓
        """
        if instrument_id in self.positions:
            if direction is None:
                return True
            pos = self.positions[instrument_id]
            if direction == tdapi.THOST_FTDC_D_Buy and pos['long_volume'] > 0:
                return True
            if direction == tdapi.THOST_FTDC_D_Sell and pos['short_volume'] > 0:
                return True
        return False
    
    def get_position_volume(self, instrument_id, direction):
        """ 获取持仓数量
        
        Args:
            instrument_id: 合约代码
            direction: 方向
                      tdapi.THOST_FTDC_D_Buy 表示多头持仓
                      tdapi.THOST_FTDC_D_Sell 表示空头持仓
        
        Returns:
            int: 持仓数量
        """
        if instrument_id in self.positions:
            pos = self.positions[instrument_id]
            if direction == tdapi.THOST_FTDC_D_Buy:
                return pos['long_volume']
            elif direction == tdapi.THOST_FTDC_D_Sell:
                return pos['short_volume']
        return 0
    
    def OnRspQryInvestorPosition(self, pInvestorPosition: tdapi.CThostFtdcInvestorPositionField,
                                  pRspInfo: tdapi.CThostFtdcRspInfoField, nRequestID: int, bIsLast: bool):
        """ 投资者持仓查询响应 """
        if pInvestorPosition:
            instrument_id = pInvestorPosition.InstrumentID
            pos_direction = pInvestorPosition.PosiDirection
            volume = pInvestorPosition.Position
            
            if instrument_id not in self.positions:
                self.positions[instrument_id] = {'long_volume': 0, 'short_volume': 0}
            
            if pos_direction == tdapi.THOST_FTDC_PD_Long:
                self.positions[instrument_id]['long_volume'] += volume
            elif pos_direction == tdapi.THOST_FTDC_PD_Short:
                self.positions[instrument_id]['short_volume'] += volume
            
            print_log(f"持仓查询: {instrument_id} 方向={pos_direction} 数量={volume}")
        
        if bIsLast:
            print_log(f"持仓查询完成，当前持仓: {self.positions}")
            self._position_query_done = True
    
    def insert_order(self, instrument_id, direction, offset_flag, price, volume):
        """ 下单 """
        
        self.order_ref += 1
        
        req = tdapi.CThostFtdcInputOrderField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user_id
        req.InstrumentID = instrument_id
        req.Direction = direction
        req.CombOffsetFlag = offset_flag
        req.CombHedgeFlag = tdapi.THOST_FTDC_HF_Speculation
        req.LimitPrice = price
        req.VolumeTotalOriginal = volume
        req.OrderPriceType = tdapi.THOST_FTDC_OPT_LimitPrice
        req.TimeCondition = tdapi.THOST_FTDC_TC_GFD
        req.VolumeCondition = tdapi.THOST_FTDC_VC_AV
        req.ContingentCondition = tdapi.THOST_FTDC_CC_Immediately
        req.ForceCloseReason = tdapi.THOST_FTDC_FCC_NotForceClose
        req.IsAutoSuspend = 0
        req.IsSwapOrder = 0
        req.UserForceClose = 0
        req.OrderRef = str(self.order_ref)
        
        direction_map = {
            tdapi.THOST_FTDC_D_Buy: "买入",
            tdapi.THOST_FTDC_D_Sell: "卖出"
        }
        offset_map = {
            tdapi.THOST_FTDC_OF_Open: "开仓",
            tdapi.THOST_FTDC_OF_Close: "平仓",
            tdapi.THOST_FTDC_OF_CloseToday: "平今",
            tdapi.THOST_FTDC_OF_CloseYesterday: "平昨"
        }
        direction_str = direction_map.get(direction, f"未知({direction})")
        offset_str = offset_map.get(offset_flag, f"未知({offset_flag})")
        print_log(f"下单: {instrument_id} {direction_str} {offset_str} 价格={price} 数量={volume}")
        
        ret = self._api.ReqOrderInsert(req, 0)
        self._check_req(req, ret)
    
    def OnRspOrderInsert(self, pInputOrder: tdapi.CThostFtdcInputOrderField, pRspInfo: tdapi.CThostFtdcRspInfoField,
                         nRequestID: int, bIsLast: bool):
        """ 报单录入请求响应 """
        self._check_rsp(pRspInfo, pInputOrder, bIsLast)
    
    def OnRtnOrder(self, pOrder: tdapi.CThostFtdcOrderField):
        """ 报单通知 """
        print_log(f"报单通知: {pOrder.InstrumentID} OrderRef={pOrder.OrderRef} Status={pOrder.StatusMsg}")
    
    def OnRtnTrade(self, pTrade: tdapi.CThostFtdcTradeField):
        """ 成交通知 """
        print_log(f"成交通知: {pTrade.InstrumentID} OrderRef={pTrade.OrderRef} 价格={pTrade.Price} 数量={pTrade.Volume}")
        
        instrument_id = pTrade.InstrumentID
        offset_flag = pTrade.OffsetFlag
        direction = pTrade.Direction
        volume = pTrade.Volume
        
        if instrument_id not in self.positions:
            self.positions[instrument_id] = {'long_volume': 0, 'short_volume': 0}
        
        if offset_flag == tdapi.THOST_FTDC_OF_Open:
            if direction == tdapi.THOST_FTDC_D_Buy:
                self.positions[instrument_id]['long_volume'] += volume
            elif direction == tdapi.THOST_FTDC_D_Sell:
                self.positions[instrument_id]['short_volume'] += volume
        elif offset_flag in [tdapi.THOST_FTDC_OF_Close, tdapi.THOST_FTDC_OF_CloseToday, tdapi.THOST_FTDC_OF_CloseYesterday]:
            if direction == tdapi.THOST_FTDC_D_Sell:
                self.positions[instrument_id]['long_volume'] = max(0, self.positions[instrument_id]['long_volume'] - volume)
            elif direction == tdapi.THOST_FTDC_D_Buy:
                self.positions[instrument_id]['short_volume'] = max(0, self.positions[instrument_id]['short_volume'] - volume)
        
        print_log(f"更新持仓: {instrument_id} 多头={self.positions[instrument_id]['long_volume']} 空头={self.positions[instrument_id]['short_volume']}")
    
    def OnErrRtnOrderInsert(self, pInputOrder: tdapi.CThostFtdcInputOrderField, pRspInfo: tdapi.CThostFtdcRspInfoField):
        """ 报单录入错误回报 """
        print_log(f"报单录入错误: {pInputOrder.InstrumentID} OrderRef={pInputOrder.OrderRef} ErrorID={pRspInfo.ErrorID} ErrorMsg={pRspInfo.ErrorMsg}")
    
    def release(self):
        """ 释放资源 """
        try:
            self._api.Release()
            print_log("交易 API 已释放")
        except Exception as e:
            print_log(f"释放交易 API 失败: {e}")


if __name__ == '__main__':
    print_log("=" * 70)
    print_log("套利交易程序启动")
    print_log("=" * 70)
    
    # 调试：显示命令行参数
    print_log(f"命令行参数: {sys.argv}")
    
    # 调试：获取环境配置
    env_config = config.get_env_config()
    print_log(f"使用的环境配置: {env_config}")
    print_log(f"交易地址: {env_config.get('td', 'N/A')}")
    print_log(f"行情地址: {env_config.get('md', 'N/A')}")
    print_log(f"用户ID: {env_config.get('user_id', 'N/A')}")
    print_log(f"经纪商ID: {env_config.get('broker_id', 'N/A')}")
    
    print_log("订阅合约: lc2605, lc2607, lc2609")
    print_log("=" * 70)
    print_log("套利对1: lc2605 - lc2607")
    print_log("  开仓条件: lc2605 - lc2607 < -1000")
    print_log("  开仓策略: 买 5 手 lc2605，卖 5 手 lc2607")
    print_log("  平仓条件: lc2605 - lc2607 > 400")
    print_log("  平仓策略: 卖 5 手 lc2605，买 5 手 lc2607")
    print_log("=" * 70)
    print_log("套利对2: lc2605 - lc2609")
    print_log("  开仓条件: lc2605 - lc2609 < -1500")
    print_log("  开仓策略: 买 5 手 lc2605，卖 5 手 lc2609")
    print_log("  平仓条件: lc2605 - lc2609 > 400")
    print_log("  平仓策略: 卖 5 手 lc2605，买 5 手 lc2609")
    print_log("=" * 70)
    
    # 初始化交易 API
    print_log("\n初始化交易 API...")
    td_spi = CTdSpi(conf=env_config)
    td_spi_instance = td_spi
    
    # 初始化行情 API
    print_log("初始化行情 API...")
    md_spi = CMdSpi(td_spi)
    md_spi_instance = md_spi
    
    # 订阅行情
    md_spi.subscribe_market_data()
    
    # 等待程序退出
    print_log("\n程序运行中，按 Ctrl+C 退出...")
    try:
        import time
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print_log("\n收到退出信号，正在关闭程序...")
