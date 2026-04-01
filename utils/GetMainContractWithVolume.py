# @Project: https://github.com/Jedore/ctp.examples
# @File:    GetMainContractWithVolume.py
# @Time:    17/02/2026
# @Author:  Assistant
# @Description: 根据持仓量获取所有产品的主力合约

import json
import sys
import os
import atexit
import logging
from datetime import datetime
from collections import defaultdict
from ctp.base_tdapi import CTdSpiBase, tdapi
from ctp.base_mdapi import CMdSpiBase, mdapi


# 配置日志
log_filename = datetime.now().strftime("GetMainContract_%Y%m%d_%H%M%S.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)  # 同时输出到控制台
    ]
)

logger = logging.getLogger(__name__)


def print_log(*args, **kwargs):
    """ 日志输出函数，替代 print """
    message = ' '.join(str(arg) for arg in args)
    logger.info(message)


# 全局变量，用于存储 API 实例
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


class CTdSpi(CTdSpiBase):
    
    def __init__(self, use_online=False):
        # 根据use_online参数获取配置
        import config
        conf = config.envs["online"] if use_online else config.envs["7x24"]
        super().__init__(conf)
        self.instruments = []
        self.product_instruments = defaultdict(list)
    
    def req(self):
        """ 请求查询所有合约 """
        
        # 重置 _is_last 标志
        self._is_last = False
        
        print_log("请求查询所有合约")
        req = tdapi.CThostFtdcQryInstrumentField()
        self._check_req(req, self._api.ReqQryInstrument(req, 0))
    
    def OnRspQryInstrument(self, pInstrument: tdapi.CThostFtdcInstrumentField, pRspInfo: tdapi.CThostFtdcRspInfoField,
                           nRequestID: int, bIsLast: bool):
        """ 请求查询合约响应 """
        
        self._check_rsp(pRspInfo, pInstrument, is_last=bIsLast)
        
        # 记录 bIsLast 的值
        print_log(f"OnRspQryInstrument: bIsLast={bIsLast}, pInstrument={'有数据' if pInstrument else 'None'}")
        
        # 保存合约信息（只保存期货合约，ProductClass = "1"）
        if pInstrument:
            # 获取 ProductClass 的值
            product_class = pInstrument.ProductClass if hasattr(pInstrument, 'ProductClass') else ""
            instrument_id = pInstrument.InstrumentID if hasattr(pInstrument, 'InstrumentID') else ""
            
            # 打印调试信息
            print_log(f"收到合约: {instrument_id}, ProductClass={product_class}")
            
            # 只保存期货合约（ProductClass = "1"）
            if product_class == "1":
                instrument_info = {
                    "InstrumentID": pInstrument.InstrumentID if hasattr(pInstrument, 'InstrumentID') and pInstrument.InstrumentID else "",
                    "InstrumentName": pInstrument.InstrumentName if hasattr(pInstrument, 'InstrumentName') and pInstrument.InstrumentName else "",
                    "ExchangeID": pInstrument.ExchangeID if hasattr(pInstrument, 'ExchangeID') and pInstrument.ExchangeID else "",
                    "ExchangeInstID": pInstrument.ExchangeInstID if hasattr(pInstrument, 'ExchangeInstID') and pInstrument.ExchangeInstID else "",
                    "ProductID": pInstrument.ProductID if hasattr(pInstrument, 'ProductID') and pInstrument.ProductID else "",
                    "ProductClass": pInstrument.ProductClass if hasattr(pInstrument, 'ProductClass') and pInstrument.ProductClass else "",
                    "VolumeMultiple": pInstrument.VolumeMultiple if hasattr(pInstrument, 'VolumeMultiple') and pInstrument.VolumeMultiple else 0,
                    "PriceTick": pInstrument.PriceTick if hasattr(pInstrument, 'PriceTick') and pInstrument.PriceTick else 0.0,
                    "CreateDate": pInstrument.CreateDate if hasattr(pInstrument, 'CreateDate') and pInstrument.CreateDate else "",
                    "OpenDate": pInstrument.OpenDate if hasattr(pInstrument, 'OpenDate') and pInstrument.OpenDate else "",
                    "ExpireDate": pInstrument.ExpireDate if hasattr(pInstrument, 'ExpireDate') and pInstrument.ExpireDate else "",
                    "StartDelivDate": pInstrument.StartDelivDate if hasattr(pInstrument, 'StartDelivDate') and pInstrument.StartDelivDate else "",
                    "EndDelivDate": pInstrument.EndDelivDate if hasattr(pInstrument, 'EndDelivDate') and pInstrument.EndDelivDate else "",
                    "IsTrading": pInstrument.IsTrading if hasattr(pInstrument, 'IsTrading') and pInstrument.IsTrading else False,
                    "PositionType": pInstrument.PositionType if hasattr(pInstrument, 'PositionType') and pInstrument.PositionType else "",
                    "PositionDateType": pInstrument.PositionDateType if hasattr(pInstrument, 'PositionDateType') and pInstrument.PositionDateType else "",
                    "LongMarginRatio": pInstrument.LongMarginRatio if hasattr(pInstrument, 'LongMarginRatio') and pInstrument.LongMarginRatio else 0.0,
                    "ShortMarginRatio": pInstrument.ShortMarginRatio if hasattr(pInstrument, 'ShortMarginRatio') and pInstrument.ShortMarginRatio else 0.0,
                    "MaxMarginSideAlgorithm": pInstrument.MaxMarginSideAlgorithm if hasattr(pInstrument, 'MaxMarginSideAlgorithm') and pInstrument.MaxMarginSideAlgorithm else "",
                }
                self.instruments.append(instrument_info)
                
                # 按产品分组
                product_id = instrument_info["ProductID"]
                if product_id:
                    self.product_instruments[product_id].append(instrument_info)
                
                print_log(f"保存期货合约: {pInstrument.InstrumentID} - {pInstrument.InstrumentName}")
        
        # 如果是最后一个响应，保存到 JSON 文件
        if bIsLast:
            print_log(f"收到最后一个响应 (bIsLast=True)，开始保存合约信息...")
            self.save_instruments_to_json()
            print_log(f"合约信息保存完成，_is_last={self._is_last}")
    
    def save_instruments_to_json(self):
        """ 保存合约信息到 JSON 文件 """
        
        filename = "instruments.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.instruments, f, ensure_ascii=False, indent=2)
        
        print_log(f"已保存 {len(self.instruments)} 个合约信息到 {filename}")
    
    def release(self):
        """ 释放资源 """
        try:
            self._api.Release()
            print_log("交易 API 已释放")
        except Exception as e:
            print_log(f"释放交易 API 失败: {e}")


class CMdSpi(CMdSpiBase):
    
    def __init__(self, instruments, use_online=False):
        # 根据use_online参数获取配置
        import config
        conf = config.envs["online"] if use_online else config.envs["7x24"]
        super().__init__(conf)
        self.instruments = instruments
        self.instrument_volume = {}
        self.instrument_open_interest = {}
    
    def subscribe_market_data(self):
        """ 订阅行情数据 """
        
        # 筛选正在交易的合约
        trading_instruments = [
            inst for inst in self.instruments 
            if inst["IsTrading"]
        ]
        
        if not trading_instruments:
            print_log("没有找到正在交易的合约")
            return
        
        # 获取合约代码列表
        instrument_ids = [inst["InstrumentID"] for inst in trading_instruments]
        
        # 分批订阅（每次最多订阅100个合约）
        batch_size = 100
        for i in range(0, len(instrument_ids), batch_size):
            batch_str = instrument_ids[i:i + batch_size]
            batch_bytes = [inst_id.encode('utf-8') for inst_id in batch_str]
            batch_count = len(batch_bytes)
            self._check_req(batch_str, self._api.SubscribeMarketData(batch_bytes, batch_count))
        
        print_log(f"已订阅 {len(instrument_ids)} 个合约的行情")
    
    def OnRtnDepthMarketData(self, pDepthMarketData: mdapi.CThostFtdcDepthMarketDataField):
        """ 行情数据推送 """
        
        if pDepthMarketData:
            instrument_id = pDepthMarketData.InstrumentID if hasattr(pDepthMarketData, 'InstrumentID') else ""
            volume = pDepthMarketData.Volume if hasattr(pDepthMarketData, 'Volume') else 0
            open_interest = pDepthMarketData.OpenInterest if hasattr(pDepthMarketData, 'OpenInterest') else 0
            
            if instrument_id:
                self.instrument_volume[instrument_id] = volume
                self.instrument_open_interest[instrument_id] = open_interest
                
                print_log(f"收到行情: {instrument_id} 成交量={volume} 持仓量={open_interest}")
    
    def release(self):
        """ 释放资源 """
        try:
            self._api.Release()
            print_log("行情 API 已释放")
        except Exception as e:
            print_log(f"释放行情 API 失败: {e}")


def calculate_main_contracts(instruments, instrument_volume, instrument_open_interest):
    """ 根据持仓量计算每个产品的主力合约 """
    
    from collections import defaultdict
    product_instruments = defaultdict(list)
    
    # 按产品分组
    for inst in instruments:
        product_id = inst["ProductID"]
        if product_id:
            product_instruments[product_id].append(inst)
    
    print_log(f"共找到 {len(product_instruments)} 个产品")
    
    main_contracts = []
    
    for product_id, instruments in product_instruments.items():
        if not instruments:
            continue
        
        # 筛选正在交易的合约
        trading_instruments = [inst for inst in instruments if inst["IsTrading"]]
        
        if not trading_instruments:
            print_log(f"产品 {product_id} 没有正在交易的合约")
            continue
        
        # 根据持仓量排序（持仓量最大的在前）
        trading_instruments_with_volume = []
        for inst in trading_instruments:
            instrument_id = inst["InstrumentID"]
            open_interest = instrument_open_interest.get(instrument_id, 0)
            volume = instrument_volume.get(instrument_id, 0)
            
            inst_with_volume = inst.copy()
            inst_with_volume["OpenInterest"] = open_interest
            inst_with_volume["Volume"] = volume
            trading_instruments_with_volume.append(inst_with_volume)
        
        # 检查是否有行情数据
        has_market_data = any(inst["OpenInterest"] > 0 for inst in trading_instruments_with_volume)
        
        if not has_market_data:
            print_log(f"产品 {product_id} 的所有合约都没有行情数据（持仓量=0）")
            print_log(f"  可能原因：")
            print_log(f"    1. 不在交易时间")
            print_log(f"    2. 行情服务器连接失败")
            print_log(f"    3. 合约已下市")
            # 如果没有行情数据，选择第一个正在交易的合约
            if trading_instruments_with_volume:
                main_contract = trading_instruments_with_volume[0]
            else:
                continue
        else:
            # 按照持仓量排序（持仓量最大的在前）
            trading_instruments_with_volume.sort(key=lambda x: x["OpenInterest"], reverse=True)
            
            # 选择持仓量最大的作为主力合约
            main_contract = trading_instruments_with_volume[0]
        
        main_contract_info = {
            "ProductID": product_id,
            "InstrumentName": main_contract["InstrumentName"],
            "MainContractID": main_contract["InstrumentID"],
            "ExchangeID": main_contract["ExchangeID"],
            "OpenDate": main_contract["OpenDate"],
            "ExpireDate": main_contract["ExpireDate"],
            "IsTrading": main_contract["IsTrading"],
            "VolumeMultiple": main_contract["VolumeMultiple"],
            "PriceTick": main_contract["PriceTick"],
            "ProductClass": main_contract["ProductClass"],
            "OpenInterest": main_contract["OpenInterest"],
            "Volume": main_contract["Volume"],
        }
        
        main_contracts.append(main_contract_info)
        print_log(f"产品 {product_id} 的主力合约: {main_contract['InstrumentID']} 持仓量={main_contract['OpenInterest']} 成交量={main_contract['Volume']}")
    
    # 保存主力合约到 JSON 文件
    filename = "main_contracts.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(main_contracts, f, ensure_ascii=False, indent=2)
    
    print_log(f"已保存 {len(main_contracts)} 个产品的主力合约到 {filename}")
    
    return main_contracts


if __name__ == '__main__':
    # 解析命令行参数
    use_online = False
    for arg in sys.argv[1:]:
        if arg == "online":
            use_online = True
            print_log("使用线上配置")
        elif arg == "test":
            use_online = False
            print_log("使用测试配置")
    
    # 输出当前配置状态
    print_log(f"当前配置模式: {'线上' if use_online else '测试'}")
    
    # 步骤1：获取合约信息（优先从文件读取）
    print_log("=" * 70)
    print_log("步骤1：获取合约信息")
    print_log("=" * 70)
    
    instruments = []
    instruments_file = "instruments.json"
    
    # 检查 instruments.json 是否存在
    if os.path.exists(instruments_file):
        print_log(f"发现 {instruments_file} 文件，直接读取...")
        try:
            with open(instruments_file, 'r', encoding='utf-8') as f:
                instruments = json.load(f)
            print_log(f"从 {instruments_file} 读取到 {len(instruments)} 个合约")
        except Exception as e:
            print_log(f"读取 {instruments_file} 失败: {e}")
            print_log("将重新查询合约信息...")
            instruments = []
    
    # 如果文件不存在或读取失败，则查询合约信息
    if not instruments:
        print_log(f"{instruments_file} 不存在或读取失败，开始查询合约信息...")
        
        td_spi = CTdSpi(use_online=use_online)
        td_spi_instance = td_spi  # 保存到全局变量
        td_spi.req()
        
        # 等待 CTP API 处理请求，避免 _is_last 标志被之前的响应影响
        print_log("等待 CTP API 处理请求...")
        import time
        time.sleep(5)
        
        # 等待查询完成（不调用 wait_last，避免卡住）
        wait_count = 0
        max_wait = 300  # 最多等待300秒（5分钟）
        
        print_log("开始等待查询完成...")
        print_log(f"初始状态: _is_last={td_spi._is_last}, 已收到 {len(td_spi.instruments)} 个合约")
        
        while not td_spi._is_last:
            time.sleep(1)
            wait_count += 1
            
            # 每10秒输出一次状态
            if wait_count % 10 == 0:
                print_log(f"等待中... 已等待 {wait_count} 秒, _is_last={td_spi._is_last}, 已收到 {len(td_spi.instruments)} 个合约")
            
            # 超时检查
            if wait_count >= max_wait:
                print_log(f"警告：等待超时（{max_wait}秒），_is_last={td_spi._is_last}")
                print_log(f"已收到 {len(td_spi.instruments)} 个合约")
                break
        
        print_log(f"等待结束: _is_last={td_spi._is_last}, 等待时间={wait_count}秒")
        
        instruments = td_spi.instruments
        print_log(f"共查询到 {len(instruments)} 个合约")
        
        # 释放交易 API 资源
        td_spi.release()
        td_spi_instance = None
    
    # 步骤2：使用行情 API 订阅行情数据
    print_log("\n" + "=" * 70)
    print_log("步骤2：订阅行情数据")
    print_log("=" * 70)
    
    md_spi = CMdSpi(instruments, use_online=use_online)
    md_spi_instance = md_spi  # 保存到全局变量
    md_spi.subscribe_market_data()
    
    # 等待行情数据（等待30秒，确保收到所有行情数据）
    print_log("\n等待行情数据...")
    import time
    time.sleep(30)
    
    # 检查收到的行情数据
    print_log(f"\n收到 {len(md_spi.instrument_volume)} 个合约的成交量数据")
    print_log(f"收到 {len(md_spi.instrument_open_interest)} 个合约的持仓量数据")
    
    # 显示前10个有行情数据的合约
    if md_spi.instrument_volume:
        print_log("\n前10个有行情数据的合约:")
        for i, (inst_id, volume) in enumerate(list(md_spi.instrument_volume.items())[:10], 1):
            open_interest = md_spi.instrument_open_interest.get(inst_id, 0)
            print_log(f"  {i}. {inst_id}: 成交量={volume}, 持仓量={open_interest}")
    else:
        print_log("\n警告：没有收到任何行情数据！")
        print_log("可能原因：")
        print_log("  1. 不在交易时间")
        print_log("  2. 等待时间不够")
        print_log("  3. 行情服务器连接失败")
    
    # 等待一段时间，让后台线程完成
    print_log("\n等待后台线程完成...")
    time.sleep(2)
    
    # 步骤3：计算主力合约
    print_log("\n" + "=" * 70)
    print_log("步骤3：计算主力合约")
    print_log("=" * 70)
    
    main_contracts = calculate_main_contracts(
        instruments,
        md_spi.instrument_volume,
        md_spi.instrument_open_interest
    )
    
    print_log("\n" + "=" * 70)
    print_log(f"完成！共找到 {len(main_contracts)} 个主力合约")
    print_log("=" * 70)
