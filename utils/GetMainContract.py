# @Project: https://github.com/Jedore/ctp.examples
# @File:    GetMainContract.py
# @Time:    17/02/2026
# @Author:  Assistant
# @Description: 获取所有产品的主力合约

import json
from collections import defaultdict
from ctp.base_tdapi import CTdSpiBase, tdapi


class CTdSpi(CTdSpiBase):
    
    def __init__(self):
        super().__init__()
        self.instruments = []
        self.product_instruments = defaultdict(list)
    
    def req(self):
        """ 请求查询所有合约 """
        
        self.print("请求查询所有合约")
        req = tdapi.CThostFtdcQryInstrumentField()
        # 一个都不填，查询全部合约
        self._check_req(req, self._api.ReqQryInstrument(req, 0))
    
    def OnRspQryInstrument(self, pInstrument: tdapi.CThostFtdcInstrumentField, pRspInfo: tdapi.CThostFtdcRspInfoField,
                           nRequestID: int, bIsLast: bool):
        """ 请求查询合约响应 """
        
        self._check_rsp(pRspInfo, pInstrument, is_last=bIsLast)
        
        # 保存合约信息
        if pInstrument:
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
            
            self.print(f"收到合约信息: {pInstrument.InstrumentID} - {pInstrument.InstrumentName}")
        
        # 如果是最后一个响应，保存到 JSON 文件并计算主力合约
        if bIsLast:
            self.save_instruments_to_json()
            self.calculate_main_contracts()
    
    def save_instruments_to_json(self):
        """ 保存合约信息到 JSON 文件 """
        
        filename = "instruments.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.instruments, f, ensure_ascii=False, indent=2)
        
        self.print(f"已保存 {len(self.instruments)} 个合约信息到 {filename}")
    
    def calculate_main_contracts(self):
        """ 计算每个产品的主力合约 """
        
        main_contracts = []
        
        for product_id, instruments in self.product_instruments.items():
            if not instruments:
                continue
            
            # 筛选正在交易的合约，并且去掉期权（ProductClass != 2）
            trading_instruments = [inst for inst in instruments if inst["IsTrading"] and inst["ProductClass"] != "2"]
            
            if not trading_instruments:
                continue
            
            # 按照以下规则筛选主力合约：
            # 1. 优先选择最近上市的合约
            # 2. 优先选择到期时间较晚的合约
            # 3. 优先选择非交割中的合约
            
            # 按照上市日期排序（最新的在前）
            trading_instruments.sort(key=lambda x: x["OpenDate"], reverse=True)
            
            # 选择第一个作为主力合约
            main_contract = trading_instruments[0]
            
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
            }
            
            main_contracts.append(main_contract_info)
            self.print(f"产品 {product_id} 的主力合约: {main_contract['InstrumentID']} (已过滤期权)")
        
        # 保存主力合约到 JSON 文件
        filename = "main_contracts.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(main_contracts, f, ensure_ascii=False, indent=2)
        
        self.print(f"已保存 {len(main_contracts)} 个产品的主力合约到 {filename}")


if __name__ == '__main__':
    spi = CTdSpi()
    spi.req()

    spi.wait_last()
