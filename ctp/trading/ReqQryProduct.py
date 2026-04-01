# @Project: https://github.com/Jedore/ctp.examples
# @File:    ReqQryProduct.py
# @Time:    21/07/2024 14:42
# @Author:  Jedore
# @Email:   jedorefight@gmail.com
# @Addr:    https://github.com/Jedore

import json
from ctp.base_tdapi import CTdSpiBase, tdapi


class CTdSpi(CTdSpiBase):
    
    def __init__(self):
        super().__init__()
        self.products = []
    
    def req(self):
        """ 请求查询产品
        doc: https://ctpdoc.jedore.top/6.6.9/JYJK/CTHOSTFTDCTRADERSPI/REQQRYPRODUCT/
        """

        self.print("请求查询产品")
        req = tdapi.CThostFtdcQryProductField()
        # 不传则查询所有产品
        # req.ExchangeID = "DCE"
        req.ProductID = "ag"
        # req.ProductClass = "1"
        self._check_req(req, self._api.ReqQryProduct(req, 0))

    def OnRspQryProduct(self, pProduct: tdapi.CThostFtdcProductField, pRspInfo: tdapi.CThostFtdcRspInfoField,
                        nRequestID: int, bIsLast: bool):
        """ 请求查询产品响应 """

        self._check_rsp(pRspInfo, pProduct, is_last=bIsLast)
        
        # 保存产品信息
        if pProduct:
            product_info = {
                "ProductID": pProduct.ProductID if hasattr(pProduct, 'ProductID') and pProduct.ProductID else "",
                "ProductName": pProduct.ProductName if hasattr(pProduct, 'ProductName') and pProduct.ProductName else "",
                "ExchangeID": pProduct.ExchangeID if hasattr(pProduct, 'ExchangeID') and pProduct.ExchangeID else "",
                "ProductClass": pProduct.ProductClass if hasattr(pProduct, 'ProductClass') and pProduct.ProductClass else "",
                "VolumeMultiple": pProduct.VolumeMultiple if hasattr(pProduct, 'VolumeMultiple') and pProduct.VolumeMultiple else 0,
                "PriceTick": pProduct.PriceTick if hasattr(pProduct, 'PriceTick') and pProduct.PriceTick else 0.0,
                "MaxMarketOrderVolume": pProduct.MaxMarketOrderVolume if hasattr(pProduct, 'MaxMarketOrderVolume') and pProduct.MaxMarketOrderVolume else 0,
                "MinMarketOrderVolume": pProduct.MinMarketOrderVolume if hasattr(pProduct, 'MinMarketOrderVolume') and pProduct.MinMarketOrderVolume else 0,
                "MaxLimitOrderVolume": pProduct.MaxLimitOrderVolume if hasattr(pProduct, 'MaxLimitOrderVolume') and pProduct.MaxLimitOrderVolume else 0,
                "MinLimitOrderVolume": pProduct.MinLimitOrderVolume if hasattr(pProduct, 'MinLimitOrderVolume') and pProduct.MinLimitOrderVolume else 0,
                "PositionType": pProduct.PositionType if hasattr(pProduct, 'PositionType') and pProduct.PositionType else "",
                "PositionDateType": pProduct.PositionDateType if hasattr(pProduct, 'PositionDateType') and pProduct.PositionDateType else "",
                "CloseDealType": pProduct.CloseDealType if hasattr(pProduct, 'CloseDealType') and pProduct.CloseDealType else "",
                "TradeCurrencyID": pProduct.TradeCurrencyID if hasattr(pProduct, 'TradeCurrencyID') and pProduct.TradeCurrencyID else "",
                "MortgageFundUseRange": pProduct.MortgageFundUseRange if hasattr(pProduct, 'MortgageFundUseRange') and pProduct.MortgageFundUseRange else "",
                "UnderlyingMultiple": pProduct.UnderlyingMultiple if hasattr(pProduct, 'UnderlyingMultiple') and pProduct.UnderlyingMultiple else 0.0,
                "ExchangeProductID": pProduct.ExchangeProductID if hasattr(pProduct, 'ExchangeProductID') and pProduct.ExchangeProductID else "",
                "OpenLimitControlLevel": pProduct.OpenLimitControlLevel if hasattr(pProduct, 'OpenLimitControlLevel') and pProduct.OpenLimitControlLevel else "",
                "OrderFreqControlLevel": pProduct.OrderFreqControlLevel if hasattr(pProduct, 'OrderFreqControlLevel') and pProduct.OrderFreqControlLevel else "",
            }
            self.products.append(product_info)
            self.print(f"收到产品信息: {pProduct.ProductID} - {pProduct.ProductName}")
        
        # 如果是最后一个响应，保存到 JSON 文件
        if bIsLast:
            self.save_products_to_json()
    
    def save_products_to_json(self):
        """ 保存产品信息到 JSON 文件 """
        
        filename = "products.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.products, f, ensure_ascii=False, indent=2)
        
        self.print(f"已保存 {len(self.products)} 个产品信息到 {filename}")


if __name__ == '__main__':
    spi = CTdSpi()
    spi.req()

    spi.wait_last()
