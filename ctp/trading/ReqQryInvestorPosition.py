# @Project: https://github.com/Jedore/ctp.examples
# @File:    ReqQryInvestorPosition.py
# @Time:    21/07/2024 14:42
# @Author:  Jedore
# @Email:   jedorefight@gmail.com
# @Addr:    https://github.com/Jedore

from ctp.base_tdapi import CTdSpiBase, tdapi


class CTdSpi(CTdSpiBase):

    def __init__(self):
        super().__init__()
        self.positions = []  # 存储所有持仓
    
    def req(self):
        """ 请求查询投资者
        doc: https://ctpdoc.jedore.top/6.6.9/JYJK/CTHOSTFTDCTRADERSPI/REQQRYINVESTORPOSITION/
        """

        self.print("请求查询投资者持仓")
        self.positions = []  # 清空持仓列表
        req = tdapi.CThostFtdcQryInvestorPositionField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user_id
        # req.InstrumentID = 'AP410'  # 不填合约查全部
        self._check_req(req, self._api.ReqQryInvestorPosition(req, 0))

    def OnRspQryInvestorPosition(self, pInvestorPosition: tdapi.CThostFtdcInvestorPositionField,
                                 pRspInfo: tdapi.CThostFtdcRspInfoField, nRequestID: int, bIsLast: bool):
        """ 请求查询投资者响应 """
        self._check_rsp(pRspInfo, pInvestorPosition, is_last=bIsLast)
        
        if pInvestorPosition:
            # 只收集持仓信息，不立即输出
            position_info = {
                "InstrumentID": pInvestorPosition.InstrumentID,
                "PosiDirection": int(pInvestorPosition.PosiDirection) if pInvestorPosition.PosiDirection else 0,
                "Position": pInvestorPosition.Position,
                "OpenCost": pInvestorPosition.OpenCost,
            }
            self.positions.append(position_info)
        
        # 如果是最后一个响应，统一输出所有持仓
        if bIsLast:
            self.print_all_positions()
    
    def print_all_positions(self):
        """ 统一输出所有持仓 """
        
        if not self.positions:
            print("当前没有持仓")
            return
        
        print("=" * 70)
        print("持仓汇总")
        print("=" * 70)
        print(f"{'合约代码':<12} {'持仓方向':<8} {'持仓数量':<10} {'开仓价格':<12}")
        print("-" * 70)
        
        direction_map = {2: "多头", 3: "空头"}
        
        for pos in self.positions:
            direction_str = direction_map.get(pos["PosiDirection"], f"未知({pos['PosiDirection']})")
            print(f"{pos['InstrumentID']:<12} {direction_str:<8} {pos['Position']:<10} {pos['OpenCost']:<12.2f}")
        
        print("=" * 70)
        print(f"共 {len(self.positions)} 个持仓")
        print("=" * 70)


if __name__ == '__main__':
    spi = CTdSpi()
    spi.req()

    spi.wait_last()
