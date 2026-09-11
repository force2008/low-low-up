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

# 将项目根目录加入模块搜索路径，支持从 utils/ 子目录直接运行
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# 输出目录：项目根目录下的 data/contracts
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "contracts")
os.makedirs(OUTPUT_DIR, exist_ok=True)

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
        from config import config
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

        filename = os.path.join(OUTPUT_DIR, "instruments.json")
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
        from config import config
        conf = config.envs["online"] if use_online else config.envs["7x24"]
        super().__init__(conf)
        self.instruments = instruments
        self.instrument_volume = {}
        self.instrument_open_interest = {}
    
    def subscribe_market_data(self, batch_size: int = 100):
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
        
        # 分批订阅（每次最多 batch_size 个合约，默认100）
        for i in range(0, len(instrument_ids), batch_size):
            batch_str = instrument_ids[i:i + batch_size]
            batch_bytes = [inst_id.encode('utf-8') for inst_id in batch_str]
            batch_count = len(batch_bytes)
            self._check_req(batch_str, self._api.SubscribeMarketData(batch_bytes, batch_count))
        
        print_log(f"已订阅 {len(instrument_ids)} 个合约的行情（batch={batch_size}）")
    
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


def calculate_main_contracts(instruments, instrument_volume, instrument_open_interest, top_n: int = 4):
    """ 根据持仓量(优先)+成交量(次优先)计算每个产品的 Top-N 主力合约（main/main2/main3/main4...）
        
        返回:  (main_contracts_list, by_product_dict)
            - main_contracts_list：兼容原格式（每个 Product 一行，字段 MainContractID=Top1），
              并新增 MainContractID2/3/4 / Top1OpenInterest / Top1Volume ... 等字段；
            - by_product_dict：新格式，{ProductID: {"exchange":..., "main":code, "main2":code,...
                                                "main_info":{...}, "top_ranking": [{contract,OI,Vol},...]}}
    """
    from collections import defaultdict
    product_instruments = defaultdict(list)

    # 按产品分组
    for inst in instruments:
        product_id = inst["ProductID"]
        if product_id:
            product_instruments[product_id].append(inst)

    print_log(f"共找到 {len(product_instruments)} 个产品")
    print_log(f"Top-N 输出: 前 {top_n} 个合约（main/main2/.../main{top_n}）")

    main_contracts = []
    by_product = {}
    skipped_products = 0
    zero_oi_products = 0

    for product_id, product_inst_list in product_instruments.items():
        if not product_inst_list:
            continue

        # 筛选正在交易的期货合约（ProductClass = "1"）
        trading_instruments = [inst for inst in product_inst_list if inst["IsTrading"] and inst.get("ProductClass") == "1"]
        if not trading_instruments:
            print_log(f"[跳过] 产品 {product_id}: 没有正在交易的期货合约")
            skipped_products += 1
            continue

        # 附加 OpenInterest/Volume
        enriched = []
        for inst in trading_instruments:
            inst_id = inst["InstrumentID"]
            oi = int(instrument_open_interest.get(inst_id, 0) or 0)
            vol = int(instrument_volume.get(inst_id, 0) or 0)
            cp = inst.copy()
            cp["OpenInterest"] = oi
            cp["Volume"] = vol
            enriched.append(cp)

        # 是否有有效行情数据（至少1个合约 OI>0）
        has_market_data = any(e["OpenInterest"] > 0 for e in enriched)
        if not has_market_data:
            zero_oi_products += 1
            print_log(f"[无行情 OI=0] 产品 {product_id}: 所有合约持仓量为0（非交易时段/行情未推送）。"
                      f"仍按 InstrumentID 字母序取前 {top_n} 作为占位，待交易时段重跑刷新。")

        # 排序规则：
        #   有行情时：OpenInterest(持仓量)降序 -> Volume(成交量)降序 -> InstrumentID
        #   无行情时：InstrumentID 字母序(YYYYMM近月优先)作为兜底占位
        if has_market_data:
            enriched.sort(key=lambda x: (-x["OpenInterest"], -x["Volume"], x["InstrumentID"]))
        else:
            enriched.sort(key=lambda x: (x["InstrumentID"] or ""))

        top_list = enriched[:max(1, top_n)]  # 至少保留 1 个（Top1=main）

        exchange_id = top_list[0]["ExchangeID"]
        volume_multiple = top_list[0]["VolumeMultiple"]
        price_tick = top_list[0]["PriceTick"]
        product_class = top_list[0]["ProductClass"]

        # 1) 构造兼容旧格式的 main_contract_info 行（每个 ProductID 一行，新增 MainContractID2/3/4 字段）
        main_contract_info = {
            "ProductID": product_id,
            "InstrumentName": top_list[0]["InstrumentName"],
            "MainContractID":  top_list[0]["InstrumentID"],           # 旧字段保持兼容，= main
            "ExchangeID": exchange_id,
            "OpenDate": top_list[0]["OpenDate"],
            "ExpireDate": top_list[0]["ExpireDate"],
            "IsTrading": top_list[0]["IsTrading"],
            "VolumeMultiple": volume_multiple,
            "PriceTick": price_tick,
            "ProductClass": product_class,
            "OpenInterest": top_list[0]["OpenInterest"],
            "Volume": top_list[0]["Volume"],
        }
        # 按 top_n 写 MainContractID2/3/4 ...（如果有对应合约）
        for i in range(1, top_n):
            key = f"MainContractID{i+1}"
            if i < len(top_list):
                main_contract_info[key] = top_list[i]["InstrumentID"]
                # 同时写入对应 OI/Vol，方便下游分析
                main_contract_info[f"Top{i+1}OpenInterest"] = top_list[i]["OpenInterest"]
                main_contract_info[f"Top{i+1}Volume"] = top_list[i]["Volume"]
            else:
                main_contract_info[key] = ""
                main_contract_info[f"Top{i+1}OpenInterest"] = 0
                main_contract_info[f"Top{i+1}Volume"] = 0
        main_contracts.append(main_contract_info)

        # 2) 构造 by_product 新格式  {ProductID: {main,main2,main3,main4,exchange,...}}
        ranking = [
            {"contract": e["InstrumentID"],
             "open_interest": e["OpenInterest"],
             "volume": e["Volume"],
             "expire_date": e.get("ExpireDate", "")}
            for e in top_list
        ]
        product_entry = {
            "ProductID": product_id,
            "exchange": exchange_id,
            "volume_multiple": volume_multiple,
            "price_tick": price_tick,
            "product_class": product_class,
            "has_market_data": has_market_data,
        }
        for i in range(top_n):
            k = "main" if i == 0 else f"main{i+1}"
            if i < len(top_list):
                product_entry[k] = top_list[i]["InstrumentID"]
                product_entry[f"{k}_open_interest"] = top_list[i]["OpenInterest"]
                product_entry[f"{k}_volume"] = top_list[i]["Volume"]
            else:
                product_entry[k] = ""
                product_entry[f"{k}_open_interest"] = 0
                product_entry[f"{k}_volume"] = 0
        product_entry["top_ranking"] = ranking
        by_product[product_id] = product_entry

        # 日志：打印 Top1~Top4 的合约/OI
        top_summary = ", ".join(
            f"#{i+1}={top_list[i]['InstrumentID']}(OI={top_list[i]['OpenInterest']},Vol={top_list[i]['Volume']})"
            for i in range(len(top_list))
        )
        print_log(f"[Top{top_n}] {product_id}  {top_summary}")

    print_log(f"有效产品 {len(main_contracts)}；"
              f"无活跃合约跳过 {skipped_products}；"
              f"无行情 OI=0 占位 {zero_oi_products}")

    # 3) 写出两种格式：
    #    a) main_contracts.json = 兼容旧格式 list（每个 ProductID 一行，已扩展 MainContractID2/3/4）
    #    b) main_contracts_by_product.json = 新格式字典 by_product；同时也把 by_product 内嵌写入 a) 文件尾部一个 "meta" 字段？
    #       —— 旧下游读取 list 时会失败，所以我们选择 "分开两个文件"，a) 仍写纯 list 完全兼容。
    list_path = os.path.join(OUTPUT_DIR, "main_contracts.json")
    with open(list_path, 'w', encoding='utf-8') as f:
        json.dump(main_contracts, f, ensure_ascii=False, indent=2)
    print_log(f"[A] 兼容旧格式 (list) 已写入: {list_path}（{len(main_contracts)} 行）")

    by_product_path = os.path.join(OUTPUT_DIR, "main_contracts_by_product.json")
    with open(by_product_path, 'w', encoding='utf-8') as f:
        json.dump(by_product, f, ensure_ascii=False, indent=2)
    print_log(f"[B] 新格式 by_product (dict) 已写入: {by_product_path}（{len(by_product)} 产品）")

    # 额外：写一份人类友好的 CSV
    csv_path = os.path.join(OUTPUT_DIR, "main_contracts_top4.csv")
    import csv as _csv
    header = ["ProductID", "Exchange",
              "main", "main_OI", "main_Vol",
              "main2", "main2_OI", "main2_Vol",
              "main3", "main3_OI", "main3_Vol",
              "main4", "main4_OI", "main4_Vol",
              "HasMarketData"]
    with open(csv_path, 'w', encoding='utf-8-sig', newline='') as cf:
        w = _csv.writer(cf)
        w.writerow(header)
        for pid, p in by_product.items():
            w.writerow([
                pid, p.get("exchange", ""),
                p.get("main", ""),  p.get("main_open_interest", ""),  p.get("main_volume", ""),
                p.get("main2", ""), p.get("main2_open_interest", ""), p.get("main2_volume", ""),
                p.get("main3", ""), p.get("main3_open_interest", ""), p.get("main3_volume", ""),
                p.get("main4", ""), p.get("main4_open_interest", ""), p.get("main4_volume", ""),
                "1" if p.get("has_market_data") else "0",
            ])
    print_log(f"[C] CSV 概览（Excel 直接打开）: {csv_path}")

    return main_contracts, by_product


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description="方式A：拉取全量合约+订阅行情持仓量/成交量，输出每个品种 Top-N (main/main2/...) 主力合约",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
示例：
  # 最常用：周一早盘 09:10 跑，拉真实行情 Top4（默认）
  python utils/GetMainContractWithVolume.py online

  # 非交易时段：用 7x24 环境跑，若 OI=0 则按 InstrumentID 占位，交易时段再重跑刷新
  python utils/GetMainContractWithVolume.py 7x24 --md-wait 120

  # 周级别定时任务：强制重新查询全量合约（不用 instruments.json 缓存）+ Top4
  python utils/GetMainContractWithVolume.py online --refresh-instruments --top-n 4

  # 只取 Top2（main + main2），减少订阅等待
  python utils/GetMainContractWithVolume.py online --top-n 2
        """
    )
    parser.add_argument("env", nargs="?", default="7x24",
                        choices=["online", "7x24", "simu", "test"],
                        help="使用的配置环境（online=实盘/仿真, 7x24=模拟7x24环境）")
    parser.add_argument("--top-n", type=int, default=4,
                        help="每个品种输出的主力合约数 Top-N（默认 4 = main/main2/main3/main4）")
    parser.add_argument("--md-wait", type=int, default=30,
                        help="订阅行情后等待推送的秒数（默认 30；非交易时段建议 60~120）")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="行情订阅分批大小（每批多少合约，默认 100）")
    parser.add_argument("--refresh-instruments", action="store_true",
                        help="忽略 instruments.json 缓存，强制重新查询全量 ReqQryInstrument（建议每周至少一次）")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="覆盖默认输出目录（默认 <项目>/data/contracts）")
    args = parser.parse_args()

    env_name = args.env
    if env_name == "test":
        env_name = "7x24"
    TOP_N = max(1, min(int(args.top_n), 10))
    MD_WAIT_SECS = max(5, int(args.md_wait))
    SUB_BATCH_SIZE = max(10, min(500, int(args.batch_size)))
    REFRESH_INST = bool(args.refresh_instruments)
    if args.output_dir:
        OUTPUT_DIR = args.output_dir
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    print_log("=" * 70)
    print_log(f"参数: env={env_name} | top_n={TOP_N} | md_wait={MD_WAIT_SECS}s | "
              f"batch={SUB_BATCH_SIZE} | refresh_instruments={REFRESH_INST}")
    print_log(f"输出目录: {OUTPUT_DIR}")
    print_log("=" * 70)

    # 输出当前配置状态
    print_log(f"当前配置模式: {env_name}")
    
    # 步骤1：获取合约信息（优先从文件读取）
    print_log("=" * 70)
    print_log("步骤1：获取合约信息")
    print_log("=" * 70)
    
    instruments = []
    instruments_file = os.path.join(OUTPUT_DIR, "instruments.json")

    # 检查 instruments.json 是否存在（除非 --refresh-instruments 强制刷新）
    if (not REFRESH_INST) and os.path.exists(instruments_file):
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

        # 关键：CTdSpi/CMdSpi 构造函数只接受布尔形参 use_online，不接受字符串 env_name
        use_online_conf = (env_name == "online")
        td_spi = CTdSpi(use_online=use_online_conf)
        td_spi_instance = td_spi  # 保存到全局变量（供 atexit cleanup 安全释放）
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
    
    md_spi = CMdSpi(instruments, use_online=use_online_conf)
    md_spi_instance = md_spi  # 保存到全局变量（供 atexit cleanup 安全释放）
    md_spi.subscribe_market_data(batch_size=SUB_BATCH_SIZE)
    
    # 等待行情数据（默认30秒，可通过 --md-wait 调大；非交易时段建议 60~120 秒让更多合约至少推一次）
    print_log(f"\n等待行情数据 {MD_WAIT_SECS} 秒（确保收到足够合约的最新 OI/Volume）...")
    import time
    waited = 0
    while waited < MD_WAIT_SECS:
        time.sleep(min(10, MD_WAIT_SECS - waited))
        waited += min(10, MD_WAIT_SECS - waited)
        vc = len(md_spi.instrument_volume)
        oc = len(md_spi.instrument_open_interest)
        # 统计有有效行情（OI>0 或 Vol>0）的合约数
        ok = 0
        for iid, oi in md_spi.instrument_open_interest.items():
            if oi > 0:
                ok += 1
            elif md_spi.instrument_volume.get(iid, 0) > 0:
                ok += 1
        print_log(f"  进度 {waited}/{MD_WAIT_SECS}s：收到 Volume 合约 {vc}，OI 合约 {oc}；"
                  f"有效数据（OI>0 或 Vol>0）合约 {ok}")
    
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
    print_log(f"步骤3：计算主力合约 Top-{TOP_N}")
    print_log("=" * 70)

    main_contracts, by_product = calculate_main_contracts(
        instruments,
        md_spi.instrument_volume,
        md_spi.instrument_open_interest,
        top_n=TOP_N
    )

    print_log("\n" + "=" * 70)
    print_log(f"完成！共处理 {len(main_contracts)} 个产品（{TOP_N} 级主力）")
    print_log("输出文件：")
    print_log("  1) main_contracts.json             — 旧格式 list（扩展了 MainContractID2/3/4），兼容旧代码")
    print_log("  2) main_contracts_by_product.json — 新格式 dict，按 ProductID 有 main/main2/main3/main4 字段")
    print_log("  3) main_contracts_top4.csv         — CSV（utf-8-sig，Excel 直接打开）")
    print_log("=" * 70)
