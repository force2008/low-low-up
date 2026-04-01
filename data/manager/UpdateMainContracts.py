# @Project: https://github.com/Jedore/ctp.examples
# @File:    UpdateMainContracts.py
# @Time:    12/03/2026
# @Description: 从 akshare 获取主力合约数据，更新到 main_contracts.json

import json
import os
import time
import logging
import sys
from datetime import datetime
import akshare as ak


# 配置日志
log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(log_dir, datetime.now().strftime("UpdateMainContracts_%Y%m%d_%H%M%S.log"))

# 创建 logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 创建文件 handler
file_handler = logging.FileHandler(log_filename, encoding='utf-8')
file_handler.setLevel(logging.INFO)

# 创建控制台 handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

# 创建 formatter
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# 设置 formatter
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# 添加 handler 到 logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

logger.info(f"日志文件：{log_filename}")
logger.info("日志系统初始化完成")


def print_log(*args, **kwargs):
    """ 日志输出函数 """
    message = ' '.join(str(arg) for arg in args)
    logger.info(message)


# 交易所映射（akshare -> CTP）
EXCHANGE_MAP = {
    'cffex': 'CFFEX',
    'czce': 'CZCE',
    'dce': 'DCE',
    'gfex': 'GFEX',
    'shfe': 'SHFE',
}


def fetch_main_contracts_from_akshare():
    """
    从 akshare 获取各交易所主力合约数据
    
    Returns:
        list: 合约列表
    """
    all_contracts = []
    
    # 交易所列表
    exchanges = ['cffex', 'czce', 'dce', 'gfex', 'shfe']
    
    for exchange_ak in exchanges:
        try:
            print_log(f"正在获取 {exchange_ak} 的主力合约...")
            
            # 获取主力合约数据
            result = ak.match_main_contract(symbol=exchange_ak)
            
            # akshare 返回的是逗号分隔的字符串（如 "IF2603,TF2606,IH2603"）
            contract_symbols = []
            
            if isinstance(result, str):
                # 分割字符串，过滤掉"无主力合约"等信息
                for item in result.split(','):
                    item = item.strip()
                    if item and '无主力合约' not in item:
                        contract_symbols.append(item)
            
            exchange_id = EXCHANGE_MAP.get(exchange_ak, exchange_ak.upper())
            
            print_log(f"  {exchange_ak}: 获取到 {len(contract_symbols)} 个合约")
            
            # 为每个合约构建数据结构
            for symbol in contract_symbols:
                # 解析合约代码
                product_id = ''.join(c for c in symbol if c.isalpha())
                
                # 构建合约数据
                contract = {
                    "ProductID": product_id,
                    "InstrumentName": symbol,
                    "MainContractID": symbol,
                    "ExchangeID": exchange_id,
                    "OpenDate": "",
                    "ExpireDate": "",
                    "IsTrading": 1,
                    "VolumeMultiple": 1,
                    "PriceTick": 1.0,
                    "ProductClass": "1",
                    "OpenInterest": 0.0,
                    "Volume": 0
                }
                
                all_contracts.append(contract)
            
            # 避免限流
            time.sleep(1.0)
            
        except Exception as e:
            print_log(f"  {exchange_ak}: 获取失败 - {e}")
            import traceback
            print_log(traceback.format_exc())
    
    return all_contracts


def load_existing_contracts(json_file):
    """
    加载现有的合约数据（用于保留额外信息）
    
    Args:
        json_file: JSON 文件路径
    
    Returns:
        dict: 以 MainContractID 为键的合约字典
    """
    if not os.path.exists(json_file):
        return {}
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            return {c.get('MainContractID', ''): c for c in data}
        elif isinstance(data, dict) and "main_contracts" in data:
            return {c.get('MainContractID', ''): c for c in data["main_contracts"]}
        else:
            return {}
    except Exception as e:
        print_log(f"读取现有合约失败：{e}")
        return {}


def merge_contracts(new_contracts, existing_contracts):
    """
    合并新旧合约数据，保留现有合约的额外信息
    
    Args:
        new_contracts: 新获取的合约列表
        existing_contracts: 现有合约字典
    
    Returns:
        list: 合并后的合约列表
    """
    merged = []
    
    for new_contract in new_contracts:
        main_id = new_contract.get('MainContractID', '')
        
        if main_id in existing_contracts:
            # 保留现有合约的数据，但更新主力合约 ID
            existing = existing_contracts[main_id].copy()
            # 更新可能变化的字段
            existing['Volume'] = new_contract.get('Volume', 0)
            existing['OpenInterest'] = new_contract.get('OpenInterest', 0)
            merged.append(existing)
        else:
            # 新合约，直接添加
            merged.append(new_contract)
    
    return merged


def save_contracts(contracts, json_file):
    """
    保存合约数据到 JSON 文件
    
    Args:
        contracts: 合约列表
        json_file: JSON 文件路径
    """
    try:
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(contracts, f, indent=2, ensure_ascii=False)
        print_log(f"已保存 {len(contracts)} 个合约到 {json_file}")
        return True
    except Exception as e:
        print_log(f"保存失败：{e}")
        return False


if __name__ == '__main__':
    try:
        print_log("=" * 70)
        print_log("主力合约数据更新程序（使用 akshare API）")
        print_log("=" * 70)
        
        # 加载现有合约数据
        json_file = "main_contracts.json"
        existing_contracts = load_existing_contracts(json_file)
        print_log(f"加载了 {len(existing_contracts)} 个现有合约")
        
        # 从 akshare 获取主力合约
        new_contracts = fetch_main_contracts_from_akshare()
        print_log(f"从 akshare 获取到 {len(new_contracts)} 个主力合约")
        
        if not new_contracts:
            print_log("警告：未获取到任何主力合约数据")
            sys.exit(1)
        
        # 合并数据
        merged_contracts = merge_contracts(new_contracts, existing_contracts)
        print_log(f"合并后共 {len(merged_contracts)} 个合约")
        
        # 按交易所和品种排序
        merged_contracts.sort(key=lambda x: (x.get('ExchangeID', ''), x.get('ProductID', '')))
        
        # 保存
        backup_file = "main_contracts.json.bak"
        if os.path.exists(json_file):
            import shutil
            shutil.copy2(json_file, backup_file)
            print_log(f"已备份原文件到 {backup_file}")
        
        save_contracts(merged_contracts, json_file)
        
        print_log("\n" + "=" * 70)
        print_log("更新完成")
        print_log("=" * 70)
        
    except Exception as e:
        print_log(f"程序运行出错：{e}")
        import traceback
        print_log(traceback.format_exc())