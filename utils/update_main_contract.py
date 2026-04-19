# @Project: low-low-up
# @File:    update_main_contract.py
# @Time:    2026/04/19
# @Author:  Assistant
# @Description: 从akshare获取最新主力合约并更新main_contracts.json

import json
import time
import akshare as ak

# 交易所列表
EXCHANGES = ["cffex", "shfe", "czce", "dce", "gfex"]

# 交易所代码到ExchangeID的映射
EXCHANGE_MAP = {
    "cffex": "CFFEX",
    "shfe": "SHFE",
    "czce": "CZCE",
    "dce": "DCE",
    "gfex": "GFEX",
}


def get_main_contracts_from_akshare():
    """从akshare获取所有交易所的主力合约"""
    all_main_contracts = {}

    for exchange in EXCHANGES:
        try:
            main_contract = ak.match_main_contract(symbol=exchange)
            print(f"===================== {EXCHANGE_MAP[exchange].upper()} =====================")
            print(f"主力合约: {main_contract}")
            all_main_contracts[exchange] = main_contract
            time.sleep(5)  # 避免请求过于频繁
        except Exception as e:
            print(f"获取 {exchange} 数据失败: {e}")

    return all_main_contracts


def parse_main_contracts(result_str):
    """解析akshare返回的主力合约字符串为字典"""
    if not result_str or result_str == "无主力合约":
        return {}

    contracts = {}
    # 分割字符串，如 "IF2606,TF2606,IH2606,IC2606,TS2606,IM2606"
    parts = [p.strip() for p in result_str.split(',') if p.strip()]

    for part in parts:
        # 尝试提取产品ID（通常是字母部分，可能1-3个字符）
        # 例如 "IF2606" -> product_id = "IF", contract_id = "IF2606"
        # 例如 "RU2609" -> product_id = "RU", contract_id = "RU2609"
        # 例如 "sc2605" -> product_id = "sc", contract_id = "sc2605"
        for i in range(1, min(4, len(part))):
            product_id = part[:i]
            # 剩余部分是数字合约代码
            contract_id = part
            # 检查数字部分是否都是数字（有效的合约代码）
            if part[i:].isdigit() and len(part[i:]) >= 4:
                contracts[product_id] = contract_id
                break

    return contracts


def update_main_contracts_json(main_contracts_data):
    """更新main_contracts.json文件"""
    input_file = "/home/ubuntu/low-low-up/data/contracts/main_contracts.json"

    # 解析所有交易所的主力合约
    exchange_contracts = {}
    for exchange, result_str in main_contracts_data.items():
        exchange_contracts[exchange] = parse_main_contracts(result_str)

    with open(input_file, 'r', encoding='utf-8') as f:
        contracts = json.load(f)

    updated_count = 0

    for contract in contracts:
        exchange_id = contract.get("ExchangeID", "").lower()

        # 跳过INE交易所（akshare不支持）
        if exchange_id == "ine":
            print(f"跳过 {contract['ProductID']}: INE交易所不支持")
            continue

        # 获取对应交易所的解析后的主力合约字典
        exchange_contract_dict = exchange_contracts.get(exchange_id, {})
        product_id = contract.get("ProductID", "")

        new_main_contract_id = exchange_contract_dict.get(product_id)

        if new_main_contract_id:
            old_main_contract_id = contract.get("MainContractID", "")

            contract["MainContractID"] = new_main_contract_id
            contract["InstrumentName"] = new_main_contract_id

            print(f"更新 {product_id}: {old_main_contract_id} -> {new_main_contract_id}")
            updated_count += 1

    # 保存更新后的数据
    output_file = input_file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(contracts, f, ensure_ascii=False, indent=2)

    print(f"\n已更新 {updated_count} 个主力合约")
    print(f"结果已保存到 {output_file}")

    return updated_count


if __name__ == "__main__":
    print("开始获取主力合约信息...")
    main_contracts_data = get_main_contracts_from_akshare()

    if main_contracts_data:
        print("\n开始更新main_contracts.json...")
        update_main_contracts_json(main_contracts_data)
    else:
        print("未能获取到主力合约数据")