import os
import glob
import csv
import json
import re
import sys
import datetime

# 从本地配置文件导入保存路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from local_config import DEFAULT_SAVE_PATH as DATA_DIR, ACCOUNT
except ImportError:
    DATA_DIR = r"E:\personal files\data"
    ACCOUNT = "wangk0402"

# ==================== 配置区域 ====================
PREFIX = f"{ACCOUNT} 所有委托"

# 飞书机器人 webhook（请替换为实际的 webhook 地址）
FEISHU_WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/6afaaa96-9685-4de8-8136-4de3b7eb4b42"
# 示例: FEISHU_WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/xxxxxx"
# ==================================================

# 已见过的报单编号持久化文件（防止 CSV 被清理后重复识别同一笔委托）
# 格式: {trading_day: [id1, id2, ...]}，按交易日隔离，避免跨天 OrderSysID 复用导致漏单
_SEEN_IDS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.seen_order_ids.json')
_SEEN_IDS_MAX_SIZE_PER_DAY = 2000
_SEEN_IDS_RETENTION_DAYS = 7

# ==================== 活跃委托池（部分成交拆分） ====================
# 用于跟踪尚未完全成交的委托
# 格式: {order_id: {"total_qty": N, "traded_qty": M, "row": {...}}}
_ACTIVE_ORDERS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.active_orders.json')


def _extract_trading_day_from_filename(filename):
    """从 CSV 文件名提取交易日，如 'jm0310 所有委托 2026-5-8 11-22-49.csv' -> '2026-05-08'"""
    m = re.search(r'(\d{4})-(\d{1,2})-(\d{1,2})', os.path.basename(filename))
    if m:
        year, month, day = m.group(1), int(m.group(2)), int(m.group(3))
        return f"{year}-{month:02d}-{day:02d}"
    return datetime.date.today().isoformat()


def _load_seen_ids():
    """加载已见过的报单编号，按交易日分组返回 dict
    兼容旧格式（纯 list）自动迁移
    """
    if os.path.exists(_SEEN_IDS_FILE):
        try:
            with open(_SEEN_IDS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
                if isinstance(data, list):
                    # 旧格式迁移：按当前日期归入 dict
                    return {datetime.date.today().isoformat(): data}
        except Exception:
            pass
    return {}


def _save_seen_ids(seen_data):
    """保存已见过的报单编号，限制单天大小并清理过期数据"""
    if not isinstance(seen_data, dict):
        seen_data = {datetime.date.today().isoformat(): seen_data if isinstance(seen_data, list) else []}

    # 清理超过保留天数的旧数据
    cutoff = (datetime.date.today() - datetime.timedelta(days=_SEEN_IDS_RETENTION_DAYS)).isoformat()
    seen_data = {k: v for k, v in seen_data.items() if k >= cutoff}

    # 限制单天大小
    for day, ids in seen_data.items():
        if len(ids) > _SEEN_IDS_MAX_SIZE_PER_DAY:
            seen_data[day] = ids[-_SEEN_IDS_MAX_SIZE_PER_DAY:]

    try:
        with open(_SEEN_IDS_FILE, 'w', encoding='utf-8') as f:
            json.dump(seen_data, f, ensure_ascii=False)
    except Exception as e:
        print(f"[警告] 保存 seen_ids 失败: {e}")


def find_latest_two_files():
    """按修改时间找出最新的两个文件"""
    pattern = os.path.join(DATA_DIR, f"{PREFIX} *.csv")
    files = glob.glob(pattern)
    if len(files) < 2:
        return None
    files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    return files[0], files[1]


def read_csv_as_dicts(filepath):
    """用 csv.DictReader 读取，返回字典列表"""
    with open(filepath, 'r', encoding='gbk', errors='ignore', newline='') as f:
        reader = csv.DictReader(f)
        return list(reader)


def detect_keys(rows):
    """自动检测时间字段和编号字段"""
    if not rows:
        return None, None
    keys = list(rows[0].keys())

    time_key = None
    id_key = None

    for key in keys:
        if time_key is None and '时间' in key:
            time_key = key
        if id_key is None and '编号' in key:
            id_key = key

    # 如果找不到，打印字段名供用户排查
    if time_key is None or id_key is None:
        print("可用字段:", keys)

    return time_key, id_key


def _time_sort_key(row, time_key):
    """处理期货交易时间排序：夜盘(20:00-05:00)在前，日盘(09:00-15:00)在后"""
    t = row.get(time_key, '')
    if not t:
        return (999, 0, 0)
    try:
        parts = t.split(':')
        h = int(parts[0])
        m = int(parts[1])
        s = int(parts[2]) if len(parts) > 2 else 0
        if 20 <= h <= 23:
            # 晚上夜盘，保持 20-23，排最前
            pass
        elif h < 6:
            # 凌晨夜盘，变为 24-29，排中间
            h += 24
        else:
            # 日盘及盘前盘后(06-19)，变为 30-43，排最后
            h += 24
        return (h, m, s)
    except (ValueError, IndexError):
        return (999, 0, 0)


def _send_feishu(text):
    """发送飞书文本通知的通用函数"""
    if not FEISHU_WEBHOOK_URL:
        print("提示: 未配置 FEISHU_WEBHOOK_URL，跳过飞书通知")
        return
    try:
        import requests
    except ImportError:
        print("缺少 requests 库，请先安装: pip install requests")
        return
    payload = {"msg_type": "text", "content": {"text": text}}
    try:
        resp = requests.post(FEISHU_WEBHOOK_URL, json=payload, timeout=10)
        print(f"飞书通知发送状态: {resp.status_code}")
    except Exception as e:
        print(f"飞书通知发送失败: {e}")


def send_feishu_notification(rows):
    """发送委托更新飞书通知（简要列出前20条）"""
    lines = []
    for row in rows[:20]:
        lines.append(json.dumps(row, ensure_ascii=False))
    text = f"委托数据有更新（共 {len(rows)} 条）:\n" + "\n".join(lines)
    if len(rows) > 20:
        text += f"\n... 等共 {len(rows)} 条"
    _send_feishu(text)


def _should_send_hold_notify_today():
    """判断今天是否已经发送过持仓通知，每天只发一次"""
    flag_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.hold_notify_date')
    today = datetime.date.today().isoformat()
    if os.path.exists(flag_path):
        with open(flag_path, 'r', encoding='utf-8') as f:
            last_date = f.read().strip()
        if last_date == today:
            return False
    with open(flag_path, 'w', encoding='utf-8') as f:
        f.write(today)
    return True


def send_feishu_hold_notification(rows):
    """发送持仓汇总飞书通知（每天只发一次）"""
    if not _should_send_hold_notify_today():
        print("持仓通知今天已发送过，跳过")
        return
    if not rows:
        _send_feishu("当前无持仓")
        return

    lines = ["当前持仓汇总:"]
    total_margin = 0.0
    total_profit = 0.0
    total_volume = 0

    for row in rows:
        contract = row.get("合约", row.get("合约名", ""))
        direction = row.get("买/卖", row.get("多空", ""))
        volume = row.get("手数", "0")
        margin = row.get("占用保证金", "0")
        profit = row.get("持仓盈亏", "0")

        try:
            v = int(str(volume).strip())
        except ValueError:
            v = 0
        try:
            m = float(str(margin).strip())
        except ValueError:
            m = 0.0
        try:
            p = float(str(profit).strip())
        except ValueError:
            p = 0.0

        total_volume += v
        total_margin += m
        total_profit += p

        lines.append(f"{contract} {direction} {v}手  保证金{m:.2f}  盈亏{p:+.2f}")

    lines.append(f"\n汇总: 共{len(rows)}个合约  {total_volume}手  保证金{total_margin:.2f}  盈亏{total_profit:+.2f}")
    _send_feishu("\n".join(lines))


def generate_hold_std():
    """从最新的持仓明细 CSV 生成 hold-std.json（只保留最新一次）"""
    # 兼容不同命名习惯：如 "jm0310当前持仓 *.csv" 或 "jm0310 持仓明细 *.csv"
    patterns = [
        os.path.join(DATA_DIR, f"{ACCOUNT}*持仓*.csv"),
        os.path.join(DATA_DIR, f"{ACCOUNT} 持仓明细 *.csv"),
    ]
    files = []
    for pattern in patterns:
        files.extend(glob.glob(pattern))
    if not files:
        print(f"未找到 '{ACCOUNT}' 持仓明细文件")
        return False

    files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    latest_file = files[0]
    print(f"最新持仓文件: {os.path.basename(latest_file)}")

    rows = read_csv_as_dicts(latest_file)
    if rows is None:
        print("持仓文件读取失败")
        return False

    # 过滤掉表头行、合计行和无效记录（防止CSV中包含多余表头或统计行）
    _INVALID_CONTRACT_NAMES = {
        "合约ID", "合约名", "合约代码", "合约名称",
        "合计", "总计", "汇总", "小计",
        "Total", "TOTAL", "total", "Summary", "SUMMARY", "summary",
    }

    def _is_valid_hold_row(r):
        contract = str(r.get("合约ID") or r.get("合约") or "").strip()
        if not contract or contract in _INVALID_CONTRACT_NAMES:
            return False
        vol = r.get("持仓量") or r.get("手数") or "0"
        try:
            if float(str(vol).strip()) <= 0:
                return False
        except (ValueError, TypeError):
            return False
        return True

    original_count = len(rows)
    rows = [r for r in rows if _is_valid_hold_row(r)]
    filtered = original_count - len(rows)
    if filtered:
        print(f"[过滤] 已剔除 {filtered} 条无效记录（表头/空合约/零持仓）")

    hold_std_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'hold-std.json')
    with open(hold_std_path, 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    print(f"标准持仓文件已写入: {hold_std_path}（共 {len(rows)} 条）")

    return True


def generate_hold():
    """
    占位函数，不再使用
    hold.json 由 PositionSyncManager 在每次同步时从 CTP 查询后更新
    """
    hold_json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'hold.json')
    # 确保文件存在，避免后续代码检查时报错
    if not os.path.exists(hold_json_path):
        with open(hold_json_path, 'w', encoding='utf-8') as f:
            json.dump([], f, ensure_ascii=False, indent=2)
    return True


def compare_hold_diff():
    """
    比较 hold-std.json（标准持仓）与 hold.json（当前持仓）的差异
    生成差异订单并写入 signal.json
    这是委托订单的唯一来源
    """
    hold_std_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'hold-std.json')
    hold_json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'hold.json')

    # 检查文件是否存在
    if not os.path.exists(hold_std_path):
        print("[差异对比] hold-std.json 不存在，跳过")
        return False
    if not os.path.exists(hold_json_path):
        print("[差异对比] hold.json 不存在，首次生成后重试")
        # 首次运行，生成 hold.json
        if not generate_hold():
            return False
        # 再次检查
        if not os.path.exists(hold_json_path):
            return False

    # 读取文件
    with open(hold_std_path, 'r', encoding='utf-8') as f:
        hold_std = json.load(f)
    with open(hold_json_path, 'r', encoding='utf-8') as f:
        hold_json = json.load(f)

    # 解析持仓为 {(合约, 方向): 手数} 格式
    def parse_hold(rows):
        result = {}
        for row in rows:
            contract = row.get("合约ID") or row.get("合约") or row.get("合约代码") or ""
            contract = str(contract).strip().upper()
            if not contract:
                continue

            direction = row.get("买/卖") or row.get("多空") or row.get("方向") or ""
            volume = row.get("持仓量") or row.get("手数") or "0"
            try:
                volume = int(str(volume).strip())
            except ValueError:
                volume = 0

            if not contract or volume == 0:
                continue

            # 确定方向
            if direction in ("买", "多头", "多", "Buy", "BUY", "buy", "B"):
                direction_key = 2  # 多头
            elif direction in ("卖", "空头", "空", "Sell", "SELL", "sell", "S"):
                direction_key = 3  # 空头
            else:
                continue

            key = (contract, direction_key)
            result[key] = result.get(key, 0) + volume
        return result

    std_positions = parse_hold(hold_std)
    actual_positions = parse_hold(hold_json)

    # 计算差异
    all_keys = set(std_positions.keys()) | set(actual_positions.keys())
    diff_orders = []

    for key in all_keys:
        contract, direction = key
        std_vol = std_positions.get(key, 0)
        actual_vol = actual_positions.get(key, 0)

        if std_vol > actual_vol:
            # 缺额，需要开仓
            diff = std_vol - actual_vol
            diff_orders.append({
                "合约": contract,
                "买卖": "买" if direction == 2 else "卖",
                "开平": "开仓",
                "volume": diff,
            })
        elif actual_vol > std_vol:
            # 超额，需要平仓
            diff = actual_vol - std_vol
            diff_orders.append({
                "合约": contract,
                "买卖": "卖" if direction == 2 else "买",
                "开平": "平仓",
                "volume": diff,
            })

    if not diff_orders:
        print("[差异对比] 标准持仓与当前持仓一致，无差异订单")
        return False

    print(f"[差异对比] 发现 {len(diff_orders)} 条差异订单:")
    for order in diff_orders:
        print(f"  {order['合约']} {order['买卖']} {order['volume']}手 ({order['开平']})")

    # 写入 signal.json
    signal_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'signal.json')
    with open(signal_path, 'w', encoding='utf-8') as f:
        json.dump(diff_orders, f, ensure_ascii=False, indent=2)
    print(f"差异订单已写入: {signal_path}")

    return True


def update_hold_json_from_ctp(positions_raw):
    """
    根据 CTP 成交回报更新 hold.json
    positions_raw: CTP 查询返回的原始持仓列表
    """
    if not positions_raw:
        return False

    # 聚合持仓
    aggregated = {}
    for pos in positions_raw:
        contract = pos.get("InstrumentID", "")
        if not contract:
            continue

        # 融航柜台可能用 YdPosition 表示昨仓
        today_vol = int(pos.get("TodayPosition", 0) or 0)
        yd_vol = int(pos.get("YdPosition", 0) or 0)
        total_vol = today_vol + yd_vol

        if total_vol == 0:
            continue

        pos_dir = pos.get("PosiDirection", 0)
        if pos_dir == 2:  # 净持仓为多
            direction = "买"
        elif pos_dir == 3:  # 净持仓为空
            direction = "卖"
        else:
            continue

        key = (contract, direction)
        aggregated[key] = aggregated.get(key, 0) + total_vol

    # 转换为 hold.json 格式
    hold_rows = []
    for (contract, direction), volume in aggregated.items():
        hold_rows.append({
            "合约ID": contract,
            "买/卖": direction,
            "手数": str(volume),
            "来源": "CTP成交回报"
        })

    hold_json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'hold.json')
    with open(hold_json_path, 'w', encoding='utf-8') as f:
        json.dump(hold_rows, f, ensure_ascii=False, indent=2)
    print(f"[更新] hold.json 已从 CTP 成交回报更新（共 {len(hold_rows)} 条）")

    return True


def main():
    # 先生成持仓标准文件
    generate_hold_std()
    print("=" * 60)

    result = find_latest_two_files()
    if result is None:
        print(f"在 {DATA_DIR} 中找不到至少两个 '{PREFIX}' CSV 文件")
        return False

    latest_file, prev_file = result
    print(f"最近文件: {os.path.basename(latest_file)}")
    print(f"对比文件: {os.path.basename(prev_file)}")
    print("-" * 60)

    latest_rows = read_csv_as_dicts(latest_file)
    prev_rows = read_csv_as_dicts(prev_file)

    if not latest_rows or not prev_rows:
        print("文件为空或读取失败")
        return False

    # 检测字段
    time_key, id_key = detect_keys(latest_rows)
    if not time_key or not id_key:
        print(f"未能自动检测字段，请检查 CSV 表头。time_key={time_key}, id_key={id_key}")
        return False

    print(f"排序字段: '{time_key}',  对比字段: '{id_key}'")
    print("-" * 60)

    # 按报单时间排序（兼容夜盘跨天：夜盘在前、日盘在后）
    latest_rows.sort(key=lambda r: _time_sort_key(r, time_key))
    prev_rows.sort(key=lambda r: _time_sort_key(r, time_key))

    # 从最新文件名提取交易日，按交易日隔离 seen_ids（防止跨天 OrderSysID 复用）
    trading_day = _extract_trading_day_from_filename(latest_file)
    print(f"交易日: {trading_day}")

    # 加载已见过的报单编号（按交易日分组）
    seen_data = _load_seen_ids()
    seen_ids = seen_data.get(trading_day, [])
    seen_set = set(seen_ids)

    # 找出新增记录：以报单编号为键，最新文件里有但旧文件里没有且没见过的新委托
    prev_ids = {row.get(id_key) for row in prev_rows}
    new_rows = [row for row in latest_rows if row.get(id_key) not in prev_ids and row.get(id_key) not in seen_set]

    # 更新已见过列表：把最新文件里的所有报单编号都记入当天
    updated = False
    for row in latest_rows:
        oid = row.get(id_key)
        if oid and oid not in seen_set:
            seen_ids.append(oid)
            seen_set.add(oid)
            updated = True
    if updated:
        seen_data[trading_day] = seen_ids
        _save_seen_ids(seen_data)

    if not new_rows:
        print("结果: 委托数据无变化（无新增委托）")
        return False

    # 有变化：只输出新增的委托数据
    print(f"结果: 委托数据有变化（新增 {len(new_rows)} 条）")
    print("\n新增委托数据:")
    for row in new_rows:
        print(json.dumps(row, ensure_ascii=False))

    # 写入信号文件（只保留新增委托）
    signal_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'signal.json')
    with open(signal_path, 'w', encoding='utf-8') as f:
        json.dump(new_rows, f, ensure_ascii=False, indent=2)
    print(f"信号已写入: {signal_path}")

    # 发送飞书通知（只通知新增委托）
    send_feishu_notification(new_rows)
    return True


if __name__ == "__main__":
    main()
