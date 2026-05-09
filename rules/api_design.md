# API 设计规范

## CTP 接口封装原则

### 基类职责分离
- `CTdSpiBase`（`base_tdapi.py`）：负责 CTP 连接、登录、心跳、底层回调分发
- `PositionSyncManager`（`trading/`）：负责业务逻辑（持仓同步、委托执行）
- `PositionManagerUI`（`trading/`）：负责可视化展示，不直接操作 CTP

### 回调处理规范
- CTP 回调只负责数据接收和状态更新，禁止在回调中执行业务逻辑
- 行情查询使用 request_id 隔离，支持并发安全

```python
def query_market_data(self, instrument_id: str, timeout=5) -> dict:
    exact_id = self._standardize_contract(instrument_id)
    with self._md_lock:
        self._md_request_id += 1
        req_id = self._md_request_id
        pending = {"event": threading.Event(), "data": None}
        self._md_pending[req_id] = pending
    # ... 发送请求 ...
    pending["event"].wait(timeout=timeout)
    with self._md_lock:
        data = pending.get("data")
        self._md_pending.pop(req_id, None)
    return data if data else {}
```

### 订单生命周期管理
- 所有报单必须记录到 `self._orders`，以 `OrderRef` 为键
- 订单状态流转：`submitted` → `queued`/`no_trade` → `all_traded`/`canceled`/`rejected`
- 成交回报更新 `_orders` 并持久化到 `orders_submitted.json`

```python
self._orders[order_ref] = {
    "event": fill_event,
    "status": "submitted",
    "sys_id": "",
    "instr": exact_id,
    "exchange": exchange_id,
    "volume": volume,
    "direction": direction,
    "offset_flag": offset_flag,
}
```

## 配置管理

### 环境配置分层
- `config/config.py`：基础环境配置（前置地址、账号密码）
- `local_config.py`：本地覆盖配置（保存路径、账号别名）
- 环境优先级：命令行参数 > 环境变量 `CTP_ENV` > 默认 `7x24`

### 运行时配置
- `run_pipeline.py` 顶部集中定义交易时段、检查间隔、开关变量
- 持仓同步参数：手数、超时、CTP 配置、环境名称

## 模块间通信

### 文件接口
- `hold-std.json`：标准持仓，由 `compare_orders.py` 从 CSV 生成
- `signal.json`：待执行委托，由 `compare_orders.py` 从 CSV 差集生成
- `orders_submitted.json`：已提交委托记录，供 UI 读取展示

### 状态持久化
- `.processed_ids.json`：已处理的 signal 报单编号，防止 CSV 差集反复识别
- `.1009_cooldown.json`：1009 拒绝冷却时间戳

## 异常边界
- `query_positions` 超时返回 `None`（区分于空列表 `[]`）
- `query_market_data` 超时返回 `{}`（空字典）
- 外部 HTTP 调用（飞书）必须带超时，失败不阻塞主流程
