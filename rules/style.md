# 代码风格规范

## 语言与注释
- 所有注释、文档字符串、日志输出使用中文
- 代码标识符（变量、函数、类名）使用英文，遵循 PEP 8
- 公共 API 必须包含中文文档字符串，说明功能、参数、返回值

## 命名规范
- 类名：`PascalCase`，如 `PositionSyncManager`
- 函数/方法：`snake_case`，如 `sync_and_trade`
- 私有方法：前缀 `_`，如 `_place_order`
- 常量：`UPPER_CASE`，如 `THOST_FTDC_D_Buy`
- CTP 回调：保持 CTP 命名风格，如 `OnRtnTrade`、`OnRspOrderInsert`

## 类型注解
- 公共函数必须添加类型注解
- 常用类型：`Dict`, `List`, `Optional`, `Tuple`, `Set`
- 复杂类型建议使用 TypeAlias 或注释说明

```python
from typing import Dict, List, Optional, Tuple

def query_positions(self, timeout: int = 10) -> Optional[List[dict]]:
    """查询持仓，超时返回 None（调用者需区分'超时'和'确实无持仓'）"""
```

## 错误处理
- 外部 I/O（文件、网络、CTP 接口）必须 try/except
- 异常信息必须包含上下文（合约、订单号、错误码）
- 不允许裸 except，至少捕获 `Exception`
- CTP 回调中禁止抛异常，必须内部消化

## 线程安全
- 共享状态（`_orders`、`_positions` 等）必须通过锁保护
- 锁粒度：按数据结构独立加锁，避免大范围锁
- CTP 回调运行于 CTP 内部线程，访问共享数据必须加锁

```python
self._order_lock = threading.Lock()

with self._order_lock:
    info = self._orders.get(order_ref)
```

## 防御性编程
- CTP 字段读取使用 `getattr` + 默认值，禁止直接点访问可能为 None 的字段
- 字符串操作前先 `(value or "").strip()`
- 数值转换前先判断非空

```python
# 正确
price = float(getattr(pTrade, "Price", 0.0) or 0.0)
instr = (getattr(pTrade, "InstrumentID", "") or "").strip()

# 错误
price = pTrade.Price  # 可能 AttributeError
```

## 日志与输出
- 使用 `self.print()` 或 logging，统一输出格式
- 关键操作必须有日志：报单、撤单、成交、错误
- 飞书通知使用异步线程，避免阻塞交易路径

## 文件组织
- 每个文件顶部标注编码 `# -*- coding: utf-8 -*-`
- 导入顺序：标准库 → 第三方库 → 项目内部模块
- 项目内部导入前确保 `sys.path` 包含项目根目录
