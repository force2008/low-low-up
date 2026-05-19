# Changelog

## 2026-05-18

### 修复：超额持仓计算错误

**问题描述**
- 持仓差异检测到 fu2609 多 2 手（标准 8 vs 实际 10）
- 但同步通知显示"缺额开仓: 0 个, 超额平仓: 0 个"，没有提交平仓委托
- 平仓委托显示"卖 0 手"，数量为 0

**根本原因**
1. **双重扣减**：`pending_close` 在两个地方被扣减
   - 在 `_build_pending_map` 中映射到了持仓方向
   - 在 `effective_actual` 计算时又被扣减了一次
   - 导致有效持仓比实际小 2 手

2. **1009 拒绝循环**：提交平仓委托时没有冷却机制，被 1009 拒绝后立即重试导致反复失败

**修复内容**

1. **[sync.py]** 不再从 `effective_actual` 中扣减 `pending_close`
   ```python
   # 修复前
   pending_close = pending_map.get((contract.upper(), direction, False), 0)
   effective_actual[key] = a_vol + pending_open - pending_close

   # 修复后
   pending_close = 0  # 不再扣减
   effective_actual[key] = a_vol + pending_open - pending_close
   ```

2. **[sync.py]** 修复双重扣减问题：excess_orders 的 volume 已经扣除了 pending_close，不需要再减
   ```python
   # 修复前
   pending_close_vol = self._get_pending_close_volume(contract, pos_dir)
   available = actual_pos - pending_close_vol

   # 修复后
   available = actual_pos  # 直接用实际持仓
   ```

3. **[sync.py]** 添加 1009 冷却机制：30秒内不重复尝试同一合约
   ```python
   # 检查该合约是否之前被 1009 拒绝过（冷却机制）
   current_time = time.time()
   last_rejected = getattr(self, '_last_1009_reject', {}).get(contract.upper(), 0)
   if current_time - last_rejected < 30:  # 30秒内不重复尝试同一合约
       self.print(f"[平] {contract} 30秒内被1009拒绝过，跳过，等待下次同步")
       skip_close[0] += 1
       continue
   ```

4. **[sync.py]** 记录 1009 拒绝时间
   ```python
   # 记录 1009 拒绝时间，用于冷却
   if not hasattr(self, '_last_1009_reject'):
       self._last_1009_reject = {}
   self._last_1009_reject[contract.upper()] = time.time()
   ```

5. **[base.py]** 1009 错误处理改为异步执行
   - 在新线程中异步处理 1009 错误，避免阻塞 CTP 回调
   - 不等待撤单响应，直接发送请求后返回

6. **[order_ops.py]** 添加 `_cancel_order_no_wait()` 方法
   - 用于快速撤单，不等待响应
   - 专门用于 CTP 回调中的异步撤单场景

7. **[sync.py]** 添加平仓调试日志
   ```python
   self.print(f"[平调试] {contract} excess_orders.volume={eo['volume']}, actual_pos={actual_pos}, pending_close_vol={pending_close_vol}, available={available}")
   self.print(f"[平调试] {contract} diff初始值={diff}")
   ```

8. **[sync.py]** 修复 close_vol_submitted 跟踪问题
   - 添加 `close_vol_submitted` 变量跟踪实际提交的平仓数量
   - 避免 `diff` 变量被修改后导致通知显示 0 手的问题

### 相关文件
- `trading/position_sync/sync.py`
- `trading/position_sync/base.py`
- `trading/position_sync/order_ops.py`
