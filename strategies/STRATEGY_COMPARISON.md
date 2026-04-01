# 策略差异分析报告：backtest_v7.py vs TrendReversalV7LiveStrategy.py

## 概述

- **backtest_v7.py**: 回测策略（2026-03-21 更新）
- **TrendReversalV7LiveStrategy.py**: 实盘策略（2026-03-22 创建）

## 核心差异

### 1. 60 分钟红柱 DIF 拐头入场逻辑

**backtest_v7.py (更完整)**:
```python
# 有 3 个入场路径
# 路径 1: 绿柱堆内 DIF 拐头预检查 → 5 分钟入场
# 路径 2: 红柱堆内 DIF 拐头预检查 → 5 分钟入场
# 路径 3: 60 分钟红柱 DIF 拐头直接入场（有 5 分钟小绿柱过滤）
# 路径 4: 传统逻辑（绿柱堆结束转红 + 5 分钟底部抬升过滤）
```

**TrendReversalV7LiveStrategy.py (缺失)**:
```python
# 只有 2 个入场路径
# 路径 1: 绿柱堆内 DIF 拐头预检查 → 5 分钟入场
# 路径 2: 红柱堆内 DIF 拐头预检查 → 5 分钟入场
# ❌ 缺失路径 3: 60 分钟红柱 DIF 拐头直接入场
# 路径 4: 传统逻辑（但实现不完整）
```

### 2. 5 分钟小绿柱过滤函数

**backtest_v7.py 有**:
```python
def check_5m_green_stack_filter(self, df_5m, idx, green_stacks_5m):
    """5 分钟绿柱堆底部抬升过滤"""
    # 检查当前绿柱堆和前一个绿柱堆的低点抬升
```

**TrendReversalV7LiveStrategy.py 缺失此函数**

### 3. 60 分钟入场检查函数

**backtest_v7.py 有完整的 check_60m_entry**:
```python
def check_60m_entry(self, df_60m, idx, green_stacks_60m):
    """
    检查 60 分钟入场条件
    - 刚结束绿柱堆，当前 MACD 转红
    - 绿柱堆低点抬升（底背离）
    - DIF 二次拐头（可选）
    """
```

**TrendReversalV7LiveStrategy.py 缺失此函数**

### 4. 底背离确认逻辑

**backtest_v7.py**:
```python
# 允许低点持平
if current_green_low == prev_green_low:
    return True, f"60m 底背离确认 (低:{prev_green_low:.2f}→{current_green_low:.2f} 持平)"
```

**TrendReversalV7LiveStrategy.py**:
```python
# 只允许低点抬升
if current_green_low < prev_green_low:
    return False, f"绿柱堆低点未抬升"
# 没有处理持平的情况
```

### 5. 信号队列处理

**backtest_v7.py**:
- 绿柱堆内信号和红柱堆内信号分别管理
- 信号过期清理逻辑完整
- 入场时再次检查底背离条件

**TrendReversalV7LiveStrategy.py**:
- 信号队列管理类似
- 但只在 `_check_strategy_on_60m_complete` 中检查，不在每根 K 线检查

### 6. 传统逻辑入场

**backtest_v7.py**:
```python
# 检查 60 分钟条件（含底背离）- 传统逻辑
cond_60m, reason_60m, curr_low_60m, prev_low_60m = strategy.check_60m_entry(...)

if cond_60m:
    # 检查 5 分钟小绿柱过滤
    cond_filter, reason_filter = strategy.check_5m_green_stack_filter(...)
    
    if cond_filter:
        # 检查 5 分钟入场条件
        cond_5m, reason_5m = strategy.check_5m_entry(...)
```

**TrendReversalV7LiveStrategy.py**:
- 缺失传统逻辑的完整实现

## 影响分析

### 信号数量差异

由于 TrendReversalV7LiveStrategy.py 缺失以下入场路径：
1. **60 分钟红柱 DIF 拐头直接入场**（路径 3）
2. **传统逻辑入场**（路径 4，实现不完整）

这导致实盘策略比回测策略产生的信号**显著减少**。

### 具体差异示例

在 backtest_v7.py 中，以下情况会产生入场信号：

| 场景 | backtest_v7 | LiveStrategy |
|------|-------------|--------------|
| 60m 绿柱堆内 DIF 拐头 + 底背离 + 5m 红柱 | ✅ | ✅ |
| 60m 红柱堆内 DIF 拐头 + 5m 红柱 | ✅ | ✅ |
| 60m 红柱 DIF 拐头 (5>3<4) + 底背离 + 5m 小绿柱 + 5m 红柱 | ✅ | ❌ |
| 60m 绿柱堆结束转红 + 底背离 + 5m 底部抬升 + 5m 红柱 | ✅ | ❌ |
| 60m 底背离低点持平 | ✅ | ❌ |

## 修复建议

### 1. 添加缺失的策略函数

在 TrendReversalV7LiveStrategy.py 中添加：

```python
def check_5m_green_stack_filter(self, df_5m, idx, green_stacks_5m):
    """5 分钟绿柱堆底部抬升过滤"""
    current_stack_id = df_5m[idx][10]
    green_ids = sorted([sid for sid in green_stacks_5m.keys() if sid <= current_stack_id])
    
    if len(green_ids) < 2:
        return False, "绿柱堆数据不足"
    
    current_green_id = green_ids[-1]
    prev_green_id = green_ids[-2]
    
    current_green_low = green_stacks_5m[current_green_id]['low']
    prev_green_low = green_stacks_5m[prev_green_id]['low']
    
    if current_green_low > prev_green_low:
        return True, f"5m 底部抬升 (前低:{prev_green_low:.2f}→当前低:{current_green_low:.2f})"
    
    return False, "5m 底部未抬升"


def check_60m_entry(self, df_60m, idx, green_stacks_60m):
    """检查 60 分钟入场条件（传统逻辑）"""
    if idx < 4:
        return False, "数据不足", None, None
    
    hist_0 = df_60m[idx][8]
    hist_1 = df_60m[idx-1][8]
    
    # 条件 1: MACD 转红
    if hist_0 <= 0:
        return False, "MACD 未转红", None, None
    
    # 条件 2: 刚结束绿柱堆
    if hist_1 >= 0:
        return False, "非刚结束绿柱堆", None, None
    
    # 条件 3: 底背离
    diver_ok, diver_reason, last_green_low, prev_prev_green_low = self.check_60m_divergence(df_60m, idx)
    
    if not diver_ok:
        return False, diver_reason, last_green_low, prev_prev_green_low
    
    return True, diver_reason, last_green_low, prev_prev_green_low
```

### 2. 修复底背离确认逻辑

允许低点持平：

```python
def check_60m_divergence(self, df_60m, idx):
    # ... 现有逻辑 ...
    
    if current_green_low < prev_green_low:
        return False, f"绿柱堆低点未抬升", current_green_low, prev_green_low
    
    # 允许持平
    if current_green_low == prev_green_low:
        return True, f"60m 底背离确认 (低:{prev_green_low:.2f}→{current_green_low:.2f} 持平)", current_green_low, prev_green_low
    
    return True, f"60m 底背离确认 (低:{prev_green_low:.2f}→{current_green_low:.2f})", current_green_low, prev_green_low
```

### 3. 添加 60 分钟红柱 DIF 拐头直接入场逻辑

在 `LiveStrategyEngine._check_5m_entry()` 中添加：

```python
# 如果预检查队列为空，检查 60 分钟红柱 DIF 拐头直接入场
if not all_signals:
    hist_60m = self.df_60m_with_macd[idx_60m][8]
    
    if hist_60m > 0:  # 红柱期间
        dif_turn_red, reason_dif_turn = self.strategy.check_60m_dif_turn_in_red(
            self.df_60m_with_macd, idx_60m
        )
        
        if dif_turn_red:
            # 检查底背离
            diver_ok, diver_reason, curr_low, prev_prev_low = self.strategy.check_60m_divergence(
                self.df_60m_with_macd, idx_60m
            )
            
            if diver_ok:
                # 检查 5 分钟小绿柱过滤
                cond_filter, reason_filter = self.strategy.check_5m_green_stack_filter(
                    self.df_5m_with_macd, idx_5m, self.green_stacks_5m
                )
                
                if cond_filter:
                    # 检查 5 分钟入场条件
                    cond_5m, reason_5m = self.strategy.check_5m_entry(
                        self.df_5m_with_macd, idx_5m, self.green_stacks_5m
                    )
                    
                    if cond_5m:
                        # 生成入场信号
                        # ...
```

### 4. 添加传统逻辑入场

在预检查队列和 60 分钟红柱 DIF 拐头检查之后，添加传统逻辑：

```python
# 传统逻辑：60 分钟绿柱堆结束转红时入场
if not precheck_entry_done:
    hist_60m = self.df_60m_with_macd[idx_60m][8]
    hist_60m_prev = self.df_60m_with_macd[idx_60m-1][8] if idx_60m > 0 else 0
    
    if hist_60m > 0 and hist_60m_prev < 0:  # 刚转红
        diver_ok, diver_reason, curr_low, prev_prev_low = self.strategy.check_60m_divergence(
            self.df_60m_with_macd, idx_60m
        )
        
        if diver_ok:
            # 检查 5 分钟底部抬升过滤
            cond_filter, reason_filter = self.strategy.check_5m_green_stack_filter(
                self.df_5m_with_macd, idx_5m, self.green_stacks_5m
            )
            
            if cond_filter:
                # 检查 5 分钟入场条件
                cond_5m, reason_5m = self.strategy.check_5m_entry(
                    self.df_5m_with_macd, idx_5m, self.green_stacks_5m
                )
                
                if cond_5m:
                    # 生成入场信号
                    # ...
```

## 总结

TrendReversalV7LiveStrategy.py 相比 backtest_v7.py 缺失了以下关键功能：

1. ❌ `check_5m_green_stack_filter()` 函数
2. ❌ `check_60m_entry()` 函数
3. ❌ 60 分钟红柱 DIF 拐头直接入场逻辑
4. ❌ 传统逻辑入场（绿柱堆结束转红）
5. ❌ 底背离低点持平的确认

这些缺失导致实盘策略信号数量大幅减少。建议按照上述修复建议补充完整策略逻辑。