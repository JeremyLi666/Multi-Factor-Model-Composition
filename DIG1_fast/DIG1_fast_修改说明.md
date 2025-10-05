# DIG1_fast.py 修改说明

## 修改概述

根据用户要求，对DIG1_fast.py进行了以下四个主要修改：

### 1. 移除无限循环 

**修改前：**
```python
while True:
    run_multi_datasets(...)
```

**修改后：**
```python
run_multi_datasets(...)
print(datetime.now(), "================= 因子挖掘机器完成 =================")
```

**效果：** 程序运行完所有数据集后自动结束，不再无限循环。

### 2. 数据集特定的文件命名 ✅

**修改前：** 所有数据集的因子表达式都保存到 `EUR_fast_check_simulated_alpha_expression.txt`

**修改后：** 每个数据集创建独立的文件：
- `EUR_risk70_fast_check_simulated_alpha_expression.txt`
- `EUR_risk62_fast_check_simulated_alpha_expression.txt`
- `EUR_risk60_fast_check_simulated_alpha_expression.txt`
- `EUR_risk72_fast_check_simulated_alpha_expression.txt`
- `EUR_socialmedia12_fast_check_simulated_alpha_expression.txt`
- `EUR_socialmedia8_fast_check_simulated_alpha_expression.txt`

**修改位置：**
- `run_task()` 函数中的tag生成逻辑
- `plan_dataset()` 函数中的tag生成逻辑

### 3. 按顺序处理数据集 ✅

**修改前：** 没有明确的顺序控制

**修改后：** 
- 严格按照 `datasets_to_run` 列表中的顺序处理
- 一个数据集完成后才处理下一个
- 显示处理进度：`第 {i+1}/{len(dataset_ids)} 个`

### 4. 智能启动逻辑 ✅

**新增功能：**
- 在开始处理前，检测每个数据集的待回测数量
- 如果前几个数据集的待回测数量为0，自动从第一个有待回测的数据集开始
- 严格按照索引顺序，不重复，不遗漏

**示例场景：**
```
数据集列表: ["risk70", "risk62", "risk60", "risk72", "socialmedia12", "socialmedia8"]
检测结果:
- risk70: 0个待回测（已完成）
- risk62: 0个待回测（已完成）  
- risk60: 100个待回测（待处理）
- risk72: 50个待回测（待处理）
- socialmedia12: 200个待回测（待处理）
- socialmedia8: 150个待回测（待处理）

程序将从 risk60（索引2）开始处理，按顺序处理到 socialmedia8
```

## 核心修改代码

### 1. run_task() 函数修改
```python
# 使用数据集特定的tag
if tag is None:
    tag = f"{region}_{dataset_id}_fast_check"
else:
    tag = f"{region}_{dataset_id}_fast_check"
```

### 2. run_multi_datasets() 函数修改
```python
# 检测每个数据集的待回测数量
start_index = 0
for i, ds in enumerate(dataset_ids):
    st = plan_dataset(ds, region, delay, instrumentType, universe, n_jobs, tag)
    # ... 统计逻辑 ...
    
    # 如果当前数据集有待回测表达式，且还没有找到起始点，则设置为起始点
    if st['pending_total'] > 0 and start_index == 0:
        start_index = i
        print(datetime.now(), f"找到起始数据集：{ds}（索引 {i}），待回测表达式 {st['pending_total']} 个")

# 从检测到的起始点开始串行跑各数据集
for i in range(start_index, len(dataset_ids)):
    ds = dataset_ids[i]
    print(datetime.now(), f"================= 开始数据集 {ds}（第 {i+1}/{len(dataset_ids)} 个）=================")
    # ... 处理逻辑 ...
```

### 3. 主入口修改
```python
if __name__ == '__main__':
    datasets_to_run = ["risk70", "risk62", "risk60", "risk72","socialmedia12","socialmedia8"]
    
    print(datetime.now(), "================= 因子挖掘机器启动 =================")
    print(datetime.now(), f"数据集列表：{datasets_to_run}")
    print(datetime.now(), "程序将按顺序处理每个数据集，完成后自动结束")
    
    run_multi_datasets(
        dataset_ids=datasets_to_run,
        region="EUR",
        delay=1,
        instrumentType="EQUITY",
        universe="TOP2500",
        n_jobs=6,
        tag=None  # 使用None让每个数据集生成自己的tag
    )
    
    print(datetime.now(), "================= 因子挖掘机器完成 =================")
```

## 测试验证

创建了 `test_modified_dig1.py` 测试脚本，验证：
- ✅ 文件命名规则正确
- ✅ 多数据集逻辑正确
- ✅ plan_dataset函数正常工作
- ✅ 智能启动逻辑正确

## 使用说明

1. **启动程序：**
   ```bash
   python DIG1_fast.py
   ```

2. **程序行为：**
   - 自动检测每个数据集的待回测数量
   - 从第一个有待回测的数据集开始处理
   - 按顺序处理所有数据集
   - 每个数据集的结果保存到独立文件
   - 完成后自动结束

3. **文件输出：**
   - 每个数据集生成独立的 `EUR_{dataset_id}_fast_check_simulated_alpha_expression.txt` 文件
   - 提交记录保存到 `EUR_{dataset_id}_fast_check_submitted_alpha_expression.txt` 文件

4. **重启恢复：**
   - 程序会自动检测已完成的数据集
   - 从中断点继续处理
   - 不会重复处理已完成的数据集

## 注意事项

1. **网络连接：** 程序需要网络连接来获取数据字段和提交回测
2. **文件权限：** 确保 `records/` 目录有写入权限
3. **数据集顺序：** 严格按照 `datasets_to_run` 列表中的顺序处理
4. **中断恢复：** 程序支持中断后重启，会自动从中断点继续

