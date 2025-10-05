# DIG1_fast.py 记录文件功能说明

## 功能概述

修改后的 `DIG1_fast.py` 现在具备智能记录文件生成功能，在开始回测之前会为每个数据集生成对应的记录文件，并在文件第一行显示待回测因子数量，方便您查看每个数据集的回测情况。

## 核心功能

### 1. 自动生成数据集记录文件

**触发时机：** 在开始回测第一个数据集之前

**生成位置：** `records/` 目录下

**文件命名规则：** `EUR_{dataset_id}_fast_check_simulated_alpha_expression.txt`

**示例文件：**
- `EUR_risk70_fast_check_simulated_alpha_expression.txt`
- `EUR_risk62_fast_check_simulated_alpha_expression.txt`
- `EUR_risk60_fast_check_simulated_alpha_expression.txt`

### 2. 文件头部信息

每个记录文件的第一行包含关键统计信息：

```
# 待回测因子数量: 320
# 数据集ID: risk70
# 官方原始字段: 32（MATRIX: 32, VECTOR: 0）
# 处理后字段: 32
# 生成表达式总数: 320
# 创建时间: 2025-08-31 02:03:55.759388
# ==================================================
```

**信息说明：**
- **待回测因子数量**：该数据集需要回测的因子表达式总数
- **数据集ID**：数据集的唯一标识符
- **官方原始字段**：原始数据字段数量（MATRIX/VECTOR类型分布）
- **处理后字段**：经过处理后的可用字段数量
- **生成表达式总数**：为该数据集生成的所有因子表达式数量
- **创建时间**：文件创建的时间戳

### 3. 智能注释处理

**新增功能：** `read_completed_alphas_with_comments()` 函数

**功能特点：**
- 自动跳过以 `#` 开头的注释行
- 只读取实际的因子表达式
- 避免将统计信息误认为已完成的表达式

## 工作流程

### 1. 预统计阶段
```
开始多数据集预统计...
- 数据集 risk70: 官方原始字段 32（M:32/V:0），待回测表达式 320
- 数据集 risk62: 官方原始字段 32（M:32/V:0），待回测表达式 2330
- 数据集 risk60: 官方原始字段 32（M:32/V:0），待回测表达式 120
合计：官方原始字段 96，待回测表达式 2770
```

### 2. 记录文件生成阶段
```
================= 生成数据集记录文件 =================
 已创建数据集 risk70 的记录文件: records/EUR_risk70_fast_check_simulated_alpha_expression.txt
   - 待回测因子数量: 320
 已创建数据集 risk62 的记录文件: records/EUR_risk62_fast_check_simulated_alpha_expression.txt
   - 待回测因子数量: 2330
 已创建数据集 risk60 的记录文件: records/EUR_risk60_fast_check_simulated_alpha_expression.txt
   - 待回测因子数量: 120
================= 数据集记录文件生成完成 =================
```

### 3. 回测执行阶段
```
从数据集 risk70 开始处理（索引 0）
================= 开始数据集 risk70（第 1/6 个）=================
```

## 优势特点

### 提前了解工作量
- 在开始回测前就能看到每个数据集的待回测数量
- 便于评估总体工作量和时间安排

### 独立文件管理
- 每个数据集有独立的记录文件
- 便于追踪单个数据集的进度
- 避免文件过大导致的性能问题

### 智能注释处理
- 统计信息不会干扰回测逻辑
- 保持文件的可读性和信息完整性

### 进度可视化
- 文件第一行直接显示待回测数量
- 便于快速查看各数据集的回测状态

## 使用示例

### 运行程序
```bash
python DIG1_fast.py
```

### 查看进度
```bash
# 查看risk70数据集的待回测数量
head -1 records/EUR_risk70_fast_check_simulated_alpha_expression.txt

# 查看所有数据集的待回测数量
for file in records/EUR_*_fast_check_simulated_alpha_expression.txt; do
    echo "$(basename $file): $(head -1 $file)"
done
```

### 监控回测进度
```bash
# 实时查看文件大小变化（Windows PowerShell）
Get-ChildItem records/EUR_*_fast_check_simulated_alpha_expression.txt | 
    Select-Object Name, Length, LastWriteTime
```

## 测试验证

运行测试脚本验证功能：
```bash
python test_dataset_records.py
```

**测试结果：**
- 成功生成所有数据集的记录文件
- 文件头部信息格式正确
- 注释处理功能正常
- 待回测数量统计准确

## 注意事项

1. **文件覆盖**：如果记录文件已存在，不会重新创建
2. **编码格式**：所有文件使用UTF-8编码
3. **路径依赖**：确保 `records/` 目录存在
4. **权限要求**：需要写入权限来创建文件

## 总结

这个功能让您能够：
- **提前规划**：在开始回测前了解每个数据集的工作量
- **实时监控**：通过文件第一行快速查看待回测数量
- **智能恢复**：程序重启时能正确识别已完成的工作
- **独立管理**：每个数据集有独立的记录文件

现在您可以更好地管理和监控因子挖掘的进度了！🚀

