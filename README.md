# 多因子挖掘框架 (Multi-Factor Composition Model)

## 项目简介

这是一个基于WorldQuant平台开发的量化投资研究项目，专注于多因子Alpha模型的自动化构建与优化。项目实现了从数据处理、因子挖掘到模型评估的完整量化投资研究流程，并在WorldQuant平台上进行了部署和测试。

## 技术架构

- **因子挖掘框架**：实现了DIG (Dynamic Indicator Generator) 系列模型，用于自动化因子生成和筛选
- **多区域市场支持**：包含美国(USA)、全球(GLB)、欧洲(EUR)、亚洲(ASI)、中国(CHN)等市场的数据处理模块
- **多资产类别处理**：支持股票市场(EQUITY)和加密货币市场(CRYPTO)的数据结构和处理逻辑
- **多源数据整合**：处理价格量(pv)、基本面(fundamental)、分析师预期(analyst)、社交媒体(socialmedia)、新闻(news)、期权(option)等数据源
- **因子优化模块**：实现了截面中性化、时序稳定性处理、多因子组合等技术方法

## 基本结构

采用模块化设计，各组件功能明确：

```
├── DIG1_fast/                # 因子生成模块
│   ├── DIG1_fast_v1.py       # 第一版因子生成器
│   ├── DIG1_fast_v2.py       # 第二版优化版本
│   └── ...                   # 相关文档
├── Following_Stage/          # 因子处理模块
│   ├── DIG2.py               # 因子筛选与评估
│   ├── DIG3.py               # 因子增强与转换
│   └── DIG4.py               # 多因子组合
├── Model_and_diversified/    # 模型与多样化策略
│   ├── Analyst_data_special_model.py  # 分析师数据处理模型
│   ├── DIG1_enhenced.py      # 增强版DIG1模型
│   └── DIG1model.py          # 基础DIG1模型
├── config.py                 # 配置文件
├── fields.py                 # 字段定义
├── machine_lib.py            # 机器学习基础库
├── machine_lib_v2.py         # 扩展机器学习库
└── records/                  # 模型输出记录
```


### 数据处理优化
- 实现了数据并行处理架构，提高大规模数据处理效率
- 开发了数据缓存机制，减少重复计算
- 实现了增量计算方法，优化因子更新效率

### 风险控制
- 实现了因子暴露度监控，控制对已知风险因子的敞口
- 开发了因子生命周期管理功能，监控因子有效性变化
- 实现了多模型集成方法，降低单一模型风险

## 使用方法

### 环境配置
- Python 3.9+
- 依赖库：numpy, pandas, scipy, statsmodels等

### 基本流程
1. 配置市场参数（config.py）
2. 定义因子搜索空间（fields.py）
3. 运行因子生成：
   ```python
   python DIG1_fast/DIG1_fast_v2.py --region USA --universe TOP3000 --delay 1
   ```
4. 执行因子评估与组合：
   ```python
   python Following_Stage/DIG4.py --input_factors ./records/USA_factors.txt
   ```
5. 分析结果（records目录）

## 理论基础

项目基于以下量化金融理论：

- **多因子模型**: 基于Fama-French框架的多因子模型
- **统计套利**: 利用价格异常进行统计套利
- **机器学习应用**: 机器学习在金融预测中的应用
- **高频数据分析**: 处理高频市场数据的方法

## 后续开发计划

- 集成深度学习方法，探索非线性因子
- 扩展支持更多资产类别
- 开发市场监控与预警功能

- 研究ESG因子整合方法

