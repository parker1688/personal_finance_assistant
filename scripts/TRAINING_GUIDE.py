#!/usr/bin/env python3
"""
📖 分类型模型训练脚本管理指南

本指南说明如何使用按资产类型分开的模型训练脚本
"""

print("""
╔════════════════════════════════════════════════════════════════════════════╗
║                                                                            ║
║             📚 分类型模型训练脚本管理指南                                  ║
║                                                                            ║
╚════════════════════════════════════════════════════════════════════════════╝


📊 现有的训练脚本清单
─────────────────────────────────────────────────────────────────────────────

| # | 脚本文件 | 资产类型 | 功能 | 输出模型 |
|---|---------|---------|------|---------|
| 1 | train_a_stock.py | A股 | 三周期预测(5/20/60日) | short/medium/long_term_model.pkl |
| 2 | train_fund.py | 基金 | 基金选择评分 | fund_model.pkl |
| 3 | train_gold.py | 黄金/白银 | 贵金属预测 | gold_model.pkl / silver_model.pkl |
| 4 | train_etf.py | ETF | ETF择时/强弱预测 | etf_model.pkl |
| 5 | train_hk_stock.py | 港股 | 三周期预测(5/20/60日) | hk_stock_*.pkl |
| 6 | train_us_stock.py | 美股 | 三周期预测(5/20/60日) | us_stock_*.pkl |
| 7 | train_asset_suite.py | 统一编排 | 按资产顺序训练 | 全部模型 |


🚀 快速使用方式
─────────────────────────────────────────────────────────────────────────────

# 1. 训练A股模型
cd /Users/parker/personal_finance_assistant
python3 scripts/train_a_stock.py

# 2. 训练基金模型
python3 scripts/train_fund.py

# 3. 训练黄金/白银模型
python3 scripts/train_gold.py

# 4. 训练ETF模型
python3 scripts/train_etf.py

# 5. 训练港股模型
python3 scripts/train_hk_stock.py

# 6. 训练美股模型
python3 scripts/train_us_stock.py

# 一键按顺序训练所有资产模型（港股/美股最后）
python3 scripts/train_asset_suite.py


📋 每个脚本的特点
─────────────────────────────────────────────────────────────────────────────

🟢 train_a_stock.py (A股)
   • 数据源: data/historical_a_stock.csv
   • 辅助数据: 资金流向、融资融券、北向资金、估值等
   • 训练模型: 短期(5日)、中期(20日)、长期(60日)
   • 输出: data/models/a_stock_short_term_model.pkl 等
   • 特点: 最完整的特征工程，包含基本面和技术面
   • 预测阈值: 2% (大于2%视为上涨)


🟠 train_fund.py (基金)
   • 数据源: data/fund_nav.csv (基金净值)
   • 功能性价: 评分模型，不是分类/预测
   • 输出: data/models/fund_model.pkl
   • 特点: 回归模型，预测30天收益率
   • 评分指标: 夏普比率、最大回撤、波动率等


🟡 train_gold.py (贵金属)
   • 数据源: data/gold_prices.csv 或 data/precious_metals.csv
   • 训练模型: 黄金5日预测 + 白银5日预测
   • 输出: data/models/gold_model.pkl, data/models/silver_model.pkl
   • 特点: 全球24小时交易，无涨跌停
   • 预测阈值: 1% (比股票低，因为波动率低)


🔵 train_hk_stock.py (港股)
   • 数据源: data/historical_hk_stock.csv
   • 训练模型: 短期(5日)、中期(20日)、长期(60日)
   • 输出: data/models/hk_stock_short_term_model.pkl 等
   • 特点: T+0交易，无涨跌停限制
   • 预测阈值: 2% (同A股)


🟣 train_us_stock.py (美股)
   • 数据源: data/historical_us_stock.csv
   • 训练模型: 短期(5日)、中期(20日)、长期(60日)
   • 输出: data/models/us_stock_short_term_model.pkl 等
   • 特点: 强调长期趋势(200日均线)，波动率低
   • 预测阈值: 1.5% (最低，美股波动最小)


⚙️ 配置参数说明
─────────────────────────────────────────────────────────────────────────────

所有股票类训练脚本使用相同的参数配置：

# XGBoost模型参数
├─ n_estimators: 100-150 (树的数量)
├─ max_depth: 5 (树的深度，防止过拟合)
├─ learning_rate: 0.05-0.1 (学习率)
├─ subsample: 0.8 (样本抽样比例)
├─ colsample_bytree: 0.8 (特征抽样比例)
└─ reg_lambda: 1.0-1.5 (L2正则化)

# 训练参数
├─ 特征标准化: StandardScaler
├─ 训练测试分割: 80% / 20%
├─ 样本步长: 每5个交易日生成一个样本
└─ 回溯期: 60天 (用于特征工程)


📊 模型输出说明
─────────────────────────────────────────────────────────────────────────────

每个模型文件 (*.pkl) 包含以下信息：

{
    'model': XGBoost/RandomForest模型对象,
    'scaler': StandardScaler标准化对象,
    'feature_columns': 特征列名列表,
    'train_accuracy': 训练集准确率,
    'val_accuracy': 验证集准确率,
    'train_date': 训练时间,
    'period_days': 预测周期,
    'asset_type': 资产类型 ('a_stock'/'fund'/'gold'/'hk_stock'/'us_stock')
}

# 加载模型示例
import pickle

with open('data/models/a_stock_short_term_model.pkl', 'rb') as f:
    model_data = pickle.load(f)
    
model = model_data['model']
scaler = model_data['scaler']
val_accuracy = model_data['val_accuracy']


🔄 工作流程建议
─────────────────────────────────────────────────────────────────────────────

周一: 数据更新 & 全量训练
  python3 scripts/collect_historical_data.py  # 采集最新数据
  python3 scripts/train_a_stock.py
  python3 scripts/train_hk_stock.py
  python3 scripts/train_us_stock.py

周中: 特定资产训练 (如需要)
  # 黄金表现异常? 重新训练黄金模型
  python3 scripts/train_gold.py
  
  # 基金大幅调整? 重新评分
  python3 scripts/train_fund.py

周末: 模型评估
  # 查看 data/models/ 目录中各模型的 val_accuracy
  # 如果准确率下降超过5%, 考虑调整参数或增加数据


💾 模型文件存储位置
─────────────────────────────────────────────────────────────────────────────

data/models/
├─ a_stock_short_term_model.pkl   (A股5日)
├─ a_stock_medium_term_model.pkl  (A股20日)
├─ a_stock_long_term_model.pkl    (A股60日)
├─ fund_model.pkl                  (基金评分)
├─ gold_model.pkl                  (黄金5日)
├─ silver_model.pkl                (白银5日)
├─ hk_stock_short_term_model.pkl   (港股5日)
├─ hk_stock_medium_term_model.pkl  (港股20日)
├─ hk_stock_long_term_model.pkl    (港股60日)
├─ us_stock_short_term_model.pkl   (美股5日)
├─ us_stock_medium_term_model.pkl  (美股20日)
└─ us_stock_long_term_model.pkl    (美股60日)

总共: 14 个模型文件


🎯 预测准确率期望值
─────────────────────────────────────────────────────────────────────────────

以下是基于类似系统的经验值 (你的实际值可能不同):

| 资产类型 | 5日预测 | 20日预测 | 60日预测 | 说明 |
|---------|--------|---------|---------|------|
| A股 | 50-55% | 52-58% | 54-60% | 最复杂，数据最多 |
| 基金 | R²: 0.3-0.5 | N/A | N/A | 回归模型，R²更重要 |
| 黄金 | 48-52% | N/A | N/A | 全球交易，相性独立 |
| 港股 | 49-54% | 51-57% | 53-59% | T+0，波动大 |
| 美股 | 50-55% | 52-58% | 54-60% | 长期趋势明显 |

注: 50%准确率 = 随机猜测，55%已经具有可用价值


⚠️ 常见问题
─────────────────────────────────────────────────────────────────────────────

Q1: 训练时间太长？
A: 这是正常的。可以减少循环中的样本步长从5改为10，这样会快2倍但样本数减半。

Q2: 准确率很低 (50%)？ 
A: 检查数据质量:
   - 数据行数是否足够 (至少需要60天历史)
   - 是否有缺失值或异常值
   - 标签生成的阈值是否太严格

Q3: 模型训练成功但加载时失败？
A: 检查pickle版本问题，可以尝试：
   import pickle
   with open('model_file.pkl', 'rb') as f:
       data = pickle.load(f, encoding='latin1')

Q4: 如何使用这些模型做预测？
A: 参考 recommenders/ 中的推荐引擎代码，那里已经有了模型加载和预测的示例。


📝 改进建议
─────────────────────────────────────────────────────────────────────────────

未来可以考虑以下改进:

1. 参数优化
   - 使用网格搜索或贝叶斯优化调整XGBoost参数
   - 针对不同资产类型使用不同的参数

2. 特征工程增强
   - 添加股债收益差、风险溢价等宏观指标
   - 使用深度学习提取更复杂的特征

3. 模型集成
   - 使用多个模型的预测取平均 (Ensemble)
   - 根据市场状况动态选择模型权重

4. 实时更新
   - 使用在线学习动态调整模型
   - 每天增量训练而不是全量重训

5. 风险管理
   - 根据模型预测的置信度调整头寸大小
   - 添加止损逻辑


🔗 相关模块
─────────────────────────────────────────────────────────────────────────────

使用这些模型的其他模块:

• recommenders/ - 调用模型进行推荐
  ├─ stock_recommender.py (A股推荐）
  ├─ fund_recommender.py (基金推荐)
  ├─ gold_recommender.py (贵金属推荐)
  └─ ...

• predictors/ - 模型框架
  ├─ short_term.py (5日预测）
  ├─ medium_term.py (20日预测)
  ├─ long_term.py (60日预测)
  └─ ...

• api/ - Web接口
  └─ recommendations.py - 提供预测API


📌 备注
─────────────────────────────────────────────────────────────────────────────

• 所有脚本都支持XGBoost和RandomForest两种模型
  如果未安装XGBoost，会自动降级到RandomForest

• 模型保存使用pickle格式, Python 3.6+都支持

• 建议定期备份 data/models/ 目录

• 模型准确率会随选股周期波动，这是正常的

════════════════════════════════════════════════════════════════════════════

现在已准备好！开始训练你的模型吧！🚀

""")

if __name__ == '__main__':
    pass
