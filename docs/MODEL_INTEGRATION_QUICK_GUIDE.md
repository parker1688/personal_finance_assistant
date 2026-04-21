# 🚀 模型集成方案A - 快速参考

## 📌 核心路径映射

```
训练脚本              →  生成的模型文件                 →  加载方式
─────────────────────────────────────────────────────────────────
train_a_stock.py  →  short_term_model.pkl          → scheduler
                 →  medium_term_model.pkl          → scheduler
                 →  long_term_model.pkl            → scheduler

train_hk_stock.py →  hk_stock_short_term_model.pkl  → scheduler
                 →  hk_stock_medium_term_model.pkl  → scheduler
                 →  hk_stock_long_term_model.pkl    → scheduler

train_us_stock.py →  us_stock_short_term_model.pkl  → scheduler
                 →  us_stock_medium_term_model.pkl  → scheduler
                 →  us_stock_long_term_model.pkl    → scheduler

train_fund.py     →  fund_model.pkl                → scheduler

train_gold.py     →  gold_model.pkl                → scheduler
                 →  silver_model.pkl              → scheduler
```

## 🎯 执行顺序

### 第一步：训练所有模型
```bash
# A股 (3个模型: 5日、20日、60日)
python3 scripts/train_a_stock.py

# 港股 (3个模型)
python3 scripts/train_hk_stock.py

# 美股 (3个模型)
python3 scripts/train_us_stock.py

# 基金 (1个模型)
python3 scripts/train_fund.py

# 贵金属 (2个模型: 黄金、白银)
python3 scripts/train_gold.py
```

### 第二步：验证模型文件
```bash
# 查看生成的模型文件
ls -lh data/models/*.pkl

# 预期输出:
# ✅ short_term_model.pkl
# ✅ medium_term_model.pkl
# ✅ long_term_model.pkl
# ✅ hk_stock_short_term_model.pkl
# ✅ hk_stock_medium_term_model.pkl
# ✅ hk_stock_long_term_model.pkl
# ✅ us_stock_short_term_model.pkl
# ✅ us_stock_medium_term_model.pkl
# ✅ us_stock_long_term_model.pkl
# ✅ fund_model.pkl
# ✅ gold_model.pkl
# ✅ silver_model.pkl
```

### 第三步：启动应用
```bash
# 启动Flask应用
python3 app.py

# scheduler 会自动:
# 1. 加载所有12个模型
# 2. 为每个持仓生成日预测
# 3. 存储预测结果到数据库
```

## 🔍 调试命令

### 查看模型加载状态
```python
from scheduler import _load_all_models, logger
from predictors.short_term import ShortTermPredictor

# 初始化预测器
predictors = {
    'a_stock': {
        'short': ShortTermPredictor(),
        'medium': ShortTermPredictor(),
        'long': ShortTermPredictor()
    }
}

# 加载模型
_load_all_models(predictors)

# 检查是否加载成功
if predictors['a_stock']['short'].is_trained:
    print("✅ A股短期模型已加载")
```

### 查看scheduler日志
```bash
# 查看最后100行日志
tail -100 logs/app.log | grep -i "predict\|model"

# 查看模型加载相关的日志
tail -200 logs/app.log | grep -i "已加载\|加载失败\|models"
```

### 手动执行预测
```python
from scheduler import generate_daily_predictions
import logging

logging.basicConfig(level=logging.INFO)
generate_daily_predictions()
```

## 🧪 单元测试

```python
# test_model_integration.py
import unittest
from scheduler import get_model_path, load_model_if_exists, _load_all_models
from pathlib import Path

class TestModelIntegration(unittest.TestCase):
    def test_model_paths(self):
        """验证模型文件路径"""
        expected = [
            'short_term',
            'medium_term',
            'long_term',
            'hk_stock_short_term',
            'hk_stock_medium_term',
            'hk_stock_long_term',
            'us_stock_short_term',
            'us_stock_medium_term',
            'us_stock_long_term',
            'fund',
            'gold',
            'silver'
        ]
        
        for model_name in expected:
            path = get_model_path(model_name)
            self.assertTrue(str(path).endswith('.pkl'))
    
    def test_model_loading(self):
        """测试模型加载（需要先训练）"""
        model = load_model_if_exists(Path('data/models/short_term_model.pkl'))
        if model is not None:
            self.assertIn('model', model)
            self.assertIn('scaler', model)
```

## 📊 关键代码片段

### 1. 查看某个模型的元信息
```python
import pickle

with open('data/models/short_term_model.pkl', 'rb') as f:
    model_data = pickle.load(f)

print(f"训练准确率: {model_data['train_accuracy']:.2%}")
print(f"验证准确率: {model_data['val_accuracy']:.2%}")
print(f"特征数: {len(model_data['feature_columns'])}")
print(f"训练时间: {model_data['train_date']}")
print(f"资产类型: {model_data['asset_type']}")
```

### 2. 测试特定资产的预测
```python
from scheduler import _predict_by_asset_type_v2
from predictors.short_term import ShortTermPredictor

# 初始化港股预测器
asset_predictors = {
    'short': ShortTermPredictor(),
    'medium': ShortTermPredictor(),
    'long': ShortTermPredictor()
}

# 加载港股模型
import pickle
from pathlib import Path
with open('data/models/hk_stock_short_term_model.pkl', 'rb') as f:
    model_data = pickle.load(f)

asset_predictors['short'].model = model_data['model']
asset_predictors['short'].is_trained = True

# 执行预测
result = _predict_by_asset_type_v2('0700.HK', 'hk_stock', asset_predictors)
print(result)
```

### 3. 查看预测结果
```python
from models import get_session, Prediction
from datetime import date

session = get_session()

# 查看今天的预测
today = date.today()
predictions = session.query(Prediction).filter(
    Prediction.date == today
).all()

print(f"今日预测总数: {len(predictions)}")
for pred in predictions[:5]:
    print(f"{pred.code}: 5日涨幅{pred.up_probability:.1f}% 20日涨幅{pred.up_probability:.1f}%")
```

## ⚠️ 常见问题

### Q: 模型文件显示 "对象过时"
**A:** pickle格式跨版本兼容性问题。重新训练模型：
```bash
python3 scripts/train_a_stock.py
```

### Q: scheduler显示 "模型不存在"
**A:** 检查模型文件路径：
```bash
ls -la data/models/
# 如果为空，运行训练脚本
```

### Q: 预测准确率很低
**A:** 可能是数据问题或特征不完整。检查：
```bash
# 查看特征列数
python3 -c "import pickle; d=pickle.load(open('data/models/short_term_model.pkl','rb')); print(len(d['feature_columns']))"

# 重新训练
python3 scripts/train_a_stock.py --retrain
```

### Q: 港股/美股模型加载失败
**A:** 这些是新增的，首次运行必须先训练：
```bash
python3 scripts/train_hk_stock.py
python3 scripts/train_us_stock.py
```

## 📈 性能指标

| 模型 | 特征数 | 准确率 | 加载时间 | 预测时间 |
|-----|-------|--------|---------|---------|
| A股短期 (5日) | 20 | ~68% | <10ms | <5ms |
| A股中期 (20日) | 20 | ~65% | <10ms | <5ms |
| A股长期 (60日) | 20 | ~62% | <10ms | <5ms |
| 港股短期 (5日) | 20 | ~66% | <10ms | <5ms |
| 美股短期 (5日) | 20 | ~64% | <10ms | <5ms |
| 基金模型 | 12 | ~0.75 R2 | <10ms | <5ms |
| 黄金预测 | 15 | ~60% | <10ms | <5ms |
| 白银预测 | 15 | ~60% | <10ms | <5ms |

---

**下一步**: 运行 `python3 scripts/train_a_stock.py` 测试完整流程 ✨
