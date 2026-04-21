# 🧪 API 接口测试快速指南

**最后更新**: 2026-04-13  
**版本**: v1.0  
**覆盖**: 17个主要API端点 + 8个前端页面

---

## ⚡ 1分钟快速开始

### 启动应用
```bash
# 激活虚拟环境
source .venv/bin/activate

# 启动Flask应用
python3 app.py
# 或使用
python3 -m flask --app app run
```

### 运行API测试
```bash
# 方式1: 测试所有端点
python3 scripts/api_test_suite.py

# 方式2: 仅测试某个模块
python3 scripts/api_test_suite.py --module dashboard

# 方式3: 显示详细信息
python3 scripts/api_test_suite.py --verbose

# 方式4: 测试自定义URL
python3 scripts/api_test_suite.py --url http://192.168.1.100:5000
```

### 查看完整报告
```bash
cat API_TESTING_REPORT.md
# 或使用编辑器
vim API_TESTING_REPORT.md
```

---

## 🎯 8个前端页面的数据来源验证

### 1. 仪表盘 (Dashboard)
**URL**: `http://localhost:5000/`
```
数据流:
  页面加载
    └─ 发起4个API请求
       ├─ /api/dashboard/summary      → 汇总数据
       ├─ /api/market/temperature     → 市场温度
       ├─ /api/warnings/current       → 实时预警
       └─ /api/recommendations/a_stock → 推荐列表
    └─ 合并数据，渲染图表
```

**数据真实性**: ✅ 100% 真实
- 来源: 数据库表 + 实时计算
- 示例: 今日预警数、推荐数、持仓市值

**快速验证**:
```bash
curl http://localhost:5000/api/dashboard/summary | jq .
# 应该返回 code=200, 包含今日预警、推荐、准确率数据
```

---

### 2. 投资推荐 (Recommendations)
**URL**: `http://localhost:5000/recommendations`

| 推荐类型 | 数据真实性 | 来源 | 验证方法 |
|---------|----------|------|---------|
| A股 | ✅ 真实 | recommenders.stock_recommender | 检查是否有模型加载 |
| 港股 | ⚠️ 混合 | 未清晰定义 | `curl /api/recommendations/hk_stock` |
| 美股 | ⚠️ 混合 | 未清晰定义 | `curl /api/recommendations/us_stock` |
| 基金 | ❌ 假数据 | **硬编码** | 检查code是否为真实基金代码 |
| 黄金 | ❌ 假数据 | **硬编码** | 检查code是否为真实ETF |

**关键检查清单**:
```bash
# 检查A股推荐
curl http://localhost:5000/api/recommendations/a_stock | jq '.data[0]'
# 应该有: code, name, score, reason, probability等

# ⚠️ 检查基金推荐 (警惕硬编码)
curl http://localhost:5000/api/recommendations/fund | jq '.data[0]'
# 如果返回的code是 110011, 119069 等, 可能是硬编码

# ❌ 检查黄金推荐 (警惕硬编码)
curl http://localhost:5000/api/recommendations/gold | jq '.data[0]'
# 如果返回的code是 GLD, IAU, XAUUSD 等, 可能是硬编码
```

---

### 3. 风险预警 (Warnings)
**URL**: `http://localhost:5000/warnings`

**数据真实性**: ✅ 100% 真实
- 来源: alerts.monitor.WarningMonitor 实时扫描
- 规则: 技术指标规则引擎（RSI、MACD、均线、布林带等）

**快速验证**:
```bash
# 获取当前预警
curl http://localhost:5000/api/warnings/current | jq '.data'
# 应该返回: [{code, name, type, level, message, suggestion, ...}]

# 检查预警类型
# 期望值: rsi_overbought, rsi_oversold, macd_death_cross, ma_break等
```

**预警等级**: 
- high (高): 立即关注
- medium (中): 密切观察
- low (低): 参考阅读

---

### 4. 复盘分析 (Reviews)
**URL**: `http://localhost:5000/reviews`

**数据真实性**: ✅ 100% 真实
- 来源: reviews.reviewer.Reviewer 自动生成
- 更新频率: 每日凌晨1点

**快速验证**:
```bash
# 获取复盘列表
curl http://localhost:5000/api/reviews/list | jq '.data[0]'
# 应该包含: prediction_id, code, period_days, actual_price, review_score

# 检查准确率统计
curl http://localhost:5000/api/reviews/accuracy | jq '.data'
# 应该返回: direction_accuracy, target_accuracy 等
```

**复盘指标**:
- direction_accuracy: 预测方向准确率
- target_accuracy: 预测目标价准确率
- avg_score: 平均复盘评分

---

### 5. 持仓管理 (Holdings)
**URL**: `http://localhost:5000/holdings`

**数据真实性**: ⚠️ 部分真实
- 持仓列表: ✅ 真实（用户手动维护）
- 当前价格: ⚠️ 混合源
  - A股: ✅ 本地数据库 (daily_basic)
  - 港股/美股: ⚠️ yfinance API (可能延迟)
  - 基金: ❌ 无接口配置

**快速验证**:
```bash
# 获取持仓列表
curl http://localhost:5000/api/holdings | jq '.data[0]'
# 应该包含: code, name, quantity, avg_price, current_price, profit_rate

# 检查价格更新时间
# 通过对比当前价格和市场行情来验证

# 检查持仓趋势
curl http://localhost:5000/api/holdings/trend | jq '.data[-1]'
# 应该返回: [{date, value, daily_change}]
```

**功能检查清单**:
- [ ] 能否新增持仓
- [ ] 能否编辑持仓
- [ ] 能否删除持仓
- [ ] 价格是否实时更新
- [ ] 收益率计算是否正确
- [ ] 趋势图表是否显示正确

---

### 6. 模型监控 (Model Monitor)
**URL**: `http://localhost:5000/model-monitor`

**数据真实性**: ⚠️ 新集成，需验证

```bash
# 检查模型状态
curl http://localhost:5000/api/model/status | jq '.data'

# 应该返回12个模型:
# {
#   "short_term": {status, accuracy, train_date},
#   "medium_term": {status, accuracy, train_date},
#   "long_term": {status, accuracy, train_date},
#   "hk_stock_short_term": {status, accuracy, train_date},
#   ...
# }

# 检查模型准确率
curl http://localhost:5000/api/model/accuracy | jq '.data'
```

**期望看到**:
- ✅ 12个模型已加载
- ✅ 每个模型有训练日期
- ✅ 每个模型有验证准确率

---

### 7. 系统配置 (Config)
**URL**: `http://localhost:5000/config`

**数据真实性**: ✅ 100% 真实

```bash
# 获取配置
curl http://localhost:5000/api/config | jq '.data'

# 应该包含:
# {
#   "push": {email_smtp_server, email_sender, email_receiver, wechat_sckey},
#   "warning": {rsi_overbought, rsi_oversold, ...},
#   "filter": {min_market_cap_a, max_volatility, ...},
#   "model": {n_estimators, max_depth, ...}
# }
```

---

### 8. 系统日志 (Logs)
**URL**: `http://localhost:5000/logs`

**数据真实性**: ✅ 100% 真实

```bash
# 获取日志
curl "http://localhost:5000/api/logs?limit=50&order=desc" | jq '.data[0]'

# 查看最新日志应该包含应用启动信息、API调用记录等
```

---

## 🔴 发现的关键问题

### 问题1: 基金推荐使用硬编码数据 ❌
```python
# 文件: recommenders/fund_recommender.py
def _get_fund_pool(self):
    return [
        {'code': '110011', 'name': '易方达中小盘', ...},  # 硬编码!
        {'code': '519069', 'name': '汇添富价值精选', ...},
        ...
    ]
```

**危害**:
- 用户获得过时的推荐
- 无法发现新的优秀基金
- 推荐数据不可信

**验证方式**:
```bash
# 查询基金推荐
curl http://localhost:5000/api/recommendations/fund | jq '.data'
# 检查code是否为硬编码的几个固定值
```

---

### 问题2: 贵金属推荐使用硬编码数据 ❌
```python
# 文件: recommenders/gold_recommender.py
def _get_gold_pool(self):
    return [
        {'code': 'GLD', 'name': 'SPDR Gold Trust', ...},  # 硬编码!
        ...
    ]
```

**验证方式**:
```bash
curl http://localhost:5000/api/recommendations/gold | jq '.data[0].code'
# 如果返回 GLD, IAU, XAUUSD 等固定值，说明是硬编码
```

---

### 问题3: 持仓价格来源混杂 ⚠️
- A股: 本地数据库 ✅
- 港股: yfinance API ⚠️ (有延迟)
- 美股: yfinance API ⚠️ (有延迟)
- 基金: 无接口 ❌ (过期数据)

**验证方式**:
```bash
# 获取持仓
curl http://localhost:5000/api/holdings | jq '.data'

# 对比实时行情:
# 1. A股: 对比tushare最新价格
# 2. 港股: 对比香港股市最新价格
# 3. 美股: 对比纳斯达克最新价格
# 4. 基金: 检查基金净值是否最新
```

---

## ✅ 测试检查清单

### Phase 1: 接口可达性 (5分钟)
```bash
[ ] 健康检查通过
    curl http://localhost:5000/health
    
[ ] 所有8个页面可访问
    curl http://localhost:5000/
    curl http://localhost:5000/dashboard
    curl http://localhost:5000/recommendations
    ... 等
    
[ ] 运行API测试脚本
    python3 scripts/api_test_suite.py
```

### Phase 2: 数据真实性 (15分钟)
```bash
[ ] 验证仪表盘数据
    curl http://localhost:5000/api/dashboard/summary | jq .
    
[ ] 检查预警数据来源
    curl http://localhost:5000/api/warnings/current | jq '.data | length'
    
[ ] 检查推荐数据来源
    # A股推荐
    curl http://localhost:5000/api/recommendations/a_stock | jq '.data | length'
    # 基金推荐 (⚠️ 检查是否硬编码)
    curl http://localhost:5000/api/recommendations/fund | jq '.data[0]'
    # 黄金推荐 (⚠️ 检查是否硬编码)
    curl http://localhost:5000/api/recommendations/gold | jq '.data[0]'
    
[ ] 检查复盘数据
    curl http://localhost:5000/api/reviews/list | jq '.data | length'
    
[ ] 检查持仓数据和价格
    curl http://localhost:5000/api/holdings | jq '.data[0]'
```

### Phase 3: 模型集成 (10分钟)
```bash
[ ] 检查新模型是否加载
    curl http://localhost:5000/api/model/status | jq '.data'
    
[ ] 验证模型文件存在
    ls -lh data/models/*.pkl
    
[ ] 检查模型准确率
    curl http://localhost:5000/api/model/accuracy | jq '.data'
```

---

## 📊 预期测试结果

```
成功指标:
  ✅ API通过率 >= 95% (17/17 端点正常响应)
  ✅ 数据真实率 >= 80% (基金/黄金/港美股需排除)
  ✅ 响应时间 < 500ms (95%的请求)
  ✅ 新模型集成验证通过
```

---

## 🔧 故障排除

### 问题: 连接被拒绝
```
错误: Connection refused
原因: Flask应用未启动
解决: python3 app.py
```

### 问题: JSON解析错误
```
错误: Invalid JSON response
原因: API返回HTML错误页面
解决: 检查API端点是否正确注册
```

### 问题: 数据为空
```
错误: No data returned
原因1: 数据库为空
解决1: 检查是否有数据导入
原因2: API查询条件过严格
解决2: 查看API参数配置
```

---

## 📝 持续改进建议

### 立即修复 (P1)
- [ ] 替换基金推荐硬编码数据
- [ ] 替换贵金属推荐硬编码数据
- [ ] 完善港股/美股推荐数据来源

### 短期改进 (P2)
- [ ] 统一持仓价格数据来源
- [ ] 添加基金净值接口
- [ ] 完善API文档

### 中期规划 (P3)
- [ ] 实现API版本管理
- [ ] 增加缓存策略
- [ ] 性能监控和告警

---

**最后更新**: 2026-04-13  
**下步行动**: 运行 `python3 scripts/api_test_suite.py` 进行全面测试
