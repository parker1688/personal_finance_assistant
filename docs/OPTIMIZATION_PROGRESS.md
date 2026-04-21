# 代码优化执行指南

**状态**: 进行中 ✅  
**更新时间**: 2026-04-12

---

## 已完成的优化 ✅

### 1. **资源泄漏修复** [app.py]
- ✅ 添加 `contextmanager` - session_scope()
- ✅ 修复 health_check() 和 ready_check() 中的资源泄漏
- ✅ 确保所有session在finally块中关闭

```python
# 改进前 ❌
session = get_session()
session.execute('SELECT 1')
session.close()  # 如果异常则不执行

# 改进后 ✅
with session_scope() as session:
    session.execute('SELECT 1')
```

### 2. **安全配置改进** [app.py]
- ✅ 修改CORS策略：从 `'*'` 改为白名单
- ✅ 添加Origin验证
- ✅ 配置Credentials支持

```python
# 改进前 ❌
response.headers['Access-Control-Allow-Origin'] = '*'

# 改进后 ✅
ALLOWED_ORIGINS = os.environ.get('ALLOWED_ORIGINS', '...').split(',')
if origin in ALLOWED_ORIGINS:
    response.headers['Access-Control-Allow-Origin'] = origin
```

### 3. **数据库连接池配置** [models/__init__.py]
- ✅ 添加连接池参数
- ✅ 配置连接回收时间
- ✅ 启用连接前检查

```python
engine = create_engine(
    DATABASE_URL,
    pool_size=10,           # ✅ 新增
    max_overflow=20,        # ✅ 新增
    pool_recycle=3600,      # ✅ 新增
    pool_pre_ping=True,     # ✅ 新增
)
```

### 4. **API调用优化工具** [api_utils.py] ✨ 新建
- ✅ 完整的重试机制（指数退避）
- ✅ 自动超时控制
- ✅ 自定义异常类（APITimeoutError等）
- ✅ 装饰器支持

```python
@retry_with_exponential_backoff(max_retries=3)
def fetch_data(url):
    response = fetch_with_timeout(url, timeout=10)
    return handle_api_response(response)
```

### 5. **改进的采集器基类** [collectors/optimized_base_collector.py] ✨ 新建
- ✅ 完整的类型注解
- ✅ 改进的缓存管理（带max_items限制）
- ✅ 数据验证框架
- ✅ 采集统计跟踪
- ✅ Context manager支持（session_scope）

```python
class OptimizedBaseCollector(ABC):
    """包含类型注解、异常处理、缓存管理的改进基类"""
    
    def collect_with_cache(self, use_cache: bool = True) -> Optional[Dict[str, Any]]:
        """类型完整的采集方法"""
        ...
```

### 6. **常量管理** [constants.py] ✨ 新建
- ✅ 集中定义所有魔数
- ✅ 按类别组织（周期、阈值、配置等）
- ✅ 完整的注释说明
- ✅ 示例使用代码

```python
# 改进前 ❌
if len(df) < 60:  # 为什么是60？

# 改进后 ✅
from constants import MIN_DATA_DAYS
if len(df) < MIN_DATA_DAYS:  # 清晰明了
```

---

## 正在进行的优化 🔄

### 7. **错误处理标准化** [进度: 50%]

**目标**: 将所有泛型 `except Exception` 改为特定异常处理

**示例改进**:
```python
# 改进前 ❌
try:
    data = collector.collect()
except Exception as e:
    logger.error(f"失败: {e}")

# 改进后 ✅
try:
    data = collector.collect()
except (APITimeoutError, APIConnectionError) as e:
    logger.error(f"API错误: {e}", exc_info=True)
    # 实现重试逻辑
except DataValidationError as e:
    logger.error(f"数据验证失败: {e}")
except Exception as e:
    logger.critical(f"未预期的错误: {e}", exc_info=True)
    # 发送告警
```

**需要更新的文件**:
- [ ] `collectors/stock_collector.py`
- [ ] `collectors/fund_collector.py`
- [ ] `collectors/news_collector.py`
- [ ] `scheduler.py` (generate_daily_predictions函数)

---

## 待执行的优化 📋

### 8. **添加类型注解** [优先级: 高]

需要为以下文件添加完整的类型注解:

```
□ utils.py                    # 20+ 函数
□ collectors/stock_collector.py
□ collectors/fund_collector.py
□ predictors/short_term.py
□ recommenders/stock_recommender.py
□ models/ (所有模型定义)
```

**示例**:
```python
# 改进前 ❌
def get_trading_dates(start_date, end_date):
    ...

# 改进后 ✅
def get_trading_dates(
    start_date: Union[str, date], 
    end_date: Union[str, date]
) -> List[date]:
    ...
```

### 9. **性能优化** [优先级: 高]

- [ ] DataFrame批量操作优化
- [ ] 数据库查询索引添加
- [ ] 缓存策略优化
- [ ] 并发采集实现

### 10. **单元测试框架** [优先级: 中]

```
tests/
├── conftest.py              # pytest配置
├── unit/
│   ├── test_utils.py
│   ├── test_api_utils.py
│   └── test_constants.py
├── integration/
│   ├── test_collectors.py
│   └── test_predictors.py
└── fixtures/
    └── sample_data.py
```

### 11. **日志系统改进** [优先级: 中]

- [ ] 添加审计日志（关键操作）
- [ ] 实现日志轮转
- [ ] 添加错误告警

### 12. **文档更新** [优先级: 低]

- [ ] API文档 (Swagger/OpenAPI)
- [ ] 数据库schema文档
- [ ] 部署指南
- [ ] 故障排查指南

---

## 优化规范 📏

### 新代码要求

所有新代码必须满足:

✅ **完整的类型注解**
```python
def collect(self, timeout: int = 10) -> Optional[Dict[str, Any]]:
    ...
```

✅ **清晰的异常处理**
```python
try:
    ...
except SpecificError as e:
    logger.error(f"描述性错误信息", exc_info=True)
except Exception as e:
    logger.critical(f"未预期: {e}", exc_info=True)
finally:
    # 资源清理
```

✅ **使用常量而不是魔数**
```python
from constants import SHORT_TERM_PERIOD, MIN_DATA_DAYS
```

✅ **完整的文档字符串**
```python
def fetch_data(url: str, timeout: int = 10) -> Dict[str, Any]:
    """
    从URL获取数据。
    
    Args:
        url: 目标URL
        timeout: 请求超时（秒）
    
    Returns:
        解析后的数据字典
    
    Raises:
        APITimeoutError: 请求超时
        APIError: 其他API错误
    
    Example:
        data = fetch_data('https://api.example.com/data')
    """
    ...
```

---

## 快速开始 - 使用优化后的工具

### 1. 使用改进的采集器基类

```python
from collectors.optimized_base_collector import OptimizedBaseCollector

class MyCollector(OptimizedBaseCollector):
    def collect(self, **kwargs):
        # 实现采集逻辑
        return data
    
    def get_data_source_name(self):
        return 'MyCollector'

# 使用
collector = MyCollector()
data = collector.collect_with_cache(use_cache=True, code='000858.SZ')
print(f"统计: {collector.get_stats()}")
collector.close()
```

### 2. 使用API工具

```python
from api_utils import setup_session_with_retries, handle_api_response

session = setup_session_with_retries(total_retries=3, timeout=10)
response = session.get('https://api.example.com/data')
data = handle_api_response(response, json_response=True)
```

### 3. 使用常量

```python
from constants import (
    SHORT_TERM_PERIOD, 
    MEDIUM_TERM_PERIOD,
    MIN_DATA_DAYS,
    RSI_OVERBOUGHT
)

# 清晰的常量使用
if rsi > RSI_OVERBOUGHT:
    logger.warning(f"RSI超买: {rsi}")
```

### 4. 使用会话范围

```python
from app import session_scope

# 在app.py或其他Flask模块中
with session_scope() as session:
    result = session.query(Model).filter(...).all()
    # 自动commit/rollback/close
```

---

## 验证清单 ✓

运行以下命令验证优化效果:

```bash
# 1. 代码质量检查
python -m pylint collectors/optimized_base_collector.py
python -m flake8 constants.py
python -m mypy api_utils.py

# 2. 导入测试
python -c "from api_utils import setup_session_with_retries; print('✅ api_utils导入成功')"
python -c "from collectors.optimized_base_collector import OptimizedBaseCollector; print('✅ optimized_base_collector导入成功')"
python -c "from constants import SHORT_TERM_PERIOD; print('✅ constants导入成功')"

# 3. 运行示例
python constants.py          # 显示所有常量
python api_utils.py          # 运行API工具示例
python collectors/optimized_base_collector.py  # 运行采集器示例

# 4. Flask应用启动测试
python -c "from app import app, session_scope; print('✅ app初始化成功'); app.test_request_context().push()"
```

---

## 下一步行动

### 本周目标
1. ✅ 完成资源泄漏修复
2. ✅ 创建优化基础设施 (api_utils, constants, optimized_base_collector)
3. ⬜ 使用新工具重构 stock_collector.py (示例改造)
4. ⬜ 添加50%的关键函数类型注解

### 下周目标
1. ⬜ 完成全部类型注解
2. ⬜ 添加基本单元测试
3. ⬜ 性能基准测试
4. ⬜ 文档更新

### 本月目标
1. ⬜ 完成所有CRITICAL级问题修复
2. ⬜ 完成所有HIGH级问题修复
3. ⬜ 添加40%以上的单元测试覆盖率
4. ⬜ 部署CI/CD检查

---

## 参考资源

- [Python类型提示指南](https://www.python.org/dev/peps/pep-0484/)
- [SQLAlchemy连接池文档](https://docs.sqlalchemy.org/en/14/core/pooling.html)
- [requests库重试机制](https://urllib3.readthedocs.io/en/latest/reference/urllib3.util.retry.html)
- [Python异常处理最佳实践](https://docs.python-guide.org/writing/structure/)
- [Flask应用工厂模式](https://flask.palletsprojects.com/en/2.0.x/patterns/appfactories/)

---

**维护人**: GitHub Copilot  
**最后更新**: 2026-04-12
