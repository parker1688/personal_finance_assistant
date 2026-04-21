# 系统改进实施指南

**版本**: 1.0  
**日期**: 2026-04-12  
**目标**: 提供Priority 1-2改进的具体实施路径和代码示例

---

## Phase 1 详细实施指南

### Part 1: 采集调度统一框架

#### 1.1 问题分析
```python
# 当前问题: 采集器分散调用，无统一管理
from collectors.stock_collector import StockCollector
from collectors.fund_collector import FundCollector

# 分别独立调用
stock = StockCollector()
stock.collect_history(code)

fund = FundCollector()
fund.collect_fund_nav(code)

# 问题:
# - 无调度顺序
# - 无速率控制
# - 无失败重试
# - 无进度追踪
```

#### 1.2 推荐方案: 统一Scheduler

文件: `collectors/scheduler.py`
```python
"""
采集任务调度器 - collectors/scheduler.py
统一管理所有采集任务的执行频率、顺序、重试等
"""

import time
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from enum import Enum
import json
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_logger

logger = get_logger(__name__)


class CollectFrequency(Enum):
    """采集频率定义"""
    REALTIME = 300      # 5分钟
    HOURLY = 3600       # 1小时
    DAILY = 86400       # 日度
    WEEKLY = 604800     # 周度


class CollectTask:
    """采集任务定义"""
    
    def __init__(self, 
                 collector_name: str,
                 collector_class,
                 method_name: str,
                 method_kwargs: Dict[str, Any],
                 frequency: CollectFrequency,
                 priority: int = 5):
        """
        Args:
            collector_name: 采集器名称
            collector_class: 采集器类
            method_name: 调用方法名
            method_kwargs: 方法参数
            frequency: 执行频率
            priority: 优先级 (1=最高, 10=最低)
        """
        self.collector_name = collector_name
        self.collector_class = collector_class
        self.method_name = method_name
        self.method_kwargs = method_kwargs
        self.frequency = frequency
        self.priority = priority
        self.last_run = None
        self.next_run = datetime.now()
        self.success_count = 0
        self.fail_count = 0
    
    def should_run(self) -> bool:
        """检查是否应该执行"""
        return datetime.now() >= self.next_run
    
    def execute(self) -> bool:
        """执行采集任务"""
        try:
            collector = self.collector_class()
            method = getattr(collector, self.method_name)
            result = method(**self.method_kwargs)
            
            self.last_run = datetime.now()
            self.next_run = self.last_run + timedelta(seconds=self.frequency.value)
            self.success_count += 1
            
            logger.info(f"采集任务成功: {self.collector_name}.{self.method_name}")
            return True
            
        except Exception as e:
            self.fail_count += 1
            logger.error(f"采集任务失败: {self.collector_name}.{self.method_name}: {e}")
            
            # 失败后延迟下次重试
            self.next_run = datetime.now() + timedelta(minutes=5)
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """获取任务状态"""
        return {
            'name': self.collector_name,
            'method': self.method_name,
            'frequency': self.frequency.name,
            'last_run': self.last_run.isoformat() if self.last_run else None,
            'next_run': self.next_run.isoformat(),
            'success_count': self.success_count,
            'fail_count': self.fail_count,
            'success_rate': self.success_count / (self.success_count + self.fail_count) if (self.success_count + self.fail_count) > 0 else 0
        }


class CollectScheduler:
    """采集任务调度器"""
    
    def __init__(self, max_concurrent: int = 3):
        """
        Args:
            max_concurrent: 最大并发数
        """
        self.tasks: List[CollectTask] = []
        self.max_concurrent = max_concurrent
        self.is_running = False
        self.progress_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'data', 'scheduler_progress.json'
        )
    
    def register_task(self, task: CollectTask) -> None:
        """注册采集任务"""
        self.tasks.append(task)
        # 按优先级排序
        self.tasks.sort(key=lambda t: t.priority)
        logger.info(f"任务已注册: {task.collector_name}")
    
    def run_once(self) -> Dict[str, Any]:
        """执行一次调度循环"""
        results = {
            'timestamp': datetime.now().isoformat(),
            'total_tasks': len(self.tasks),
            'executed': 0,
            'failed': 0,
            'tasks': []
        }
        
        # 按优先级执行应该运行的任务
        runnable_tasks = [t for t in self.tasks if t.should_run()]
        
        for task in runnable_tasks[:self.max_concurrent]:
            success = task.execute()
            results['executed'] += 1
            if not success:
                results['failed'] += 1
            results['tasks'].append(task.get_status())
        
        # 保存进度
        self._save_progress(results)
        
        return results
    
    def run_continuous(self, interval: int = 60) -> None:
        """持续运行调度器"""
        self.is_running = True
        logger.info(f"调度器已启动: 间隔{interval}秒")
        
        try:
            while self.is_running:
                results = self.run_once()
                if results['executed'] > 0:
                    logger.info(f"本轮执行: {results['executed']}个任务, 失败{results['failed']}个")
                
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("调度器已停止")
            self.is_running = False
    
    def _save_progress(self, results: Dict) -> None:
        """保存进度到文件"""
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(results, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"保存进度失败: {e}")
    
    def get_status(self) -> Dict[str, Any]:
        """获取调度器状态"""
        return {
            'is_running': self.is_running,
            'total_tasks': len(self.tasks),
            'tasks': [t.get_status() for t in self.tasks]
        }


# 使用示例
if __name__ == '__main__':
    from collectors.stock_collector import StockCollector
    from collectors.fund_collector import FundCollector
    
    scheduler = CollectScheduler(max_concurrent=3)
    
    # 注册股票采集任务 - 每5分钟
    scheduler.register_task(CollectTask(
        collector_name='StockRealtime',
        collector_class=StockCollector,
        method_name='collect_realtime',
        method_kwargs={'code': '000858.SZ', 'market': 'A'},
        frequency=CollectFrequency.REALTIME,
        priority=1
    ))
    
    # 注册基金采集任务 - 每小时
    scheduler.register_task(CollectTask(
        collector_name='FundNav',
        collector_class=FundCollector,
        method_name='collect_fund_nav',
        method_kwargs={'fund_code': '110011', 'days': 30},
        frequency=CollectFrequency.HOURLY,
        priority=2
    ))
    
    # 运行调度器
    scheduler.run_continuous(interval=60)
```

---

### Part 2: API文档和标准化

#### 2.1 创建OpenAPI规范

文件: `api/openapi.py`
```python
"""
OpenAPI/Swagger 规范生成 - api/openapi.py
"""

from flask import Blueprint, jsonify
import os

openapi_bp = Blueprint('openapi', __name__)

OPENAPI_SPEC = {
    "openapi": "3.0.0",
    "info": {
        "title": "个人AI理财助手 API",
        "version": "1.0.0",
        "description": "金融投资分析和推荐系统"
    },
    "servers": [
        {"url": "http://localhost:8080", "description": "开发环境"},
        {"url": "https://api.example.com", "description": "生产环境"}
    ],
    "paths": {
        "/health": {
            "get": {
                "tags": ["系统管理"],
                "summary": "健康检查",
                "responses": {
                    "200": {
                        "description": "系统健康",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "status": {"type": "string", "enum": ["ok", "degraded"]},
                                        "components": {
                                            "type": "object",
                                            "properties": {
                                                "database": {"type": "string"},
                                                "app": {"type": "string"}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        "/api/dashboard/summary": {
            "get": {
                "tags": ["仪表板"],
                "summary": "获取仪表板汇总数据",
                "responses": {
                    "200": {
                        "description": "成功",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "code": {"type": "integer", "example": 200},
                                        "data": {
                                            "type": "object",
                                            "properties": {
                                                "today_warnings": {"type": "integer"},
                                                "today_recommendations": {"type": "integer"}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        "/api/recommendations/{type}": {
            "get": {
                "tags": ["推荐"],
                "summary": "获取推荐列表",
                "parameters": [
                    {
                        "name": "type",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "string", "enum": ["stock", "fund", "etf"]},
                        "description": "资产类型"
                    },
                    {
                        "name": "sort_by",
                        "in": "query",
                        "schema": {"type": "string", "enum": ["score", "price", "volatility"]},
                        "description": "排序字段"
                    },
                    {
                        "name": "limit",
                        "in": "query",
                        "schema": {"type": "integer", "default": 20},
                        "description": "返回数量"
                    }
                ],
                "responses": {
                    "200": {
                        "description": "成功"
                    },
                    "400": {
                        "description": "参数错误"
                    }
                }
            }
        }
    },
    "components": {
        "schemas": {
            "Error": {
                "type": "object",
                "properties": {
                    "type": {"type": "string", "description": "错误类型},
                    "title": {"type": "string"},
                    "status": {"type": "integer"},
                    "detail": {"type": "string"},
                    "instance": {"type": "string"}
                }
            }
        }
    }
}

@openapi_bp.route('/openapi/spec', methods=['GET'])
def get_openapi_spec():
    """获取OpenAPI规范"""
    return jsonify(OPENAPI_SPEC)

@openapi_bp.route('/docs', methods=['GET'])
def swagger_ui():
    """Swagger UI"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <link rel="stylesheet" type="text/css" href="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/4.15.5/swagger-ui.css" />
    </head>
    <body>
        <div id="swagger-ui"></div>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/4.15.5/swagger-ui.bundle.js"></script>
        <script>
            window.onload = function() {
                SwaggerUIBundle({
                    url: "/openapi/spec",
                    dom_id: '#swagger-ui',
                    presets: [SwaggerUIBundle.presets.apis],
                    layout: "BaseLayout"
                })
            }
        </script>
    </body>
    </html>
    """
```

#### 2.2 统一错误处理

文件: `api/errors.py`
```python
"""
统一错误处理 - api/errors.py
按 RFC 7807 Problem Details 标准
"""

from flask import jsonify
from werkzeug.exceptions import HTTPException
from typing import Dict, Any
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_logger

logger = get_logger(__name__)


class APIErrorCode:
    """错误代码定义"""
    # 客户端错误
    INVALID_PARAM = "INVALID_PARAM"
    NOT_FOUND = "NOT_FOUND"
    UNAUTHORIZED = "UNAUTHORIZED"
    FORBIDDEN = "FORBIDDEN"
    RATE_LIMITED = "RATE_LIMITED"
    
    # 服务器错误
    INTERNAL_ERROR = "INTERNAL_ERROR"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    TIMEOUT = "TIMEOUT"


def create_error_response(
    error_code: str,
    message: str,
    status_code: int,
    detail: str = None,
    context: Dict[str, Any] = None
) -> tuple:
    """
    创建标准错误响应
    
    Args:
        error_code: 错误代码
        message: 错误信息 (用户友好)
        status_code: HTTP状态码
        detail: 错误详情 (技术细节)
        context: 额外上下文
    
    Returns:
        (response_dict, http_status_code)
    """
    response = {
        "type": f"/errors/{error_code}",
        "title": error_code,
        "status": status_code,
        "detail": detail or message,
        "timestamp": __import__('datetime').datetime.now().isoformat(),
    }
    
    if context:
        response["context"] = context
    
    return response, status_code


def register_error_handlers(app):
    """注册全局错误处理器"""
    
    @app.errorhandler(400)
    def handle_bad_request(e):
        response, code = create_error_response(
            error_code=APIErrorCode.INVALID_PARAM,
            message="请求参数无效",
            status_code=400,
            detail=str(e)
        )
        return jsonify(response), code
    
    @app.errorhandler(404)
    def handle_not_found(e):
        response, code = create_error_response(
            error_code=APIErrorCode.NOT_FOUND,
            message="资源未找到",
            status_code=404
        )
        return jsonify(response), code
    
    @app.errorhandler(500)
    def handle_internal_error(e):
        logger.error(f"Internal error: {e}", exc_info=True)
        response, code = create_error_response(
            error_code=APIErrorCode.INTERNAL_ERROR,
            message="系统内部错误",
            status_code=500,
            detail=str(e) if app.debug else "请稍后重试"
        )
        return jsonify(response), code
    
    @app.errorhandler(503)
    def handle_service_unavailable(e):
        response, code = create_error_response(
            error_code=APIErrorCode.SERVICE_UNAVAILABLE,
            message="服务暂时不可用",
            status_code=503
        )
        return jsonify(response), code
```

#### 2.3 分页标准

文件: `api/pagination.py`
```python
"""
分页支持 - api/pagination.py
"""

from typing import List, Dict, Any, Optional
from flask import request


class Paginator:
    """分页器"""
    
    DEFAULT_PAGE_SIZE = 20
    MAX_PAGE_SIZE = 100
    
    @staticmethod
    def paginate(query, 
                 default_page: int = 1,
                 default_size: int = DEFAULT_PAGE_SIZE):
        """
        对数据库查询进行分页
        
        Args:
            query: SQLAlchemy查询对象
            default_page: 默认页码
            default_size: 默认页大小
        
        Returns:
            dict: 包含分页信息和数据
        """
        page = request.args.get('page', default_page, type=int)
        size = request.args.get('size', default_size, type=int)
        
        # 验证参数
        page = max(1, page)
        size = max(1, min(size, Paginator.MAX_PAGE_SIZE))
        
        # 执行分页查询
        total = query.count()
        items = query.offset((page - 1) * size).limit(size).all()
        
        total_pages = (total + size - 1) // size
        
        return {
            'data': items,
            'pagination': {
                'page': page,
                'size': size,
                'total': total,
                'total_pages': total_pages,
                'has_next': page < total_pages,
                'has_prev': page > 1
            }
        }
    
    @staticmethod
    def to_response(paginated_data: Dict, serializer=None) -> Dict:
        """
        转换为API响应格式
        
        Args:
            paginated_data: 分页数据
            serializer: 序列化函数
        
        Returns:
            标准化的API响应
        """
        data = paginated_data['data']
        if serializer:
            data = [serializer(item) for item in data]
        
        return {
            'code': 200,
            'status': 'success',
            'data': data,
            'pagination': paginated_data['pagination']
        }
```

---

### Part 3: 数据验证框架

文件: `validators.py`
```python
"""
数据验证框架 - validators.py
"""

from typing import Any, List, Optional, Callable
from abc import ABC, abstractmethod
from datetime import datetime, date
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils import get_logger

logger = get_logger(__name__)


class ValidationError(Exception):
    """数据验证异常"""
    
    def __init__(self, field: str, message: str, value: Any = None):
        self.field = field
        self.message = message
        self.value = value
        super().__init__(f"字段'{field}': {message}")


class Validator(ABC):
    """验证器基类"""
    
    @abstractmethod
    def validate(self, value: Any) -> bool:
        """验证值"""
        pass
    
    @abstractmethod
    def get_error_message(self) -> str:
        """获取错误信息"""
        pass


class NotNullValidator(Validator):
    """非空验证"""
    
    def validate(self, value: Any) -> bool:
        return value is not None and value != ''
    
    def get_error_message(self) -> str:
        return "字段不能为空"


class RangeValidator(Validator):
    """范围验证"""
    
    def __init__(self, min_val: float = None, max_val: float = None):
        self.min_val = min_val
        self.max_val = max_val
    
    def validate(self, value: Any) -> bool:
        if self.min_val is not None and value < self.min_val:
            return False
        if self.max_val is not None and value > self.max_val:
            return False
        return True
    
    def get_error_message(self) -> str:
        if self.min_val and self.max_val:
            return f"字段值必须在{self.min_val}到{self.max_val}之间"
        elif self.min_val:
            return f"字段值不能小于{self.min_val}"
        elif self.max_val:
            return f"字段值不能大于{self.max_val}"
        return "字段值超出范围"


class ExtremesDetector(Validator):
    """极值检测 (用于股价数据)"""
    
    def __init__(self, std_devs: float = 5.0, base_prices: List[float] = None):
        """
        Args:
            std_devs: 标准差倍数
            base_prices: 基准价格序列 (用于计算均值和标差)
        """
        self.std_devs = std_devs
        self.base_prices = base_prices
        self.mean = None
        self.std = None
        
        if base_prices:
            self._calculate_stats()
    
    def _calculate_stats(self):
        """计算统计信息"""
        import numpy as np
        self.mean = np.mean(self.base_prices)
        self.std = np.std(self.base_prices)
    
    def validate(self, value: Any) -> bool:
        if self.mean is None or self.std is None:
            return True  # 无法验证
        
        # 检查是否超出 mean ± std_devs*std
        upper_bound = self.mean + self.std_devs * self.std
        lower_bound = self.mean - self.std_devs * self.std
        
        return lower_bound <= value <= upper_bound
    
    def get_error_message(self) -> str:
        return f"字段值可能是极值 (超出±{self.std_devs}σ)"


class TypeValidator(Validator):
    """类型验证"""
    
    def __init__(self, expected_type):
        self.expected_type = expected_type
    
    def validate(self, value: Any) -> bool:
        return isinstance(value, self.expected_type)
    
    def get_error_message(self) -> str:
        return f"字段类型应该是{self.expected_type.__name__}"


class DataFrame():
    """单条数据验证schema"""
    
    def __init__(self):
        self.rules: Dict[str, List[Validator]] = {}
    
    def add_rule(self, field: str, validator: Validator) -> 'DataFrameSchema':
        """添加验证规则"""
        if field not in self.rules:
            self.rules[field] = []
        self.rules[field].append(validator)
        return self
    
    def validate(self, data: dict) -> List[ValidationError]:
        """验证数据，返回所有错误"""
        errors = []
        
        for field, validators in self.rules.items():
            value = data.get(field)
            
            for validator in validators:
                if not validator.validate(value):
                    errors.append(ValidationError(
                        field=field,
                        message=validator.get_error_message(),
                        value=value
                    ))
        
        return errors
    
    def validate_strict(self, data: dict) -> bool:
        """严格验证，任何错误即抛异常"""
        errors = self.validate(data)
        if errors:
            error_msg = "; ".join([str(e) for e in errors])
            raise ValidationError(field="", message=f"数据验证失败: {error_msg}")
        return True


# 使用示例
def create_stock_price_schema() -> DataFrameSchema:
    """创建股票价格验证schema"""
    schema = DataFrameSchema()
    
    schema.add_rule('code', NotNullValidator())
    schema.add_rule('code', TypeValidator(str))
    
    schema.add_rule('close', NotNullValidator())
    schema.add_rule('close', TypeValidator(float))
    schema.add_rule('close', RangeValidator(min_val=0.01, max_val=10000))
    
    schema.add_rule('volume', NotNullValidator())
    schema.add_rule('volume', TypeValidator(int))
    schema.add_rule('volume', RangeValidator(min_val=0))
    
    return schema
```

使用示例:
```python
# 在采集器中使用
from validators import create_stock_price_schema

schema = create_stock_price_schema()

for record in collected_data:
    try:
        schema.validate_strict(record)
        # 数据有效，入库
        session.add(RawStockData(**record))
    except ValidationError as e:
        logger.error(f"数据验证失败: {e}")
        # 记录到异常数据表 (新增)
        session.add(InvalidData(
            source=collector_name,
            raw_data=json.dumps(record),
            error_message=str(e)
        ))
```

---

## Phase 2 改进要点

### 3. 模型训练框架

关键建议:
1. 建立标准化训练流程
2. 添加交叉验证
3. 实现超参优化 (grid search / bayesian)
4. 记录所有训练元数据

### 4. 多维度预警规则

扩展预警维度:
- ✅ 已有: 技术面 (RSI、MACD)
- ⚠️ 需添加: 基本面 (PE大幅上升)、宏观 (利率上升)、情感 (舆情反转)

### 5. 特征工程

多时间框架特征示例:
```python
# 日级特征
daily_features = {
    'rsi_5': calculate_rsi(close, 5),
    'rsi_14': calculate_rsi(close, 14),
}

# 周级特征 (5个交易日)
weekly_features = {
    'weekly_return': calculate_return(close, 5),
    'weekly_ma20': calculate_ma(close, 20),
}

# 月级特征 (20个交易日)
monthly_features = {
    'monthly_return': calculate_return(close, 20),
    'monthly_ma60': calculate_ma(close, 60),
}

# 合并
all_features = {**daily_features, **weekly_features, **monthly_features}
```

---

## 验证清单

### Phase 1验证
- [ ] Scheduler 成功调度所有采集器
- [ ] API 返回标准化错误格式
- [ ] 数据验证框架拦截异常数据
- [ ] 单元测试通过率 > 90%

### Phase 2验证
- [ ] 模型评估指标完整 (Precision、Recall、F1、AUC)
- [ ] 预警覆盖风险类型 > 80%
- [ ] 特征工程提升预测准确率 +5-10%

### Phase 3验证
- [ ] 个性化推荐采纳率 > 35%
- [ ] 系统可用性 > 99%
- [ ] 平均响应时间 < 500ms

---

**下一步**: 选择 Priority 1 中的任务，开始实施
