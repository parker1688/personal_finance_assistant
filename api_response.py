"""
API 响应标准化模块 - api_response.py
定义统一的API响应格式和错误处理
"""

from typing import Optional, Dict, Any, List
from enum import Enum
from datetime import datetime
import json
import logging

logger = logging.getLogger(__name__)


class ResponseCode(Enum):
    """API 响应代码"""
    SUCCESS = 0
    INVALID_PARAMS = 1001
    DATA_VALIDATION_ERROR = 1002
    DATABASE_ERROR = 1003
    API_ERROR = 1004
    AUTH_ERROR = 1005
    NOT_FOUND = 1006
    CONFLICT = 1007
    INTERNAL_ERROR = 9999


class APIResponse:
    """标准化API响应类"""
    
    def __init__(
        self,
        code: int = ResponseCode.SUCCESS.value,
        message: str = "Success",
        data: Optional[Any] = None,
        errors: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        初始化API响应
        
        Args:
            code: 响应代码
            message: 响应消息
            data: 响应数据
            errors: 错误列表
            metadata: 元数据 (分页、统计等)
        """
        self.code = code
        self.message = message
        self.data = data
        self.errors = errors or []
        self.metadata = metadata or {}
        self.timestamp = datetime.now().isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'code': self.code,
            'message': self.message,
            'data': self.data,
            'errors': self.errors if self.errors else None,
            'metadata': self.metadata if self.metadata else None,
            'timestamp': self.timestamp
        }
    
    def to_json(self) -> str:
        """转换为JSON字符串"""
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)
    
    @staticmethod
    def success(
        data: Optional[Any] = None,
        message: str = "Success",
        metadata: Optional[Dict[str, Any]] = None
    ) -> 'APIResponse':
        """创建成功响应"""
        return APIResponse(
            code=ResponseCode.SUCCESS.value,
            message=message,
            data=data,
            metadata=metadata
        )
    
    @staticmethod
    def error(
        code: int = ResponseCode.INTERNAL_ERROR.value,
        message: str = "Internal Error",
        errors: Optional[List[str]] = None,
        data: Optional[Any] = None
    ) -> 'APIResponse':
        """创建错误响应"""
        return APIResponse(
            code=code,
            message=message,
            errors=errors,
            data=data
        )
    
    @staticmethod
    def validation_error(
        errors: List[str],
        data: Optional[Any] = None
    ) -> 'APIResponse':
        """创建数据验证错误响应"""
        return APIResponse(
            code=ResponseCode.DATA_VALIDATION_ERROR.value,
            message="Data Validation Failed",
            errors=errors,
            data=data
        )
    
    @staticmethod
    def not_found(message: str = "Resource Not Found") -> 'APIResponse':
        """创建404响应"""
        return APIResponse(
            code=ResponseCode.NOT_FOUND.value,
            message=message
        )
    
    @staticmethod
    def invalid_params(
        errors: List[str],
        message: str = "Invalid Parameters"
    ) -> 'APIResponse':
        """创建参数错误响应"""
        return APIResponse(
            code=ResponseCode.INVALID_PARAMS.value,
            message=message,
            errors=errors
        )
    
    @staticmethod
    def pagination(
        data: List[Any],
        page: int,
        page_size: int,
        total: int,
        message: str = "Success"
    ) -> 'APIResponse':
        """创建分页响应"""
        return APIResponse(
            code=ResponseCode.SUCCESS.value,
            message=message,
            data=data,
            metadata={
                'page': page,
                'page_size': page_size,
                'total': total,
                'total_pages': (total + page_size - 1) // page_size
            }
        )


class APIError(Exception):
    """API错误基类"""
    
    def __init__(
        self,
        code: int,
        message: str,
        errors: Optional[List[str]] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        self.code = code
        self.message = message
        self.errors = errors or []
        self.details = details or {}
        super().__init__(self.message)
    
    def to_response(self) -> APIResponse:
        """转换为API响应"""
        return APIResponse.error(
            code=self.code,
            message=self.message,
            errors=self.errors,
            data=self.details
        )


class ValidationError(APIError):
    """数据验证错误"""
    
    def __init__(
        self,
        errors: List[str],
        details: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            code=ResponseCode.DATA_VALIDATION_ERROR.value,
            message="Data Validation Failed",
            errors=errors,
            details=details
        )


class NotFoundError(APIError):
    """资源未找到错误"""
    
    def __init__(
        self,
        message: str = "Resource Not Found",
        resource_id: Optional[str] = None
    ):
        super().__init__(
            code=ResponseCode.NOT_FOUND.value,
            message=message,
            details={'resource_id': resource_id} if resource_id else {}
        )


class ConflictError(APIError):
    """冲突错误（资源已存在等）"""
    
    def __init__(
        self,
        message: str = "Resource Conflict",
        existing_id: Optional[str] = None
    ):
        super().__init__(
            code=ResponseCode.CONFLICT.value,
            message=message,
            details={'existing_id': existing_id} if existing_id else {}
        )


class InvalidParamsError(APIError):
    """无效参数错误"""
    
    def __init__(
        self,
        errors: List[str],
        message: str = "Invalid Parameters"
    ):
        super().__init__(
            code=ResponseCode.INVALID_PARAMS.value,
            message=message,
            errors=errors
        )


class DatabaseError(APIError):
    """数据库错误"""
    
    def __init__(
        self,
        message: str = "Database Error",
        operation: Optional[str] = None
    ):
        super().__init__(
            code=ResponseCode.DATABASE_ERROR.value,
            message=message,
            details={'operation': operation} if operation else {}
        )


def create_response(
    success: bool,
    code: int,
    message: str,
    data: Optional[Any] = None,
    errors: Optional[List[str]] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> APIResponse:
    """工厂函数：创建响应"""
    if success:
        return APIResponse(
            code=code,
            message=message,
            data=data,
            metadata=metadata
        )
    else:
        return APIResponse(
            code=code,
            message=message,
            errors=errors,
            data=data
        )


# 示例使用
if __name__ == '__main__':
    # 示例1: 成功响应
    resp = APIResponse.success(
        data={'id': 1, 'name': 'Test'},
        message='Data fetched successfully'
    )
    print("✅ Success Response:")
    print(resp.to_json())
    print()
    
    # 示例2: 分页响应
    resp = APIResponse.pagination(
        data=[{'id': 1}, {'id': 2}],
        page=1,
        page_size=2,
        total=100
    )
    print("📄 Pagination Response:")
    print(resp.to_json())
    print()
    
    # 示例3: 验证错误
    resp = APIResponse.validation_error(
        errors=['Missing field: name', 'Invalid email format'],
        data={'provided': {}}
    )
    print("❌ Validation Error:")
    print(resp.to_json())
    print()
    
    # 示例4: 使用异常
    try:
        raise ValidationError(
            errors=['Missing required field: email'],
            details={'provided_fields': ['name']}
        )
    except ValidationError as e:
        print("💥 Exception:")
        print(e.to_response().to_json())
