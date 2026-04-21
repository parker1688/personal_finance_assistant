"""
采集器优化基类 - collectors/optimized_base_collector.py
改进的采集器基类，包含更好的错误处理、类型注解和超时控制
"""

import time
import json
import hashlib
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from contextlib import contextmanager
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_logger
from api_utils import (
    setup_session_with_retries, 
    APIError, 
    APITimeoutError,
    APIConnectionError
)

logger = get_logger(__name__)


class CollectorException(Exception):
    """采集器异常基类"""
    pass


class DataValidationError(CollectorException):
    """数据验证异常"""
    pass


class APICallError(CollectorException):
    """API调用异常"""
    pass


class SimpleCache:
    """简单的内存缓存，支持TTL"""
    
    def __init__(self, ttl: int = 300, max_items: int = 1000):
        """
        Args:
            ttl: 缓存过期时间（秒）
            max_items: 最大项数
        """
        self.ttl = ttl
        self.max_items = max_items
        self.cache: Dict[str, Dict[str, Any]] = {}
    
    def set(self, key: str, value: Any) -> None:
        """设置缓存"""
        if len(self.cache) >= self.max_items:
            # 移除最旧的项
            oldest_key = min(self.cache.keys(), 
                           key=lambda k: self.cache[k]['timestamp'])
            del self.cache[oldest_key]
        
        self.cache[key] = {
            'value': value,
            'timestamp': time.time()
        }
    
    def get(self, key: str) -> Optional[Any]:
        """获取缓存"""
        if key not in self.cache:
            return None
        
        entry = self.cache[key]
        if time.time() - entry['timestamp'] > self.ttl:
            del self.cache[key]
            return None
        
        return entry['value']
    
    def clear(self) -> None:
        """清空缓存"""
        self.cache.clear()
    
    def clean_expired(self) -> int:
        """清理过期项，返回清理数量"""
        now = time.time()
        expired_keys = [
            k for k, v in self.cache.items()
            if now - v['timestamp'] > self.ttl
        ]
        for key in expired_keys:
            del self.cache[key]
        return len(expired_keys)
    
    def size(self) -> int:
        """缓存项数"""
        return len(self.cache)


class OptimizedBaseCollector(ABC):
    """优化的采集器基类
    
    特性：
    - 完整的类型注解
    - 更好的异常处理
    - 自动超时和重试
    - 缓存管理
    - 数据验证
    """
    
    def __init__(self, 
                 cache_ttl: int = 300, 
                 max_cache_items: int = 1000,
                 request_timeout: int = 10,
                 max_retries: int = 3):
        """
        Args:
            cache_ttl: 缓存过期时间（秒）
            max_cache_items: 最大缓存项数
            request_timeout: 请求超时时间（秒）
            max_retries: 最大重试次数
        """
        self.cache = SimpleCache(ttl=cache_ttl, max_items=max_cache_items)
        self.request_timeout = request_timeout
        self.max_retries = max_retries
        self.session = setup_session_with_retries(
            total_retries=max_retries,
            timeout=request_timeout
        )
        self.stats = {
            'collected': 0,
            'cached': 0,
            'errors': 0,
            'api_calls': 0
        }
    
    @abstractmethod
    def collect(self, **kwargs) -> Optional[Dict[str, Any]]:
        """
        采集数据（子类必须实现）
        
        Returns:
            采集的数据字典，或None表示失败
        """
        pass
    
    @abstractmethod
    def get_data_source_name(self) -> str:
        """
        获取数据源名称
        
        Returns:
            数据源名称
        """
        pass
    
    def validate_data(self, data: Dict[str, Any]) -> bool:
        """
        验证数据有效性（子类可覆盖）
        
        Args:
            data: 数据字典
        
        Returns:
            True表示有效，False表示无效
        """
        return data is not None and len(data) > 0
    
    def get_cache_key(self, **kwargs) -> str:
        """
        生成缓存键
        
        Args:
            **kwargs: 参数
        
        Returns:
            缓存键
        """
        key_str = f"{self.get_data_source_name()}_{json.dumps(kwargs, sort_keys=True)}"
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def get_cached(self, **kwargs) -> Optional[Dict[str, Any]]:
        """
        获取缓存数据
        
        Args:
            **kwargs: 参数
        
        Returns:
            缓存数据或None
        """
        key = self.get_cache_key(**kwargs)
        data = self.cache.get(key)
        if data is not None:
            self.stats['cached'] += 1
            logger.debug(f"缓存命中: {self.get_data_source_name()}")
        return data
    
    def cache_put(self, data: Dict[str, Any], **kwargs) -> None:
        """
        保存数据到缓存
        
        Args:
            data: 数据字典
            **kwargs: 参数
        """
        key = self.get_cache_key(**kwargs)
        self.cache.set(key, data)
    
    def collect_with_cache(self, use_cache: bool = True, **kwargs) -> Optional[Dict[str, Any]]:
        """
        采集数据（支持缓存）
        
        Args:
            use_cache: 是否使用缓存
            **kwargs: 采集参数
        
        Returns:
            采集的数据
        """
        # 尝试获取缓存
        if use_cache:
            cached = self.get_cached(**kwargs)
            if cached is not None:
                return cached
        
        # 采集新数据
        try:
            data = self.collect(**kwargs)
            
            if data is not None:
                # 验证数据
                if not self.validate_data(data):
                    raise DataValidationError(
                        f"数据验证失败: {self.get_data_source_name()}"
                    )
                
                # 保存到缓存
                self.cache_put(data, **kwargs)
                self.stats['collected'] += 1
                return data
            else:
                self.stats['errors'] += 1
                return None
                
        except (APITimeoutError, APIConnectionError) as e:
            logger.error(f"API错误 ({self.get_data_source_name()}): {e}")
            self.stats['errors'] += 1
            raise APICallError(f"采集失败: {self.get_data_source_name()}") from e
        
        except DataValidationError as e:
            logger.error(f"数据验证失败: {e}")
            self.stats['errors'] += 1
            raise
        
        except Exception as e:
            logger.error(f"未预期的错误 ({self.get_data_source_name()}): {e}", 
                        exc_info=True)
            self.stats['errors'] += 1
            raise CollectorException(f"采集异常: {str(e)}") from e
    
    def get_stats(self) -> Dict[str, int]:
        """获取采集统计"""
        return {
            **self.stats,
            'cache_size': self.cache.size()
        }
    
    def reset_stats(self) -> None:
        """重置统计"""
        self.stats = {
            'collected': 0,
            'cached': 0,
            'errors': 0,
            'api_calls': 0
        }
    
    @contextmanager
    def session_scope(self):
        """上下文管理器 - 用于数据库操作"""
        from models import get_session
        session = get_session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"数据库操作失败: {e}", exc_info=True)
            raise
        finally:
            session.close()
    
    def close(self) -> None:
        """关闭采集器（清理资源）"""
        if self.session:
            self.session.close()
        self.cache.clear()
        logger.info(f"{self.get_data_source_name()} 采集器已关闭")


# 示例使用
if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    
    class ExampleCollector(OptimizedBaseCollector):
        def collect(self, **kwargs) -> Optional[Dict[str, Any]]:
            return {'example': 'data', 'timestamp': datetime.now().isoformat()}
        
        def get_data_source_name(self) -> str:
            return 'ExampleCollector'
    
    collector = ExampleCollector()
    data = collector.collect_with_cache()
    print(f"采集结果: {data}")
    print(f"统计: {collector.get_stats()}")
    collector.close()
