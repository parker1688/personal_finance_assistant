"""
API 缓存装饰器 - api_cache.py
为 Flask API 端点提供智能缓存能力

特性:
- 自动生成缓存键（基于方法、URL、参数）
- 可配置的缓存过期时间
- 支持缓存失效触发器
- 线程安全的内存缓存
"""

import functools
import hashlib
import json
from datetime import datetime, timedelta
from threading import RLock
from flask import request

from utils import get_logger

logger = get_logger(__name__)


class APICacheManager:
    """API 缓存管理器"""
    
    def __init__(self, max_size=1000, default_ttl=300):
        """
        初始化缓存管理器
        
        Args:
            max_size: 最大缓存条数
            default_ttl: 默认过期时间（秒）
        """
        self.cache = {}
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.lock = RLock()
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0
        }
    
    def _generate_key(self, endpoint, params):
        """生成缓存键"""
        key_parts = [endpoint]
        
        # 排序参数以保证一致性
        if isinstance(params, dict):
            for k, v in sorted(params.items()):
                key_parts.append(f"{k}={v}")
        
        key_str = "|".join(key_parts)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def get(self, endpoint, params):
        """获取缓存值"""
        key = self._generate_key(endpoint, params)
        
        with self.lock:
            if key in self.cache:
                value, expire_at = self.cache[key]
                
                # 检查是否过期
                if datetime.now() < expire_at:
                    self.stats['hits'] += 1
                    logger.debug(f"缓存命中: {endpoint}")
                    return value
                else:
                    # 清理过期缓存
                    del self.cache[key]
            
            self.stats['misses'] += 1
            return None
    
    def set(self, endpoint, params, value, ttl=None):
        """设置缓存值"""
        key = self._generate_key(endpoint, params)
        ttl = ttl or self.default_ttl
        expire_at = datetime.now() + timedelta(seconds=ttl)
        
        with self.lock:
            # 检查缓存大小，必要时清理过期项
            if len(self.cache) >= self.max_size:
                self._evict_expired()
                if len(self.cache) >= self.max_size * 0.9:
                    self._evict_oldest()
            
            self.cache[key] = (value, expire_at)
            logger.debug(f"缓存设置: {endpoint} (TTL={ttl}s)")
    
    def clear(self):
        """清空所有缓存"""
        with self.lock:
            count = len(self.cache)
            self.cache.clear()
            logger.info(f"缓存已清空，共清理 {count} 条记录")
    
    def clear_by_pattern(self, pattern):
        """按模式清理缓存 (支持通配符)"""
        with self.lock:
            # 这里简化处理，实际可以用正则
            keys_to_delete = [k for k in self.cache.keys() if pattern in k]
            for k in keys_to_delete:
                del self.cache[k]
            logger.info(f"按模式清理缓存: {pattern} ({len(keys_to_delete)} 条)")
    
    def _evict_expired(self):
        """清理所有过期的缓存"""
        now = datetime.now()
        expired_keys = [
            k for k, (_, expire_at) in self.cache.items()
            if now >= expire_at
        ]
        for k in expired_keys:
            del self.cache[k]
        if expired_keys:
            logger.debug(f"清理过期缓存: {len(expired_keys)} 条")
    
    def _evict_oldest(self):
        """删除最早的缓存"""
        if self.cache:
            oldest_key = min(self.cache.keys())
            del self.cache[oldest_key]
            self.stats['evictions'] += 1
            logger.debug(f"驱逐最旧缓存: {len(self.cache) + 1} -> {len(self.cache)}")
    
    def get_stats(self):
        """获取缓存统计信息"""
        total_requests = self.stats['hits'] + self.stats['misses']
        hit_rate = (self.stats['hits'] / total_requests * 100) if total_requests > 0 else 0
        
        return {
            'hits': self.stats['hits'],
            'misses': self.stats['misses'],
            'evictions': self.stats['evictions'],
            'hit_rate': round(hit_rate, 2),
            'cache_size': len(self.cache),
            'max_size': self.max_size
        }


# 全局缓存管理器实例
_cache_manager = APICacheManager(max_size=500, default_ttl=300)


def api_cache(ttl=300, key_params=None):
    """
    API 缓存装饰器
    
    Args:
        ttl: 缓存过期时间（秒）
        key_params: 用于生成缓存键的参数列表 (None 表示使用所有参数)
    
    示例:
        @app.route('/api/recommendations/<type>')
        @api_cache(ttl=600, key_params=['type'])
        def get_recommendations(type):
            ...
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # 获取当前请求的端点和参数
            endpoint = request.endpoint or func.__name__
            
            # 构建缓存参数
            if key_params:
                params = {k: request.args.get(k) or kwargs.get(k) for k in key_params}
            else:
                params = dict(request.args)
            
            # 尝试从缓存获取
            cached_value = _cache_manager.get(endpoint, params)
            if cached_value is not None:
                return cached_value
            
            # 执行函数
            result = func(*args, **kwargs)
            
            # 存储到缓存
            _cache_manager.set(endpoint, params, result, ttl=ttl)
            
            return result
        
        return wrapper
    
    return decorator


def clear_api_cache(pattern=None):
    """
    清理 API 缓存
    
    Args:
        pattern: 清理包含此模式的缓存，None 表示清理所有
    """
    if pattern is None:
        _cache_manager.clear()
    else:
        _cache_manager.clear_by_pattern(pattern)


def get_cache_stats():
    """获取缓存统计信息"""
    return _cache_manager.get_stats()


def register_cache_routes(app):
    """注册缓存管理路由"""
    
    @app.route('/api/cache/stats', methods=['GET'])
    def cache_stats():
        """获取缓存统计"""
        return {
            'code': 200,
            'data': get_cache_stats()
        }
    
    @app.route('/api/cache/clear', methods=['POST'])
    def cache_clear():
        """清理缓存"""
        pattern = request.json.get('pattern') if request.json else None
        clear_api_cache(pattern)
        return {
            'code': 200,
            'message': f'缓存已清理 (pattern={pattern})'
        }
    
    logger.info("✅ 缓存管理路由已注册")
