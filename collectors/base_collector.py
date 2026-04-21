"""
采集器基类 - collectors/base_collector.py
定义所有采集器的公共接口和工具方法
"""

import time
import json
import hashlib
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_logger, retry, SimpleCache

logger = get_logger(__name__)


class BaseCollector(ABC):
    """采集器基类"""
    
    def __init__(self, cache_ttl=300):
        """
        Args:
            cache_ttl: 缓存有效期（秒），默认5分钟
        """
        self.cache = SimpleCache(ttl=cache_ttl)
        self.session = None
    
    @abstractmethod
    def collect(self, **kwargs):
        """
        采集数据（子类必须实现）
        Returns:
            dict: 采集的数据
        """
        pass
    
    @abstractmethod
    def get_data_source_name(self):
        """
        获取数据源名称
        Returns:
            str: 数据源名称
        """
        pass
    
    def get_cache_key(self, **kwargs):
        """
        生成缓存键
        Args:
            **kwargs: 参数
        Returns:
            str: 缓存键
        """
        key_str = f"{self.get_data_source_name()}_{json.dumps(kwargs, sort_keys=True)}"
        return hashlib.md5(key_str.encode()).hexdigest()
    
    @retry(max_attempts=3, delay=1, backoff=2)
    def collect_with_cache(self, force_refresh=False, **kwargs):
        """
        带缓存的采集
        Args:
            force_refresh: 是否强制刷新
            **kwargs: 采集参数
        Returns:
            dict: 采集的数据
        """
        cache_key = self.get_cache_key(**kwargs)
        
        if not force_refresh:
            cached_data = self.cache.get(cache_key)
            if cached_data is not None:
                logger.debug(f"使用缓存数据: {cache_key}")
                return cached_data
        
        data = self.collect(**kwargs)
        
        if data:
            self.cache.set(cache_key, data)
        
        return data
    
    def log_collect_start(self, **kwargs):
        """记录采集开始日志"""
        logger.info(f"开始采集 {self.get_data_source_name()} 数据: {kwargs}")
    
    def log_collect_end(self, data_count=0):
        """记录采集结束日志"""
        logger.info(f"采集完成 {self.get_data_source_name()}，共 {data_count} 条数据")
    
    def log_collect_error(self, error):
        """记录采集错误日志"""
        logger.error(f"采集 {self.get_data_source_name()} 失败: {error}")
    
    def format_date(self, date_input):
        """
        格式化日期
        Args:
            date_input: 日期（字符串、datetime、date）
        Returns:
            str: YYYY-MM-DD格式的日期字符串
        """
        if isinstance(date_input, str):
            return date_input
        elif isinstance(date_input, datetime):
            return date_input.strftime('%Y-%m-%d')
        elif hasattr(date_input, 'strftime'):
            return date_input.strftime('%Y-%m-%d')
        else:
            return str(date_input)
    
    def parse_date(self, date_str):
        """
        解析日期字符串
        Args:
            date_str: YYYY-MM-DD格式的日期字符串
        Returns:
            date: date对象
        """
        from datetime import date
        if isinstance(date_str, str):
            parts = date_str.split('-')
            if len(parts) == 3:
                return date(int(parts[0]), int(parts[1]), int(parts[2]))
        return date_str
    
    def safe_get(self, data, keys, default=None):
        """
        安全地从嵌套字典中获取值
        Args:
            data: 字典
            keys: 键路径，可以是列表或字符串（用.分隔）
            default: 默认值
        Returns:
            any: 获取的值
        """
        if isinstance(keys, str):
            keys = keys.split('.')
        
        current = data
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key)
                if current is None:
                    return default
            else:
                return default
        
        return current if current is not None else default
    
    def is_valid_data(self, data, required_fields):
        """
        验证数据是否有效
        Args:
            data: 数据字典
            required_fields: 必需字段列表
        Returns:
            bool: 是否有效
        """
        if not data or not isinstance(data, dict):
            return False
        
        for field in required_fields:
            if field not in data or data[field] is None:
                return False
        
        return True
    
    def normalize_code(self, code, market='A'):
        """
        标准化股票代码
        Args:
            code: 原始代码
            market: 市场（A/H/US）
        Returns:
            str: 标准化后的代码
        """
        code = str(code).strip().upper()
        
        if market == 'A':
            # A股代码处理
            if '.' not in code:
                if code.startswith('6'):
                    code = f"{code}.SH"
                elif code.startswith(('0', '3')):
                    code = f"{code}.SZ"
        elif market == 'H':
            # 港股代码处理
            if '.' not in code:
                code = f"{code}.HK"
        elif market == 'US':
            # 美股代码处理
            code = code.split('.')[0]
        
        return code
    
    def close(self):
        """关闭资源"""
        if self.session:
            self.session.close()
            self.session = None