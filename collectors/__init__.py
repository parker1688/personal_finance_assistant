"""
采集模块初始化 - collectors/__init__.py
导出所有采集器类
"""

from collectors.base_collector import BaseCollector
from collectors.stock_collector import StockCollector
from collectors.fund_collector import FundCollector
from collectors.news_collector import NewsCollector
from collectors.macro_collector import MacroCollector

__all__ = [
    'BaseCollector',
    'StockCollector',
    'FundCollector',
    'NewsCollector',
    'MacroCollector'
]