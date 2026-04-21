"""
推荐模块初始化 - recommenders/__init__.py
导出所有推荐引擎类
"""

from recommenders.base_recommender import BaseRecommender
from recommenders.stock_recommender import StockRecommender
from recommenders.fund_recommender import FundRecommender
from recommenders.etf_recommender import ETFRecommender
from recommenders.gold_recommender import GoldRecommender

__all__ = [
    'BaseRecommender',
    'StockRecommender',
    'FundRecommender',
    'ETFRecommender',
    'GoldRecommender'
]