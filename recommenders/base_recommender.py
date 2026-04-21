"""
推荐引擎基类 - recommenders/base_recommender.py
定义推荐引擎的公共接口
"""

from abc import ABC, abstractmethod
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_logger

logger = get_logger(__name__)


class BaseRecommender(ABC):
    """推荐引擎基类"""
    
    def __init__(self):
        self.cache = {}
    
    @abstractmethod
    def get_recommendations(self, limit=20):
        """
        获取推荐列表
        Args:
            limit: 推荐数量
        Returns:
            list: 推荐列表
        """
        pass
    
    @abstractmethod
    def get_asset_type(self):
        """
        获取资产类型
        Returns:
            str: 资产类型
        """
        pass
    
    def filter_by_volatility(self, items, max_volatility=0.5):
        """
        按波动率过滤
        Args:
            items: 待过滤列表
            max_volatility: 最大波动率
        Returns:
            list: 过滤后的列表
        """
        return [item for item in items if item.get('volatility', 0) <= max_volatility]
    
    def filter_by_market_cap(self, items, min_market_cap=0):
        """
        按市值过滤
        """
        return [item for item in items if item.get('market_cap', 0) >= min_market_cap]
    
    def sort_by_score(self, items):
        """
        按综合评分排序 (支持 total_score 和 score 两种 key)
        """
        return sorted(items, key=lambda x: x.get('total_score', x.get('score', 0)), reverse=True)
    
    def add_rank(self, items):
        """
        添加排名
        """
        for i, item in enumerate(items):
            item['rank'] = i + 1
        return items
    
    def get_recommendation_summary(self, recommendations):
        """
        获取推荐摘要
        """
        if not recommendations:
            return "暂无推荐"
        
        top3 = recommendations[:3]
        summary = f"推荐TOP {len(recommendations)} 个标的，其中：\n"
        for rec in top3:
            summary += f"  {rec['rank']}. {rec.get('name', rec.get('code'))} - 评分: {rec.get('total_score', 0)}\n"
        
        return summary