"""
API模块初始化 - api/__init__.py
导出所有API路由注册函数
"""

from api.dashboard import register_dashboard_routes
from api.recommendations import register_recommendations_routes
from api.warnings import register_warnings_routes
from api.holdings import register_holdings_routes
from api.reviews import register_reviews_routes
from api.routes import register_config_routes
from api.model import register_model_routes
from api.logs import register_logs_routes
from api.backfill import register_backfill_routes

__all__ = [
    'register_dashboard_routes',
    'register_recommendations_routes', 
    'register_warnings_routes',
    'register_holdings_routes',
    'register_reviews_routes',
    'register_config_routes',
    'register_model_routes',
    'register_logs_routes',
    'register_backfill_routes'
]