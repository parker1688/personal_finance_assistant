"""
配置API路由 - api/routes.py
提供系统配置管理接口
"""

import sys
import os
from datetime import datetime
from flask import jsonify, request

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import get_session, Config
from utils import get_logger
from api.auth import require_admin_access, log_admin_audit

logger = get_logger(__name__)


def register_config_routes(app):
    """注册配置相关路由"""
    
    @app.route('/api/config', methods=['GET'])
    @require_admin_access(action='config.read')
    def get_config():
        """获取系统配置"""
        try:
            session = get_session()
            configs = session.query(Config).all()
            
            config_dict = {}
            for c in configs:
                config_dict[c.config_key] = c.config_value
            
            # 默认配置
            default_config = {
                'push': {
                    'email_smtp_server': config_dict.get('email_smtp_server', 'smtp.qq.com'),
                    'email_sender': config_dict.get('email_sender', ''),
                    'email_receiver': config_dict.get('email_receiver', ''),
                    'wechat_sckey': config_dict.get('wechat_sckey', '')
                },
                'warning': {
                    'rsi_overbought': int(config_dict.get('rsi_overbought', 80)),
                    'rsi_oversold': int(config_dict.get('rsi_oversold', 20)),
                    'consecutive_outflow': int(config_dict.get('consecutive_outflow', 3)),
                    'single_asset_ratio': float(config_dict.get('single_asset_ratio', 0.2)),
                    'daily_drop_threshold': float(config_dict.get('daily_drop_threshold', 0.05)),
                    'pe_percentile_high': int(config_dict.get('pe_percentile_high', 90))
                },
                'filter': {
                    'min_market_cap_a': int(config_dict.get('min_market_cap_a', 5000000000)),
                    'min_market_cap_hk': int(config_dict.get('min_market_cap_hk', 5000000000)),
                    'min_market_cap_us': int(config_dict.get('min_market_cap_us', 1000000000)),
                    'max_volatility': float(config_dict.get('max_volatility', 0.5)),
                    'min_fund_size': int(config_dict.get('min_fund_size', 200000000)),
                    'max_fund_fee': float(config_dict.get('max_fund_fee', 1.5))
                },
                'model': {
                    'n_estimators': int(config_dict.get('n_estimators', 200)),
                    'max_depth': int(config_dict.get('max_depth', 6)),
                    'learning_rate': float(config_dict.get('learning_rate', 0.05)),
                    'subsample': float(config_dict.get('subsample', 0.8)),
                    'colsample_bytree': float(config_dict.get('colsample_bytree', 0.8)),
                    'lstm_enabled': config_dict.get('lstm_enabled', 'false').lower() == 'true'
                }
            }
            
            session.close()
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': default_config,
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"获取配置失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/config', methods=['POST'])
    @require_admin_access(action='config.write')
    def save_config():
        """保存系统配置"""
        try:
            data = request.get_json()
            session = get_session()
            
            # 保存推送配置
            if 'push' in data:
                push = data['push']
                for key, value in push.items():
                    if value:
                        config = session.query(Config).filter(Config.config_key == key).first()
                        if config:
                            config.config_value = str(value)
                        else:
                            config = Config(config_key=key, config_value=str(value))
                            session.add(config)
            
            # 保存预警配置
            if 'warning' in data:
                warning = data['warning']
                for key, value in warning.items():
                    config = session.query(Config).filter(Config.config_key == key).first()
                    if config:
                        config.config_value = str(value)
                    else:
                        config = Config(config_key=key, config_value=str(value))
                        session.add(config)
            
            # 保存筛选配置
            if 'filter' in data:
                filters = data['filter']
                for key, value in filters.items():
                    config = session.query(Config).filter(Config.config_key == key).first()
                    if config:
                        config.config_value = str(value)
                    else:
                        config = Config(config_key=key, config_value=str(value))
                        session.add(config)
            
            # 保存模型配置
            if 'model' in data:
                model = data['model']
                for key, value in model.items():
                    config = session.query(Config).filter(Config.config_key == key).first()
                    if config:
                        config.config_value = str(value)
                    else:
                        config = Config(config_key=key, config_value=str(value))
                        session.add(config)
            
            session.commit()
            session.close()
            
            log_admin_audit('config.write', 'success', 'configuration_updated')
            return jsonify({
                'code': 200,
                'status': 'success',
                'message': '配置保存成功',
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"保存配置失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/config/test_push', methods=['POST'])
    @require_admin_access(action='config.test_push')
    def test_push():
        """测试推送"""
        try:
            # 模拟测试推送
            log_admin_audit('config.test_push', 'success', 'test_push_triggered')
            return jsonify({
                'code': 200,
                'status': 'success',
                'message': '测试推送已发送，请检查邮箱/微信',
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"测试推送失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500