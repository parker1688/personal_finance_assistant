"""
Flask主入口 - app.py
个人AI理财助手Web应用主程序 - 优化版
"""

import os
import sys
import logging
from contextlib import contextmanager
from datetime import datetime
from flask import Flask, render_template, send_from_directory, jsonify, request
from sqlalchemy import text

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import SECRET_KEY, DEBUG, HOST, PORT, SQLITE_PATH
from models import init_database, get_session
from utils import get_logger, ensure_dir

# 初始化日志
logger = get_logger(__name__)
logging.getLogger('werkzeug').setLevel(logging.WARNING)

# CORS配置 - 从环境变量读取允许的源
ALLOWED_ORIGINS = os.environ.get('ALLOWED_ORIGINS', 'http://localhost:5000,http://localhost:3000').split(',')

# 创建Flask应用
app = Flask(__name__)
app.secret_key = SECRET_KEY
app.config['JSON_AS_ASCII'] = False

# 确保必要目录存在
ensure_dir('logs')
ensure_dir(os.path.dirname(SQLITE_PATH))

# 初始化数据库
try:
    init_database()
    logger.info("数据库初始化成功")
except Exception as e:
    logger.error(f"数据库初始化失败: {e}")


# ==================== 数据库会话管理 ====================

@contextmanager
def session_scope():
    """提供事务作用域的会话"""
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


# ==================== CORS支持 ====================

@app.after_request
def add_cors_headers(response):
    """添加CORS头 - 仅允许配置的源"""
    origin = request.headers.get('Origin')
    
    # 检查源是否在允许列表中
    if origin and origin in ALLOWED_ORIGINS:
        response.headers['Access-Control-Allow-Origin'] = origin
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        response.headers['Access-Control-Max-Age'] = '3600'
        response.headers['Access-Control-Allow-Credentials'] = 'true'
    
    return response


# ==================== 健康检查 ====================

@app.route('/health', methods=['GET'])
def health_check():
    """健康检查端点 - 支持Kubernetes健康检查"""
    try:
        # 检查数据库连接
        with session_scope() as session:
            session.execute(text('SELECT 1'))
        db_status = 'healthy'
        http_code = 200
    except Exception as e:
        db_status = f'unhealthy: {type(e).__name__}'
        http_code = 503
        logger.warning(f"健康检查失败: {e}")
    
    return jsonify({
        'status': 'ok' if http_code == 200 else 'degraded',
        'timestamp': datetime.now().isoformat(),
        'components': {
            'database': db_status,
            'app': 'running'
        },
        'version': '2.0.0'
    }), http_code


@app.route('/ready', methods=['GET'])
def ready_check():
    """就绪检查端点（用于容器化部署 - K8s readinessProbe）"""
    try:
        with session_scope() as session:
            session.execute(text('SELECT 1'))
        return jsonify({'status': 'ready', 'timestamp': datetime.now().isoformat()}), 200
    except Exception as e:
        logger.error(f"就绪检查失败: {e}")
        return jsonify({
            'status': 'not_ready',
            'reason': str(e),
            'timestamp': datetime.now().isoformat()
        }), 503


# ==================== 页面路由 ====================

@app.route('/')
def dashboard():
    """仪表盘首页"""
    return render_template('dashboard.html', active_page='dashboard')


@app.route('/recommendations')
def recommendations():
    """投资推荐页面"""
    return render_template('recommendations.html', active_page='recommendations')


@app.route('/warnings')
def warnings():
    """风险预警页面"""
    return render_template('warnings.html', active_page='warnings')


@app.route('/reviews')
def reviews():
    """复盘分析页面"""
    return render_template('reviews.html', active_page='reviews')


@app.route('/holdings')
def holdings():
    """持仓管理页面"""
    return render_template('holdings.html', active_page='holdings')


@app.route('/config')
def config_page():
    """系统配置页面"""
    return render_template('config.html', active_page='config')


@app.route('/model-monitor')
def model_monitor():
    """模型监控页面"""
    return render_template('model_monitor.html', active_page='model')


@app.route('/logs')
def logs_page():
    """系统日志页面"""
    return render_template('logs.html', active_page='logs')


@app.route('/data-management')
def data_management_page():
    """数据管理页面"""
    return render_template('data_management.html', active_page='data_management')


@app.route('/reflection')
def reflection():
    """反思报告页面"""
    return render_template('reflection.html', active_page='reflection')


# ==================== 静态文件服务 ====================

@app.route('/static/<path:filename>')
def serve_static(filename):
    """提供静态文件服务"""
    return send_from_directory('static', filename)


# ==================== 错误处理 ====================

@app.errorhandler(404)
def not_found(error):
    """404错误处理"""
    return render_template('404.html'), 404


@app.errorhandler(500)
def internal_error(error):
    """500错误处理"""
    logger.error(f"内部错误: {error}")
    return render_template('500.html'), 500


# ==================== 上下文处理器 ====================

@app.context_processor
def inject_now():
    """注入当前时间到模板"""
    return {'now': datetime.now()}


# ==================== API路由注册（带错误处理） ====================

def register_api_routes(app):
    """注册API路由（带错误处理）"""
    routes_to_register = [
        ('api.dashboard', 'register_dashboard_routes'),
        ('api.recommendations', 'register_recommendations_routes'),
        ('api.warnings', 'register_warnings_routes'),
        ('api.holdings', 'register_holdings_routes'),
        ('api.reviews', 'register_reviews_routes'),
        ('api.routes', 'register_config_routes'),
        ('api.model', 'register_model_routes'),
        ('api.logs', 'register_logs_routes'),
        ('api.backfill', 'register_backfill_routes'),
    ]
    
    for module_name, func_name in routes_to_register:
        try:
            module = __import__(module_name, fromlist=[func_name])
            register_func = getattr(module, func_name)
            register_func(app)
            logger.info(f"✅ API路由注册成功: {module_name}.{func_name}")
        except ImportError as e:
            logger.warning(f"⚠️ API模块导入失败 {module_name}: {e}")
        except Exception as e:
            logger.error(f"❌ API路由注册失败 {module_name}: {e}")


# ==================== 启动前初始化 ====================

def init_app():
    """应用初始化"""
    logger.info("=" * 50)
    logger.info("个人AI理财助手启动中...")
    logger.info("=" * 50)
    
    # 注册API路由
    register_api_routes(app)
    
    # 启动定时任务
    # 只在“实际服务进程”中初始化一次：
    # - 未启用 reloader 时，应直接启动后台任务
    # - 启用 reloader 时，仅在子进程中启动
    use_reloader = os.environ.get('USE_RELOADER', 'false').lower() == 'true'
    is_reloader_child = os.environ.get('WERKZEUG_RUN_MAIN') == 'true'
    should_start_background = (not use_reloader) or is_reloader_child

    if should_start_background:
        try:
            from scheduler import init_scheduler, start_auto_backfill_current_year_async
            init_scheduler()
            logger.info("✅ 定时任务初始化成功")

            # 默认不在Web进程启动时触发自动补采，避免第三方库并发导致进程崩溃。
            # 如需启用，可设置环境变量 AUTO_BACKFILL_ON_STARTUP=true。
            auto_backfill_on_startup = os.environ.get('AUTO_BACKFILL_ON_STARTUP', 'false').lower() == 'true'
            if auto_backfill_on_startup:
                start_auto_backfill_current_year_async()
                logger.info("✅ 已触发当年数据自动补采（后台）")
            else:
                logger.info("ℹ️ 已跳过启动自动补采（AUTO_BACKFILL_ON_STARTUP=false）")
        except ImportError as e:
            logger.warning(f"⚠️ 定时任务模块导入失败: {e}")
        except Exception as e:
            logger.error(f"❌ 定时任务初始化失败: {e}")
    else:
        logger.info("Reloader 父进程，跳过定时任务初始化")
    
    logger.info("=" * 50)
    logger.info(f"应用启动完成！")
    logger.info(f"Web访问: http://{HOST}:{PORT}")
    logger.info(f"健康检查: http://{HOST}:{PORT}/health")
    logger.info("=" * 50)


# 初始化应用（注册路由等）- 确保 gunicorn 也能加载
init_app()


# ==================== 启动入口 ====================

if __name__ == '__main__':
    # 无终端后台启动时默认关闭 reloader，可通过 USE_RELOADER=true 显式开启
    use_reloader = os.environ.get('USE_RELOADER', 'false').lower() == 'true'
    app.run(host=HOST, port=PORT, debug=DEBUG, use_reloader=use_reloader)