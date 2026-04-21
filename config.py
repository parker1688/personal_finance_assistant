"""
配置文件 - config.py
个人AI理财助手系统配置 - 优化版
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

# ==================== 基础配置 ====================

# 项目根目录
BASE_DIR = Path(__file__).resolve().parent

# 数据目录统一配置
DATA_DIR = BASE_DIR / 'data'
MODELS_DIR = DATA_DIR / 'models'
DATABASE_DIR = DATA_DIR / 'database'
LOGS_DIR = BASE_DIR / 'logs'
REPORTS_DIR = DATA_DIR / 'reports'
PROGRESS_DIR = DATA_DIR / 'collect_progress'
CACHE_DIR = DATA_DIR / 'cache'
BACKUP_DIR = DATA_DIR / 'backup'
TEMPLATES_DIR = BASE_DIR / 'templates'
STATIC_DIR = BASE_DIR / 'static'

# 确保所有目录存在
for dir_path in [DATA_DIR, MODELS_DIR, DATABASE_DIR, LOGS_DIR, 
                  REPORTS_DIR, PROGRESS_DIR, CACHE_DIR, BACKUP_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# ==================== 数据库配置（支持 SQLite 和 MySQL） ====================

# 数据库类型选择：'sqlite' 或 'mysql'
DB_TYPE = os.environ.get('DB_TYPE', 'sqlite').lower()

# MySQL 配置（当 DB_TYPE = 'mysql' 时使用）
MYSQL_HOST = os.environ.get('MYSQL_HOST', 'localhost')
MYSQL_PORT = int(os.environ.get('MYSQL_PORT', 3306))
MYSQL_USER = os.environ.get('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', '')
MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE', 'finance')

# SQLite 配置
SQLITE_PATH = DATABASE_DIR / 'finance.db'

# 根据 DB_TYPE 构建 DATABASE_URL
if DB_TYPE == 'mysql':
    # 需要安装 pymysql: pip install pymysql
    DATABASE_URL = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}?charset=utf8mb4"
else:
    DATABASE_URL = f"sqlite:///{SQLITE_PATH}"

# 数据库连接池配置（仅 MySQL 生效）
DB_POOL_SIZE = int(os.environ.get('DB_POOL_SIZE', 10))
DB_POOL_MAX_OVERFLOW = int(os.environ.get('DB_POOL_MAX_OVERFLOW', 20))
DB_POOL_RECYCLE = int(os.environ.get('DB_POOL_RECYCLE', 3600))

# Flask配置
SECRET_KEY = os.environ.get('SECRET_KEY', 'your-secret-key-here-change-in-production')
DEBUG = os.environ.get('DEBUG', 'True').lower() == 'true'
HOST = os.environ.get('HOST', '0.0.0.0')
PORT = int(os.environ.get('PORT', 8080))

# ==================== 数据文件路径统一配置 ====================

# 股票基础数据
STOCK_BASIC_FILE = DATA_DIR / 'stock_basic.csv'
LEGACY_STOCK_POOL_FILE = DATA_DIR / 'all_stocks.csv'


def resolve_data_file(preferred_file, *fallback_files):
    """返回首个存在的数据文件路径；若都不存在，则返回首选路径。"""
    candidates = [preferred_file, *fallback_files]
    for candidate in candidates:
        if not candidate:
            continue
        path = candidate if isinstance(candidate, Path) else Path(candidate)
        if not path.is_absolute():
            path = BASE_DIR / path
        if os.path.exists(path):
            return path

    default_path = preferred_file if isinstance(preferred_file, Path) else Path(preferred_file)
    if not default_path.is_absolute():
        default_path = BASE_DIR / default_path
    return default_path

# 历史行情数据
HISTORICAL_A_STOCK_FILE = DATA_DIR / 'historical_a_stock.csv'
HISTORICAL_HK_STOCK_FILE = DATA_DIR / 'historical_hk_stock.csv'
HISTORICAL_US_STOCK_FILE = DATA_DIR / 'historical_us_stock.csv'

# 资金流向数据
MONEYFLOW_FILE = DATA_DIR / 'moneyflow_all.csv'
NORTH_MONEY_FILE = DATA_DIR / 'north_money_all.csv'
MARGIN_FILE = DATA_DIR / 'margin_all.csv'

# 市场数据
TOP_LIST_FILE = DATA_DIR / 'top_list.csv'
DAILY_BASIC_FILE = DATA_DIR / 'daily_basic.csv'
FINANCIAL_INDICATOR_FILE = DATA_DIR / 'financial_indicator.csv'

# 新闻和研报
NEWS_FILE = DATA_DIR / 'news_all.csv'
RESEARCH_REPORT_FILE = DATA_DIR / 'research_report.csv'
ANNOUNCEMENT_FILE = DATA_DIR / 'announcement.csv'

# 缓存文件
STOCK_POOL_CACHE_FILE = DATA_DIR / 'stock_pool_cache.json'
FUND_POOL_CACHE_FILE = DATA_DIR / 'fund_pool_cache.json'

# 进度文件
COLLECT_PROGRESS_FILE = PROGRESS_DIR / 'collect_progress.json'
NORTH_MONEY_PROGRESS_FILE = PROGRESS_DIR / 'north_money_progress.json'
MARGIN_PROGRESS_FILE = PROGRESS_DIR / 'margin_progress.json'
MONEYFLOW_PROGRESS_FILE = PROGRESS_DIR / 'moneyflow_progress.json'
FINANCIAL_PROGRESS_FILE = PROGRESS_DIR / 'financial_progress.json'
NEWS_PROGRESS_FILE = PROGRESS_DIR / 'news_progress.json'

# 模型文件路径
SHORT_TERM_MODEL_FILE = MODELS_DIR / 'short_term_model.pkl'
MEDIUM_TERM_MODEL_FILE = MODELS_DIR / 'medium_term_model.pkl'
LONG_TERM_MODEL_FILE = MODELS_DIR / 'long_term_model.pkl'
GOLD_MODEL_FILE = MODELS_DIR / 'gold_model.pkl'
SILVER_MODEL_FILE = MODELS_DIR / 'silver_model.pkl'

# 训练统计文件
TRAINING_STATS_FILE = MODELS_DIR / 'training_stats.json'

# 日志文件
LOG_FILE = LOGS_DIR / 'app.log'

# ==================== 数据源配置 ====================

# Tushare配置（A股数据，需要注册获取token）
# 注册地址：https://tushare.pro/register
TUSHARE_TOKEN = os.environ.get('TUSHARE_TOKEN', "87985a35b6278f37cc3f854b012aa1d9da716a1d9601094c24bc2b9b")

# NewsAPI配置（新闻数据，需要注册获取key）
# 注册地址：https://newsapi.org/register
NEWSAPI_KEY = os.environ.get('NEWSAPI_KEY', '6f786802e88d4dcb9147bc29a6b486f7')
NEWSAPI_ENABLED = os.environ.get('NEWSAPI_ENABLED', 'True').lower() == 'true'

# 新浪财经API（免费，无需key）
SINA_API_BASE = 'http://hq.sinajs.cn/list'

# Yahoo Finance（免费，无需key，用于港股/美股/黄金/白银）
YAHOO_FINANCE_ENABLED = os.environ.get('YAHOO_FINANCE_ENABLED', 'True').lower() == 'true'

# 数据采集配置
REQUEST_DELAY = 0.3          # 请求间隔（秒）
BATCH_SIZE = 100             # 批量插入大小
CACHE_TTL = 300              # 缓存有效期（秒）
MAX_RETRIES = 3              # 最大重试次数
RETRY_DELAY = 1              # 重试初始延迟（秒）
RETRY_BACKOFF = 2            # 重试退避倍数

# 数据保留天数
RAW_DATA_RETENTION_DAYS = 365        # 原始数据保留1年
BACKUP_RETENTION_DAYS = 30           # 备份保留30天
SNAPSHOT_RETENTION_DAYS = 365        # 持仓快照保留1年

# 数据采集频率（秒）
COLLECT_REAL_TIME_INTERVAL = 300     # 5分钟
COLLECT_DAILY_TIME = os.environ.get('COLLECT_DAILY_TIME', '15:10')         # 收盘后采集时间
COLLECT_FUND_TIME = '20:30'          # 基金净值采集时间
COLLECT_NEWS_INTERVAL = 14400        # 4小时

# 交易日操作时点配置
MARKET_ACTION_CUTOFF_TIME = os.environ.get('MARKET_ACTION_CUTOFF_TIME', '15:00')
DAILY_SNAPSHOT_TIME = os.environ.get('DAILY_SNAPSHOT_TIME', '15:05')
DAILY_RECOMMENDATION_TIME = os.environ.get('DAILY_RECOMMENDATION_TIME', '18:00')
FUTURE_SIGNAL_ALERT_MORNING_TIME = os.environ.get('FUTURE_SIGNAL_ALERT_MORNING_TIME', '09:35')
FUTURE_SIGNAL_ALERT_PRE_CLOSE_TIME = os.environ.get('FUTURE_SIGNAL_ALERT_PRE_CLOSE_TIME', '14:40')

# 多市场时段配置（可按本地部署环境覆盖）
HK_MARKET_OPEN_TIME = os.environ.get('HK_MARKET_OPEN_TIME', '09:30')
HK_MARKET_CLOSE_TIME = os.environ.get('HK_MARKET_CLOSE_TIME', '16:00')
HK_MARKET_ACTION_CUTOFF_TIME = os.environ.get('HK_MARKET_ACTION_CUTOFF_TIME', '15:40')
US_MARKET_OPEN_TIME = os.environ.get('US_MARKET_OPEN_TIME', '21:30')
US_MARKET_CLOSE_TIME = os.environ.get('US_MARKET_CLOSE_TIME', '04:00')
US_MARKET_ACTION_CUTOFF_TIME = os.environ.get('US_MARKET_ACTION_CUTOFF_TIME', '03:30')

# 节假日白名单（逗号分隔，格式 YYYY-MM-DD）
CN_MARKET_HOLIDAYS = {d.strip() for d in os.environ.get('CN_MARKET_HOLIDAYS', '').split(',') if d.strip()}
HK_MARKET_HOLIDAYS = {d.strip() for d in os.environ.get('HK_MARKET_HOLIDAYS', '').split(',') if d.strip()}
US_MARKET_HOLIDAYS = {d.strip() for d in os.environ.get('US_MARKET_HOLIDAYS', '').split(',') if d.strip()}

# ==================== 股票池配置 ====================

# 全市场采集开关
USE_FULL_MARKET = os.environ.get('USE_FULL_MARKET', 'True').lower() == 'true'

# 各市场最大采集数量（0 表示不设上限，按数据源可用范围全量放开）
MAX_A_STOCKS = int(os.environ.get('MAX_A_STOCKS', 0))
MAX_HK_STOCKS = int(os.environ.get('MAX_HK_STOCKS', 0))
MAX_US_STOCKS = int(os.environ.get('MAX_US_STOCKS', 0))
MAX_FUNDS = int(os.environ.get('MAX_FUNDS', 0))

# 港股主板代码范围
HK_MAIN_BOARD_MIN = 1
HK_MAIN_BOARD_MAX = 3999

# 股票池缓存有效期（秒）
STOCK_POOL_CACHE_TTL = 7 * 24 * 3600  # 7天

# ==================== 预警阈值配置 ====================

# 技术面阈值
RSI_OVERBOUGHT = 80          # RSI超买阈值（>80触发预警）
RSI_OVERSOLD = 20            # RSI超卖阈值（<20触发预警）
RSI_WARNING_OVERBOUGHT = 75  # RSI关注阈值
RSI_WARNING_OVERSOLD = 25    # RSI关注阈值

# MACD阈值
MACD_DEAD_CROSS_ENABLED = True   # 是否启用MACD死叉预警

# 均线阈值
MA_BREAK_DAYS = 20           # 跌破多少日均线触发预警
MA_CROSS_UP_DAYS = 20        # 均线上穿多少日均线关注

# 资金面阈值
CONSECUTIVE_OUTFLOW_DAYS = 3     # 连续流出天数预警
OUTFLOW_THRESHOLD_RATIO = 0.01   # 流出金额占市值比例阈值（1%）
MAIN_FLOW_THRESHOLD = 50000000   # 主力资金净流出超过5000万预警（元）

# 波动阈值
DAILY_DROP_THRESHOLD = 0.05      # 单日跌幅超过5%预警
DAILY_RISE_THRESHOLD = 0.07      # 单日涨幅超过7%预警
WEEKLY_DROP_THRESHOLD = 0.10     # 周跌幅超过10%预警
MONTHLY_DROP_THRESHOLD = 0.15    # 月跌幅超过15%预警

# 估值阈值
PE_PERCENTILE_HIGH = 90          # PE分位数超过90%预警
PE_PERCENTILE_LOW = 20           # PE分位数低于20%关注
PB_PERCENTILE_HIGH = 90          # PB分位数超过90%预警
PB_PERCENTILE_LOW = 20           # PB分位数低于20%关注

# 组合集中度阈值
SINGLE_ASSET_RATIO = 0.20        # 单资产占总资产比例超过20%预警
SINGLE_INDUSTRY_RATIO = 0.40     # 单行业占比超过40%预警
TOP3_ASSET_RATIO = 0.50          # 前三大资产占比超过50%预警

# 新闻情绪阈值
SENTIMENT_NEGATIVE_THRESHOLD = -0.3   # 情感得分低于-0.3触发预警
SENTIMENT_POSITIVE_THRESHOLD = 0.3    # 情感得分高于0.3关注
SENTIMENT_STRONG_NEGATIVE = -0.5      # 强烈负面情绪阈值

# 预警去重时间（小时）
WARNING_DEDUP_HOURS = 24

# ==================== 推荐筛选规则 ====================

# 市值筛选（单位：元/港元/美元）
MIN_MARKET_CAP_A = 5_000_000_000      # A股最小市值 50亿
MIN_MARKET_CAP_HK = 5_000_000_000     # 港股最小市值 50亿港元
MIN_MARKET_CAP_US = 1_000_000_000     # 美股最小市值 10亿美元
MIN_MARKET_CAP_ETF = 500_000_000      # ETF最小规模 5亿

# 价格筛选
MIN_PRICE_HK = 1.0        # 港股最小股价 1港元
MIN_PRICE_US = 5.0        # 美股最小股价 5美元
MIN_PRICE_A = 2.0         # A股最小股价 2元

# 成交额筛选（单位：元/港元/美元）
MIN_VOLUME_A = 50_000_000        # A股最小日成交额 5000万
MIN_VOLUME_HK = 10_000_000       # 港股最小日成交额 1000万港元
MIN_VOLUME_US = 1_000_000        # 美股最小日成交额 100万美元
MIN_VOLUME_ETF = 10_000_000      # ETF最小日成交额 1000万

# 基金规模筛选（单位：元）
MIN_FUND_SIZE = 200_000_000      # 最小基金规模 2亿

# 基金成立年限（年）
MIN_FUND_YEARS = 3               # 最小成立年限 3年

# 最大费率（%）
MAX_FUND_FEE = 1.5               # 最大管理费率 1.5%
MAX_ETF_FEE = 0.6                # 最大ETF管理费率 0.6%

# ETF跟踪误差阈值（%）
MAX_ETF_TRACKING_ERROR = 2.0     # 最大跟踪误差 2%

# 波动率筛选（年化波动率）
MAX_VOLATILITY = 0.50            # 最大波动率 50%
MAX_VOLATILITY_HK = 0.55         # 港股最大波动率 55%
MAX_VOLATILITY_GOLD = 0.25       # 黄金最大波动率 25%
MAX_VOLATILITY_SILVER = 0.35     # 白银最大波动率 35%
MAX_VOLATILITY_FUND = 0.30       # 基金最大波动率 30%
MAX_VOLATILITY_ETF = 0.35        # ETF最大波动率 35%

# 推荐数量
DEFAULT_RECOMMENDATION_LIMIT = 20

# ==================== 预测模型配置 ====================

# 预测周期（天）
PREDICTION_PERIODS = [5, 20, 60]

# 预测上涨阈值（收益率超过此值才算上涨）
PREDICTION_THRESHOLD = 0.02      # 2%

# 模型参数
MODEL_RETRAIN_DAY = 0            # 每周日重新训练（0=周日）
MODEL_RETRAIN_HOUR = 2           # 凌晨2点训练

# 特征窗口长度（天）
FEATURE_WINDOW = 60              # 使用过去60天数据作为特征

# 训练/验证集分割比例
TRAIN_TEST_SPLIT = 0.8

# XGBoost参数
XGBOOST_PARAMS = {
    'n_estimators': 200,
    'max_depth': 6,
    'learning_rate': 0.05,
    'subsample': 0.8,
    'colsample_bytree': 0.8,
    'reg_lambda': 1.0,
    'reg_alpha': 0.5,
    'random_state': 42
}

# LSTM参数（可选）
LSTM_ENABLED = False             # 是否启用LSTM（需要更多计算资源）
LSTM_HIDDEN_SIZE = 64
LSTM_NUM_LAYERS = 2
LSTM_DROPOUT = 0.2
LSTM_EPOCHS = 100
LSTM_BATCH_SIZE = 32

# 模型版本管理
MAX_MODEL_VERSIONS = 10          # 最多保留模型版本数
AUTO_ACTIVATE_BEST_MODEL = True  # 自动激活验证集准确率最高的模型

# ==================== 推送配置 ====================

# 邮件推送配置
EMAIL_ENABLED = os.environ.get('EMAIL_ENABLED', 'False').lower() == 'true'
EMAIL_SMTP_SERVER = os.environ.get('EMAIL_SMTP_SERVER', 'smtp.qq.com')
EMAIL_SMTP_PORT = int(os.environ.get('EMAIL_SMTP_PORT', 465))
EMAIL_SENDER = os.environ.get('EMAIL_SENDER', '')
EMAIL_PASSWORD = os.environ.get('EMAIL_PASSWORD', '')
EMAIL_RECEIVER = os.environ.get('EMAIL_RECEIVER', '')

# 微信推送配置（SCKEY）
WECHAT_ENABLED = os.environ.get('WECHAT_ENABLED', 'False').lower() == 'true'
WECHAT_SCKEY = os.environ.get('WECHAT_SCKEY', '')  # SCKEY

# 推送去重时间（小时）
PUSH_DEDUP_HOURS = 24

# 推送队列大小
PUSH_QUEUE_SIZE = 100

# ==================== 复盘配置 ====================

# 复盘检查频率（秒）
REVIEW_CHECK_INTERVAL = 3600     # 每小时检查一次

# 复盘报告保留天数
REVIEW_REPORT_RETENTION_DAYS = 90

# 准确率统计周期（天）
ACCURACY_STAT_DAYS = [7, 30, 90, 365]

# 反思学习触发条件
REFLECTION_TRIGGER_ERROR_RATE = 0.4  # 错误率超过40%触发反思

# ==================== 日志配置 ====================

LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
LOG_FILE = LOGS_DIR / 'app.log'
LOG_MAX_BYTES = 10 * 1024 * 1024     # 10MB
LOG_BACKUP_COUNT = 30                # 保留30个备份
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# 控制台日志级别
CONSOLE_LOG_LEVEL = 'INFO'

# ==================== 缓存配置 ====================

# Redis配置（可选）
REDIS_ENABLED = os.environ.get('REDIS_ENABLED', 'False').lower() == 'true'
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB = int(os.environ.get('REDIS_DB', 0))
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', '')

# 本地缓存配置
LOCAL_CACHE_TTL = 300               # 默认缓存5分钟
PRICE_CACHE_TTL = 3600              # 价格缓存1小时
MARKET_DATA_CACHE_TTL = 86400       # 市场数据缓存24小时

# ==================== 辅助函数 ====================

def get_config_summary() -> Dict[str, Any]:
    """获取配置摘要"""
    return {
        'database_path': str(DATABASE_PATH),
        'debug': DEBUG,
        'prediction_periods': PREDICTION_PERIODS,
        'max_volatility': MAX_VOLATILITY,
        'email_enabled': EMAIL_ENABLED,
        'wechat_enabled': WECHAT_ENABLED,
        'tushare_configured': bool(TUSHARE_TOKEN),
        'newsapi_configured': bool(NEWSAPI_KEY),
        'newsapi_enabled': NEWSAPI_ENABLED,
        'use_full_market': USE_FULL_MARKET,
        'redis_enabled': REDIS_ENABLED,
        'lstm_enabled': LSTM_ENABLED
    }


def validate_config() -> Dict[str, Any]:
    """验证配置有效性"""
    errors = []
    warnings = []
    
    # 检查数据库目录
    if not DATABASE_DIR.exists():
        errors.append(f"数据库目录不存在: {DATABASE_DIR}")
    
    # 检查日志目录
    if not LOGS_DIR.exists():
        errors.append(f"日志目录不存在: {LOGS_DIR}")
    
    # 检查阈值合理性
    if RSI_OVERBOUGHT <= RSI_OVERSOLD:
        errors.append("RSI超买阈值必须大于超卖阈值")
    
    if SINGLE_ASSET_RATIO > 1 or SINGLE_ASSET_RATIO < 0:
        errors.append("单资产集中度阈值必须在0-1之间")
    
    if PREDICTION_THRESHOLD < 0 or PREDICTION_THRESHOLD > 0.2:
        warnings.append(f"预测阈值 {PREDICTION_THRESHOLD} 可能不合理，建议在0.01-0.05之间")
    
    # 检查邮箱配置
    if EMAIL_ENABLED:
        if not EMAIL_SENDER:
            errors.append("邮件推送已启用但未配置发件人邮箱")
        if not EMAIL_PASSWORD:
            errors.append("邮件推送已启用但未配置密码")
        if not EMAIL_RECEIVER:
            errors.append("邮件推送已启用但未配置收件人")
    
    # 检查微信配置
    if WECHAT_ENABLED and not WECHAT_SCKEY:
        errors.append("微信推送已启用但未配置SCKEY")
    
    # 警告信息
    if not TUSHARE_TOKEN:
        warnings.append("Tushare token未配置，A股数据采集将受限")
    
    if not NEWSAPI_KEY:
        warnings.append("NewsAPI key未配置，新闻采集将受限")
    
    if not EMAIL_ENABLED and not WECHAT_ENABLED:
        warnings.append("邮件和微信推送均未启用，预警将只能输出到控制台")
    
    if USE_FULL_MARKET and not TUSHARE_TOKEN:
        warnings.append("全市场模式需要Tushare token，否则将使用预设股票池")
    
    return {
        'valid': len(errors) == 0,
        'errors': errors,
        'warnings': warnings
    }


def get_paths() -> Dict[str, str]:
    """获取所有路径配置"""
    return {
        'base_dir': str(BASE_DIR),
        'data_dir': str(DATA_DIR),
        'models_dir': str(MODELS_DIR),
        'database_dir': str(DATABASE_DIR),
        'logs_dir': str(LOGS_DIR),
        'reports_dir': str(REPORTS_DIR),
        'cache_dir': str(CACHE_DIR),
        'backup_dir': str(BACKUP_DIR),
        'templates_dir': str(TEMPLATES_DIR),
        'static_dir': str(STATIC_DIR)
    }


def get_thresholds() -> Dict[str, Any]:
    """获取所有阈值配置"""
    return {
        'rsi_overbought': RSI_OVERBOUGHT,
        'rsi_oversold': RSI_OVERSOLD,
        'ma_break_days': MA_BREAK_DAYS,
        'consecutive_outflow_days': CONSECUTIVE_OUTFLOW_DAYS,
        'daily_drop_threshold': DAILY_DROP_THRESHOLD,
        'daily_rise_threshold': DAILY_RISE_THRESHOLD,
        'single_asset_ratio': SINGLE_ASSET_RATIO,
        'sentiment_negative': SENTIMENT_NEGATIVE_THRESHOLD,
        'pe_percentile_high': PE_PERCENTILE_HIGH
    }


def update_config(key: str, value: Any) -> bool:
    """
    动态更新配置（运行时）
    注意：此更新仅在内存中生效，不会持久化
    """
    global_dict = globals()
    if key in global_dict:
        global_dict[key] = value
        return True
    return False


def reload_from_env() -> None:
    """从环境变量重新加载配置"""
    global DEBUG, HOST, PORT, TUSHARE_TOKEN, NEWSAPI_KEY, NEWSAPI_ENABLED
    global EMAIL_ENABLED, WECHAT_ENABLED, USE_FULL_MARKET, LOG_LEVEL
    
    DEBUG = os.environ.get('DEBUG', 'True').lower() == 'true'
    HOST = os.environ.get('HOST', '0.0.0.0')
    PORT = int(os.environ.get('PORT', 8080))
    TUSHARE_TOKEN = os.environ.get('TUSHARE_TOKEN', TUSHARE_TOKEN)
    NEWSAPI_KEY = os.environ.get('NEWSAPI_KEY', NEWSAPI_KEY)
    NEWSAPI_ENABLED = os.environ.get('NEWSAPI_ENABLED', 'True').lower() == 'true'
    EMAIL_ENABLED = os.environ.get('EMAIL_ENABLED', 'False').lower() == 'true'
    WECHAT_ENABLED = os.environ.get('WECHAT_ENABLED', 'False').lower() == 'true'
    USE_FULL_MARKET = os.environ.get('USE_FULL_MARKET', 'True').lower() == 'true'
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')


# ==================== 业务常数 ====================
# 以下常数原来在constants.py中，现已整合到此

# 数据周期常数
SHORT_TERM_PERIOD = 5          # 短期预测周期
MEDIUM_TERM_PERIOD = 20        # 中期预测周期
LONG_TERM_PERIOD = 60          # 长期预测周期
MIN_DATA_DAYS = 60             # 所需最少历史数据天数
SHORT_MOVING_AVERAGE = 5       # 短期移动平均天数
MEDIUM_MOVING_AVERAGE = 20     # 中期移动平均天数
LONG_MOVING_AVERAGE = 60       # 长期移动平均天数
API_REQUEST_TIMEOUT = 10       # API请求超时（秒）
API_RETRY_ATTEMPTS = 3         # API重试次数

# 技术指标阈值
MACD_FAST_PERIOD = 12          # MACD快线周期
MACD_SLOW_PERIOD = 26          # MACD慢线周期
MACD_SIGNAL_PERIOD = 9         # MACD信号线周期
BOLLINGER_BAND_PERIOD = 20     # 布林带周期
BOLLINGER_BAND_STD = 2         # 布林带标准差
ATR_PERIOD = 14                # ATR周期

# 风险预警阈值
MAX_SINGLE_ASSET_RATIO = 0.20  # 单资产最大持仓占比
MIN_PORTFOLIO_DIVERSIFICATION = 5  # 最低投资品种数
MARGIN_WARNING_RATIO = 0.8     # 融资余额占比预警线

# 数据库相关
DB_POOL_SIZE = 10              # 连接池大小
DB_POOL_MAX_OVERFLOW = 20      # 最大溢出连接数
DB_POOL_RECYCLE = 3600         # 连接回收时间（秒）
BATCH_INSERT_SIZE = 1000       # 批量插入大小
BATCH_DELETE_SIZE = 1000       # 批量删除大小
LOG_RETENTION_DAYS = 90        # 日志保留天数（3个月）

# 模型相关
MODEL_TRAIN_TEST_SPLIT = 0.8   # 训练/测试数据比例
MODEL_VALIDATION_SPLIT = 0.2   # 验证数据比例
MODEL_RANDOM_SEED = 42         # 随机种子
MIN_MODEL_ACCURACY = 0.55      # 最低模型精度
MIN_MODEL_F1_SCORE = 0.50      # 最低F1分数
MIN_SHORT_HORIZON_AUC = 0.62   # 5日短周期最小AUC
MAX_SHORT_HORIZON_BRIER = 0.25 # 5日短周期最大Brier
ALLOW_LEGACY_UNVALIDATED_MODEL = os.environ.get('ALLOW_LEGACY_UNVALIDATED_MODEL', 'false').lower() == 'true'

# 推荐相关
DEFAULT_RECOMMENDATION_COUNT = 20    # 默认推荐数量
MIN_RECOMMENDATION_COUNT = 3         # 最少推荐数量
MAX_RECOMMENDATION_COUNT = 50        # 最多推荐数量
WEIGHT_TECHNICAL = 0.3         # 技术面权重
WEIGHT_FUNDAMENTAL = 0.3       # 基本面权重
WEIGHT_MOMENTUM = 0.2          # 动量权重
WEIGHT_SENTIMENT = 0.2         # 情感权重

# 采集器相关
MAX_CACHE_ITEMS = 1000         # 采集器缓存最大项数
MAX_REQUESTS_PER_MINUTE = 60   # 每分钟最大请求数
STOCK_POOL_SIZE_A = 100        # A股股票池大小
STOCK_POOL_SIZE_HK = 50        # 港股股票池大小
STOCK_POOL_SIZE_US = 50        # 美股股票池大小

# 消息/通知
EMAIL_SENDER_NAME = 'AI理财助手'  # 邮件发送者名称
EMAIL_SEND_INTERVAL = 300      # 邮件发送间隔（秒）
NOTIFICATION_PRIORITY_HIGH = 'high'      # 高优先级
NOTIFICATION_PRIORITY_MEDIUM = 'medium'  # 中优先级
NOTIFICATION_PRIORITY_LOW = 'low'        # 低优先级

# 调度相关
SCHEDULER_CHECK_INTERVAL = 60  # 调度器检查间隔（秒）
MAX_SCHEDULER_WORKERS = 5      # 最多并发任务数
MARKET_OPEN_HOUR = 9           # 市场开盘时刻（9:30）
MARKET_CLOSE_HOUR = 15         # 市场收盘时刻（15:00）
CONTINUOUS_LEARNING_ENABLED = os.environ.get('CONTINUOUS_LEARNING_ENABLED', 'true').lower() == 'true'
CONTINUOUS_LEARNING_INTERVAL_HOURS = max(1, int(os.environ.get('CONTINUOUS_LEARNING_INTERVAL_HOURS', '6')))
CONTINUOUS_LEARNING_STARTUP_DELAY_MINUTES = max(0, int(os.environ.get('CONTINUOUS_LEARNING_STARTUP_DELAY_MINUTES', '3')))
CONTINUOUS_FULL_RETRAIN_COOLDOWN_HOURS = max(
    CONTINUOUS_LEARNING_INTERVAL_HOURS,
    int(os.environ.get('CONTINUOUS_FULL_RETRAIN_COOLDOWN_HOURS', '24')),
)

# 日志级别
LOG_LEVEL_DEBUG = 'DEBUG'
LOG_LEVEL_INFO = 'INFO'
LOG_LEVEL_WARNING = 'WARNING'
LOG_LEVEL_ERROR = 'ERROR'
LOG_LEVEL_CRITICAL = 'CRITICAL'
MAX_LOG_SIZE_MB = 100          # 日志文件最大大小（MB）

# 系统状态码
STATUS_SUCCESS = 200
STATUS_BAD_REQUEST = 400
STATUS_UNAUTHORIZED = 401
STATUS_FORBIDDEN = 403
STATUS_NOT_FOUND = 404
STATUS_SERVER_ERROR = 500
STATUS_SERVICE_UNAVAILABLE = 503

# 业务状态码
SUCCESS = 0
ERROR_GENERAL = 1000
ERROR_VALIDATION = 1001
ERROR_DATABASE = 1002
ERROR_API = 1003
ERROR_AUTH = 1004

# 资产类型
ASSET_TYPE_STOCK = 'stock'     # 股票
ASSET_TYPE_FUND = 'fund'       # 基金
ASSET_TYPE_ETF = 'etf'         # ETF
ASSET_TYPE_BOND = 'bond'       # 债券
ASSET_TYPE_GOLD = 'gold'       # 黄金
ASSET_TYPE_SILVER = 'silver'   # 白银
ASSET_TYPE_CRYPTO = 'crypto'   # 加密货币

# 市场类型
MARKET_A = 'A'                 # A股
MARKET_HK = 'HK'               # 港股
MARKET_US = 'US'               # 美股

# 功能开关
FEATURE_PREDICT_AI = True      # 是否启用AI预测
FEATURE_AUTO_ALERT = True      # 是否启用自动预警
FEATURE_EMAIL_NOTIFY = True    # 是否启用邮件通知
FEATURE_SMS_NOTIFY = False     # 是否启用短信通知（可选）

def get_db_info() -> Dict[str, Any]:
    """获取数据库连接信息"""
    return {
        'type': DB_TYPE,
        'url': DATABASE_URL.replace(MYSQL_PASSWORD, '***') if MYSQL_PASSWORD else DATABASE_URL,
        'pool_size': DB_POOL_SIZE if DB_TYPE == 'mysql' else None,
        'pool_max_overflow': DB_POOL_MAX_OVERFLOW if DB_TYPE == 'mysql' else None,
    }


if __name__ == '__main__':
    print("=" * 60)
    print("配置验证")
    print("=" * 60)
    
    validation = validate_config()
    
    if validation['valid']:
        print("✅ 配置验证通过")
    else:
        print("❌ 配置验证失败")
        for error in validation['errors']:
            print(f"  错误: {error}")
    
    if validation['warnings']:
        print("\n⚠️ 警告:")
        for warning in validation['warnings']:
            print(f"  {warning}")
    
    print("\n📁 路径配置:")
    paths = get_paths()
    for key, value in paths.items():
        print(f"  {key}: {value}")
    
    print("\n⚙️ 当前配置:")
    summary = get_config_summary()
    for key, value in summary.items():
        print(f"  {key}: {value}")
    
    print("\n📊 阈值配置:")
    thresholds = get_thresholds()
    for key, value in thresholds.items():
        print(f"  {key}: {value}")