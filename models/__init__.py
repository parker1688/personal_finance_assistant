"""
数据库模型模块 - models/__init__.py
定义所有数据表结构
"""

from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Date, 
    DateTime, Text, Boolean, ForeignKey, Index, event
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from datetime import datetime
import os

# 导入配置
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL

# 创建数据库引擎 - 针对 SQLite 并发写入做兼容优化
_engine_kwargs = {
    'echo': False,
    'pool_recycle': 3600,
    'pool_pre_ping': True,
}

if str(DATABASE_URL).startswith('sqlite'):
    _engine_kwargs.update({
        'connect_args': {
            'check_same_thread': False,
            'timeout': 30,
        }
    })
else:
    _engine_kwargs.update({
        'pool_size': 10,
        'max_overflow': 20,
    })

engine = create_engine(DATABASE_URL, **_engine_kwargs)

if str(DATABASE_URL).startswith('sqlite'):
    @event.listens_for(engine, 'connect')
    def _set_sqlite_pragma(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute('PRAGMA journal_mode=WAL;')
        cursor.execute('PRAGMA synchronous=NORMAL;')
        cursor.execute('PRAGMA busy_timeout=30000;')
        cursor.execute('PRAGMA foreign_keys=ON;')
        cursor.close()

Base = declarative_base()
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


# ==================== 原始数据表 ====================

class RawStockData(Base):
    """原始股票行情数据表"""
    __tablename__ = 'raw_stock_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(20), nullable=False, comment='股票代码')
    name = Column(String(100), comment='股票名称')
    date = Column(Date, nullable=False, comment='交易日期')
    open = Column(Float, comment='开盘价')
    high = Column(Float, comment='最高价')
    low = Column(Float, comment='最低价')
    close = Column(Float, nullable=False, comment='收盘价')
    volume = Column(Integer, comment='成交量')
    market = Column(String(10), comment='市场(A/H/US)')
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    
    __table_args__ = (
        Index('idx_stock_code_date', 'code', 'date'),
        Index('idx_date', 'date'),
    )


class RawFundData(Base):
    """原始基金数据表"""
    __tablename__ = 'raw_fund_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(20), nullable=False, comment='基金代码')
    name = Column(String(100), comment='基金名称')
    date = Column(Date, nullable=False, comment='净值日期')
    nav = Column(Float, comment='单位净值')
    accumulated_nav = Column(Float, comment='累计净值')
    daily_return = Column(Float, comment='日收益率(%)')
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    
    __table_args__ = (
        Index('idx_fund_code_date', 'code', 'date'),
    )


# ==================== 每日价格表（新增） ====================

class DailyPrice(Base):
    """每日价格汇总表"""
    __tablename__ = 'daily_prices'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(20), nullable=False, comment='代码')
    date = Column(Date, nullable=False, comment='日期')
    open = Column(Float, comment='开盘价')
    high = Column(Float, comment='最高价')
    low = Column(Float, comment='最低价')
    close = Column(Float, nullable=False, comment='收盘价')
    volume = Column(Integer, comment='成交量')
    market = Column(String(10), comment='市场')
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    
    __table_args__ = (
        Index('idx_daily_code_date', 'code', 'date'),
        Index('idx_daily_date', 'date'),
    )


# ==================== 指标数据表 ====================

class Indicator(Base):
    """技术/估值指标数据表"""
    __tablename__ = 'indicators'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(20), nullable=False, comment='标的代码')
    date = Column(Date, nullable=False, comment='日期')
    
    # 技术指标
    rsi = Column(Float, comment='RSI值(0-100)')
    macd_dif = Column(Float, comment='MACD DIF线')
    macd_dea = Column(Float, comment='MACD DEA线')
    macd_hist = Column(Float, comment='MACD柱状图')
    ma5 = Column(Float, comment='5日均线')
    ma20 = Column(Float, comment='20日均线')
    ma60 = Column(Float, comment='60日均线')
    bb_upper = Column(Float, comment='布林带上轨')
    bb_middle = Column(Float, comment='布林带中轨')
    bb_lower = Column(Float, comment='布林带下轨')
    volatility = Column(Float, comment='年化波动率')
    volume_ratio = Column(Float, comment='量比')
    
    # 估值指标
    pe = Column(Float, comment='市盈率')
    pe_percentile = Column(Float, comment='PE历史分位数')
    pb = Column(Float, comment='市净率')
    pb_percentile = Column(Float, comment='PB历史分位数')
    dividend_yield = Column(Float, comment='股息率(%)')
    
    # 资金指标
    north_money = Column(Float, comment='北向资金净流入(万元)')
    main_money = Column(Float, comment='主力资金净流入(万元)')
    money_score = Column(Float, comment='资金面得分(1-5)')
    
    # 情绪指标
    sentiment_score = Column(Float, comment='情感得分(-0.5~0.5)')
    sentiment_positive_ratio = Column(Float, comment='正面新闻占比')
    sentiment_negative_ratio = Column(Float, comment='负面新闻占比')
    
    # 综合评分
    tech_score = Column(Float, comment='技术面得分(1-5)')
    value_score = Column(Float, comment='估值得分(1-5)')
    total_score = Column(Float, comment='综合评分(1-5)')
    
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    
    __table_args__ = (
        Index('idx_indicator_code_date', 'code', 'date'),
    )


# ==================== 预测数据表 ====================

class Prediction(Base):
    """预测结果表"""
    __tablename__ = 'predictions'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(20), nullable=False, comment='标的代码')
    name = Column(String(100), comment='标的名称')
    asset_type = Column(String(20), default='stock', comment='资产类型')  # 新增字段
    date = Column(Date, nullable=False, comment='预测日期')
    period_days = Column(Integer, nullable=False, comment='预测周期(5/20/60)')
    
    # 预测概率
    up_probability = Column(Float, nullable=False, comment='上涨概率(%)')
    down_probability = Column(Float, nullable=False, comment='下跌概率(%)')
    target_low = Column(Float, comment='目标价下限')
    target_high = Column(Float, comment='目标价上限')
    confidence = Column(Float, comment='置信度(%)')
    
    # 止损建议
    stop_loss = Column(Float, comment='建议止损价')
    
    # 到期验证
    expiry_date = Column(Date, nullable=False, comment='到期日期')
    is_expired = Column(Boolean, default=False, comment='是否已到期')
    actual_price = Column(Float, comment='到期实际价格')
    actual_return = Column(Float, comment='实际收益率(%)')
    is_direction_correct = Column(Boolean, comment='方向是否正确')
    is_target_correct = Column(Boolean, comment='是否在目标区间内')
    
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    
    __table_args__ = (
        Index('idx_code_date_period', 'code', 'date', 'period_days'),
        Index('idx_expiry', 'expiry_date', 'is_expired'),
        Index('idx_asset_type', 'asset_type'),
    )


# ==================== 预测准确率统计表（新增） ====================

class PredictionAccuracy(Base):
    """预测准确率统计表"""
    __tablename__ = 'prediction_accuracy'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    period_days = Column(Integer, nullable=False, comment='预测周期')
    total_predictions = Column(Integer, default=0, comment='总预测数')
    correct_predictions = Column(Integer, default=0, comment='正确数')
    accuracy = Column(Float, default=0, comment='准确率(%)')
    avg_error = Column(Float, default=0, comment='平均误差(%)')
    last_updated = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='最后更新时间')
    
    __table_args__ = (
        Index('idx_period_days', 'period_days'),
    )


# ==================== 推荐结果表 ====================

class Recommendation(Base):
    """推荐结果表"""
    __tablename__ = 'recommendations'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False, comment='推荐日期')
    code = Column(String(20), nullable=False, comment='标的代码')
    name = Column(String(100), comment='标的名称')
    type = Column(String(20), nullable=False, comment='品类(a_stock/hk_stock/us_stock/active_fund/etf/gold/silver)')
    rank = Column(Integer, nullable=False, comment='排名')
    
    # 评分
    total_score = Column(Float, nullable=False, comment='综合评分')
    tech_score = Column(Float, comment='技术面得分')
    value_score = Column(Float, comment='估值得分')
    money_score = Column(Float, comment='资金面得分')
    news_score = Column(Float, comment='消息面得分')
    
    # 5日预测数据
    up_probability_5d = Column(Float, comment='5日上涨概率(%)')
    target_low_5d = Column(Float, comment='5日目标价下限')
    target_high_5d = Column(Float, comment='5日目标价上限')
    stop_loss_5d = Column(Float, comment='5日止损价')
    
    # 20日预测数据
    up_probability_20d = Column(Float, comment='20日上涨概率(%)')
    target_low_20d = Column(Float, comment='20日目标价下限')
    target_high_20d = Column(Float, comment='20日目标价上限')
    stop_loss_20d = Column(Float, comment='20日止损价')
    
    # 60日预测数据
    up_probability_60d = Column(Float, default=50, comment='60日上涨概率(%)')
    target_low_60d = Column(Float, comment='60日目标价下限')
    target_high_60d = Column(Float, comment='60日目标价上限')
    stop_loss_60d = Column(Float, comment='60日止损价')
    
    # 推荐理由
    reason_summary = Column(Text, comment='推荐理由摘要')
    risk_warning = Column(Text, comment='风险提示')
    
    # 当前价格
    current_price = Column(Float, comment='当前价格')
    volatility_level = Column(String(10), comment='波动率等级')
    
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    
    __table_args__ = (
        Index('idx_date_type_rank', 'date', 'type', 'rank'),
    )


# ==================== 预警记录表 ====================

class Warning(Base):
    """预警记录表"""
    __tablename__ = 'warnings'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(20), nullable=False, comment='标的代码')
    name = Column(String(100), nullable=False, comment='标的名称')
    warning_time = Column(DateTime, nullable=False, comment='预警时间')
    warning_type = Column(String(50), nullable=False, comment='预警类型')
    level = Column(String(10), nullable=False, comment='严重程度(high/medium/low)')
    trigger_value = Column(String(100), comment='触发值')
    message = Column(Text, nullable=False, comment='预警消息')
    suggestion = Column(Text, comment='建议操作')
    is_sent = Column(Boolean, default=False, comment='是否已推送')
    sent_at = Column(DateTime, comment='推送时间')
    sent_method = Column(String(20), comment='推送方式')
    
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    
    __table_args__ = (
        Index('idx_warning_time', 'warning_time'),
        Index('idx_warning_code', 'code'),
        Index('idx_warning_level', 'level'),
    )


# ==================== 用户持仓表 ====================

class Holding(Base):
    """用户持仓表"""
    __tablename__ = 'holdings'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    asset_type = Column(String(20), default='stock', comment='资产类型(stock/fund/etf/gold/silver)')
    code = Column(String(20), nullable=False, comment='标的代码')
    name = Column(String(100), nullable=False, comment='标的名称')
    quantity = Column(Integer, nullable=False, comment='持有数量')
    cost_price = Column(Float, nullable=False, comment='成本价')
    buy_date = Column(Date, nullable=False, comment='买入日期')
    notes = Column(Text, comment='备注')
    
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')
    
    __table_args__ = (
        Index('idx_code', 'code'),
    )


# ==================== 每日持仓快照表 ====================

class HoldingSnapshot(Base):
    """每日持仓快照表"""
    __tablename__ = 'holding_snapshots'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_date = Column(Date, nullable=False, comment='快照日期')
    holding_id = Column(Integer, nullable=False, comment='持仓ID')
    asset_type = Column(String(20), comment='资产类型')
    code = Column(String(20), comment='代码')
    name = Column(String(100), comment='名称')
    quantity = Column(Float, comment='数量')
    cost_price = Column(Float, comment='成本价')
    market_price = Column(Float, comment='当日市价')
    market_value = Column(Float, comment='市值')
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    
    __table_args__ = (
        Index('idx_snapshot_date', 'snapshot_date'),
        Index('idx_holding_id', 'holding_id'),
        Index('idx_snapshot_code_date', 'code', 'snapshot_date'),
    )


# ==================== 复盘记录表 ====================

class Review(Base):
    """复盘记录表"""
    __tablename__ = 'reviews'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    prediction_id = Column(Integer, nullable=False, comment='预测ID')
    code = Column(String(20), nullable=False, comment='标的代码')
    name = Column(String(100), comment='标的名称')
    period_days = Column(Integer, nullable=False, comment='预测周期')
    
    # 预测信息
    predicted_up_prob = Column(Float, nullable=False, comment='预测上涨概率')
    predicted_target_low = Column(Float, comment='预测目标价下限')
    predicted_target_high = Column(Float, comment='预测目标价上限')
    
    # 实际结果
    actual_price = Column(Float, nullable=False, comment='到期实际价格')
    actual_return = Column(Float, nullable=False, comment='实际收益率(%)')
    is_direction_correct = Column(Boolean, nullable=False, comment='方向是否正确')
    is_target_correct = Column(Boolean, comment='是否在目标区间内')
    
    # 分析
    error_analysis = Column(Text, comment='误差分析')
    review_score = Column(Float, comment='复盘评分(0-100)')
    
    reviewed_at = Column(DateTime, default=datetime.now, comment='复盘时间')
    
    __table_args__ = (
        Index('idx_prediction_id', 'prediction_id'),
        Index('idx_review_code', 'code'),
    )


# ==================== 准确率统计表 ====================

class AccuracyStat(Base):
    """准确率统计表"""
    __tablename__ = 'accuracy_stats'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    stat_date = Column(Date, nullable=False, comment='统计日期')
    period_days = Column(Integer, nullable=False, comment='预测周期')
    asset_type = Column(String(20), nullable=False, comment='资产类型')
    total_count = Column(Integer, nullable=False, default=0, comment='总预测数')
    correct_count = Column(Integer, nullable=False, default=0, comment='正确数')
    accuracy = Column(Float, nullable=False, default=0, comment='准确率(%)')
    
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    
    __table_args__ = (
        Index('idx_stat_date', 'stat_date'),
        Index('idx_period_asset', 'period_days', 'asset_type'),
    )


# ==================== 模型版本表 ====================

class ModelVersion(Base):
    """模型版本表"""
    __tablename__ = 'model_versions'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    version = Column(String(20), nullable=False, comment='版本号')
    model_type = Column(String(20), nullable=False, comment='模型类型(xgboost/lstm)')
    period_days = Column(Integer, nullable=False, comment='预测周期')
    train_date = Column(Date, nullable=False, comment='训练日期')
    validation_accuracy = Column(Float, comment='验证集准确率(%)')
    train_data_count = Column(Integer, comment='训练数据量')
    model_path = Column(String(200), comment='模型文件路径')
    params = Column(Text, comment='模型参数(JSON)')
    is_active = Column(Boolean, default=False, comment='是否当前使用')
    
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    
    __table_args__ = (
        Index('idx_version', 'version'),
        Index('idx_active', 'is_active'),
    )


# ==================== 系统配置表 ====================

class Config(Base):
    """系统配置表"""
    __tablename__ = 'configs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    config_key = Column(String(100), nullable=False, unique=True, comment='配置键')
    config_value = Column(Text, nullable=False, comment='配置值')
    config_type = Column(String(20), default='string', comment='配置类型')
    description = Column(String(200), comment='说明')
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='更新时间')
    
    __table_args__ = (
        Index('idx_key', 'config_key'),
    )


# ==================== 系统日志表 ====================

class Log(Base):
    """系统日志表"""
    __tablename__ = 'logs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    log_time = Column(DateTime, nullable=False, comment='日志时间')
    level = Column(String(10), nullable=False, comment='日志级别(INFO/WARNING/ERROR/DEBUG)')
    module = Column(String(50), nullable=False, comment='模块名称')
    message = Column(Text, nullable=False, comment='日志内容')
    stack_trace = Column(Text, comment='堆栈信息')
    
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    
    __table_args__ = (
        Index('idx_log_time', 'log_time'),
        Index('idx_log_level', 'level'),
        Index('idx_module', 'module'),
    )


# ==================== 学习洞察表 ====================

class LearningInsight(Base):
    """学习洞察表 - 持久化预测反思结果"""
    __tablename__ = 'learning_insights'

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    period_days = Column(Integer, comment='预测周期(5/20/60, None表示全局)')
    asset_type = Column(String(20), comment='资产类型(all表示全局)')
    error_rate = Column(Float, comment='错误率(%)')
    total_analyzed = Column(Integer, comment='分析样本总数')
    dominant_pattern = Column(String(50), comment='主要错误模式')
    pattern_ratio = Column(Float, comment='主要模式占比(%)')
    suggestion = Column(Text, comment='改进建议')
    retrain_triggered = Column(Boolean, default=False, comment='是否建议重训模型')
    days_analyzed = Column(Integer, comment='分析覆盖天数')

    __table_args__ = (
        Index('idx_insight_created', 'created_at'),
        Index('idx_insight_period_asset', 'period_days', 'asset_type'),
    )


# ==================== 工具函数 ====================

def init_database():
    """初始化数据库，创建所有表"""
    Base.metadata.create_all(engine)
    print("✅ 数据库表创建完成")
    
    # 打印所有表名
    from sqlalchemy import inspect
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    print(f"📊 已创建 {len(tables)} 个表: {', '.join(tables)}")


def get_session():
    """获取数据库会话"""
    return SessionLocal()


def drop_all_tables():
    """删除所有表（谨慎使用）"""
    confirm = input("⚠️ 确定要删除所有表吗？这将丢失所有数据！(yes/no): ")
    if confirm.lower() == 'yes':
        Base.metadata.drop_all(engine)
        print("✅ 所有表已删除")
    else:
        print("操作已取消")


def get_model_count():
    """获取所有模型的数量统计"""
    session = get_session()
    try:
        from sqlalchemy import inspect
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        stats = {}
        for table in tables:
            result = session.execute(f"SELECT COUNT(*) FROM {table}")
            stats[table] = result.scalar()
        
        return stats
    finally:
        session.close()


if __name__ == '__main__':
    # 测试数据库连接和表创建
    print("=" * 50)
    print("数据库初始化")
    print("=" * 50)
    
    try:
        init_database()
        print("\n✅ 数据库初始化成功！")
        
        # 显示表统计
        stats = get_model_count()
        print("\n📊 数据统计:")
        for table, count in stats.items():
            print(f"  {table}: {count} 条记录")
            
    except Exception as e:
        print(f"\n❌ 数据库初始化失败: {e}")