"""
定时任务模块 - scheduler.py
配置和管理所有定时任务 - 优化版
"""

import sys
import os
import time
import logging
from datetime import datetime, timedelta
import threading
from copy import deepcopy
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict, Any

# 添加项目根目录到路径
sys.path.append(str(Path(__file__).resolve().parent))

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from utils import get_logger, get_next_trading_day

# 初始化日志
logger = get_logger(__name__)
logging.getLogger('apscheduler').setLevel(logging.ERROR)
logging.getLogger('apscheduler.executors').setLevel(logging.ERROR)

# 导入采集编排器
try:
    from scheduler.collection_director import CollectionDirector
    HAS_COLLECTION_DIRECTOR = True
except ImportError as e:
    logger.warning(f"⚠️ CollectionDirector 未可用: {e}")
    HAS_COLLECTION_DIRECTOR = False

# 导入配置
try:
    from config import (
        MODELS_DIR, DATA_DIR,
        COLLECT_REAL_TIME_INTERVAL, COLLECT_DAILY_TIME,
        MODEL_RETRAIN_DAY, MODEL_RETRAIN_HOUR,
        WARNING_DEDUP_HOURS, RAW_DATA_RETENTION_DAYS, SNAPSHOT_RETENTION_DAYS,
        PREDICTION_PERIODS, DAILY_SNAPSHOT_TIME, DAILY_RECOMMENDATION_TIME,
        FUTURE_SIGNAL_ALERT_MORNING_TIME, FUTURE_SIGNAL_ALERT_PRE_CLOSE_TIME,
        MARKET_ACTION_CUTOFF_TIME, MARKET_OPEN_HOUR, MARKET_CLOSE_HOUR,
        HK_MARKET_OPEN_TIME, HK_MARKET_CLOSE_TIME, HK_MARKET_ACTION_CUTOFF_TIME,
        US_MARKET_OPEN_TIME, US_MARKET_CLOSE_TIME, US_MARKET_ACTION_CUTOFF_TIME,
        CN_MARKET_HOLIDAYS, HK_MARKET_HOLIDAYS, US_MARKET_HOLIDAYS,
        CONTINUOUS_LEARNING_ENABLED, CONTINUOUS_LEARNING_INTERVAL_HOURS,
        CONTINUOUS_LEARNING_STARTUP_DELAY_MINUTES, CONTINUOUS_FULL_RETRAIN_COOLDOWN_HOURS
    )
except ImportError:
    # 默认配置
    BASE_DIR = Path(__file__).resolve().parent
    MODELS_DIR = BASE_DIR / 'data' / 'models'
    DATA_DIR = BASE_DIR / 'data'
    COLLECT_REAL_TIME_INTERVAL = 300  # 5分钟
    COLLECT_DAILY_TIME = '15:30'
    MODEL_RETRAIN_DAY = 'sun'  # 周日
    MODEL_RETRAIN_HOUR = 2
    WARNING_DEDUP_HOURS = 24
    RAW_DATA_RETENTION_DAYS = 365
    SNAPSHOT_RETENTION_DAYS = 365
    PREDICTION_PERIODS = [5, 20, 60]
    DAILY_SNAPSHOT_TIME = '15:10'
    DAILY_RECOMMENDATION_TIME = '18:00'
    FUTURE_SIGNAL_ALERT_MORNING_TIME = '09:35'
    FUTURE_SIGNAL_ALERT_PRE_CLOSE_TIME = '14:40'
    MARKET_ACTION_CUTOFF_TIME = '15:00'
    MARKET_OPEN_HOUR = 9
    MARKET_CLOSE_HOUR = 15
    HK_MARKET_OPEN_TIME = '09:30'
    HK_MARKET_CLOSE_TIME = '16:00'
    HK_MARKET_ACTION_CUTOFF_TIME = '15:40'
    US_MARKET_OPEN_TIME = '21:30'
    US_MARKET_CLOSE_TIME = '04:00'
    US_MARKET_ACTION_CUTOFF_TIME = '03:30'
    CN_MARKET_HOLIDAYS = set()
    HK_MARKET_HOLIDAYS = set()
    US_MARKET_HOLIDAYS = set()
    CONTINUOUS_LEARNING_ENABLED = os.environ.get('CONTINUOUS_LEARNING_ENABLED', 'true').lower() == 'true'
    CONTINUOUS_LEARNING_INTERVAL_HOURS = max(1, int(os.environ.get('CONTINUOUS_LEARNING_INTERVAL_HOURS', '6')))
    CONTINUOUS_LEARNING_STARTUP_DELAY_MINUTES = max(0, int(os.environ.get('CONTINUOUS_LEARNING_STARTUP_DELAY_MINUTES', '3')))
    CONTINUOUS_FULL_RETRAIN_COOLDOWN_HOURS = max(
        CONTINUOUS_LEARNING_INTERVAL_HOURS,
        int(os.environ.get('CONTINUOUS_FULL_RETRAIN_COOLDOWN_HOURS', '24')),
    )

# 全局调度器
scheduler = BackgroundScheduler(job_defaults={
    'coalesce': True,
    'max_instances': 1,
    'misfire_grace_time': 120,
})

# 任务状态跟踪
_task_status: Dict[str, Dict[str, Any]] = {}

# 全局采集编排器实例
_collection_director = None
_realtime_collector = None
_managed_collection_lock = threading.Lock()
_managed_collection_running = False

# 启动补采状态（避免重复并发启动）
_backfill_lock = threading.Lock()
_backfill_running = False
_backfill_progress_lock = threading.Lock()
_backfill_steps = [
    '北向资金',
    '融资融券',
    '龙虎榜',
    '每日估值(全量股票)',
    '个股资金流向(全量股票)',
    '券商研报',
    '新闻舆情',
    '财务指标(全量股票)',
    '股票行情(当年历史+实时)',
    '基金数据',
    '宏观数据快照',
]
_backfill_progress: Dict[str, Any] = {
    'running': False,
    'start_time': None,
    'end_time': None,
    'start_date': None,
    'end_date': None,
    'current_step': None,
    'total_steps': len(_backfill_steps),
    'completed_steps': 0,
    'success_steps': 0,
    'failed_steps': 0,
    'progress_percent': 0.0,
    'steps': [],
    'message': '尚未开始自动补采',
    'last_error': None,
    'recent_steps': [],
    'current_pipeline_stage': None,
    'recent_pipeline_stages': [],
    'slowest_pipeline_stages': [],
}

# 增量训练触发策略
# - after_collect(默认): 补采全部结束后仅训练一次
# - interval: 先积累一定交易日，再按固定间隔触发
# - off: 关闭自动训练
_backfill_training_mode = os.environ.get('BACKFILL_TRAINING_MODE', 'after_collect').lower()
_backfill_training_min_days = 20
_backfill_training_interval_days = 20
_backfill_trade_chunk_days = max(1, int(os.environ.get('BACKFILL_TRADE_CHUNK_DAYS', '1')))
_backfill_stage_retry_times = max(1, int(os.environ.get('BACKFILL_STAGE_RETRY_TIMES', '3')))
TRAINING_PROGRESS_FILE = Path(MODELS_DIR) / 'training_progress.json'
LEARNING_STATUS_FILE = Path(DATA_DIR) / 'cache' / 'learning_loop_status.json'
_continuous_learning_lock = threading.Lock()
_continuous_learning_running = False


def _parse_hhmm(value: str, default: str) -> tuple[int, int]:
    raw = str(value or default)
    try:
        hour_str, minute_str = raw.split(':', 1)
        return int(hour_str), int(minute_str)
    except Exception:
        hour_str, minute_str = default.split(':', 1)
        return int(hour_str), int(minute_str)


def _minutes_since_midnight(now: datetime) -> int:
    return (now.hour * 60) + now.minute


def _normalize_market(market: Optional[str] = None) -> str:
    raw = str(market or 'CN').strip().upper()
    mapping = {
        'A': 'CN',
        'ASHARE': 'CN',
        'A_STOCK': 'CN',
        'ACTIVE_FUND': 'CN',
        'ETF': 'CN',
        'FUND': 'CN',
        'CN': 'CN',
        'MAINLAND': 'CN',
        'HK': 'HK',
        'HK_STOCK': 'HK',
        'H': 'HK',
        'US': 'US',
        'US_STOCK': 'US',
    }
    return mapping.get(raw, 'CN')


def _normalize_holiday_set(raw_value) -> set[str]:
    if not raw_value:
        return set()
    if isinstance(raw_value, str):
        return {item.strip() for item in raw_value.split(',') if item.strip()}
    if isinstance(raw_value, (list, tuple, set)):
        return {str(item).strip() for item in raw_value if str(item).strip()}
    return {str(raw_value).strip()}


def _get_market_schedule(market: Optional[str] = None) -> Dict[str, Any]:
    market_code = _normalize_market(market)
    if market_code == 'HK':
        return {
            'market': 'HK',
            'open': _parse_hhmm(HK_MARKET_OPEN_TIME, '09:30'),
            'close': _parse_hhmm(HK_MARKET_CLOSE_TIME, '16:00'),
            'cutoff': _parse_hhmm(HK_MARKET_ACTION_CUTOFF_TIME, '15:40'),
            'lunch_start': (12, 0),
            'lunch_end': (13, 0),
            'holidays': _normalize_holiday_set(HK_MARKET_HOLIDAYS),
        }
    if market_code == 'US':
        return {
            'market': 'US',
            'open': _parse_hhmm(US_MARKET_OPEN_TIME, '21:30'),
            'close': _parse_hhmm(US_MARKET_CLOSE_TIME, '04:00'),
            'cutoff': _parse_hhmm(US_MARKET_ACTION_CUTOFF_TIME, '03:30'),
            'lunch_start': None,
            'lunch_end': None,
            'holidays': _normalize_holiday_set(US_MARKET_HOLIDAYS),
        }
    return {
        'market': 'CN',
        'open': (MARKET_OPEN_HOUR, 30),
        'close': (MARKET_CLOSE_HOUR, 0),
        'cutoff': _parse_hhmm(MARKET_ACTION_CUTOFF_TIME, '15:00'),
        'lunch_start': (11, 30),
        'lunch_end': (13, 0),
        'holidays': _normalize_holiday_set(CN_MARKET_HOLIDAYS),
    }


def _time_to_minutes(time_tuple: tuple[int, int]) -> int:
    return (time_tuple[0] * 60) + time_tuple[1]


def _is_market_holiday(trade_date, market: Optional[str] = None) -> bool:
    schedule = _get_market_schedule(market)
    if trade_date.weekday() >= 5:
        return True
    return trade_date.isoformat() in schedule['holidays']


def _get_next_market_trading_day(trade_date, market: Optional[str] = None):
    current = trade_date
    while True:
        current += timedelta(days=1)
        if not _is_market_holiday(current, market):
            return current


def _resolve_market_reference_date(now: datetime, market: Optional[str] = None):
    schedule = _get_market_schedule(market)
    open_minutes = _time_to_minutes(schedule['open'])
    close_minutes = _time_to_minutes(schedule['close'])
    minute_of_day = _minutes_since_midnight(now)
    trade_date = now.date()
    if close_minutes <= open_minutes and minute_of_day < close_minutes:
        trade_date -= timedelta(days=1)
    return trade_date


def _is_time_in_window(minute_of_day: int, start_minutes: int, end_minutes: int) -> bool:
    if start_minutes <= end_minutes:
        return start_minutes <= minute_of_day < end_minutes
    return minute_of_day >= start_minutes or minute_of_day < end_minutes


def _is_actionable_market_window(now: Optional[datetime] = None, market: Optional[str] = None) -> bool:
    """是否仍处于可操作窗口：支持A股/港股/美股不同交易时段。"""
    current = now or datetime.now()
    schedule = _get_market_schedule(market)
    trade_date = _resolve_market_reference_date(current, market)
    if _is_market_holiday(trade_date, market):
        return False

    minute_of_day = _minutes_since_midnight(current)
    open_minutes = _time_to_minutes(schedule['open'])
    cutoff_minutes = _time_to_minutes(schedule['cutoff'])
    lunch_start = _time_to_minutes(schedule['lunch_start']) if schedule['lunch_start'] else None
    lunch_end = _time_to_minutes(schedule['lunch_end']) if schedule['lunch_end'] else None

    in_primary_window = _is_time_in_window(minute_of_day, open_minutes + 5, cutoff_minutes)
    if not in_primary_window:
        return False

    if lunch_start is not None and lunch_end is not None and lunch_start <= minute_of_day < lunch_end:
        return False
    return True


def _is_intraday_collection_window(now: Optional[datetime] = None, market: Optional[str] = None) -> bool:
    """实时采集只在对应市场盘中执行，避免午休/收盘后无意义轮询。"""
    current = now or datetime.now()
    schedule = _get_market_schedule(market)
    trade_date = _resolve_market_reference_date(current, market)
    if _is_market_holiday(trade_date, market):
        return False

    minute_of_day = _minutes_since_midnight(current)
    open_minutes = _time_to_minutes(schedule['open'])
    close_minutes = _time_to_minutes(schedule['close'])
    lunch_start = _time_to_minutes(schedule['lunch_start']) if schedule['lunch_start'] else None
    lunch_end = _time_to_minutes(schedule['lunch_end']) if schedule['lunch_end'] else None

    in_session = _is_time_in_window(minute_of_day, open_minutes, close_minutes)
    if not in_session:
        return False
    if lunch_start is not None and lunch_end is not None and lunch_start <= minute_of_day < lunch_end:
        return False
    return True


def _resolve_operational_trade_date(now: Optional[datetime] = None, market: Optional[str] = None):
    """根据不同市场收盘规则，决定操作建议归属的交易日。"""
    current = now or datetime.now()
    schedule = _get_market_schedule(market)
    trade_date = _resolve_market_reference_date(current, market)

    if _is_market_holiday(trade_date, market):
        candidate = trade_date
        while _is_market_holiday(candidate, market):
            candidate += timedelta(days=1)
        return candidate

    minute_of_day = _minutes_since_midnight(current)
    open_minutes = _time_to_minutes(schedule['open'])
    close_minutes = _time_to_minutes(schedule['close'])
    cutoff_minutes = _time_to_minutes(schedule['cutoff'])

    if close_minutes <= open_minutes:
        if minute_of_day < close_minutes:
            return _get_next_market_trading_day(trade_date, market) if minute_of_day >= cutoff_minutes else trade_date
        return trade_date

    return _get_next_market_trading_day(trade_date, market) if minute_of_day >= cutoff_minutes else trade_date


def _is_transient_stage_error(err: Exception) -> bool:
    """判断是否为可重试的瞬时网络错误。"""
    text = str(err).lower()
    transient_tokens = [
        'read timed out',
        'timed out',
        'connection reset',
        'temporarily unavailable',
        'httpconnectionpool',
        'max retries exceeded',
        'remote disconnected',
    ]
    return any(t in text for t in transient_tokens)


def _init_backfill_progress(start_dt: datetime, end_dt: datetime, steps: Optional[List[str]] = None):
    """初始化补采进度状态。"""
    step_names = steps if steps else _backfill_steps
    with _backfill_progress_lock:
        _backfill_progress.update({
            'running': True,
            'start_time': datetime.now().isoformat(),
            'end_time': None,
            'start_date': start_dt.strftime('%Y-%m-%d'),
            'end_date': end_dt.strftime('%Y-%m-%d'),
            'current_step': None,
            'total_steps': len(step_names),
            'completed_steps': 0,
            'success_steps': 0,
            'failed_steps': 0,
            'progress_percent': 0.0,
            'steps': [
                {
                    'name': name,
                    'status': 'pending',
                    'message': '',
                    'error': None,
                    'started_at': None,
                    'updated_at': None,
                    'duration_seconds': None,
                    'counted': False,
                    'retrying_from_failed': False,
                }
                for name in step_names
            ],
            'message': '自动补采进行中',
            'last_error': None,
            'recent_steps': [],
            'current_pipeline_stage': None,
            'recent_pipeline_stages': [],
            'slowest_pipeline_stages': [],
        })


def _update_backfill_step(step_name: str, status: str, message: str = '', error: Optional[str] = None):
    """更新单个补采步骤状态。"""
    now = datetime.now().isoformat()
    with _backfill_progress_lock:
        _backfill_progress['current_step'] = step_name if status == 'running' else _backfill_progress.get('current_step')

        for step in _backfill_progress.get('steps', []):
            if step.get('name') != step_name:
                continue

            previous_status = step.get('status')

            if status == 'running':
                step['started_at'] = now
                step['duration_seconds'] = None
                if previous_status != 'failed':
                    step['retrying_from_failed'] = False

            step['status'] = status
            step['message'] = message
            step['error'] = error
            step['updated_at'] = now

            if status in ('success', 'failed'):
                started_at = step.get('started_at')
                try:
                    if started_at:
                        elapsed = datetime.fromisoformat(now) - datetime.fromisoformat(started_at)
                        step['duration_seconds'] = round(max(elapsed.total_seconds(), 0.0), 2)
                except Exception:
                    step['duration_seconds'] = step.get('duration_seconds')

                recent = _backfill_progress.get('recent_steps', [])
                if not isinstance(recent, list):
                    recent = []
                recent.append({
                    'name': step_name,
                    'status': status,
                    'updated_at': now,
                    'duration_seconds': step.get('duration_seconds'),
                    'message': message,
                })
                _backfill_progress['recent_steps'] = recent[-10:]

            # 只有第一次从未完成态变为最终态时才累计计数
            if status in ('success', 'failed') and not step.get('counted', False):
                _backfill_progress['completed_steps'] += 1
                if status == 'success':
                    _backfill_progress['success_steps'] += 1
                else:
                    _backfill_progress['failed_steps'] += 1
                    _backfill_progress['last_error'] = error
                step['counted'] = True

            # 重试时：failed -> running 后，如果最终转为 success，回滚失败数并增加成功数。
            if previous_status == 'failed' and status == 'running':
                step['error'] = None
                step['retrying_from_failed'] = True

            if (
                previous_status == 'running'
                and status == 'success'
                and step.get('counted', False)
                and step.get('retrying_from_failed', False)
            ):
                if _backfill_progress.get('failed_steps', 0) > 0:
                    _backfill_progress['failed_steps'] -= 1
                _backfill_progress['success_steps'] += 1
                step['retrying_from_failed'] = False
            break

        total = _backfill_progress.get('total_steps', 0) or 0
        completed = _backfill_progress.get('completed_steps', 0) or 0
        _backfill_progress['progress_percent'] = round((completed / total) * 100, 2) if total else 0.0
        _backfill_progress['message'] = message or _backfill_progress.get('message', '')


def _finish_backfill_progress():
    """结束补采进度状态。"""
    with _backfill_progress_lock:
        _backfill_progress['running'] = False
        _backfill_progress['current_step'] = None
        _backfill_progress['current_pipeline_stage'] = None
        _backfill_progress['end_time'] = datetime.now().isoformat()
        if _backfill_progress.get('failed_steps', 0) > 0:
            _backfill_progress['message'] = '自动补采已结束（部分模块失败）'
        else:
            _backfill_progress['message'] = '自动补采已结束（全部完成）'


def get_auto_backfill_progress() -> Dict[str, Any]:
    """获取自动补采进度（线程安全快照）。"""
    with _backfill_progress_lock:
        return deepcopy(_backfill_progress)


def _get_a_share_trading_dates(start_dt: datetime, end_dt: datetime) -> List[str]:
    """获取A股交易日列表，优先TuShare交易日历，失败时回退到工作日。"""
    try:
        from utils import get_tushare_pro

        pro = get_tushare_pro()
        if pro is not None:
            cal = pro.trade_cal(
                exchange='SSE',
                start_date=start_dt.strftime('%Y%m%d'),
                end_date=end_dt.strftime('%Y%m%d'),
                is_open='1',
            )
            if cal is not None and len(cal) > 0 and 'cal_date' in cal.columns:
                cal = cal.sort_values('cal_date')
                return [str(x) for x in cal['cal_date'].tolist()]
    except Exception as e:
        logger.warning(f"获取交易日历失败，回退工作日: {e}")

    dates: List[str] = []
    cur = start_dt
    while cur <= end_dt:
        if cur.weekday() < 5:
            dates.append(cur.strftime('%Y%m%d'))
        cur += timedelta(days=1)
    return dates


def _run_incremental_short_term_training(trade_days_done: int):
    """在补采过程中触发一次短期模型增量训练。"""
    try:
        _update_backfill_step('增量训练(短期模型)', 'running', f'正在触发增量训练（已完成{trade_days_done}个交易日）')
        from predictors.model_trainer import ModelTrainer

        trainer = ModelTrainer()
        codes = trainer._get_default_training_codes(limit=140)
        acc = trainer.train_short_term_model(stock_codes=codes)

        msg = f'增量训练完成: trade_days={trade_days_done}, codes={len(codes)}, val_acc={acc}'
        logger.info(msg)
        _update_backfill_step('增量训练(短期模型)', 'success', msg)
    except Exception as e:
        logger.error(f"增量训练失败: {e}")
        _update_backfill_step('增量训练(短期模型)', 'failed', '增量训练失败', str(e))


def _chunk_trade_dates(trade_dates: List[str], chunk_size: int) -> List[List[str]]:
    """按固定块大小切分交易日列表。"""
    if chunk_size <= 1:
        return [[d] for d in trade_dates]
    return [trade_dates[i:i + chunk_size] for i in range(0, len(trade_dates), chunk_size)]


def _set_pipeline_stage_running(stage: str, start_date: str, end_date: str):
    """记录交易日流水线当前阶段。"""
    with _backfill_progress_lock:
        _backfill_progress['current_pipeline_stage'] = {
            'stage': stage,
            'start_date': start_date,
            'end_date': end_date,
            'started_at': datetime.now().isoformat(),
        }


def _record_pipeline_stage_result(
    stage: str,
    start_date: str,
    end_date: str,
    status: str,
    elapsed_seconds: float,
    error: Optional[str] = None,
):
    """记录交易日流水线阶段耗时与状态（用于前端可观测性）。"""
    record = {
        'stage': stage,
        'start_date': start_date,
        'end_date': end_date,
        'status': status,
        'elapsed_seconds': round(max(elapsed_seconds, 0.0), 2),
        'updated_at': datetime.now().isoformat(),
        'error': error,
    }

    with _backfill_progress_lock:
        _backfill_progress['current_pipeline_stage'] = None

        recent = _backfill_progress.get('recent_pipeline_stages', [])
        if not isinstance(recent, list):
            recent = []
        recent.append(record)
        _backfill_progress['recent_pipeline_stages'] = recent[-20:]

        slowest = _backfill_progress.get('slowest_pipeline_stages', [])
        if not isinstance(slowest, list):
            slowest = []
        slowest.append(record)
        slowest = sorted(slowest, key=lambda x: x.get('elapsed_seconds') or 0, reverse=True)
        _backfill_progress['slowest_pipeline_stages'] = slowest[:10]


def _run_trade_date_pipeline_range(start_date: str, end_date: str, daily_collectors: Dict[str, Any], days_in_chunk: int):
    """按日期区间执行交易日流水线采集。"""
    def run_stage(stage_name: str, stage_func, retry_times: int = 1):
        _set_pipeline_stage_running(stage_name, start_date, end_date)
        max_attempts = max(1, int(retry_times))
        last_err = None

        for attempt in range(1, max_attempts + 1):
            stage_start = time.perf_counter()
            try:
                stage_func()
                elapsed = time.perf_counter() - stage_start
                logger.info(
                    f"交易日流水线阶段完成: {stage_name} {start_date}~{end_date}, elapsed={elapsed:.2f}s"
                    + (f", attempt={attempt}/{max_attempts}" if max_attempts > 1 else "")
                )
                _record_pipeline_stage_result(stage_name, start_date, end_date, 'success', elapsed)
                return
            except Exception as e:
                last_err = e
                elapsed = time.perf_counter() - stage_start
                transient = _is_transient_stage_error(e)
                should_retry = transient and attempt < max_attempts
                logger.error(
                    f"交易日流水线阶段失败: {stage_name} {start_date}~{end_date}, elapsed={elapsed:.2f}s, "
                    f"attempt={attempt}/{max_attempts}, retry={should_retry}, err={e}"
                )
                if should_retry:
                    time.sleep(min(2 ** attempt, 8))
                    continue
                _record_pipeline_stage_result(stage_name, start_date, end_date, 'failed', elapsed, str(e))
                raise

        if last_err is not None:
            raise last_err

    run_stage('north', lambda: daily_collectors['north'].collect(start_date, end_date, resume=True, strict=True))
    run_stage('margin', lambda: daily_collectors['margin'].collect_by_date(start_date, end_date, resume=True))
    run_stage('top_list', lambda: daily_collectors['top_list'].collect(start_date, end_date, resume=True))
    run_stage(
        'daily_basic',
        lambda: daily_collectors['daily_basic'].collect_all(
            start_date=start_date,
            end_date=end_date,
            max_stocks=None,
            resume=True,
            mode='by_date',
        ),
    )
    run_stage(
        'moneyflow',
        lambda: daily_collectors['moneyflow'].collect_by_date(
            start_date,
            end_date,
            max_stocks=None,
            resume=True,
        ),
        retry_times=_backfill_stage_retry_times,
    )

    # 新闻采集已解耦为自然日补采与小时级任务，这里不再绑定交易日流水线。

def get_collection_director() -> Optional['CollectionDirector']:
    """获取或创建采集编排器实例"""
    global _collection_director
    if HAS_COLLECTION_DIRECTOR:
        if _collection_director is None:
            try:
                dedup_seconds = max(30, int(COLLECT_REAL_TIME_INTERVAL * 0.4))
                _collection_director = CollectionDirector(
                    max_workers=5,
                    dedup_window_seconds=dedup_seconds,
                )
                logger.info("✅ CollectionDirector 已初始化")
            except Exception as e:
                logger.error(f"❌ 初始化 CollectionDirector 失败: {e}")
                return None
        return _collection_director
    return None


def _get_realtime_collector():
    """获取或创建实时采集器单例，减少高频任务下的重复初始化开销。"""
    global _realtime_collector
    if _realtime_collector is None:
        from collectors.stock_collector import StockCollector
        _realtime_collector = StockCollector()
    return _realtime_collector


def get_model_path(model_name: str) -> Path:
    """获取模型文件路径"""
    return MODELS_DIR / f"{model_name}_model.pkl"


def load_model_if_exists(model_path: Path):
    """加载模型（如果存在）"""
    import pickle
    if model_path.exists():
        try:
            with open(model_path, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            logger.error(f"加载模型失败 {model_path}: {e}")
    return None


def _load_all_models(predictors: dict):
    """加载所有资产类型的运行时模型，并同步特征/校准元数据。"""
    from predictors.model_manager import ModelManager

    model_manager = ModelManager()
    model_configs = {
        'a_stock': [
            ('short', 'short_term', 5),
            ('medium', 'medium_term', 20),
            ('long', 'long_term', 60),
        ],
        'etf': [('short', 'etf', 5)],
        'hk_stock': [
            ('short', 'hk_stock_short_term', 5),
            ('medium', 'hk_stock_medium_term', 20),
            ('long', 'hk_stock_long_term', 60),
        ],
        'us_stock': [
            ('short', 'us_stock_short_term', 5),
            ('medium', 'us_stock_medium_term', 20),
            ('long', 'us_stock_long_term', 60),
        ],
        'fund': [('short', 'fund', 5)],
        'gold': [('short', 'gold', 5)],
        'silver': [('short', 'silver', 5)],
    }

    for asset_type, model_list in model_configs.items():
        if asset_type not in predictors:
            continue

        for period_key, model_name, period_days in model_list:
            if period_key not in predictors[asset_type]:
                continue

            predictor = predictors[asset_type][period_key]
            model_path = get_model_path(model_name)
            bundle = model_manager.load_runtime_model_bundle(
                model_path=str(model_path),
                period_days=period_days,
                allow_legacy=None,
            )

            if not bundle.get('loaded'):
                logger.warning(f"⚠️  {asset_type} - {period_key}日模型未加载: {bundle.get('reason')}")
                continue

            predictor.model = bundle.get('model')
            predictor.is_trained = predictor.model is not None
            if hasattr(predictor, 'feature_columns'):
                predictor.feature_columns = bundle.get('feature_columns')
            if hasattr(predictor, 'calibrator'):
                predictor.calibrator = bundle.get('calibrator')
            if hasattr(predictor, 'calibration_method'):
                predictor.calibration_method = bundle.get('calibration_method', 'none')
            if hasattr(predictor, 'regime_models'):
                predictor.regime_models = bundle.get('regime_models', {}) or {}
            if hasattr(predictor, 'volatility_split'):
                predictor.volatility_split = bundle.get('volatility_split')
            if hasattr(predictor, 'blend_model'):
                predictor.blend_model = bundle.get('blend_model')
            if hasattr(predictor, 'blend_weight'):
                predictor.blend_weight = bundle.get('blend_weight')
            if hasattr(predictor, 'blend_enabled'):
                predictor.blend_enabled = bool(bundle.get('blend_enabled', False))

            logger.debug(f"✅ 已加载 {asset_type} - {period_key}日模型")



def _build_prediction_predictors():
    """构建每日预测使用的资产预测器注册表。"""
    from predictors.short_term import ShortTermPredictor
    from predictors.medium_term import MediumTermPredictor
    from predictors.long_term import LongTermPredictor

    return {
        'a_stock': {
            'short': ShortTermPredictor(),
            'medium': MediumTermPredictor(),
            'long': LongTermPredictor()
        },
        'etf': {
            'short': ShortTermPredictor(),
            'medium': MediumTermPredictor(),
            'long': LongTermPredictor()
        },
        'hk_stock': {
            'short': ShortTermPredictor(),
            'medium': MediumTermPredictor(),
            'long': LongTermPredictor()
        },
        'us_stock': {
            'short': ShortTermPredictor(),
            'medium': MediumTermPredictor(),
            'long': LongTermPredictor()
        },
        'fund': {
            'short': ShortTermPredictor(),
            'medium': MediumTermPredictor(),
            'long': LongTermPredictor()
        },
        'gold': {
            'short': ShortTermPredictor(),
            'medium': ShortTermPredictor(),
            'long': ShortTermPredictor()
        },
        'silver': {
            'short': ShortTermPredictor(),
            'medium': ShortTermPredictor(),
            'long': ShortTermPredictor()
        }
    }


def _normalize_yfinance_symbol(code: str) -> str:
    """将本地代码转换为 yfinance 兼容格式。"""
    symbol = str(code or '').strip().upper()
    if symbol.endswith('.SH'):
        return f"{symbol[:-3]}.SS"
    return symbol


def _normalize_runtime_prediction_asset_type(asset_type: Optional[str], code: str = '') -> str:
    """统一预测运行时资产类型口径。"""
    raw = str(asset_type or '').strip().lower()
    mapping = {
        'stock': 'a_stock',
        'a_stock': 'a_stock',
        'hk_stock': 'hk_stock',
        'us_stock': 'us_stock',
        'fund': 'fund',
        'active_fund': 'fund',
        'etf': 'etf',
        'gold': 'gold',
        'silver': 'silver',
    }
    if raw in mapping:
        return mapping[raw]

    from utils import get_asset_type_from_code

    inferred = get_asset_type_from_code(str(code or ''))
    if inferred == 'fund':
        return 'fund'
    if inferred == 'etf':
        return 'etf'
    if inferred in ('gold', 'silver'):
        return inferred

    upper_code = str(code or '').upper()
    if upper_code.endswith('.HK'):
        return 'hk_stock'
    if upper_code.endswith(('.SH', '.SZ', '.BJ')):
        return 'a_stock'
    return 'us_stock'


FOCUSED_UNIVERSE_LIMITS = {
    'a_stock': 120,
    'hk_stock': 40,
    'us_stock': 40,
    'etf': 30,
    'fund': 40,
}


def _resolve_target_prediction_periods(target: Optional[Dict[str, Any]]) -> List[int]:
    """持仓/推荐保留完整周期；全市场补充样本优先积累5日可复核数据。"""
    target = target or {}
    source = str(target.get('source') or 'universe').lower()
    asset_type = str(target.get('asset_type') or '').lower()

    if source in {'holding', 'recommendation'}:
        return [5, 20, 60]
    if asset_type in {'gold', 'silver'}:
        return [5, 20, 60]
    return [5]


def _count_expected_prediction_records(targets: List[Dict[str, Any]]) -> int:
    return sum(len(_resolve_target_prediction_periods(item)) for item in (targets or []))


def _collect_prediction_targets(session, target_date) -> List[Dict[str, Any]]:
    """收集需要生成预测的目标：持仓 + 最近推荐批次 + 限量全市场种子池。"""
    from models import Holding, Recommendation, RawFundData
    from collectors.stock_collector import StockCollector

    targets: List[Dict[str, Any]] = []
    seen = set()
    universe_counts: Dict[str, int] = {key: 0 for key in FOCUSED_UNIVERSE_LIMITS}

    def _append_target(code: str, name: Optional[str], asset_type: Optional[str], source: str):
        normalized_code = str(code or '').strip()
        if not normalized_code:
            return

        normalized_type = _normalize_runtime_prediction_asset_type(asset_type, normalized_code)
        key = (normalized_code.upper(), normalized_type)
        if key in seen:
            return

        if source == 'universe':
            limit = FOCUSED_UNIVERSE_LIMITS.get(normalized_type)
            if limit is not None and universe_counts.get(normalized_type, 0) >= limit:
                return

        seen.add(key)
        if source == 'universe' and normalized_type in universe_counts:
            universe_counts[normalized_type] += 1
        targets.append({
            'code': normalized_code,
            'name': name or normalized_code,
            'asset_type': normalized_type,
            'source': source,
        })

    for holding in session.query(Holding).all():
        _append_target(holding.code, holding.name, getattr(holding, 'asset_type', None), 'holding')

    latest_batch = session.query(Recommendation.date).filter(
        Recommendation.date <= target_date
    ).order_by(Recommendation.date.desc()).first()
    latest_batch_date = latest_batch[0] if latest_batch else None

    if latest_batch_date:
        recommendation_rows = session.query(Recommendation).filter(
            Recommendation.date == latest_batch_date
        ).all()
        for item in recommendation_rows:
            _append_target(item.code, item.name, getattr(item, 'type', None), 'recommendation')

    try:
        collector = StockCollector()
        for code in getattr(collector, 'a_stock_pool', []) or []:
            _append_target(code, code, 'a_stock', 'universe')
        for code in getattr(collector, 'hk_stock_pool', []) or []:
            _append_target(code, code, 'hk_stock', 'universe')
        for code in getattr(collector, 'us_stock_pool', []) or []:
            _append_target(code, code, 'us_stock', 'universe')
    except Exception as e:
        logger.warning(f"收集股票全市场预测目标失败: {e}")

    try:
        etf_path = DATA_DIR / 'historical_etf.csv'
        if etf_path.exists():
            etf_df = pd.read_csv(
                etf_path,
                dtype=str,
                low_memory=False,
                usecols=lambda c: str(c).strip().lower() in {'code', 'ts_code', 'symbol'}
            )
            if etf_df is not None and not etf_df.empty:
                code_col = next((col for col in etf_df.columns if str(col).strip().lower() in {'code', 'ts_code', 'symbol'}), None)
                if code_col:
                    for code in etf_df[code_col].dropna().astype(str).str.strip().unique().tolist():
                        _append_target(code, code, 'etf', 'universe')
    except Exception as e:
        logger.warning(f"收集ETF预测目标失败: {e}")

    try:
        fund_rows = session.query(RawFundData.code, RawFundData.name).distinct().all()
        for code, name in fund_rows:
            _append_target(code, name, 'active_fund', 'universe')
    except Exception as e:
        logger.warning(f"收集基金预测目标失败: {e}")

    _append_target('gold', '黄金', 'gold', 'universe')
    _append_target('silver', '白银', 'silver', 'universe')

    return targets


def generate_daily_predictions():
    """每日生成预测（支持所有资产类型）- 优化版"""
    try:
        from models import get_session, Prediction, Holding
        from predictors.short_term import ShortTermPredictor
        from predictors.medium_term import MediumTermPredictor
        from predictors.long_term import LongTermPredictor
        from datetime import date, timedelta
        import yfinance as yf
        import pickle
        
        logger.info("开始生成每日预测...")
        
        session = get_session()
        
        today = _resolve_operational_trade_date()
        targets = _collect_prediction_targets(session, today)
        
        if not targets:
            logger.warning("无持仓或推荐数据，跳过预测生成")
            session.close()
            return
        
        # 初始化预测器字典（支持多资产类型）
        predictors = _build_prediction_predictors()
        shared_stock_collector = None
        try:
            from collectors.stock_collector import StockCollector
            shared_stock_collector = StockCollector()
        except Exception as e:
            logger.warning(f"共享股票采集器初始化失败，将按需回退: {e}")
        
        # 加载所有可用模型
        _load_all_models(predictors)
        logger.info("✅ 模型加载完成")
        
        if today != datetime.now().date():
            logger.info(f"当前已过{MARKET_ACTION_CUTOFF_TIME}，本次预测结果归入下个交易日: {today}")
        prediction_count = 0
        error_count = 0
        batch_size = 20
        processed_count = 0
        
        for target in targets:
            code = target['code']
            name = target.get('name') or code
            asset_type = _normalize_runtime_prediction_asset_type(target.get('asset_type'), code)
            if asset_type not in predictors:
                logger.warning(f"未知资产类型: {asset_type}，跳过 {code}")
                continue
            
            try:
                asset_predictors = predictors.get(asset_type)
                if not asset_predictors:
                    error_count += 1
                    continue
                
                result = _predict_by_asset_type_v2(
                    code, asset_type, asset_predictors, stock_collector=shared_stock_collector
                )
                
                if result is None:
                    error_count += 1
                    continue
                
                short_result, medium_result, long_result = result
                
                prediction_periods = _resolve_target_prediction_periods(target)
                predictions_saved = _save_predictions(
                    session, code, name, today, 
                    short_result, medium_result, long_result,
                    periods=prediction_periods,
                )
                prediction_count += predictions_saved
                processed_count += 1

                if processed_count % batch_size == 0:
                    session.commit()
                    logger.info(
                        f"预测进度: {processed_count}/{len(targets)}，当前累计写入 {prediction_count} 条，失败 {error_count} 个标的"
                    )
                
                logger.debug(f"已生成 {code} ({asset_type}, source={target.get('source')}) 的预测")
                
            except Exception as e:
                logger.error(f"生成 {code} 预测失败: {e}")
                error_count += 1
                processed_count += 1
                if processed_count % batch_size == 0:
                    session.commit()
                    logger.info(
                        f"预测进度: {processed_count}/{len(targets)}，当前累计写入 {prediction_count} 条，失败 {error_count} 个标的"
                    )
                continue
        
        session.commit()
        session.close()
        
        logger.info(f"每日预测生成完成: 覆盖 {len(targets)} 个目标, 新增 {prediction_count} 条预测, 失败 {error_count} 个标的")
        
    except Exception as e:
        logger.error(f"每日预测生成失败: {e}", exc_info=True)


def _predict_by_asset_type_v2(code: str, asset_type: str, asset_predictors: dict, stock_collector=None):
    """根据资产类型进行预测（新版本，支持所有资产类型）- 优化版"""
    import yfinance as yf
    
    try:
        # 获取对应的预测器
        short_pred = asset_predictors.get('short')
        medium_pred = asset_predictors.get('medium')
        long_pred = asset_predictors.get('long')
        
        if not short_pred:
            default_pred = _get_default_prediction()
            return default_pred, default_pred, default_pred
        
        # 根据资产类型获取数据
        if asset_type == 'a_stock':
            # A股数据从本地数据库或数据源
            try:
                from collectors.stock_collector import StockCollector
                collector = stock_collector or StockCollector()
                df = collector.get_stock_data_from_db(code)
                if df is None or len(df) < 60:
                    logger.warning(f"A股 {code} 数据不足，回退默认预测")
                    default_pred = _get_default_prediction()
                    return default_pred, default_pred, default_pred
            except Exception as e:
                logger.debug(f"获取A股数据失败 {code}: {e}")
                default_pred = _get_default_prediction()
                return default_pred, default_pred, default_pred
        
        elif asset_type == 'hk_stock':
            ticker = yf.Ticker(_normalize_yfinance_symbol(code))
            df = ticker.history(period='6mo')
            if len(df) < 60:
                logger.warning(f"港股 {code} 数据不足，回退默认预测")
                default_pred = _get_default_prediction()
                return default_pred, default_pred, default_pred
            df.columns = [col.lower() for col in df.columns]
        
        elif asset_type == 'us_stock':
            ticker = yf.Ticker(_normalize_yfinance_symbol(code))
            df = ticker.history(period='6mo')
            if len(df) < 60:
                logger.warning(f"美股 {code} 数据不足，回退默认预测")
                default_pred = _get_default_prediction()
                return default_pred, default_pred, default_pred
            df.columns = [col.lower() for col in df.columns]
        
        elif asset_type == 'gold':
            ticker = yf.Ticker('GC=F')
            df = ticker.history(period='6mo')
            if len(df) < 60:
                logger.warning(f"黄金数据不足，回退默认预测")
                default_pred = _get_default_prediction()
                return default_pred, default_pred, default_pred
            df.columns = [col.lower() for col in df.columns]
        
        elif asset_type == 'silver':
            ticker = yf.Ticker('SI=F')
            df = ticker.history(period='6mo')
            if len(df) < 60:
                logger.warning(f"白银数据不足，回退默认预测")
                default_pred = _get_default_prediction()
                return default_pred, default_pred, default_pred
            df.columns = [col.lower() for col in df.columns]
        
        elif asset_type == 'etf':
            try:
                from collectors.stock_collector import StockCollector
                collector = stock_collector or StockCollector()
                df = collector.get_stock_data_from_db(code)
            except Exception:
                df = None

            if df is None or len(df) < 60:
                try:
                    ticker = yf.Ticker(_normalize_yfinance_symbol(code))
                    df = ticker.history(period='6mo')
                    if df is not None and not df.empty:
                        df.columns = [col.lower() for col in df.columns]
                except Exception:
                    df = None

            if df is None or len(df) < 60:
                logger.warning(f"ETF {code} 数据不足，回退默认预测")
                default_pred = _get_default_prediction()
                return default_pred, default_pred, default_pred

        elif asset_type == 'fund':
            # 主动基金通常使用评分而不是短线模型，先落默认预测保证覆盖完整
            default_pred = _get_default_prediction()
            logger.debug(f"基金 {code} 使用默认预测")
            return default_pred, default_pred, default_pred
        
        else:
            logger.warning(f"不支持的资产类型: {asset_type}，回退默认预测")
            default_pred = _get_default_prediction()
            return default_pred, default_pred, default_pred
        
        # 执行预测
        short_result = short_pred.get_prediction_result(df) if short_pred.is_trained else _get_default_prediction()
        medium_result = medium_pred.get_prediction_result(df) if medium_pred and medium_pred.is_trained else _get_default_prediction()
        long_result = long_pred.get_prediction_result(df) if long_pred and long_pred.is_trained else _get_default_prediction()
        
        return short_result, medium_result, long_result
        
    except Exception as e:
        logger.error(f"预测 {code} ({asset_type}) 失败: {e}")
        default_pred = _get_default_prediction()
        return default_pred, default_pred, default_pred


def _predict_by_asset_type(code: str, asset_type: str, short_predictor, 
                           medium_predictor, long_predictor):
    """根据资产类型进行预测 - 已弃用，保留用于兼容性"""
    import yfinance as yf
    
    # 股票/ETF
    if asset_type in ['stock', 'etf']:
        ticker = yf.Ticker(code)
        df = ticker.history(period='6mo')
        if len(df) < 60:
            logger.warning(f"{code} 数据不足，跳过")
            return None
        df.columns = [col.lower() for col in df.columns]
        
        short_result = short_predictor.get_prediction_result(df)
        medium_result = medium_predictor.get_prediction_result(df)
        long_result = long_predictor.get_prediction_result(df)
        
        return short_result, medium_result, long_result
    
    # 黄金
    elif asset_type == 'gold':
        ticker = yf.Ticker('GC=F')
        df = ticker.history(period='6mo')
        if len(df) < 60:
            logger.warning(f"黄金数据不足")
            return None
        df.columns = [col.lower() for col in df.columns]
        
        # 使用黄金模型或默认预测
        gold_model_path = get_model_path('gold')
        gold_model_data = load_model_if_exists(gold_model_path)
        
        if gold_model_data:
            # 使用模型预测（简化实现）
            short_result = _predict_with_gold_model(df, gold_model_data)
        else:
            short_result = _get_default_prediction()
        
        medium_result = short_result
        long_result = short_result
        
        return short_result, medium_result, long_result
    
    # 白银
    elif asset_type == 'silver':
        ticker = yf.Ticker('SI=F')
        df = ticker.history(period='6mo')
        if len(df) < 60:
            logger.warning(f"白银数据不足")
            return None
        df.columns = [col.lower() for col in df.columns]
        
        silver_model_path = get_model_path('silver')
        silver_model_data = load_model_if_exists(silver_model_path)
        
        if silver_model_data:
            short_result = _predict_with_silver_model(df, silver_model_data)
        else:
            short_result = _get_default_prediction()
        
        medium_result = short_result
        long_result = short_result
        
        return short_result, medium_result, long_result
    
    # 基金
    elif asset_type == 'fund':
        default_pred = _get_default_prediction()
        return default_pred, default_pred, default_pred
    
    else:
        logger.warning(f"未知资产类型: {asset_type}")
        return None


def _predict_with_gold_model(df, model_data):
    """使用黄金模型预测"""
    try:
        from sklearn.preprocessing import StandardScaler
        from indicators.feature_extractor import get_feature_extractor
        
        extractor = get_feature_extractor()
        X = extractor.extract_features_from_df(df)
        
        if X is not None:
            model = model_data.get('model')
            scaler = model_data.get('scaler')
            
            if scaler:
                X_scaled = scaler.transform(X)
            else:
                X_scaled = X.values
            
            prob = model.predict_proba(X_scaled)[0]
            up_prob = prob[1] * 100
        else:
            up_prob = 52
    except Exception as e:
        logger.warning(f"黄金模型预测失败: {e}, 使用默认值")
        up_prob = 52
    
    return {
        'up_probability': up_prob,
        'down_probability': 100 - up_prob,
        'target_low': 0,
        'target_high': 0,
        'stop_loss': 0,
        'confidence': 55
    }


def _predict_with_silver_model(df, model_data):
    """使用白银模型预测"""
    try:
        from sklearn.preprocessing import StandardScaler
        from indicators.feature_extractor import get_feature_extractor
        
        extractor = get_feature_extractor()
        X = extractor.extract_features_from_df(df)
        
        if X is not None:
            model = model_data.get('model')
            scaler = model_data.get('scaler')
            
            if scaler:
                X_scaled = scaler.transform(X)
            else:
                X_scaled = X.values
            
            prob = model.predict_proba(X_scaled)[0]
            up_prob = prob[1] * 100
        else:
            up_prob = 50
    except Exception as e:
        logger.warning(f"白银模型预测失败: {e}, 使用默认值")
        up_prob = 50
    
    return {
        'up_probability': up_prob,
        'down_probability': 100 - up_prob,
        'target_low': 0,
        'target_high': 0,
        'stop_loss': 0,
        'confidence': 50
    }


def _get_default_prediction():
    """获取默认预测值"""
    return {
        'up_probability': 50,
        'down_probability': 50,
        'target_low': 0,
        'target_high': 0,
        'stop_loss': 0,
        'confidence': 50
    }


def ensure_daily_predictions_current(now: Optional[datetime] = None) -> Dict[str, Any]:
    """若服务启动时错过了08:00预测任务，则自动补生成当日预测。"""
    session = None
    target_date = _resolve_operational_trade_date(now)

    try:
        from models import get_session as _db_get_session, Prediction, Holding

        session_factory = globals().get('get_session', _db_get_session)
        session = session_factory()

        targets = _collect_prediction_targets(session, target_date)
        target_count = len(targets)
        if target_count <= 0:
            logger.info("启动补偿检查: 无可用资产目标，跳过当日预测补生成")
            return {
                'triggered': False,
                'reason': 'no_targets',
                'trade_date': str(target_date),
                'holding_count': 0,
                'existing_count': 0,
            }

        existing_count = session.query(Prediction).filter(
            Prediction.date == target_date
        ).count()
        expected_count = _count_expected_prediction_records(targets)
        minimum_healthy_count = max(target_count, int(expected_count * 0.8))

        if existing_count >= minimum_healthy_count:
            logger.info(f"启动补偿检查: {target_date} 已有 {existing_count} 条预测，目标覆盖约 {target_count} 个资产，无需补生成")
            return {
                'triggered': False,
                'reason': 'already_current',
                'trade_date': str(target_date),
                'holding_count': target_count,
                'existing_count': existing_count,
            }
    except Exception as e:
        logger.warning(f"启动补偿检查失败: {e}")
        return {
            'triggered': False,
            'reason': f'check_failed: {e}',
            'trade_date': str(target_date),
            'holding_count': 0,
            'existing_count': 0,
        }
    finally:
        if session:
            session.close()

    logger.info(f"启动补偿检查: {target_date} 缺少预测记录，已转入后台补生成")
    threading.Thread(target=generate_daily_predictions, daemon=True).start()
    return {
        'triggered': True,
        'reason': 'missing_today_predictions_async',
        'trade_date': str(target_date),
    }


def _save_predictions(session, code: str, name: str, today, 
                      short_result, medium_result, long_result, periods=None) -> int:
    """保存预测记录，按 代码+日期+周期 做幂等更新，避免重复写入。"""
    from models import Prediction
    from datetime import timedelta
    from utils import get_asset_type_from_code

    def _normalize_prediction_asset_type(target_code: str) -> str:
        inferred = get_asset_type_from_code(target_code)
        upper = str(target_code or '').upper()
        if inferred == 'fund':
            return 'active_fund'
        if inferred in ('etf', 'gold', 'silver'):
            return inferred
        if upper.endswith('.HK'):
            return 'hk_stock'
        if upper.endswith(('.SH', '.SZ', '.BJ')):
            return 'a_stock'
        return 'us_stock'

    def _upsert_prediction(period_days: int, result: dict, expiry_days: int) -> int:
        existing = session.query(Prediction).filter(
            Prediction.code == code,
            Prediction.date == today,
            Prediction.period_days == period_days
        ).first()

        payload = {
            'name': name,
            'asset_type': _normalize_prediction_asset_type(code),
            'up_probability': result['up_probability'],
            'down_probability': result['down_probability'],
            'target_low': result.get('target_low', 0),
            'target_high': result.get('target_high', 0),
            'stop_loss': result.get('stop_loss', 0),
            'confidence': result.get('confidence', 50),
            'expiry_date': today + timedelta(days=expiry_days),
            'is_expired': False,
            'actual_price': None,
            'actual_return': None,
            'is_direction_correct': None,
            'is_target_correct': None,
        }

        if existing:
            for key, value in payload.items():
                setattr(existing, key, value)
            return 0

        session.add(Prediction(
            code=code,
            date=today,
            period_days=period_days,
            **payload
        ))
        return 1

    selected_periods = {int(p) for p in (periods or PREDICTION_PERIODS)}
    count = 0
    if 5 in selected_periods:
        count += _upsert_prediction(5, short_result, 5)
    if 20 in selected_periods:
        count += _upsert_prediction(20, medium_result, 20)
    if 60 in selected_periods:
        count += _upsert_prediction(60, long_result, 60)
    return count


def _build_stock_recommendations_from_predictions(session, stock_recommender, rec_type: str, limit: int = 20):
    """优先使用最新预测快照筛出更有看涨潜力的股票候选，避免全市场慢扫描。"""
    from models import Prediction

    market_map = {'a_stock': 'A', 'hk_stock': 'H', 'us_stock': 'US'}
    market = market_map.get(rec_type, 'A')

    def _matches_market(code: str) -> bool:
        upper = str(code or '').upper()
        if rec_type == 'a_stock':
            return upper.endswith(('.SH', '.SZ', '.BJ'))
        if rec_type == 'hk_stock':
            return upper.endswith('.HK')
        if rec_type == 'us_stock':
            return '.' not in upper or not upper.endswith(('.SH', '.SZ', '.BJ', '.HK'))
        return True

    candidate_codes = []
    seen = set()
    latest_date_row = (
        session.query(Prediction.date)
        .filter(Prediction.asset_type == rec_type)
        .order_by(Prediction.date.desc())
        .first()
    )
    latest_date = latest_date_row[0] if latest_date_row else None

    if latest_date is not None:
        for period in (20, 5, 60):
            rows = (
                session.query(Prediction.code, Prediction.up_probability, Prediction.confidence)
                .filter(Prediction.asset_type == rec_type)
                .filter(Prediction.date == latest_date)
                .filter(Prediction.period_days == period)
                .order_by(Prediction.up_probability.desc(), Prediction.confidence.desc())
                .limit(limit * 10)
                .all()
            )
            for code, up_prob, confidence in rows:
                upper = str(code or '').upper()
                if not upper or upper in seen or not _matches_market(upper):
                    continue
                # 过滤纯默认中性值，优先保留真正有方向性的候选
                if abs(float(up_prob or 50.0) - 50.0) < 0.05 and abs(float(confidence or 50.0) - 50.0) < 0.05:
                    continue
                seen.add(upper)
                candidate_codes.append(upper)

    if not candidate_codes:
        return stock_recommender.get_top_recommendations(market, limit)

    candidate_codes = [code for code in candidate_codes if stock_recommender._has_sufficient_local_history(code, min_rows=60)]
    if not candidate_codes:
        return stock_recommender.get_top_recommendations(market, limit)

    recommendations = []
    for code in candidate_codes[: max(limit * 6, 60)]:
        try:
            analysis = stock_recommender.get_stock_analysis(str(code), market)
            if analysis:
                recommendations.append({
                    'code': str(code),
                    'name': str(code).split('.')[0],
                    'current_price': analysis['current_price'],
                    'total_score': analysis['total_score'],
                    'up_probability_5d': analysis['predictions']['short_term']['up_probability'],
                    'up_probability_20d': analysis['predictions']['medium_term']['up_probability'],
                    'up_probability_60d': analysis['predictions']['long_term']['up_probability'],
                    'trend_direction': analysis.get('unified_trend', {}).get('trend_direction', 'neutral'),
                    'trend_score': analysis.get('unified_trend', {}).get('trend_score', 50.0),
                    'trend_confidence': analysis.get('unified_trend', {}).get('trend_confidence', 20.0),
                    'advisor_action': analysis.get('advisor_view', {}).get('action', 'hold'),
                    'advisor_confidence': analysis.get('advisor_view', {}).get('confidence', 'low'),
                    'risk_level': analysis.get('advisor_view', {}).get('risk_level', 'medium'),
                    'position_size_pct': analysis.get('advisor_view', {}).get('position_size_pct', 0),
                    'stop_loss_pct': analysis.get('advisor_view', {}).get('stop_loss_pct', 0.07),
                    'take_profit_pct': analysis.get('advisor_view', {}).get('take_profit_pct', 0.16),
                    'volatility_level': analysis['volatility_level'],
                    'reason_summary': analysis.get('advisor_view', {}).get('summary', analysis['reason'][:100] if analysis['reason'] else '')
                })
        except Exception as e:
            logger.debug(f"基于预测重建股票推荐失败 {code}: {e}")
            continue

    if not recommendations:
        return stock_recommender.get_top_recommendations(market, limit)

    action_priority = {'buy': 5, 'add': 4, 'hold': 3, 'watch': 2, 'reduce': 1, 'sell': 0}
    recommendations.sort(
        key=lambda x: (
            action_priority.get(x.get('advisor_action', 'watch'), 2),
            float(x.get('up_probability_20d', 50.0) or 50.0),
            float(x.get('up_probability_5d', 50.0) or 50.0),
            float(x.get('total_score', 0.0) or 0.0),
        ),
        reverse=True,
    )
    for i, rec in enumerate(recommendations[:limit]):
        rec['rank'] = i + 1
    return recommendations[:limit]


def rebuild_today_recommendations() -> Dict[str, Any]:
    """重建当日推荐并写入数据库，供调度与API手动刷新共用。"""
    try:
        from recommenders.stock_recommender import StockRecommender
        from recommenders.fund_recommender import FundRecommender
        from recommenders.etf_recommender import ETFRecommender
        from recommenders.gold_recommender import GoldRecommender
        from recommendation_probability import build_empirical_calibrators, derive_probabilities
        from models import get_session, Recommendation
        from datetime import date
        
        logger.info("开始重建今日投资推荐...")
        
        stock_recommender = StockRecommender()
        fund_recommender = FundRecommender()
        etf_recommender = ETFRecommender()
        gold_recommender = GoldRecommender()
        
        session = get_session()

        recommendations = {
            'a_stock': _build_stock_recommendations_from_predictions(session, stock_recommender, 'a_stock', 20),
            'hk_stock': _build_stock_recommendations_from_predictions(session, stock_recommender, 'hk_stock', 20),
            'us_stock': _build_stock_recommendations_from_predictions(session, stock_recommender, 'us_stock', 20),
            'active_fund': fund_recommender.get_recommendations(20),
            'etf': etf_recommender.get_recommendations(20),
            'gold': gold_recommender.get_gold_recommendations(),
            'silver': gold_recommender.get_silver_recommendations(),
        }
        today = _resolve_operational_trade_date()
        if today != datetime.now().date():
            logger.info(f"当前已过{MARKET_ACTION_CUTOFF_TIME}，本次推荐归入下个交易日: {today}")
        calibrators = build_empirical_calibrators(
            session=session,
            recommendation_model=Recommendation,
            today=today,
            rec_types=['a_stock', 'hk_stock', 'us_stock', 'active_fund', 'etf', 'gold', 'silver'],
            lookback_days=240,
        )
        
        # 删除今日旧推荐
        deleted = session.query(Recommendation).filter(Recommendation.date == today).delete()
        if deleted > 0:
            logger.info(f"删除今日旧推荐: {deleted} 条")
        
        total_count = 0
        for rec_type, rec_list in recommendations.items():
            for i, rec in enumerate(rec_list):
                try:
                    total_score = rec.get('total_score', rec.get('score', 3.0))
                    up5, up20, up60 = derive_probabilities(rec, rec_type, calibrators)
                    db_rec = Recommendation(
                        date=today,
                        code=rec['code'],
                        name=rec.get('name', rec['code']),
                        type=rec_type,
                        rank=i+1,
                        total_score=float(total_score) if total_score is not None else 3.0,
                        current_price=rec.get('current_price', 0),
                        up_probability_5d=up5,
                        up_probability_20d=up20,
                        up_probability_60d=up60,
                        volatility_level=rec.get('volatility_level', 'medium'),
                        reason_summary=rec.get('reason_summary', rec.get('reason', ''))
                    )
                    session.add(db_rec)
                    total_count += 1
                except Exception as rec_err:
                    logger.warning(f"写入推荐失败[{rec_type}] {rec.get('code', 'unknown')}: {rec_err}")
                    continue
        
        session.commit()
        session.close()
        
        logger.info(f"今日推荐重建完成: 共 {total_count} 条推荐")
        return {
            'success': True,
            'total_count': total_count,
            'date': today.isoformat(),
        }
        
    except Exception as e:
        logger.error(f"重建今日推荐失败: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e),
        }


def generate_daily_recommendations():
    """每日生成投资推荐（收盘后生成，默认用于下个交易日）"""
    result = rebuild_today_recommendations()
    if not result.get('success'):
        logger.error(f"每日推荐生成失败: {result.get('error')}")


def take_daily_snapshot():
    """每日持仓快照 - 优化版"""
    try:
        from models import get_session, Holding, HoldingSnapshot
        from datetime import date
        from api.holdings import get_current_price
        
        session = get_session()
        today = date.today()
        
        # 检查今日快照是否已存在
        existing = session.query(HoldingSnapshot).filter(
            HoldingSnapshot.snapshot_date == today
        ).first()
        
        if existing:
            logger.info(f"今日快照已存在: {today}")
            session.close()
            return
        
        holdings = session.query(Holding).all()
        if not holdings:
            logger.info("无持仓数据，跳过快照")
            session.close()
            return
        
        snapshot_count = 0
        error_count = 0
        
        for h in holdings:
            asset_type = getattr(h, 'asset_type', 'stock')
            try:
                price = get_current_price(h.code, asset_type)
                if price is None or price <= 0:
                    price = h.cost_price
                    logger.debug(f"{h.code} 获取实时价格失败，使用成本价")
            except Exception as e:
                logger.warning(f"获取价格失败 {h.code}: {e}, 使用成本价")
                price = h.cost_price
                error_count += 1
            
            market_value = h.quantity * price
            
            snapshot = HoldingSnapshot(
                snapshot_date=today,
                holding_id=h.id,
                asset_type=asset_type,
                code=h.code,
                name=h.name,
                quantity=h.quantity,
                cost_price=h.cost_price,
                market_price=price,
                market_value=market_value
            )
            session.add(snapshot)
            snapshot_count += 1
        
        session.commit()
        session.close()
        
        logger.info(f"每日快照已保存: {today}, 共 {snapshot_count} 条记录, 价格获取失败 {error_count} 个")
        
    except Exception as e:
        logger.error(f"每日快照失败: {e}", exc_info=True)


def cleanup_old_data():
    """清理过期数据 - 优化版"""
    from models import get_session, RawStockData, RawFundData, HoldingSnapshot, Warning as WarningModel, Log
    from datetime import timedelta

    def cleanup_csv_data(csv_cutoff_date):
        """清理data目录下按日期增量累积的CSV，仅保留最近一年。"""
        # 仅处理时序数据文件，避免误删基础主数据（如 stock_basic / all_stocks）
        csv_date_columns = {
            'daily_basic.csv': ['trade_date'],
            'financial_indicator.csv': ['end_date', 'ann_date'],
            'historical_a_stock.csv': ['date'],
            'historical_hk_stock.csv': ['date'],
            'historical_us_stock.csv': ['date'],
            'macro_cpi.csv': ['month'],
            'macro_pmi.csv': ['MONTH', 'month'],
            'macro_shibor.csv': ['date'],
            'margin_all.csv': ['trade_date'],
            'moneyflow_all.csv': ['trade_date'],
            'news_all.csv': ['datetime'],
            'north_money_all.csv': ['trade_date'],
            'research_report.csv': ['trade_date', 'publish_date'],
            'top_list.csv': ['trade_date'],
        }

        deleted_rows = 0
        touched_files = 0

        def parse_csv_dates(series: pd.Series) -> pd.Series:
            """稳健解析CSV日期列，优先识别常见数字日期格式。"""
            text = series.astype(str).str.strip()
            ratio_8 = text.str.fullmatch(r'\d{8}').mean()
            ratio_6 = text.str.fullmatch(r'\d{6}').mean()

            if ratio_8 >= 0.8:
                return pd.to_datetime(text, format='%Y%m%d', errors='coerce')
            if ratio_6 >= 0.8:
                return pd.to_datetime(text, format='%Y%m', errors='coerce')

            return pd.to_datetime(series, errors='coerce')

        for filename, candidates in csv_date_columns.items():
            csv_path = DATA_DIR / filename
            if not csv_path.exists():
                continue

            try:
                df = pd.read_csv(csv_path)
                if df.empty:
                    continue

                date_col = next((c for c in candidates if c in df.columns), None)
                if not date_col:
                    logger.warning(f"CSV清理跳过[{filename}]: 未找到日期列 {candidates}")
                    continue

                parsed_dates = parse_csv_dates(df[date_col])
                valid_date_mask = parsed_dates.notna()
                valid_count = int(valid_date_mask.sum())
                valid_ratio = (valid_count / len(df)) if len(df) else 0

                if valid_count == 0:
                    logger.warning(f"CSV清理跳过[{filename}]: 日期列无法解析")
                    continue

                # 安全阈值：有效日期比例过低时跳过，避免误判导致大规模误删
                if valid_ratio < 0.5:
                    logger.warning(
                        f"CSV清理跳过[{filename}]: 日期解析有效比例过低({valid_ratio:.2%})"
                    )
                    continue

                keep_mask = (~valid_date_mask) | (parsed_dates.dt.date >= csv_cutoff_date)
                original_len = len(df)
                kept_len = int(keep_mask.sum())
                deleted = original_len - kept_len

                if deleted > 0:
                    df.loc[keep_mask].to_csv(csv_path, index=False)
                    deleted_rows += deleted
                    touched_files += 1
                    logger.info(f"CSV清理[{filename}]: 删除 {deleted} 行，保留 {kept_len} 行")
            except Exception as e:
                logger.error(f"CSV清理失败[{filename}]: {e}")

        return deleted_rows, touched_files
    
    try:
        session = get_session()
        cutoff_date = datetime.now().date() - timedelta(days=RAW_DATA_RETENTION_DAYS)
        
        # 清理股票数据
        deleted_stock = session.query(RawStockData).filter(
            RawStockData.date < cutoff_date
        ).delete()
        
        # 清理基金数据
        deleted_fund = session.query(RawFundData).filter(
            RawFundData.date < cutoff_date
        ).delete()
        
        # 清理快照数据（按快照保留策略）
        snapshot_cutoff = datetime.now().date() - timedelta(days=SNAPSHOT_RETENTION_DAYS)
        deleted_snapshots = session.query(HoldingSnapshot).filter(
            HoldingSnapshot.snapshot_date < snapshot_cutoff
        ).delete()
        
        # 清理旧预警（保留30天）
        warning_cutoff = datetime.now() - timedelta(days=30)
        deleted_warnings = session.query(WarningModel).filter(
            WarningModel.warning_time < warning_cutoff
        ).delete()

        # 清理系统日志（保留7天）
        log_cutoff = datetime.now() - timedelta(days=7)
        deleted_logs = session.query(Log).filter(
            Log.log_time < log_cutoff
        ).delete()

        # 清理CSV增量数据（保留最近一年）
        deleted_csv_rows, touched_csv_files = cleanup_csv_data(cutoff_date)
        
        session.commit()
        session.close()
        
        logger.info(f"数据清理完成: "
                   f"删除 {deleted_stock} 条股票数据, "
                   f"{deleted_fund} 条基金数据, "
                   f"{deleted_snapshots} 条快照数据, "
                   f"{deleted_warnings} 条预警数据, "
                   f"{deleted_logs} 条系统日志, "
                   f"{deleted_csv_rows} 行CSV数据({touched_csv_files}个文件)")
        
    except Exception as e:
        logger.error(f"数据清理失败: {e}", exc_info=True)


def update_asset_pools():
    """每日更新资产池 - 优化版"""
    try:
        from collectors.stock_collector import StockCollector
        from collectors.fund_collector import FundCollector
        
        logger.info("开始更新资产池...")
        
        stock_collector = StockCollector()
        fund_collector = FundCollector()
        
        # 更新股票池
        stock_result = stock_collector.update_all_stock_pools()
        logger.info(f"股票池更新完成: {stock_result}")
        
        # 更新基金池
        fund_count = fund_collector.update_fund_pool()
        logger.info(f"基金池更新完成: {fund_count}只")
        
        logger.info("资产池更新任务完成")
        
    except Exception as e:
        logger.error(f"资产池更新失败: {e}", exc_info=True)


def refresh_recommendations():
    """刷新推荐（手动触发）"""
    try:
        result = rebuild_today_recommendations()
        if result.get('success'):
            logger.info(f"推荐已手动刷新: {result.get('total_count', 0)} 条")
        else:
            logger.error(f"推荐手动刷新失败: {result.get('error')}")
    except Exception as e:
        logger.error(f"刷新推荐失败: {e}")


def incremental_backfill_recommendation_history(days: int = 3):
    """增量回填推荐历史样本，维持概率校准数据新鲜度。"""
    try:
        from scripts.backfill_recommendation_history import backfill

        safe_days = max(1, int(days))
        logger.info(f"开始增量回填推荐历史样本: 最近{safe_days}天")
        backfill(safe_days)
        logger.info("增量回填推荐历史样本完成")
    except Exception as e:
        logger.error(f"增量回填推荐历史样本失败: {e}", exc_info=True)


def deep_probability_recalibration():
    """周期性深度校准：完整重算60天样本以检查参数漂移。"""
    try:
        from scripts.backfill_recommendation_history import backfill
        from models import get_session, Recommendation

        logger.info("开始周期性深度概率校准...")
        
        # 完整重算近60天
        backfill(60)

        # 使用数据库本地统计替代HTTP回调，避免进程内网络耦合。
        session = None
        try:
            session = get_session()
            today = datetime.now().date()
            recent_days = 60
            sample_count = session.query(Recommendation).filter(
                Recommendation.date >= (today - timedelta(days=recent_days))
            ).count()
            logger.info(f"周期校准完成 - 最近{recent_days}天推荐样本数: {sample_count}")
        finally:
            if session:
                session.close()
        
        logger.info("周期性深度校准已完成")
    except Exception as e:
        logger.error(f"周期性深度校准失败: {e}", exc_info=True)


def send_daily_future_signals_alert(force: bool = False):
    """发送每日未来信号告警（仅在15:00前作为操作提醒发送）"""
    try:
        if not force and not _is_actionable_market_window():
            logger.info(f"当前已过{MARKET_ACTION_CUTOFF_TIME}，未来信号仅作为下个交易日参考，跳过即时告警")
            return
        from api.holdings import build_future_signals_data
        from alerts.notifier import Notifier

        signals_data = build_future_signals_data()
        
        # 2. 发送告警通知
        notifier = Notifier()
        result = notifier.send_future_signals_alert(signals_data)
        
        summary = signals_data.get('summary', {})
        action_count = summary.get('action_count', 0)
        risk_count = summary.get('risk_alert_count', 0)
        unheld_count = summary.get('unheld_count', 0)
        
        if result.get('sent'):
            logger.info(
                f"未来信号告警已发送 - "
                f"操作建议: {action_count}, 风险预警: {risk_count}, 推荐资产: {unheld_count}"
            )
        else:
            logger.debug(f"无未来信号或推送配置未完成 - 操作: {action_count}, 风险: {risk_count}")
        
        notifier.close()
        
    except Exception as e:
        logger.error(f"发送未来信号告警失败: {e}", exc_info=True)


def _safe_float(value, default=0.0):
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _latest_close_and_change(symbol: str, period: str = '5d') -> Dict[str, float]:
    """获取标的最新收盘及单日变化率。"""
    try:
        import yfinance as yf

        df = yf.Ticker(symbol).history(period=period)
        if df is None or len(df) == 0:
            return {'last': 0.0, 'chg_1d': 0.0}

        close = pd.to_numeric(df['Close'], errors='coerce').dropna()
        if len(close) == 0:
            return {'last': 0.0, 'chg_1d': 0.0}

        last = _safe_float(close.iloc[-1], 0.0)
        prev = _safe_float(close.iloc[-2], last) if len(close) > 1 else last
        chg_1d = ((last - prev) / prev) if abs(prev) > 1e-12 else 0.0
        return {'last': last, 'chg_1d': chg_1d}
    except Exception:
        return {'last': 0.0, 'chg_1d': 0.0}


def _read_realtime_news_signal(hours: int = 6) -> Dict[str, Any]:
    """读取最近新闻并提取快频风险关键词信号。"""
    path = DATA_DIR / 'news_all.csv'
    if not path.exists():
        return {
            'recent_count': 0,
            'geo_hits': 0,
            'hawk_hits': 0,
            'dove_hits': 0,
            'avg_sentiment': 0.0,
        }

    try:
        df = pd.read_csv(path, low_memory=False)
        if len(df) == 0:
            return {
                'recent_count': 0,
                'geo_hits': 0,
                'hawk_hits': 0,
                'dove_hits': 0,
                'avg_sentiment': 0.0,
            }

        time_col = None
        for c in ['datetime', 'pub_date', 'created_at']:
            if c in df.columns:
                time_col = c
                break

        if time_col is not None:
            ts = pd.to_datetime(df[time_col], errors='coerce')
            cutoff = datetime.now() - timedelta(hours=hours)
            recent = df[ts >= cutoff].copy()
        else:
            recent = df.tail(500).copy()

        if len(recent) == 0:
            recent = df.tail(500).copy()

        title = recent['title'].astype(str) if 'title' in recent.columns else pd.Series([''] * len(recent))
        content = recent['content'].astype(str) if 'content' in recent.columns else pd.Series([''] * len(recent))
        text = (title + ' ' + content).str.lower().fillna('')

        geo_keywords = [
            '战争', '冲突', '制裁', '停火', '军事', '袭击', '导弹', '边境',
            'war', 'conflict', 'sanction', 'strike', 'ceasefire', 'missile',
        ]
        hawk_keywords = ['加息', '紧缩', '鹰派', '缩表', 'rate hike', 'hawkish', 'tightening']
        dove_keywords = ['降息', '宽松', '鸽派', 'qe', 'rate cut', 'dovish', 'easing']

        def _count_hits(keywords):
            if len(text) == 0:
                return 0
            return int(sum(text.str.contains(k, regex=False).sum() for k in keywords))

        geo_hits = _count_hits(geo_keywords)
        hawk_hits = _count_hits(hawk_keywords)
        dove_hits = _count_hits(dove_keywords)

        avg_sentiment = 0.0
        if 'sentiment' in recent.columns:
            avg_sentiment = _safe_float(pd.to_numeric(recent['sentiment'], errors='coerce').mean(), 0.0)

        return {
            'recent_count': int(len(recent)),
            'geo_hits': geo_hits,
            'hawk_hits': hawk_hits,
            'dove_hits': dove_hits,
            'avg_sentiment': avg_sentiment,
        }
    except Exception as e:
        logger.warning(f"读取快频舆情失败: {e}")
        return {
            'recent_count': 0,
            'geo_hits': 0,
            'hawk_hits': 0,
            'dove_hits': 0,
            'avg_sentiment': 0.0,
        }


def _build_realtime_risk_regime(news_sig: Dict[str, Any], market_sig: Dict[str, Any]) -> Dict[str, Any]:
    """构建快频风险状态。"""
    score = 0
    reasons: List[str] = []

    vix = _safe_float(market_sig.get('vix'), 0.0)
    tnx_chg = _safe_float(market_sig.get('tnx_chg_1d'), 0.0)
    dxy_chg = _safe_float(market_sig.get('dxy_chg_1d'), 0.0)

    geo_hits = int(news_sig.get('geo_hits', 0) or 0)
    hawk_hits = int(news_sig.get('hawk_hits', 0) or 0)
    dove_hits = int(news_sig.get('dove_hits', 0) or 0)

    if vix >= 30:
        score += 3
        reasons.append(f"VIX高位({vix:.1f})")
    elif vix >= 24:
        score += 2
        reasons.append(f"VIX偏高({vix:.1f})")

    if tnx_chg >= 0.02:
        score += 2
        reasons.append(f"10Y利率单日上行({tnx_chg*100:.2f}%)")
    elif tnx_chg <= -0.02:
        score -= 1
        reasons.append(f"10Y利率单日回落({tnx_chg*100:.2f}%)")

    if dxy_chg >= 0.01:
        score += 1
        reasons.append(f"美元指数走强({dxy_chg*100:.2f}%)")

    if geo_hits >= 3:
        score += 3
        reasons.append(f"地缘风险新闻高频({geo_hits})")
    elif geo_hits >= 1:
        score += 1
        reasons.append(f"地缘风险新闻出现({geo_hits})")

    if hawk_hits > dove_hits:
        score += 1
        reasons.append(f"加息/鹰派信号占优({hawk_hits}:{dove_hits})")
    elif dove_hits > hawk_hits:
        score -= 1
        reasons.append(f"降息/鸽派信号占优({dove_hits}:{hawk_hits})")

    if score >= 5:
        regime = 'stress'
    elif score >= 3:
        regime = 'risk_off'
    else:
        regime = 'neutral'

    return {
        'score': score,
        'regime': regime,
        'reasons': reasons,
    }


def _suggest_action_for_holding(asset_type: str, regime: Dict[str, Any], news_sig: Dict[str, Any]) -> Dict[str, str]:
    """根据快频风险状态给出持仓动作建议。"""
    r = regime.get('regime', 'neutral')
    score = int(regime.get('score', 0) or 0)
    geo_hits = int(news_sig.get('geo_hits', 0) or 0)
    hawk_hits = int(news_sig.get('hawk_hits', 0) or 0)
    dove_hits = int(news_sig.get('dove_hits', 0) or 0)

    asset_type = (asset_type or 'stock').lower()

    if asset_type in ['gold', 'silver']:
        if geo_hits >= 1 or r in ['risk_off', 'stress']:
            return {'action': '增仓', 'level': 'medium'}
        if dove_hits > hawk_hits and score <= 1:
            return {'action': '持有', 'level': 'low'}
        return {'action': '持有', 'level': 'low'}

    # 权益类：股票/ETF/基金
    if r == 'stress':
        return {'action': '清仓', 'level': 'high'}
    if r == 'risk_off':
        return {'action': '减仓', 'level': 'medium'}
    if dove_hits > hawk_hits + 1:
        return {'action': '增仓', 'level': 'low'}
    return {'action': '持有', 'level': 'low'}


def collect_hourly_news_snapshot(hours: int = 48):
    """每小时新闻采集快照（按自然日）"""
    try:
        from collectors.news_collector import NewsCollector

        lookback_hours = max(6, min(72, int(hours)))
        max_pages = max(10, int(os.environ.get('HOURLY_NEWS_MAX_PAGES', '50')))
        NewsCollector().collect_recent_hours(hours=lookback_hours, max_pages=max_pages)
        logger.info(f"小时级新闻采集完成: lookback_hours={lookback_hours}, max_pages={max_pages}")
    except Exception as e:
        logger.warning(f"小时级新闻采集失败: {e}")


def collect_daily_research_snapshot(days: int = 5):
    """每日研报增量补采（受 TuShare 日限额约束）。"""
    try:
        from collectors.research_collector import ResearchCollector

        safe_days = max(1, min(5, int(days)))
        ResearchCollector().collect_latest(days=safe_days)
        logger.info(f"每日研报补采完成: days={safe_days}")
    except Exception as e:
        logger.warning(f"每日研报补采失败: {e}")


def scan_realtime_event_impact() -> int:
    """小时级快频扫描：舆情/地缘/利率冲击 -> 持仓动作建议。"""
    session = None
    notifier = None
    inserted = 0
    try:
        from models import get_session

        # 1) 快频市场信号
        vix_sig = _latest_close_and_change('^VIX', period='5d')
        dxy_sig = _latest_close_and_change('DX-Y.NYB', period='5d')
        if _safe_float(dxy_sig.get('last'), 0.0) <= 0:
            dxy_sig = _latest_close_and_change('UUP', period='5d')
        tnx_sig = _latest_close_and_change('^TNX', period='5d')

        market_sig = {
            'vix': _safe_float(vix_sig.get('last'), 0.0),
            'dxy': _safe_float(dxy_sig.get('last'), 0.0),
            'tnx': _safe_float(tnx_sig.get('last'), 0.0),
            'dxy_chg_1d': _safe_float(dxy_sig.get('chg_1d'), 0.0),
            'tnx_chg_1d': _safe_float(tnx_sig.get('chg_1d'), 0.0),
        }

        news_sig = _read_realtime_news_signal(hours=6)
        regime = _build_realtime_risk_regime(news_sig, market_sig)

        session = get_session()
        from models import Holding, Warning as WarningModel
        holdings = session.query(Holding).all()
        if not holdings:
            logger.info("小时级事件扫描：无持仓，跳过")
            return 0

        now = datetime.now()
        dedup_cutoff = now - timedelta(hours=2)

        for h in holdings:
            decision = _suggest_action_for_holding(getattr(h, 'asset_type', 'stock'), regime, news_sig)
            action = decision['action']
            level = decision['level']

            # 仅对“可操作动作”发预警，持有不重复刷屏
            if action == '持有':
                continue

            trigger_value = f"regime={regime['regime']};score={regime['score']};action={action}"

            dup = session.query(WarningModel).filter(
                WarningModel.code == h.code,
                WarningModel.warning_type == 'event_impact_hourly',
                WarningModel.warning_time >= dedup_cutoff,
                WarningModel.trigger_value == trigger_value,
            ).first()
            if dup:
                continue

            reasons = '；'.join(regime.get('reasons', [])[:4]) or '快频风险因子触发'
            msg = (
                f"快频事件影响: {h.name}({h.code}) 建议{action}。"
                f"风险状态={regime['regime']}({regime['score']})，"
                f"VIX={market_sig['vix']:.1f}, DXY={market_sig['dxy']:.2f}, TNX={market_sig['tnx']:.2f}, "
                f"地缘新闻命中={news_sig['geo_hits']}"
            )
            suggestion = (
                f"建议{action}，并在下一交易时段复核。"
                f"触发依据: {reasons}。"
                "若持仓高波动权益资产，优先控制仓位。"
            )

            w = WarningModel(
                code=h.code,
                name=h.name,
                warning_time=now,
                warning_type='event_impact_hourly',
                level=level,
                trigger_value=trigger_value,
                message=msg,
                suggestion=suggestion,
                is_sent=False,
            )
            session.add(w)
            inserted += 1

        session.commit()

        # 高频动作建议：仅推送 high/medium，避免噪声
        if inserted > 0:
            try:
                from alerts.notifier import Notifier
                notifier = Notifier()
                recent = session.query(WarningModel).filter(
                    WarningModel.warning_type == 'event_impact_hourly',
                    WarningModel.warning_time >= dedup_cutoff,
                    WarningModel.level.in_(['high', 'medium']),
                    WarningModel.is_sent.is_(False),
                ).all()
                for w in recent:
                    notifier.send_warning(w)
            except Exception as e:
                logger.warning(f"小时级预警推送失败: {e}")

        logger.info(
            f"小时级事件扫描完成: 新增预警={inserted}, regime={regime['regime']}, score={regime['score']}, "
            f"geo_hits={news_sig['geo_hits']}, hawk={news_sig['hawk_hits']}, dove={news_sig['dove_hits']}"
        )
        return inserted
    except Exception as e:
        if session:
            session.rollback()
        logger.error(f"小时级事件扫描失败: {e}", exc_info=True)
        return 0
    finally:
        if notifier:
            try:
                notifier.close()
            except Exception:
                pass
        if session:
            session.close()


def scan_warnings(force: bool = False):
    """扫描预警（默认仅在盘中可操作窗口执行）"""
    try:
        if not force and not _is_actionable_market_window():
            logger.debug("当前非盘中可操作窗口，跳过预警扫描")
            return 0

        from alerts.monitor import WarningMonitor
        monitor = WarningMonitor()
        count = monitor.scan_all_holdings()
        monitor.close()
        logger.info(f"预警扫描完成，发现 {count} 条新预警")
        return count
    except Exception as e:
        logger.error(f"预警扫描失败: {e}")
        return 0


def _read_json_state(path: Path, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    fallback = deepcopy(default or {})
    try:
        target = Path(path)
        if not target.exists():
            return fallback
        raw = pd.read_json(target, typ='series')
        data = raw.to_dict() if hasattr(raw, 'to_dict') else dict(raw)
        return data if isinstance(data, dict) else fallback
    except Exception:
        try:
            import json
            target = Path(path)
            if not target.exists():
                return fallback
            data = json.loads(target.read_text(encoding='utf-8'))
            return data if isinstance(data, dict) else fallback
        except Exception:
            return fallback


def _parse_status_time(value: Optional[str]):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    except Exception:
        return None


def _is_pid_alive(pid: Optional[int]) -> bool:
    try:
        if pid in (None, '', 0):
            return False
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False


def _is_training_in_progress(progress: Optional[Dict[str, Any]] = None) -> bool:
    state = progress or _read_json_state(TRAINING_PROGRESS_FILE)
    if str(state.get('status', '')).lower() != 'running':
        return False
    pid = state.get('pid')
    return _is_pid_alive(pid) if pid else True


def _default_continuous_learning_request() -> Dict[str, Any]:
    return {
        'enabled': True,
        'only_assets': ['a_stock', 'hk_stock', 'us_stock', 'etf'],
        'periods': [5],
        'summary': '资产=a_stock、hk_stock、us_stock、etf；周期=5日',
    }


def _resolve_continuous_learning_request(force: bool = False) -> Dict[str, Any]:
    progress = _read_json_state(TRAINING_PROGRESS_FILE)
    if _is_training_in_progress(progress):
        return {'should_run': False, 'reason': 'training_in_progress', 'request': None}

    status = _read_json_state(LEARNING_STATUS_FILE)
    reflection = status.get('last_reflection') or {}
    retrain = status.get('last_retrain') or {}
    auto_request = reflection.get('auto_request') or {}
    retrain_status = str(retrain.get('status', 'idle')).lower()
    retrain_time = _parse_status_time(retrain.get('time'))
    now = datetime.now()

    if force:
        return {
            'should_run': True,
            'reason': 'forced',
            'request': auto_request if auto_request.get('enabled') else _default_continuous_learning_request(),
        }

    if auto_request.get('enabled'):
        if retrain_status in {'idle', 'failed', 'skipped'}:
            return {'should_run': True, 'reason': 'reflection_pending', 'request': auto_request}
        if retrain_time is None or (now - retrain_time) >= timedelta(hours=CONTINUOUS_LEARNING_INTERVAL_HOURS):
            return {'should_run': True, 'reason': 'reflection_refresh', 'request': auto_request}

    if retrain_time is None:
        return {'should_run': True, 'reason': 'bootstrap_refresh', 'request': _default_continuous_learning_request()}

    if (now - retrain_time) >= timedelta(hours=CONTINUOUS_FULL_RETRAIN_COOLDOWN_HOURS):
        return {'should_run': True, 'reason': 'scheduled_refresh', 'request': _default_continuous_learning_request()}

    return {'should_run': False, 'reason': 'cooldown_active', 'request': auto_request or None}


def run_continuous_learning_cycle(force: bool = False):
    """常驻持续学习循环：服务启动后按间隔自动训练，并结合反思结果做定向优化。"""
    global _continuous_learning_running

    if not CONTINUOUS_LEARNING_ENABLED and not force:
        logger.info('持续学习开关已关闭，跳过本轮自动训练')
        return {'success': True, 'executed': False, 'reason': 'disabled'}

    if _continuous_learning_running:
        logger.info('持续学习任务已在运行，跳过重复触发')
        return {'success': True, 'executed': False, 'reason': 'already_running'}

    with _continuous_learning_lock:
        if _continuous_learning_running:
            return {'success': True, 'executed': False, 'reason': 'already_running'}
        _continuous_learning_running = True

    try:
        decision = _resolve_continuous_learning_request(force=force)
        if not decision.get('should_run'):
            logger.info(f"本轮持续学习跳过: {decision.get('reason')}")
            return {'success': True, 'executed': False, **decision}

        from scripts.train_asset_suite import build_training_plan, run_training_plan

        request = decision.get('request') or _default_continuous_learning_request()
        plan = build_training_plan(
            only_assets=request.get('only_assets') or None,
            periods=request.get('periods') or None,
            include_late_markets=True,
        )
        if not plan:
            logger.info('持续学习未生成有效训练计划，跳过本轮训练')
            return {'success': True, 'executed': False, 'reason': 'empty_plan', 'request': request}

        logger.info(f"启动持续学习优化: {request.get('summary') or decision.get('reason')}")
        results = run_training_plan(
            plan=plan,
            dry_run=False,
            stop_on_error=False,
            skip_existing=False,
            enable_self_optimization=False,
        )
        failed = [item for item in results if item.get('status') == 'failed']
        return {
            'success': not failed,
            'executed': True,
            'reason': decision.get('reason'),
            'request': request,
            'results': results,
        }
    except Exception as e:
        logger.error(f'持续学习任务失败: {e}', exc_info=True)
        return {'success': False, 'executed': False, 'reason': str(e)}
    finally:
        _continuous_learning_running = False


def run_ordered_asset_training_suite():
    """按资产优先级顺序执行模型训练，并在末尾触发多轮复盘优化。"""
    optimization_rounds = max(1, int(os.environ.get('CONTINUOUS_MODEL_OPT_ROUNDS', '2')))
    try:
        from scripts.train_asset_suite import build_training_plan, run_training_plan
        from predictors.model_trainer import ModelTrainer

        logger.info("开始执行统一资产训练编排: A股 -> 基金 -> 黄金 -> 白银 -> ETF -> 港股 -> 美股")
        plan = build_training_plan()
        results = run_training_plan(plan=plan, dry_run=False, stop_on_error=False, skip_existing=False)

        failed = [item for item in results if item.get('status') == 'failed']
        success = [item for item in results if item.get('status') == 'success']
        skipped = [item for item in results if item.get('status') == 'skipped']

        trainer = ModelTrainer()
        advisor_model_results = trainer.train_all_models(
            target_periods=[5, 20, 60],
            auto_optimize_short_term=True,
            auto_optimize_medium_term=True,
            auto_optimize_long_term=True,
            continuous_improvement_rounds=optimization_rounds,
        )

        if failed:
            logger.warning(
                f"统一资产训练完成，但存在失败步骤: success={len(success)}, skipped={len(skipped)}, failed={len(failed)}"
            )
        else:
            logger.info(
                f"统一资产训练全部完成: success={len(success)}, skipped={len(skipped)}，并已执行 {optimization_rounds} 轮全模型复盘优化"
            )
        return {
            'success': not failed,
            'results': results,
            'advisor_model_results': advisor_model_results,
        }
    except Exception as e:
        logger.warning(f"统一资产训练编排不可用，回退默认连续模型训练: {e}")
        from predictors.model_trainer import ModelTrainer
        trainer = ModelTrainer()
        return trainer.train_all_models(
            target_periods=[5, 20, 60],
            auto_optimize_short_term=True,
            auto_optimize_medium_term=True,
            auto_optimize_long_term=True,
            continuous_improvement_rounds=optimization_rounds,
        )


def ensure_data_inventory_current(force: bool = False):
    """启动后/收盘后检查数据资产是否存在明显缺口，有则自动补采。"""
    try:
        if get_auto_backfill_progress().get('running'):
            logger.info("自动补采已在运行，跳过重复触发")
            return False

        from api.backfill import _build_dataset_inventory, _get_meaningful_backfill_candidates, _start_missing_only_backfill_async

        inventory = _build_dataset_inventory()
        candidates = _get_meaningful_backfill_candidates(inventory)
        if not candidates:
            if force:
                logger.info("数据资产检查完成：未发现需要自动补采的明显缺口")
            return False

        labels = '、'.join(item.get('label', '') for item in candidates[:6])
        logger.info(f"发现 {len(candidates)} 个待自动补采数据项，准备异步执行: {labels}")
        _start_missing_only_backfill_async()
        return True
    except Exception as e:
        logger.warning(f"启动自动缺口补采检查失败: {e}")
        return False


def init_scheduler():
    """初始化定时任务 - 优化版"""
    logger.info("初始化定时任务...")
    
    try:
        from reviews.reviewer import Reviewer
        from reviews.reporter import Reporter
        
        reviewer = Reviewer()
        reporter = Reporter()
        
        # 解析关键时点
        daily_hour, daily_minute = _parse_hhmm(COLLECT_DAILY_TIME, '15:10')
        snapshot_hour, snapshot_minute = _parse_hhmm(DAILY_SNAPSHOT_TIME, '15:05')
        recommendation_hour, recommendation_minute = _parse_hhmm(DAILY_RECOMMENDATION_TIME, '18:00')
        alert_morning_hour, alert_morning_minute = _parse_hhmm(FUTURE_SIGNAL_ALERT_MORNING_TIME, '09:35')
        alert_pre_close_hour, alert_pre_close_minute = _parse_hhmm(FUTURE_SIGNAL_ALERT_PRE_CLOSE_TIME, '14:40')
        
        # 解析模型重训时间
        retrain_hour = MODEL_RETRAIN_HOUR
        retrain_day = MODEL_RETRAIN_DAY
        
        # 任务1: 每日预测生成（08:00 - 开盘前）
        scheduler.add_job(
            func=generate_daily_predictions,
            trigger=CronTrigger(hour=8, minute=0),
            id='daily_predictions',
            name='每日预测生成',
            replace_existing=True
        )
        logger.info("已添加任务: 每日预测生成 (每日08:00)")
        
        # 任务2: 盘中数据采集（交易时间每5分钟）
        scheduler.add_job(
            func=execute_intraday_managed_collection,
            trigger=IntervalTrigger(seconds=COLLECT_REAL_TIME_INTERVAL),
            id='collect_realtime',
            name='盘中数据采集',
            replace_existing=True
        )
        logger.info(f"已添加任务: 盘中数据采集 (交易时段每{COLLECT_REAL_TIME_INTERVAL//60}分钟)")
        
        # 任务3: 收盘后数据采集（收盘后执行，沉淀当日收盘数据）
        scheduler.add_job(
            func=execute_managed_collection,
            trigger=CronTrigger(hour=daily_hour, minute=daily_minute),
            id='collect_daily',
            name='收盘数据采集',
            replace_existing=True
        )
        logger.info(f"已添加任务: 收盘数据采集 (每日{COLLECT_DAILY_TIME})")
        
        # 任务4: 预警扫描（仅在盘中可操作窗口执行）
        scheduler.add_job(
            func=scan_warnings,
            trigger=IntervalTrigger(minutes=10),
            id='scan_warnings',
            name='预警扫描',
            replace_existing=True
        )
        logger.info("已添加任务: 预警扫描 (盘中每10分钟)")
        
        # 任务5: 复盘检查（每日20:00）
        scheduler.add_job(
            func=lambda: reviewer.check_expired_predictions(),
            trigger=CronTrigger(hour=20, minute=0),
            id='check_reviews',
            name='复盘检查',
            replace_existing=True
        )
        logger.info("已添加任务: 复盘检查 (每日20:00)")
        
        # 任务6: 每日推荐生成（每日18:00）
        scheduler.add_job(
            func=generate_daily_recommendations,
            trigger=CronTrigger(hour=recommendation_hour, minute=recommendation_minute),
            id='daily_recommendations',
            name='每日推荐生成',
            replace_existing=True
        )
        logger.info(f"已添加任务: 每日推荐生成 (每日{DAILY_RECOMMENDATION_TIME}，收盘后用于下一交易日)")
        
        # 任务7: 每日持仓快照（收盘后尽快固化）
        scheduler.add_job(
            func=take_daily_snapshot,
            trigger=CronTrigger(hour=snapshot_hour, minute=snapshot_minute),
            id='daily_snapshot',
            name='每日持仓快照',
            replace_existing=True
        )
        logger.info(f"已添加任务: 每日持仓快照 (每日{DAILY_SNAPSHOT_TIME})")
        
        # 任务8: 周报生成（每周一08:00）
        scheduler.add_job(
            func=lambda: reporter.generate_weekly_report(),
            trigger=CronTrigger(day_of_week='mon', hour=8, minute=0),
            id='weekly_report',
            name='周报生成',
            replace_existing=True
        )
        logger.info("已添加任务: 周报生成 (每周一08:00)")
        
        # 任务9: 月报生成（每月1日08:00）
        scheduler.add_job(
            func=lambda: reporter.generate_monthly_report(),
            trigger=CronTrigger(day=1, hour=8, minute=0),
            id='monthly_report',
            name='月报生成',
            replace_existing=True
        )
        logger.info("已添加任务: 月报生成 (每月1日08:00)")
        
        # 任务10: 模型训练（按资产顺序，港股/美股放最后）
        scheduler.add_job(
            func=run_ordered_asset_training_suite,
            trigger=CronTrigger(day_of_week=retrain_day, hour=retrain_hour, minute=0),
            id='train_models',
            name='模型训练',
            replace_existing=True
        )
        logger.info(f"已添加任务: 模型训练 (每周{retrain_day} {retrain_hour}:00，按资产顺序执行)")
        
        # 任务11: 更新资产池（每日02:00）
        scheduler.add_job(
            func=update_asset_pools,
            trigger=CronTrigger(hour=2, minute=0),
            id='update_asset_pools',
            name='更新资产池',
            replace_existing=True
        )
        logger.info("已添加任务: 更新资产池 (每日02:00)")
        
        # 任务12: 数据清理（每日03:00）
        scheduler.add_job(
            func=cleanup_old_data,
            trigger=CronTrigger(hour=3, minute=0),
            id='cleanup_data',
            name='数据清理',
            replace_existing=True
        )
        logger.info("已添加任务: 数据清理 (每日03:00)")

        # 任务13: 概率校准样本增量回填（每日21:30）
        scheduler.add_job(
            func=lambda: incremental_backfill_recommendation_history(3),
            trigger=CronTrigger(hour=21, minute=30),
            id='probability_history_backfill',
            name='概率校准样本增量回填',
            replace_existing=True
        )
        logger.info("已添加任务: 概率校准样本增量回填 (每日21:30)")
        
        # 任务14: 周期性深度概率校准（每周日04:00）
        scheduler.add_job(
            func=deep_probability_recalibration,
            trigger=CronTrigger(day_of_week='sun', hour=4, minute=0),
            id='deep_probability_recalibration',
            name='周期性深度概率校准',
            replace_existing=True
        )
        logger.info("已添加任务: 周期性深度概率校准 (每周日04:00)")
        
        # 任务15: 每日未来信号告警（晨间 + 临近收盘）- 保证仍具可操作性
        scheduler.add_job(
            func=send_daily_future_signals_alert,
            trigger=CronTrigger(hour=alert_morning_hour, minute=alert_morning_minute),
            id='future_signals_alert_morning',
            name='未来信号告警(晨间)',
            replace_existing=True
        )
        logger.info(f"已添加任务: 未来信号告警(晨间) (每日{FUTURE_SIGNAL_ALERT_MORNING_TIME})")
        
        scheduler.add_job(
            func=send_daily_future_signals_alert,
            trigger=CronTrigger(hour=alert_pre_close_hour, minute=alert_pre_close_minute),
            id='future_signals_alert_pre_close',
            name='未来信号告警(临近收盘)',
            replace_existing=True
        )
        logger.info(f"已添加任务: 未来信号告警(临近收盘) (每日{FUTURE_SIGNAL_ALERT_PRE_CLOSE_TIME})")

        # 任务16: 小时级新闻采集（自然日维度）
        scheduler.add_job(
            func=collect_hourly_news_snapshot,
            trigger=IntervalTrigger(hours=1),
            id='collect_news_hourly',
            name='小时级新闻采集',
            replace_existing=True
        )
        logger.info("已添加任务: 小时级新闻采集 (每1小时)")

        # 任务17: 每日研报增量补采（受接口限额影响，按日滚动补齐）
        scheduler.add_job(
            func=collect_daily_research_snapshot,
            trigger=CronTrigger(hour=6, minute=30),
            id='collect_research_daily',
            name='每日研报增量补采',
            replace_existing=True
        )
        logger.info("已添加任务: 每日研报增量补采 (每日06:30)")

        # 任务18: 小时级快频事件扫描（舆情/地缘/利率冲击）
        scheduler.add_job(
            func=scan_realtime_event_impact,
            trigger=IntervalTrigger(hours=1),
            id='scan_realtime_event_impact',
            name='小时级事件影响评估',
            replace_existing=True
        )
        logger.info("已添加任务: 小时级事件影响评估 (每1小时)")

        if CONTINUOUS_LEARNING_ENABLED:
            scheduler.add_job(
                func=run_continuous_learning_cycle,
                trigger=IntervalTrigger(hours=CONTINUOUS_LEARNING_INTERVAL_HOURS),
                id='continuous_learning_cycle',
                name='持续学习优化',
                replace_existing=True,
                next_run_time=datetime.now() + timedelta(minutes=CONTINUOUS_LEARNING_STARTUP_DELAY_MINUTES),
            )
            logger.info(
                f"已添加任务: 持续学习优化 (每{CONTINUOUS_LEARNING_INTERVAL_HOURS}小时，启动后{CONTINUOUS_LEARNING_STARTUP_DELAY_MINUTES}分钟开始)"
            )
        else:
            logger.info("已关闭持续学习优化任务（CONTINUOUS_LEARNING_ENABLED=false）")
        
        scheduler.start()
        logger.info(f"定时任务调度器已启动，共 {len(scheduler.get_jobs())} 个任务")

        try:
            ensure_daily_predictions_current()
        except Exception as e:
            logger.warning(f"启动后预测补偿执行失败: {e}")

        try:
            ensure_data_inventory_current(force=False)
        except Exception as e:
            logger.warning(f"启动后数据补采检查失败: {e}")
        
        # 注意：不在这里关闭资源，因为调度器需要这些对象在后台运行
        # collector, monitor, reviewer 会在后台线程中使用，不能关闭
        
        return True
        
    except Exception as e:
        logger.error(f"初始化定时任务失败: {e}", exc_info=True)
        return False


def auto_backfill_current_year():
    """
    自动补采当年缺失数据。

    说明:
    - 仅补采当年(1月1日至今)
    - 使用各采集器的断点续传机制，已完成日期会自动跳过
    - 设计为“幂等”: 多次运行不会重复采集已完成日期
    """
    global _backfill_running

    if _backfill_running:
        logger.info("自动补采任务已在运行，跳过重复触发")
        return

    with _backfill_lock:
        if _backfill_running:
            return
        _backfill_running = True

    try:
        today = datetime.now()
        start_dt = datetime(today.year, 1, 1)
        end_dt = today

        logger.info(
            f"开始自动补采（当年，按交易日流水线）: {start_dt.strftime('%Y-%m-%d')} ~ {end_dt.strftime('%Y-%m-%d')}"
        )

        trade_dates = _get_a_share_trading_dates(start_dt, end_dt)
        step_names = [
            '股票行情(当年历史+实时)',
            *[f'交易日流水线 {d}' for d in trade_dates],
            '新闻舆情(日历日补采)',
            '券商研报(按日限额补采)',
            '财务指标(全量股票)',
            '基金数据',
            '宏观数据快照',
            '增量训练(短期模型)',
        ]
        _init_backfill_progress(start_dt, end_dt, steps=step_names)

        # 1) 股票行情（当年历史日线 + 实时补一次）
        try:
            _update_backfill_step('股票行情(当年历史+实时)', 'running', '正在补采股票行情(当年历史+实时)')
            from collectors.stock_collector import StockCollector
            c = StockCollector()

            for market, pool in [('A', c.a_stock_pool), ('H', c.hk_stock_pool), ('US', c.us_stock_pool)]:
                for code in pool:
                    try:
                        df = c.collect_history(code, period='1y', interval='1d')
                        if df is None or len(df) == 0:
                            continue

                        # 仅保留当年数据
                        if 'date' in df.columns:
                            df = df[pd.to_datetime(df['date']).dt.year == today.year]
                        if len(df) == 0:
                            continue

                        # 逐日入库（_save_to_database 内部按 code+date 去重/更新）
                        for _, row in df.iterrows():
                            c._save_to_database({
                                'code': str(code),
                                'name': str(code).split('.')[0],
                                'market': market,
                                'date': pd.to_datetime(row['date']).date(),
                                'open': float(row.get('open', 0) or 0),
                                'high': float(row.get('high', 0) or 0),
                                'low': float(row.get('low', 0) or 0),
                                'close': float(row.get('close', 0) or 0),
                                'volume': int(row.get('volume', 0) or 0),
                            })
                    except Exception as e:
                        logger.error(f"自动补采失败[股票历史 {code}]: {e}")

            # 再补一次实时行情，覆盖当天最新值
            c.collect_all_realtime()
            logger.info("自动补采完成: 股票行情(当年历史+实时)")
            _update_backfill_step('股票行情(当年历史+实时)', 'success', '自动补采完成: 股票行情(当年历史+实时)')
        except Exception as e:
            logger.error(f"自动补采失败[股票行情]: {e}")
            _update_backfill_step('股票行情(当年历史+实时)', 'failed', '自动补采失败: 股票行情(当年历史+实时)', str(e))

        # 2) 交易日维度：按交易日块批采，失败时降级逐日重试
        daily_collectors = {}
        try:
            from collectors.north_money_collector import NorthMoneyCollector
            from collectors.margin_collector import MarginCollector
            from collectors.top_list_collector import TopListCollector
            from collectors.daily_basic_collector import DailyBasicCollector
            from collectors.moneyflow_collector import MoneyflowCollector

            daily_collectors = {
                'north': NorthMoneyCollector(),
                'margin': MarginCollector(),
                'top_list': TopListCollector(),
                'daily_basic': DailyBasicCollector(),
                'moneyflow': MoneyflowCollector(),
            }
        except Exception as e:
            logger.error(f"初始化交易日采集器失败: {e}")

        completed_trade_days = 0
        trained_times = 0

        trade_chunks = _chunk_trade_dates(trade_dates, _backfill_trade_chunk_days)
        logger.info(f"交易日流水线批采: total_days={len(trade_dates)}, chunk_size={_backfill_trade_chunk_days}, chunks={len(trade_chunks)}")

        def _maybe_train_interval():
            nonlocal trained_times
            should_train = (
                completed_trade_days >= _backfill_training_min_days
                and (completed_trade_days - _backfill_training_min_days) % _backfill_training_interval_days == 0
                and trained_times <= ((completed_trade_days - _backfill_training_min_days) // _backfill_training_interval_days)
            )
            if should_train:
                _run_incremental_short_term_training(completed_trade_days)
                trained_times += 1

        for chunk in trade_chunks:
            chunk_start = chunk[0]
            chunk_end = chunk[-1]

            for d in chunk:
                step_name = f'交易日流水线 {d}'
                if chunk_start == chunk_end:
                    _update_backfill_step(step_name, 'running', f'正在采集交易日 {chunk_start}')
                else:
                    _update_backfill_step(step_name, 'running', f'正在采集交易日区间 {chunk_start} ~ {chunk_end}')

            if not daily_collectors:
                for d in chunk:
                    step_name = f'交易日流水线 {d}'
                    _update_backfill_step(step_name, 'failed', f'交易日 {d} 采集器初始化失败')
                continue

            try:
                _run_trade_date_pipeline_range(chunk_start, chunk_end, daily_collectors, len(chunk))
                for d in chunk:
                    step_name = f'交易日流水线 {d}'
                    _update_backfill_step(step_name, 'success', f'交易日 {d} 采集完成（批采）')
                    completed_trade_days += 1
                    if _backfill_training_mode == 'interval':
                        _maybe_train_interval()
            except Exception as e:
                err = str(e)
                logger.error(f"交易日块流水线失败[{chunk_start}~{chunk_end}]: {err}")

                # 块失败时降级为逐日重试，尽量不丢进度。
                for d in chunk:
                    step_name = f'交易日流水线 {d}'
                    try:
                        _retry_trade_date_pipeline(d, daily_collectors=daily_collectors)
                        _update_backfill_step(step_name, 'success', f'交易日 {d} 采集完成（块失败后逐日降级）')
                        completed_trade_days += 1
                        if _backfill_training_mode == 'interval':
                            _maybe_train_interval()
                    except Exception as day_err:
                        _update_backfill_step(step_name, 'failed', f'交易日 {d} 采集失败', str(day_err))

        # 3) 新闻舆情（按日历日补采）
        try:
            _update_backfill_step('新闻舆情(日历日补采)', 'running', '正在按日历日补采新闻舆情')
            from collectors.news_collector import NewsCollector

            n = NewsCollector()
            n.collect_historical(
                start_date=start_dt.strftime('%Y%m%d'),
                end_date=end_dt.strftime('%Y%m%d'),
                resume=False,
            )
            logger.info("自动补采完成: 新闻舆情(日历日补采)")
            _update_backfill_step('新闻舆情(日历日补采)', 'success', '自动补采完成: 新闻舆情(日历日补采)')
        except Exception as e:
            logger.error(f"自动补采失败[新闻舆情(日历日补采)]: {e}")
            _update_backfill_step('新闻舆情(日历日补采)', 'failed', '自动补采失败: 新闻舆情(日历日补采)', str(e))

        # 4) 券商研报（受日限额约束，按配额补采）
        try:
            _update_backfill_step('券商研报(按日限额补采)', 'running', '正在补采券商研报（按日限额）')
            from collectors.research_collector import ResearchCollector
            c = ResearchCollector()
            c.collect(start_dt, end_dt, resume=True)
            logger.info("自动补采完成: 券商研报(按日限额补采)")
            _update_backfill_step('券商研报(按日限额补采)', 'success', '自动补采完成: 券商研报(按日限额补采)')
        except Exception as e:
            logger.error(f"自动补采失败[券商研报]: {e}")
            _update_backfill_step('券商研报(按日限额补采)', 'failed', '自动补采失败: 券商研报(按日限额补采)', str(e))

        # 5) 财务指标（全量股票，断点续传）
        try:
            _update_backfill_step('财务指标(全量股票)', 'running', '正在补采财务指标(全量股票)')
            from collectors.financial_collector import FinancialCollector
            c = FinancialCollector()
            c.collect_all(max_stocks=None, resume=True)
            logger.info("自动补采完成: 财务指标(全量股票)")
            _update_backfill_step('财务指标(全量股票)', 'success', '自动补采完成: 财务指标(全量股票)')
        except Exception as e:
            logger.error(f"自动补采失败[财务指标]: {e}")
            _update_backfill_step('财务指标(全量股票)', 'failed', '自动补采失败: 财务指标(全量股票)', str(e))

        # 6) 基金数据（全量基金池）
        try:
            _update_backfill_step('基金数据', 'running', '正在补采基金数据')
            from collectors.fund_collector import FundCollector
            c = FundCollector()
            c.collect_all_funds()
            logger.info("自动补采完成: 基金数据")
            _update_backfill_step('基金数据', 'success', '自动补采完成: 基金数据')
        except Exception as e:
            logger.error(f"自动补采失败[基金数据]: {e}")
            _update_backfill_step('基金数据', 'failed', '自动补采失败: 基金数据', str(e))

        # 7) 宏观数据（当前快照）
        try:
            _update_backfill_step('宏观数据快照', 'running', '正在补采宏观数据快照')
            from collectors.macro_collector import MacroCollector
            c = MacroCollector()
            if hasattr(c, 'export_macro_feature_csvs'):
                c.export_macro_feature_csvs()
            else:
                c.get_all_macro_data()
            logger.info("自动补采完成: 宏观数据快照")
            _update_backfill_step('宏观数据快照', 'success', '自动补采完成: 宏观数据快照')
        except Exception as e:
            logger.error(f"自动补采失败[宏观数据]: {e}")
            _update_backfill_step('宏观数据快照', 'failed', '自动补采失败: 宏观数据快照', str(e))

        # 8) 短期模型训练（默认在全部采集后执行一次）
        if _backfill_training_mode == 'off':
            _update_backfill_step('增量训练(短期模型)', 'success', '已跳过增量训练（BACKFILL_TRAINING_MODE=off）')
        elif _backfill_training_mode == 'after_collect':
            if completed_trade_days > 0:
                _run_incremental_short_term_training(completed_trade_days)
            else:
                _update_backfill_step('增量训练(短期模型)', 'success', '已跳过增量训练（未完成任何交易日补采）')
        else:
            # interval 模式下仅在未命中阈值时标记已跳过，避免步骤一直pending。
            with _backfill_progress_lock:
                training_step = None
                for step in _backfill_progress.get('steps', []):
                    if step.get('name') == '增量训练(短期模型)':
                        training_step = step
                        break
            if training_step and training_step.get('status') == 'pending':
                _update_backfill_step('增量训练(短期模型)', 'success', '区间触发条件未满足，已跳过本次训练')

        logger.info("自动补采任务结束（当年）")
    finally:
        _finish_backfill_progress()
        _backfill_running = False


def start_auto_backfill_current_year_async():
    """后台启动自动补采，不阻塞 Web 服务启动。"""
    t = threading.Thread(
        target=auto_backfill_current_year,
        daemon=True,
        name='auto_backfill_current_year',
    )
    t.start()
    logger.info("已启动后台自动补采线程（当年数据，全量采集）")


def _update_backfill_step_if_exists(step_name: str, status: str, message: str = '', error: Optional[str] = None):
    """仅当步骤存在于进度中时更新状态。"""
    with _backfill_progress_lock:
        step_names = {s.get('name') for s in _backfill_progress.get('steps', [])}
    if step_name in step_names:
        _update_backfill_step(step_name, status, message, error)


def _retry_trade_date_pipeline(trade_date: str, daily_collectors: Optional[Dict[str, Any]] = None):
    """重试单个交易日流水线。"""
    if daily_collectors is None:
        from collectors.north_money_collector import NorthMoneyCollector
        from collectors.margin_collector import MarginCollector
        from collectors.top_list_collector import TopListCollector
        from collectors.daily_basic_collector import DailyBasicCollector
        from collectors.moneyflow_collector import MoneyflowCollector

        daily_collectors = {
            'north': NorthMoneyCollector(),
            'margin': MarginCollector(),
            'top_list': TopListCollector(),
            'daily_basic': DailyBasicCollector(),
            'moneyflow': MoneyflowCollector(),
        }

    daily_collectors['north'].collect(trade_date, trade_date, resume=True, strict=True)
    daily_collectors['margin'].collect_by_date(trade_date, trade_date, resume=True)
    daily_collectors['top_list'].collect(trade_date, trade_date, resume=True)
    daily_collectors['daily_basic'].collect_all(
        start_date=trade_date,
        end_date=trade_date,
        max_stocks=None,
        resume=True,
        mode='by_date',
    )
    daily_collectors['moneyflow'].collect_by_date(
        trade_date,
        trade_date,
        max_stocks=None,
        resume=True,
    )


def retry_backfill_step(step_name: str) -> Dict[str, Any]:
    """重试单个补采步骤。"""
    try:
        if not step_name:
            return {'success': False, 'message': 'step_name 不能为空'}

        _update_backfill_step_if_exists(step_name, 'running', f'正在重试: {step_name}')

        if step_name.startswith('交易日流水线 '):
            trade_date = step_name.replace('交易日流水线 ', '').strip()
            _retry_trade_date_pipeline(trade_date)
            _update_backfill_step_if_exists(step_name, 'success', f'重试成功: {step_name}')
            return {'success': True, 'message': f'重试成功: {step_name}'}

        if step_name == '股票行情(当年历史+实时)':
            from collectors.stock_collector import StockCollector

            today = datetime.now()
            c = StockCollector()
            for market, pool in [('A', c.a_stock_pool), ('H', c.hk_stock_pool), ('US', c.us_stock_pool)]:
                for code in pool:
                    df = c.collect_history(code, period='1y', interval='1d')
                    if df is None or len(df) == 0:
                        continue
                    if 'date' in df.columns:
                        df = df[pd.to_datetime(df['date']).dt.year == today.year]
                    if len(df) == 0:
                        continue
                    for _, row in df.iterrows():
                        c._save_to_database({
                            'code': str(code),
                            'name': str(code).split('.')[0],
                            'market': market,
                            'date': pd.to_datetime(row['date']).date(),
                            'open': float(row.get('open', 0) or 0),
                            'high': float(row.get('high', 0) or 0),
                            'low': float(row.get('low', 0) or 0),
                            'close': float(row.get('close', 0) or 0),
                            'volume': int(row.get('volume', 0) or 0),
                        })
            c.collect_all_realtime()
            _update_backfill_step_if_exists(step_name, 'success', f'重试成功: {step_name}')
            return {'success': True, 'message': f'重试成功: {step_name}'}

        if step_name == '券商研报(按日限额补采)':
            from collectors.research_collector import ResearchCollector

            today = datetime.now()
            start_dt = datetime(today.year, 1, 1)
            c = ResearchCollector()
            c.collect(start_dt, today, resume=True)
            _update_backfill_step_if_exists(step_name, 'success', f'重试成功: {step_name}')
            return {'success': True, 'message': f'重试成功: {step_name}'}

        if step_name == '新闻舆情(日历日补采)':
            from collectors.news_collector import NewsCollector

            today = datetime.now()
            start_dt = datetime(today.year, 1, 1)
            n = NewsCollector()
            n.collect_historical(
                start_date=start_dt.strftime('%Y%m%d'),
                end_date=today.strftime('%Y%m%d'),
                resume=False,
            )
            _update_backfill_step_if_exists(step_name, 'success', f'重试成功: {step_name}')
            return {'success': True, 'message': f'重试成功: {step_name}'}

        if step_name == '财务指标(全量股票)':
            from collectors.financial_collector import FinancialCollector

            c = FinancialCollector()
            c.collect_all(max_stocks=None, resume=True)
            _update_backfill_step_if_exists(step_name, 'success', f'重试成功: {step_name}')
            return {'success': True, 'message': f'重试成功: {step_name}'}

        if step_name == '基金数据':
            from collectors.fund_collector import FundCollector

            c = FundCollector()
            c.collect_all_funds()
            _update_backfill_step_if_exists(step_name, 'success', f'重试成功: {step_name}')
            return {'success': True, 'message': f'重试成功: {step_name}'}

        if step_name == '宏观数据快照':
            from collectors.macro_collector import MacroCollector

            c = MacroCollector()
            if hasattr(c, 'export_macro_feature_csvs'):
                c.export_macro_feature_csvs()
            else:
                c.get_all_macro_data()
            _update_backfill_step_if_exists(step_name, 'success', f'重试成功: {step_name}')
            return {'success': True, 'message': f'重试成功: {step_name}'}

        if step_name == '增量训练(短期模型)':
            _run_incremental_short_term_training(0)
            return {'success': True, 'message': f'重试成功: {step_name}'}

        return {'success': False, 'message': f'不支持重试的步骤: {step_name}'}

    except Exception as e:
        err = str(e)
        logger.error(f"重试补采步骤失败[{step_name}]: {err}")
        _update_backfill_step_if_exists(step_name, 'failed', f'重试失败: {step_name}', err)
        return {'success': False, 'message': f'重试失败: {step_name}', 'error': err}


def start_retry_backfill_step_async(step_name: str):
    """后台重试单个补采步骤。"""
    t = threading.Thread(
        target=retry_backfill_step,
        args=(step_name,),
        daemon=True,
        name=f'retry_backfill_step_{step_name}',
    )
    t.start()
    logger.info(f"已启动后台重试线程: {step_name}")


def shutdown_scheduler():
    """关闭调度器"""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("定时任务调度器已关闭")


def list_jobs() -> List[Dict[str, Any]]:
    """列出所有定时任务"""
    jobs = scheduler.get_jobs()
    job_list = []
    
    print("\n当前定时任务列表:")
    print("=" * 60)
    for job in jobs:
        job_info = {
            'id': job.id,
            'name': job.name,
            'next_run_time': job.next_run_time,
            'trigger': str(job.trigger)
        }
        job_list.append(job_info)
        
        print(f"  📌 ID: {job.id}")
        print(f"     名称: {job.name}")
        print(f"     下次运行: {job.next_run_time}")
        print(f"     触发器: {job.trigger}")
        print("-" * 60)
    
    return job_list


def trigger_job_manually(job_id: str) -> bool:
    """手动触发指定任务（用于测试）"""
    try:
        job = scheduler.get_job(job_id)
        if job:
            job.modify(next_run_time=datetime.now())
            logger.info(f"已手动触发任务: {job_id}")
            return True
        else:
            logger.warning(f"未找到任务: {job_id}")
            return False
    except Exception as e:
        logger.error(f"手动触发任务失败: {e}")
        return False


def get_job_status(job_id: str) -> Optional[Dict[str, Any]]:
    """获取任务状态"""
    try:
        job = scheduler.get_job(job_id)
        if job:
            return {
                'id': job.id,
                'name': job.name,
                'next_run_time': job.next_run_time,
                'pending': job.next_run_time is not None
            }
    except Exception as e:
        logger.error(f"获取任务状态失败: {e}")
    return None


def is_scheduler_running() -> bool:
    """检查调度器是否运行中"""
    return scheduler.running


def execute_intraday_managed_collection():
    """仅在交易时段执行实时采集。"""
    if not _is_intraday_collection_window():
        logger.debug("当前非盘中时段，跳过实时采集")
        return
    return execute_managed_collection()


def execute_managed_collection():
    """
    执行受管采集任务（使用CollectionDirector）
    
    这是新的采集入口点，使用CollectionDirector进行：
    - 采集任务去重
    - 资源冲突检测
    - 采集编排优化
    """
    global _managed_collection_running

    # 防止高频任务重入导致并发采集冲突。
    with _managed_collection_lock:
        if _managed_collection_running:
            logger.info("⏭️ 受管采集正在执行中，跳过本次触发")
            return
        _managed_collection_running = True

    try:
        director = get_collection_director()
        if director is None:
            logger.warning("⚠️ CollectionDirector 不可用，使用传统采集方式")
            _get_realtime_collector().collect_all_realtime()
            return

        from scheduler.collection_director import CollectionTask

        realtime_task = CollectionTask(
            task_id='collect_all_realtime',
            task_type='stock_realtime',
            target='all_markets',
            priority=1,
            name='全市场实时行情采集',
        )
        director.register_task(
            realtime_task,
            collector_func=lambda: _get_realtime_collector().collect_all_realtime(),
        )
        
        # 获取执行计划
        plan = director.get_execution_plan()
        logger.info(f"📊 采集计划: {len(plan)} 个任务")
        
        # 执行采集
        for task_id in plan:
            try:
                result = director.execute_task(task_id)
                logger.info(f"✅ 采集任务 {task_id} 完成: {result}")
            except Exception as e:
                logger.error(f"❌ 采集任务 {task_id} 失败: {e}")

        if not _is_intraday_collection_window():
            ensure_data_inventory_current(force=True)
                
    except Exception as e:
        logger.error(f"❌ 受管采集执行失败: {e}", exc_info=True)
    finally:
        with _managed_collection_lock:
            _managed_collection_running = False


if __name__ == '__main__':
    import time
    
    print("=" * 60)
    print("定时任务调度器测试")
    print("=" * 60)
    
    success = init_scheduler()
    if success:
        list_jobs()
        print("\n调度器运行中，按 Ctrl+C 停止...")
        
        try:
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            print("\n正在停止调度器...")
            shutdown_scheduler()
            print("调度器已停止")
    else:
        print("调度器初始化失败")