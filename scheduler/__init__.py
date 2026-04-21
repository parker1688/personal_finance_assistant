"""scheduler包 - 定时任务管理。

通过固定模块键加载并复用 scheduler.py，避免重复exec导致多实例状态分裂。
"""

import importlib.util
import logging
import sys
from pathlib import Path

CORE_MODULE_KEY = '_personal_finance_scheduler_core'

try:
    _scheduler_module = sys.modules.get(CORE_MODULE_KEY)
    if _scheduler_module is None:
        scheduler_path = Path(__file__).resolve().parent.parent / 'scheduler.py'
        spec = importlib.util.spec_from_file_location(CORE_MODULE_KEY, scheduler_path)
        _scheduler_module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(_scheduler_module)
        sys.modules[CORE_MODULE_KEY] = _scheduler_module
    
    # 导出关键函数和变量
    init_scheduler = _scheduler_module.init_scheduler
    shutdown_scheduler = _scheduler_module.shutdown_scheduler
    list_jobs = _scheduler_module.list_jobs
    trigger_job_manually = _scheduler_module.trigger_job_manually
    get_job_status = _scheduler_module.get_job_status
    get_collection_director = _scheduler_module.get_collection_director
    generate_daily_predictions = _scheduler_module.generate_daily_predictions
    rebuild_today_recommendations = _scheduler_module.rebuild_today_recommendations
    generate_daily_recommendations = _scheduler_module.generate_daily_recommendations
    auto_backfill_current_year = _scheduler_module.auto_backfill_current_year
    start_auto_backfill_current_year_async = _scheduler_module.start_auto_backfill_current_year_async
    get_auto_backfill_progress = _scheduler_module.get_auto_backfill_progress
    retry_backfill_step = _scheduler_module.retry_backfill_step
    start_retry_backfill_step_async = _scheduler_module.start_retry_backfill_step_async
    scan_warnings = _scheduler_module.scan_warnings
    run_ordered_asset_training_suite = _scheduler_module.run_ordered_asset_training_suite
    run_continuous_learning_cycle = _scheduler_module.run_continuous_learning_cycle
    scheduler = _scheduler_module.scheduler
    HAS_COLLECTION_DIRECTOR = _scheduler_module.HAS_COLLECTION_DIRECTOR
    
    __all__ = [
        'init_scheduler',
        'shutdown_scheduler', 
        'list_jobs',
        'trigger_job_manually',
        'get_job_status',
        'get_collection_director',
        'generate_daily_predictions',
        'rebuild_today_recommendations',
        'generate_daily_recommendations',
        'auto_backfill_current_year',
        'start_auto_backfill_current_year_async',
        'get_auto_backfill_progress',
        'retry_backfill_step',
        'start_retry_backfill_step_async',
        'scan_warnings',
        'run_ordered_asset_training_suite',
        'run_continuous_learning_cycle',
        'scheduler',
        'HAS_COLLECTION_DIRECTOR'
    ]
    
except Exception as e:
    logging.error(f"Failed to import scheduler module: {e}", exc_info=True)
    raise
