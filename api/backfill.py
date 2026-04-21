"""
补采进度API - api/backfill.py
提供自动补采任务进度查询接口
"""

import sys
import os
import threading
from datetime import datetime, timedelta
import pandas as pd
from flask import jsonify, request
from sqlalchemy import func

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pathlib import Path

from models import get_session, RawStockData, RawFundData, DailyPrice, Recommendation, Prediction
from utils import get_logger
from api.auth import require_admin_access, log_admin_audit

logger = get_logger(__name__)


def _get_project_data_dir():
    return Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) / 'data'


def _export_raw_stock_csv(filename, market=None, codes=None):
    session = get_session()
    try:
        query = session.query(
            RawStockData.code,
            RawStockData.name,
            RawStockData.date,
            RawStockData.open,
            RawStockData.high,
            RawStockData.low,
            RawStockData.close,
            RawStockData.volume,
            RawStockData.market,
        )
        if market:
            query = query.filter(RawStockData.market == market)

        rows = query.all()
        if not rows:
            return 0

        df = pd.DataFrame(rows, columns=['code', 'name', 'date', 'open', 'high', 'low', 'close', 'volume', 'market'])
        if codes:
            allowed = {str(code).strip().upper() for code in (codes or []) if code}
            df = df[df['code'].astype(str).str.upper().isin(allowed)]
        if df.empty:
            return 0

        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d')
        df = df.dropna(subset=['code', 'date']).sort_values(['code', 'date']).drop_duplicates(subset=['code', 'date'], keep='last')

        output_path = _get_project_data_dir() / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False, encoding='utf-8')
        return int(len(df))
    finally:
        session.close()


def _export_fund_nav_csv(filename='fund_nav.csv'):
    session = get_session()
    try:
        rows = session.query(
            RawFundData.code,
            RawFundData.name,
            RawFundData.date,
            RawFundData.nav,
            RawFundData.accumulated_nav,
            RawFundData.daily_return,
        ).all()
        if not rows:
            return 0

        df = pd.DataFrame(rows, columns=['code', 'name', 'date', 'nav', 'accumulated_nav', 'daily_return'])
        if df.empty:
            return 0

        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d')
        df = df.dropna(subset=['code', 'date']).sort_values(['code', 'date']).drop_duplicates(subset=['code', 'date'], keep='last')

        output_path = _get_project_data_dir() / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False, encoding='utf-8')
        return int(len(df))
    finally:
        session.close()


def _safe_collection_size(value):
    if value is None:
        return 0
    try:
        return int(len(value))
    except Exception:
        return 0


def _summarize_collection_result(result):
    if isinstance(result, dict):
        parts = []
        exported_rows = result.get('exported_rows')
        synced_rows = result.get('synced_rows')
        collected = result.get('collected')
        if exported_rows is not None:
            parts.append(f"导出 {int(exported_rows)} 条")
        if synced_rows is not None:
            parts.append(f"同步 {int(synced_rows)} 条")
        if isinstance(collected, dict):
            success = int(collected.get('success') or 0)
            total = int(collected.get('total') or 0)
            if total > 0:
                parts.append(f"成功 {success}/{total} 个标的")
        elif isinstance(collected, int):
            parts.append(f"处理 {int(collected)} 条")
        if result.get('message'):
            parts.append(str(result.get('message')))
        return '，'.join(parts) if parts else '采集完成'
    if isinstance(result, (list, tuple, set)):
        return f"处理 {len(result)} 项"
    if isinstance(result, int):
        return f"处理 {int(result)} 条"
    return '采集完成'


def _collect_stock_realtime_snapshot():
    from collectors.stock_collector import StockCollector

    collected = StockCollector().collect_all_realtime()
    synced_rows = _sync_latest_daily_prices(window_days=30)
    return {'collected': len(collected or []), 'synced_rows': synced_rows}


def _collect_historical_stock_csv(market, years=3, limit=120):
    from collectors.stock_collector import StockCollector

    collector = StockCollector()
    market = str(market or 'A').upper()

    if market == 'H':
        fetched = collector.fetch_all_hk_stocks_from_akshare() or []
        codes = [item.get('code') for item in fetched if item.get('code')] or list(collector.hk_stock_pool or [])
        collected = collector.collect_hk_stocks_batch(codes=codes[:limit], years=years, limit=limit)
        filename = 'historical_hk_stock.csv'
    elif market == 'US':
        fetched = collector.fetch_all_us_stocks() or []
        codes = [item.get('code') for item in fetched if item.get('code')] or list(collector.us_stock_pool or [])
        collected = collector.collect_us_stocks_batch(codes=codes[:limit], years=years, limit=limit)
        filename = 'historical_us_stock.csv'
    else:
        fetched = collector.fetch_all_a_stocks_from_akshare() or []
        codes = [item.get('code') for item in fetched if item.get('code')] or list(collector.a_stock_pool or [])
        collected = collector.collect_batch(codes[:limit], market='A', years=years, delay=0.2)
        filename = 'historical_a_stock.csv'
        market = 'A'

    exported_rows = _export_raw_stock_csv(filename, market=market)
    synced_rows = _sync_latest_daily_prices(window_days=120)
    return {'collected': collected, 'exported_rows': exported_rows, 'synced_rows': synced_rows}


def _collect_fund_nav_dataset(days=365):
    from collectors.fund_collector import FundCollector

    collector = FundCollector()
    collected = collector.collect_all_funds()
    exported_rows = _export_fund_nav_csv('fund_nav.csv')
    synced_rows = _sync_latest_daily_prices(window_days=120)
    return {'collected': len(collected or []), 'exported_rows': exported_rows, 'synced_rows': synced_rows}


def _collect_etf_history_dataset(years=3):
    from collectors.stock_collector import StockCollector

    collector = StockCollector()
    etf_funds = []
    try:
        from recommenders.etf_recommender import ETFRecommender
        etf_funds.extend([
            {
                'code': str(item.get('code') or '').strip(),
                'name': str(item.get('name') or item.get('code') or '').strip(),
                'type': 'etf',
            }
            for item in (ETFRecommender().etf_pool or [])
            if item and item.get('code')
        ])
    except Exception:
        pass

    if not etf_funds:
        etf_funds = [
            {'code': '510300.SH', 'name': '沪深300ETF', 'type': 'etf'},
            {'code': '510500.SH', 'name': '中证500ETF', 'type': 'etf'},
            {'code': '510050.SH', 'name': '上证50ETF', 'type': 'etf'},
            {'code': '159915.SZ', 'name': '创业板ETF', 'type': 'etf'},
            {'code': '588000.SH', 'name': '科创50ETF', 'type': 'etf'},
            {'code': '512880.SH', 'name': '证券ETF', 'type': 'etf'},
            {'code': '512690.SH', 'name': '酒ETF', 'type': 'etf'},
            {'code': '515030.SH', 'name': '新能源车ETF', 'type': 'etf'},
            {'code': '512010.SH', 'name': '医药ETF', 'type': 'etf'},
            {'code': '518880.SH', 'name': '黄金ETF', 'type': 'etf'},
        ]

    deduped = []
    seen = set()
    for item in etf_funds:
        code = str(item.get('code') or '').strip()
        if not code or code in seen:
            continue
        seen.add(code)
        deduped.append(item)

    collected = collector.collect_funds_batch(funds=deduped[:50], years=years, limit=None)
    exported_rows = _export_raw_stock_csv('historical_etf.csv', codes=[item['code'] for item in deduped])
    synced_rows = _sync_latest_daily_prices(window_days=120)
    return {'collected': len(collected or []), 'exported_rows': exported_rows, 'synced_rows': synced_rows}


def _collect_precious_metals_dataset(subset='all', years=3):
    from collectors.stock_collector import StockCollector

    gold_codes = {'GC=F', 'XAUUSD=X', 'GLD', 'IAU', 'GLDM', 'SGOL', '518880.SH', '518800.SH', '159934.SZ'}
    silver_codes = {'SI=F', 'XAGUSD=X', 'SLV', 'SIVR', 'PSLV'}

    collector = StockCollector()
    collected = collector.collect_precious_metals(years=years)

    if subset == 'gold':
        exported_rows = _export_raw_stock_csv('gold_prices.csv', codes=gold_codes)
    elif subset == 'silver':
        exported_rows = _export_raw_stock_csv('silver_prices.csv', codes=silver_codes)
    else:
        exported_rows = _export_raw_stock_csv('precious_metals.csv', codes=(gold_codes | silver_codes))
        _export_raw_stock_csv('gold_prices.csv', codes=gold_codes)
        _export_raw_stock_csv('silver_prices.csv', codes=silver_codes)

    synced_rows = _sync_latest_daily_prices(window_days=120)
    return {'collected': len(collected or []), 'exported_rows': exported_rows, 'synced_rows': synced_rows}


def _collect_moneyflow_dataset():
    from collectors.moneyflow_collector import MoneyflowCollector

    collector = MoneyflowCollector()
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=120)).strftime('%Y%m%d')
    collected = collector.collect_by_date(start_date, end_date, max_stocks=None, resume=True)
    return {'collected': _safe_collection_size(collected), 'message': '资金流补采完成'}


def _collect_north_money_dataset():
    from collectors.north_money_collector import NorthMoneyCollector

    collected = NorthMoneyCollector().collect_latest(days=60)
    return {'collected': _safe_collection_size(collected), 'message': '北向资金补采完成'}


def _collect_margin_dataset():
    from collectors.margin_collector import MarginCollector

    collected = MarginCollector().collect_latest(days=60)
    return {'collected': _safe_collection_size(collected), 'message': '融资融券补采完成'}


def _collect_top_list_dataset():
    from collectors.top_list_collector import TopListCollector

    collected = TopListCollector().collect_latest(days=60)
    return {'collected': _safe_collection_size(collected), 'message': '龙虎榜补采完成'}


def _collect_news_dataset():
    from collectors.news_collector import NewsCollector

    collected = NewsCollector().collect_latest(days=30)
    return {'collected': _safe_collection_size(collected), 'message': '新闻舆情补采完成'}


def _collect_daily_basic_dataset():
    from collectors.daily_basic_collector import DailyBasicCollector

    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
    collected = DailyBasicCollector().collect_all(start_date=start_date, end_date=end_date, resume=True)
    return {'collected': _safe_collection_size(collected), 'message': '日度估值补采完成'}


def _collect_financial_indicator_dataset():
    from collectors.financial_collector import FinancialCollector

    collected = FinancialCollector().collect_all(max_stocks=300, resume=True)
    return {'collected': _safe_collection_size(collected), 'message': '财务指标补采完成'}


def _collect_research_dataset():
    from collectors.research_collector import ResearchCollector

    collected = ResearchCollector().collect_latest(days=30)
    return {'collected': _safe_collection_size(collected), 'message': '研报补采完成'}


def _collect_macro_features_dataset():
    from collectors.macro_collector import MacroCollector

    result = MacroCollector().export_macro_feature_csvs()
    return {'collected': len(result.get('updated_files') or []), 'message': '宏观特征刷新完成'}


def _rebuild_recommendation_snapshot():
    scheduler_core = _get_scheduler_core()
    result = scheduler_core.rebuild_today_recommendations()
    if not result.get('success'):
        raise RuntimeError(result.get('error') or '推荐快照重建失败')
    return {'message': '今日推荐快照已重建'}


def _get_collectable_dataset_specs():
    return {
        'stock_realtime_snapshot': {'label': '股票原始行情', 'runner': _collect_stock_realtime_snapshot},
        'historical_a_stock': {'label': 'A股历史行情CSV', 'runner': lambda: _collect_historical_stock_csv('A', limit=180)},
        'historical_hk_stock': {'label': '港股历史行情CSV', 'runner': lambda: _collect_historical_stock_csv('H', limit=180)},
        'historical_us_stock': {'label': '美股历史行情CSV', 'runner': lambda: _collect_historical_stock_csv('US', limit=180)},
        'fund_nav': {'label': '基金净值CSV', 'runner': _collect_fund_nav_dataset},
        'historical_etf': {'label': 'ETF历史行情CSV', 'runner': _collect_etf_history_dataset},
        'precious_metals': {'label': '贵金属历史CSV', 'runner': _collect_precious_metals_dataset},
        'gold_prices': {'label': '黄金价格CSV', 'runner': lambda: _collect_precious_metals_dataset(subset='gold')},
        'silver_prices': {'label': '白银价格CSV', 'runner': lambda: _collect_precious_metals_dataset(subset='silver')},
        'news_all': {'label': '新闻舆情CSV', 'runner': _collect_news_dataset},
        'moneyflow_all': {'label': '资金流CSV', 'runner': _collect_moneyflow_dataset},
        'north_money_all': {'label': '北向资金CSV', 'runner': _collect_north_money_dataset},
        'margin_all': {'label': '融资融券CSV', 'runner': _collect_margin_dataset},
        'top_list': {'label': '龙虎榜CSV', 'runner': _collect_top_list_dataset},
        'daily_basic': {'label': '日度估值CSV', 'runner': _collect_daily_basic_dataset},
        'financial_indicator': {'label': '财务指标CSV', 'runner': _collect_financial_indicator_dataset},
        'research_report': {'label': '研报CSV', 'runner': _collect_research_dataset},
        'macro_features': {'label': '宏观特征CSV', 'runner': _collect_macro_features_dataset},
        'macro_cpi': {'label': '宏观CPI CSV', 'runner': _collect_macro_features_dataset},
        'macro_pmi': {'label': '宏观PMI CSV', 'runner': _collect_macro_features_dataset},
        'macro_shibor': {'label': '宏观Shibor CSV', 'runner': _collect_macro_features_dataset},
        'cross_asset_daily': {'label': '跨资产日频CSV', 'runner': _collect_macro_features_dataset},
        'daily_price_sync': {'label': '统一日价格表', 'runner': lambda: {'synced_rows': _sync_latest_daily_prices(window_days=120)}},
        'recommendation_snapshot': {'label': '投资推荐结果', 'runner': _rebuild_recommendation_snapshot},
    }


def _get_inventory_collect_key(item):
    filename = str(item.get('filename') or '').strip()
    if filename:
        return Path(filename).stem

    name_map = {
        '股票原始行情': 'stock_realtime_snapshot',
        '基金净值数据': 'fund_nav',
        '统一日价格表': 'daily_price_sync',
        '投资推荐结果': 'recommendation_snapshot',
        '宏观CPI CSV': 'macro_features',
        '宏观PMI CSV': 'macro_features',
        '宏观Shibor CSV': 'macro_features',
        '跨资产日频CSV': 'macro_features',
    }
    if item.get('category') == '推荐覆盖':
        return 'recommendation_snapshot'
    return name_map.get(str(item.get('name') or '').strip(), '')


def _run_single_dataset_collect(collect_key):
    collect_key = str(collect_key or '').strip()
    specs = _get_collectable_dataset_specs()
    spec = specs.get(collect_key)
    if not spec:
        raise ValueError(f'不支持的数据类型: {collect_key}')

    scheduler_core = _get_scheduler_core()
    _init_backfill_progress = scheduler_core._init_backfill_progress
    _update_backfill_step = scheduler_core._update_backfill_step
    _finish_backfill_progress = scheduler_core._finish_backfill_progress

    label = spec.get('label') or collect_key
    step_name = f'单项采集: {label}'
    now = datetime.now()
    _init_backfill_progress(now, now, steps=[step_name])

    try:
        _update_backfill_step(step_name, 'running', f'正在执行 {label} 单项采集')
        result = spec['runner']()
        message = _summarize_collection_result(result)
        _update_backfill_step(step_name, 'success', f'{label} 已完成：{message}')
    except Exception as e:
        logger.error(f'单项采集失败[{label}]: {e}')
        _update_backfill_step(step_name, 'failed', f'{label} 采集失败', str(e))
    finally:
        _finish_backfill_progress()


def _start_single_dataset_collect_async(collect_key):
    t = threading.Thread(
        target=_run_single_dataset_collect,
        args=(collect_key,),
        daemon=True,
        name=f'single_collect_{collect_key}',
    )
    t.start()
    logger.info(f'已启动单项补采线程: {collect_key}')


def _get_scheduler_core():
    """统一获取 scheduler.py 的核心模块，避免与 scheduler 包名冲突。"""
    import scheduler as scheduler_pkg
    return getattr(scheduler_pkg, '_scheduler_module', scheduler_pkg)


def _upsert_daily_price(session, code, date_value, close_price, market='UNKNOWN', open_price=None, high_price=None, low_price=None, volume=None):
    existing = (
        session.query(DailyPrice)
        .filter(DailyPrice.code == code)
        .filter(DailyPrice.date == date_value)
        .first()
    )
    if existing:
        existing.open = open_price if open_price is not None else close_price
        existing.high = high_price if high_price is not None else close_price
        existing.low = low_price if low_price is not None else close_price
        existing.close = close_price
        existing.volume = volume or 0
        existing.market = market
        return 0

    session.add(DailyPrice(
        code=code,
        date=date_value,
        open=open_price if open_price is not None else close_price,
        high=high_price if high_price is not None else close_price,
        low=low_price if low_price is not None else close_price,
        close=close_price,
        volume=volume or 0,
        market=market,
    ))
    return 1


def _sync_latest_daily_prices(window_days=30):
    """把最近窗口内的股票/基金价格同步到统一日价格表，补齐多资产共用历史价格。"""
    session = get_session()
    try:
        synced = 0
        cutoff_date = None
        if window_days and int(window_days) > 0:
            cutoff_date = datetime.now().date() - timedelta(days=int(window_days) - 1)

        stock_query = session.query(RawStockData)
        if cutoff_date is not None:
            stock_query = stock_query.filter(RawStockData.date >= cutoff_date)

        stock_rows = stock_query.order_by(RawStockData.date.asc()).all()
        for row in stock_rows:
            close_price = float(row.close or 0)
            if close_price <= 0:
                continue
            synced += _upsert_daily_price(
                session,
                code=row.code,
                date_value=row.date,
                close_price=close_price,
                market=row.market or 'STOCK',
                open_price=float(row.open or close_price),
                high_price=float(row.high or close_price),
                low_price=float(row.low or close_price),
                volume=int(row.volume or 0),
            )

        fund_query = session.query(RawFundData)
        if cutoff_date is not None:
            fund_query = fund_query.filter(RawFundData.date >= cutoff_date)

        fund_rows = fund_query.order_by(RawFundData.date.asc()).all()
        for row in fund_rows:
            base_code = str(row.code or '').split('.')[0]
            nav = float(row.nav or 0)
            if not base_code or nav <= 0:
                continue
            synced += _upsert_daily_price(
                session,
                code=base_code,
                date_value=row.date,
                close_price=nav,
                market='FUND',
                open_price=nav,
                high_price=float(row.accumulated_nav or nav),
                low_price=nav,
                volume=0,
            )

        session.commit()
        logger.info(f"统一日价格表同步完成: {synced} 条，窗口 {window_days} 天")
        return synced
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _build_recommendation_coverage(session):
    latest_date = session.query(Recommendation.date).order_by(Recommendation.date.desc()).limit(1).scalar()
    coverage = []
    expected = [
        ('a_stock', '推荐快照-A股'),
        ('hk_stock', '推荐快照-港股'),
        ('us_stock', '推荐快照-美股'),
        ('active_fund', '推荐快照-主动基金'),
        ('etf', '推荐快照-ETF'),
        ('gold', '推荐快照-黄金'),
        ('silver', '推荐快照-白银'),
    ]
    for rec_type, display_name in expected:
        row_count = 0
        latest_update = None
        if latest_date:
            row_count = int(session.query(Recommendation).filter(
                Recommendation.date == latest_date,
                Recommendation.type == rec_type
            ).count())
            latest_update = session.query(func.max(Recommendation.created_at)).filter(
                Recommendation.date == latest_date,
                Recommendation.type == rec_type
            ).scalar()
        coverage.append({
            'name': display_name,
            'category': '推荐覆盖',
            'row_count': row_count,
            'latest_update': _format_latest_update(latest_update or latest_date),
        })
    return coverage


def _build_progress_summary(progress):
    steps = progress.get('steps', []) or []
    daily_steps = [s for s in steps if str(s.get('name', '')).startswith('交易日流水线 ')]
    daily_done = [s for s in daily_steps if s.get('status') in ('success', 'failed')]
    daily_success = [s for s in daily_steps if s.get('status') == 'success']
    daily_failed = [s for s in daily_steps if s.get('status') == 'failed']

    failed_steps = []
    for s in steps:
        if s.get('status') == 'failed':
            failed_steps.append({
                'name': s.get('name'),
                'message': s.get('message', ''),
                'error': s.get('error'),
                'updated_at': s.get('updated_at'),
                'retryable': True,
            })

    total_days = len(daily_steps)
    completed_days = len(daily_done)
    progress_percent_days = round((completed_days / total_days) * 100, 2) if total_days else 0.0

    recent_steps = progress.get('recent_steps', []) or []

    completed_with_duration = []
    for s in steps:
        if s.get('status') not in ('success', 'failed'):
            continue
        duration = s.get('duration_seconds')
        if duration is None:
            continue
        completed_with_duration.append({
            'name': s.get('name'),
            'status': s.get('status'),
            'duration_seconds': duration,
            'updated_at': s.get('updated_at'),
        })

    slowest_steps = sorted(
        completed_with_duration,
        key=lambda x: x.get('duration_seconds') or 0,
        reverse=True,
    )[:5]

    current_step_name = progress.get('current_step')
    current_step_elapsed_seconds = None
    current_step_started_at = None
    if current_step_name and progress.get('running'):
        current = next((s for s in steps if s.get('name') == current_step_name), None)
        if current:
            current_step_started_at = current.get('started_at')
            try:
                if current_step_started_at:
                    current_step_elapsed_seconds = round(
                        max((datetime.now() - datetime.fromisoformat(current_step_started_at)).total_seconds(), 0.0),
                        2,
                    )
            except Exception:
                current_step_elapsed_seconds = None

    elapsed_total_seconds = None
    start_time = progress.get('start_time')
    if start_time:
        try:
            end_time = progress.get('end_time')
            end_dt = datetime.fromisoformat(end_time) if end_time else datetime.now()
            elapsed_total_seconds = round(max((end_dt - datetime.fromisoformat(start_time)).total_seconds(), 0.0), 2)
        except Exception:
            elapsed_total_seconds = None

    avg_day_duration_seconds = None
    daily_durations = [
        s.get('duration_seconds') for s in daily_done
        if s.get('duration_seconds') is not None
    ]
    if daily_durations:
        avg_day_duration_seconds = round(sum(daily_durations) / len(daily_durations), 2)

    pipeline_current_stage = progress.get('current_pipeline_stage')
    pipeline_recent_stages = progress.get('recent_pipeline_stages', []) or []
    pipeline_slowest_stages = (progress.get('slowest_pipeline_stages', []) or [])[:5]

    return {
        'total_days': total_days,
        'completed_days': completed_days,
        'success_days': len(daily_success),
        'failed_days': len(daily_failed),
        'progress_percent_days': progress_percent_days,
        'failed_steps': failed_steps,
        'recent_steps': recent_steps,
        'slowest_steps': slowest_steps,
        'current_step_elapsed_seconds': current_step_elapsed_seconds,
        'current_step_started_at': current_step_started_at,
        'elapsed_total_seconds': elapsed_total_seconds,
        'avg_day_duration_seconds': avg_day_duration_seconds,
        'pipeline_current_stage': pipeline_current_stage,
        'pipeline_recent_stages': pipeline_recent_stages,
        'pipeline_slowest_stages': pipeline_slowest_stages,
    }


def _safe_csv_profile(path):
    profile = {
        'row_count': 0,
        'unique_codes': 0,
        'latest_data_time': '',
    }
    if not path.exists():
        return profile

    try:
        df = pd.read_csv(path)
        profile['row_count'] = int(len(df))

        for code_col in ['ts_code', 'code', 'symbol']:
            if code_col in df.columns:
                profile['unique_codes'] = int(df[code_col].dropna().astype(str).nunique())
                break

        for date_col in ['trade_date', 'date', 'datetime', 'month', 'ann_date', 'publish_date']:
            if date_col in df.columns:
                series = pd.to_datetime(df[date_col].astype(str), errors='coerce').dropna()
                if len(series) > 0:
                    profile['latest_data_time'] = series.max().strftime('%Y-%m-%d %H:%M:%S')
                break
    except Exception:
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                profile['row_count'] = max(sum(1 for _ in f) - 1, 0)
        except Exception:
            pass

    return profile


def _format_latest_update(value):
    if value is None or value == '':
        return ''
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    if hasattr(value, 'strftime'):
        return value.strftime('%Y-%m-%d 00:00:00')
    try:
        parsed = datetime.fromisoformat(str(value))
        return parsed.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return str(value)


def _parse_latest_update_dt(value):
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    except Exception:
        try:
            return pd.to_datetime(value, errors='coerce').to_pydatetime()
        except Exception:
            return None


def _expected_freshness_days(item):
    name = str(item.get('name') or '').strip()
    category = str(item.get('category') or '').strip()

    if name in ('宏观CPI CSV', '宏观PMI CSV'):
        return 70
    if name == '股票标的池CSV':
        return 14
    if '基金净值' in name:
        return 5
    if name in (
        'A股历史行情CSV', '港股历史行情CSV', '美股历史行情CSV',
        'ETF历史行情CSV', '贵金属历史CSV', '黄金价格CSV', '白银价格CSV',
        '新闻舆情CSV', '资金流CSV', '北向资金CSV', '融资融券CSV', '龙虎榜CSV',
        '日度估值CSV', '财务指标CSV',
    ):
        return 4
    if category == '宏观特征':
        return 5
    if category in ('数据库', '推荐覆盖', '训练特征', 'CSV文件'):
        return 4
    return None


def _decorate_inventory_item(item, reference_time=None):
    reference_time = reference_time or datetime.now()
    row_count = int(item.get('row_count') or 0)
    latest_update = item.get('latest_update')
    latest_dt = _parse_latest_update_dt(latest_update)
    coverage_count = int(item.get('coverage_count') or 0)
    expected_min_codes = int(item.get('expected_min_codes') or 0)
    item['lag_days'] = None

    if row_count <= 0:
        item['status'] = 'empty'
        item['status_text'] = '待补采'
        item['note'] = '当前无数据'
        return item

    if expected_min_codes > 0 and coverage_count > 0:
        coverage_floor = max(1, int(expected_min_codes * 0.8))
        if coverage_count < coverage_floor:
            item['status'] = 'stale'
            item['status_text'] = '覆盖偏少'
            item['note'] = f'当前覆盖 {coverage_count} 个标的，低于建议值 {expected_min_codes}'
            return item

    if latest_dt:
        freshness_days = _expected_freshness_days(item)
        age_days = max((reference_time - latest_dt).total_seconds() / 86400.0, 0.0)
        item['lag_days'] = round(age_days, 2)
        if freshness_days is not None and age_days > freshness_days:
            item['status'] = 'stale'
            item['status_text'] = '待补采'
            item['note'] = f'最近约 {int(age_days)} 天未更新，建议执行单项补采'
        else:
            item['status'] = 'ready'
            item['status_text'] = '正常'
            item['note'] = ''
        return item

    item['status'] = 'stale'
    item['status_text'] = '待校验'
    item['note'] = '缺少最近更新时间'
    return item


def _build_dataset_inventory():
    session = get_session()
    data_dir = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) / 'data'
    try:
        inventories = [
            {
                'name': '股票原始行情',
                'category': '数据库',
                'row_count': int(session.query(RawStockData).count()),
                'latest_update': _format_latest_update(
                    session.query(func.max(RawStockData.created_at)).scalar()
                    or session.query(func.max(RawStockData.date)).scalar()
                ),
            },
            {
                'name': '基金净值数据',
                'category': '数据库',
                'row_count': int(session.query(RawFundData).count()),
                'latest_update': _format_latest_update(
                    session.query(func.max(RawFundData.created_at)).scalar()
                    or session.query(func.max(RawFundData.date)).scalar()
                ),
            },
            {
                'name': '统一日价格表',
                'category': '数据库',
                'row_count': int(session.query(DailyPrice).count()),
                'latest_update': _format_latest_update(
                    session.query(func.max(DailyPrice.created_at)).scalar()
                    or session.query(func.max(DailyPrice.date)).scalar()
                ),
            },
            {
                'name': '投资推荐结果',
                'category': '数据库',
                'row_count': int(session.query(Recommendation).count()),
                'latest_update': _format_latest_update(
                    session.query(func.max(Recommendation.created_at)).scalar()
                    or session.query(func.max(Recommendation.date)).scalar()
                ),
            },
            {
                'name': '预测记录',
                'category': '数据库',
                'row_count': int(session.query(Prediction).count()),
                'latest_update': _format_latest_update(
                    session.query(func.max(Prediction.created_at)).scalar()
                    or session.query(func.max(Prediction.date)).scalar()
                ),
            },
        ]

        csv_files = [
            {'name': '股票标的池CSV', 'filename': 'stock_basic.csv', 'category': '标的池', 'min_codes': 1000},
            {'name': 'A股历史行情CSV', 'filename': 'historical_a_stock.csv', 'category': 'CSV文件', 'min_codes': 1000},
            {'name': '港股历史行情CSV', 'filename': 'historical_hk_stock.csv', 'category': 'CSV文件', 'min_codes': 100},
            {'name': '美股历史行情CSV', 'filename': 'historical_us_stock.csv', 'category': 'CSV文件', 'min_codes': 100},
            {'name': '基金净值CSV', 'filename': 'fund_nav.csv', 'category': 'CSV文件', 'min_codes': 100},
            {'name': 'ETF历史行情CSV', 'filename': 'historical_etf.csv', 'category': 'CSV文件', 'min_codes': 10},
            {'name': '贵金属历史CSV', 'filename': 'precious_metals.csv', 'category': 'CSV文件', 'min_codes': 2},
            {'name': '黄金价格CSV', 'filename': 'gold_prices.csv', 'category': 'CSV文件', 'min_codes': 1},
            {'name': '白银价格CSV', 'filename': 'silver_prices.csv', 'category': 'CSV文件', 'min_codes': 1},
            {'name': '新闻舆情CSV', 'filename': 'news_all.csv', 'category': 'CSV文件'},
            {'name': '资金流CSV', 'filename': 'moneyflow_all.csv', 'category': 'CSV文件', 'min_codes': 500},
            {'name': '北向资金CSV', 'filename': 'north_money_all.csv', 'category': 'CSV文件'},
            {'name': '融资融券CSV', 'filename': 'margin_all.csv', 'category': 'CSV文件'},
            {'name': '龙虎榜CSV', 'filename': 'top_list.csv', 'category': 'CSV文件', 'min_codes': 100},
            {'name': '日度估值CSV', 'filename': 'daily_basic.csv', 'category': '训练特征', 'min_codes': 1000},
            {'name': '财务指标CSV', 'filename': 'financial_indicator.csv', 'category': '训练特征', 'min_codes': 1000},
            {'name': '研报CSV', 'filename': 'research_report.csv', 'category': 'CSV文件', 'min_codes': 100},
            {'name': '宏观CPI CSV', 'filename': 'macro_cpi.csv', 'category': '宏观特征'},
            {'name': '宏观PMI CSV', 'filename': 'macro_pmi.csv', 'category': '宏观特征'},
            {'name': '宏观Shibor CSV', 'filename': 'macro_shibor.csv', 'category': '宏观特征'},
            {'name': '跨资产日频CSV', 'filename': 'cross_asset_daily.csv', 'category': '宏观特征'},
        ]
        for item in csv_files:
            csv_path = data_dir / item['filename']
            profile = _safe_csv_profile(csv_path)
            unique_codes = int(profile.get('unique_codes') or 0)
            coverage_text = f"{unique_codes}个标的" if unique_codes > 0 else '--'
            inventories.append({
                'name': item['name'],
                'filename': item['filename'],
                'category': item.get('category', 'CSV文件'),
                'row_count': int(profile.get('row_count') or 0),
                'latest_update': profile.get('latest_data_time') or (datetime.fromtimestamp(csv_path.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S') if csv_path.exists() else ''),
                'coverage_count': unique_codes,
                'coverage_text': coverage_text,
                'expected_min_codes': int(item.get('min_codes') or 0),
            })

        inventories.extend(_build_recommendation_coverage(session))

        collect_specs = _get_collectable_dataset_specs()

        for item in inventories:
            item = _decorate_inventory_item(item)
            collect_key = _get_inventory_collect_key(item)
            spec = collect_specs.get(collect_key)
            item['collectable'] = bool(spec)
            item['collect_key'] = collect_key if spec else ''
            item['collect_action_text'] = (
                '立即补采' if bool(spec) and item.get('status') in ('empty', 'stale') else ('单独刷新' if bool(spec) else '')
            )

        return inventories
    finally:
        session.close()


def _get_meaningful_backfill_candidates(inventories=None):
    inventories = inventories or _build_dataset_inventory()
    candidates = []
    seen = set()

    for item in inventories:
        collect_key = str(item.get('collect_key') or '').strip()
        if not collect_key or collect_key in seen:
            continue
        if item.get('status') not in ('empty', 'stale'):
            continue

        lag_days = float(item.get('lag_days') or 0.0)
        status_text = str(item.get('status_text') or '')
        should_collect = (
            item.get('status') == 'empty'
            or lag_days >= 3
            or status_text == '覆盖偏少'
        )
        if not should_collect:
            continue

        seen.add(collect_key)
        candidates.append({
            'collect_key': collect_key,
            'label': item.get('name') or collect_key,
        })

    rec_keys = {
        'stock_realtime_snapshot', 'historical_a_stock', 'historical_hk_stock', 'historical_us_stock',
        'fund_nav', 'historical_etf', 'precious_metals', 'gold_prices', 'silver_prices',
        'moneyflow_all', 'north_money_all', 'margin_all', 'top_list', 'daily_basic', 'financial_indicator'
    }
    if any(item['collect_key'] in rec_keys for item in candidates) and 'recommendation_snapshot' not in seen:
        candidates.append({'collect_key': 'recommendation_snapshot', 'label': '今日推荐快照'})

    return candidates


def _run_missing_only_backfill():
    """仅针对明显缺口或真实过期的数据执行补采。"""
    scheduler_core = _get_scheduler_core()
    _init_backfill_progress = scheduler_core._init_backfill_progress
    _update_backfill_step = scheduler_core._update_backfill_step
    _finish_backfill_progress = scheduler_core._finish_backfill_progress

    now = datetime.now()
    inventories = _build_dataset_inventory()
    collect_specs = _get_collectable_dataset_specs()
    candidates = _get_meaningful_backfill_candidates(inventories)

    step_names = [f"补缺: {item['label']}" for item in candidates] or ['补缺检查']
    _init_backfill_progress(now, now, steps=step_names)

    try:
        if not candidates:
            _update_backfill_step('补缺检查', 'success', '当前未发现明显缺口，数据覆盖已基本完整')
            return

        for item in candidates:
            step_name = f"补缺: {item['label']}"
            collect_key = item['collect_key']
            _update_backfill_step(step_name, 'running', f"正在执行 {step_name}")
            try:
                spec = collect_specs.get(collect_key)
                if not spec or not callable(spec.get('runner')):
                    raise RuntimeError(f'未找到补采器: {collect_key}')
                result = spec['runner']()
                message = _summarize_collection_result(result)
                _update_backfill_step(step_name, 'success', f"已完成 {step_name}：{message}")
            except Exception as step_err:
                logger.error(f"缺失补采失败[{step_name}]: {step_err}")
                _update_backfill_step(step_name, 'failed', f"补采失败: {step_name}", str(step_err))
    finally:
        _finish_backfill_progress()


def _start_missing_only_backfill_async():
    t = threading.Thread(
        target=_run_missing_only_backfill,
        daemon=True,
        name='missing_only_backfill',
    )
    t.start()
    logger.info('已启动后台缺失补采线程（仅补缺，不做全量重扫）')


def register_backfill_routes(app):
    """注册补采进度相关路由"""

    @app.route('/api/backfill/progress', methods=['GET'])
    def get_backfill_progress():
        """获取自动补采进度"""
        try:
            from scheduler import get_auto_backfill_progress

            progress = get_auto_backfill_progress()
            summary = _build_progress_summary(progress)
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': progress,
                'summary': summary,
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"获取补采进度失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500

    @app.route('/api/backfill/dataset-inventory', methods=['GET'])
    def get_dataset_inventory():
        """获取采集数据类型、数据量与最近更新时间。"""
        try:
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': _build_dataset_inventory(),
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"获取数据资产盘点失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500

    @app.route('/api/backfill/start', methods=['POST'])
    @require_admin_access(action='backfill.start')
    def start_backfill_task():
        """启动自动补采任务（后台线程）。"""
        try:
            from scheduler import get_auto_backfill_progress, start_auto_backfill_current_year_async

            progress = get_auto_backfill_progress()
            if progress.get('running'):
                return jsonify({
                    'code': 400,
                    'status': 'error',
                    'message': '自动补采任务已在运行中',
                    'timestamp': datetime.now().isoformat()
                }), 400

            body = request.get_json(silent=True) or {}
            mode = str(body.get('mode') or 'missing_only').strip().lower()

            if mode == 'full':
                start_auto_backfill_current_year_async()
                message = '已启动全量补采任务'
            else:
                _start_missing_only_backfill_async()
                message = '已启动补缺补采任务（默认仅处理缺失数据）'

            log_admin_audit('backfill.start', 'success', f"mode={mode}")
            return jsonify({
                'code': 200,
                'status': 'success',
                'message': message,
                'data': {'mode': mode},
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"启动自动补采失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500

    @app.route('/api/backfill/collect-dataset', methods=['POST'])
    @require_admin_access(action='backfill.collect_dataset')
    def collect_single_dataset_api():
        """触发单个数据类型的后台采集。"""
        try:
            body = request.get_json(silent=True) or {}
            collect_key = str(body.get('collect_key') or '').strip()
            if not collect_key:
                return jsonify({
                    'code': 400,
                    'status': 'error',
                    'message': '缺少 collect_key',
                    'timestamp': datetime.now().isoformat()
                }), 400

            from scheduler import get_auto_backfill_progress

            progress = get_auto_backfill_progress()
            if progress.get('running'):
                return jsonify({
                    'code': 400,
                    'status': 'error',
                    'message': '当前已有补采任务运行中，请稍后再试',
                    'timestamp': datetime.now().isoformat()
                }), 400

            spec = _get_collectable_dataset_specs().get(collect_key)
            if not spec:
                return jsonify({
                    'code': 404,
                    'status': 'error',
                    'message': f'暂不支持单独采集: {collect_key}',
                    'timestamp': datetime.now().isoformat()
                }), 404

            _start_single_dataset_collect_async(collect_key)
            log_admin_audit('backfill.collect_dataset', 'success', f"collect_key={collect_key}")
            return jsonify({
                'code': 200,
                'status': 'success',
                'message': f"已启动单项采集：{spec.get('label') or collect_key}",
                'data': {
                    'collect_key': collect_key,
                    'label': spec.get('label') or collect_key,
                },
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"启动单项采集失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500

    @app.route('/api/backfill/retry-step', methods=['POST'])
    @require_admin_access(action='backfill.retry_step')
    def retry_backfill_step_api():
        """重试单个失败步骤。"""
        try:
            body = request.get_json(silent=True) or {}
            step_name = (body.get('step_name') or '').strip()
            if not step_name:
                return jsonify({
                    'code': 400,
                    'status': 'error',
                    'message': '缺少 step_name',
                    'timestamp': datetime.now().isoformat()
                }), 400

            from scheduler import start_retry_backfill_step_async

            start_retry_backfill_step_async(step_name)
            log_admin_audit('backfill.retry_step', 'success', f"step_name={step_name}")
            return jsonify({
                'code': 200,
                'status': 'success',
                'message': f'已触发重试: {step_name}',
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"重试补采步骤失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
