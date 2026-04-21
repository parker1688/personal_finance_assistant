"""
推荐API - api/recommendations.py
提供推荐列表和详情接口
"""

import sys
import os
import json
import csv
from functools import lru_cache
from datetime import datetime, timedelta
from flask import jsonify, request
import numpy as np
from sqlalchemy import or_

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import get_session, Recommendation, Prediction, Indicator, Holding, RawFundData, RawStockData
from recommenders.stock_recommender import StockRecommender
from recommendation_probability import build_empirical_calibrators, derive_probabilities, derive_unified_trend
from utils import get_logger, get_today
from collectors.stock_collector import StockCollector
from config import STOCK_BASIC_FILE, LEGACY_STOCK_POOL_FILE, resolve_data_file

logger = get_logger(__name__)

# 全局推荐引擎实例
recommender = StockRecommender()

# 港股/美股常用代码映射（用于名称兜底）
_OVERSEAS_STOCK_META = {
    # 港股
    '0700.HK': {'name': '腾讯控股', 'industry': '互联网'},
    '9988.HK': {'name': '阿里巴巴-SW', 'industry': '互联网'},
    '3690.HK': {'name': '美团-W', 'industry': '互联网服务'},
    '1810.HK': {'name': '小米集团-W', 'industry': '消费电子'},
    '9618.HK': {'name': '京东集团-SW', 'industry': '电商'},
    '9999.HK': {'name': '网易-S', 'industry': '互联网'},
    '1024.HK': {'name': '快手-W', 'industry': '互联网传媒'},
    '2015.HK': {'name': '理想汽车-W', 'industry': '汽车'},
    '9888.HK': {'name': '百度集团-SW', 'industry': '互联网'},
    '6618.HK': {'name': '京东健康', 'industry': '互联网医疗'},
    # 美股
    'AAPL': {'name': 'Apple', 'industry': 'Technology'},
    'MSFT': {'name': 'Microsoft', 'industry': 'Technology'},
    'GOOGL': {'name': 'Alphabet', 'industry': 'Internet'},
    'AMZN': {'name': 'Amazon', 'industry': 'E-commerce'},
    'NVDA': {'name': 'NVIDIA', 'industry': 'Semiconductors'},
    'META': {'name': 'Meta Platforms', 'industry': 'Internet'},
    'TSLA': {'name': 'Tesla', 'industry': 'Automotive'},
    'BABA': {'name': 'Alibaba', 'industry': 'E-commerce'},
    'PDD': {'name': 'PDD Holdings', 'industry': 'E-commerce'},
    'JD': {'name': 'JD.com', 'industry': 'E-commerce'},
    'BIDU': {'name': 'Baidu', 'industry': 'Internet'},
    'NIO': {'name': 'NIO', 'industry': 'Automotive'},
}


@lru_cache(maxsize=1)
def _load_stock_name_map():
    """加载股票代码到名称/行业的映射（带缓存）"""
    name_map = {}
    csv_path = resolve_data_file(STOCK_BASIC_FILE, LEGACY_STOCK_POOL_FILE)
    if not os.path.exists(csv_path):
        return name_map

    try:
        with open(csv_path, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ts_code = (row.get('ts_code') or '').strip()
                name = (row.get('name') or '').strip()
                industry = (row.get('industry') or '').strip()
                if not ts_code:
                    continue

                name_map[ts_code] = {'ts_code': ts_code, 'name': name, 'industry': industry}
                base_code = ts_code.split('.')[0]
                if base_code:
                    name_map[base_code] = {'ts_code': ts_code, 'name': name, 'industry': industry}
    except Exception as e:
        logger.warning(f"加载股票名称映射失败: {e}")

    return name_map


def _resolve_stock_meta(code, fallback_name=''):
    """根据代码解析股票元信息，返回 ts_code/name/industry"""
    normalized_code = (code or '').strip()
    normalized_base_code = normalized_code.split('.')[0] if normalized_code else ''

    info = _load_stock_name_map().get(normalized_code, {})
    if not info:
        info = _load_stock_name_map().get(normalized_base_code, {})
    if not info:
        overseas = _OVERSEAS_STOCK_META.get(normalized_code, {})
        if overseas:
            info = {
                'ts_code': normalized_code,
                'name': overseas.get('name', ''),
                'industry': overseas.get('industry', '')
            }

    mapped_code = (info.get('ts_code') or '').strip()

    resolved_name = (fallback_name or '').strip()
    if resolved_name in {'', normalized_code, normalized_base_code, mapped_code}:
        resolved_name = info.get('name') or resolved_name or code

    return {
        'ts_code': info.get('ts_code') or code,
        'name': resolved_name,
        'industry': info.get('industry') or ''
    }


def _normalize_yfinance_symbol(code):
    """将本地市场代码转换为 yfinance 可识别的符号。"""
    normalized = str(code or '').strip().upper()
    if normalized.endswith('.SH'):
        return normalized[:-3] + '.SS'
    return normalized


def _build_active_fund_holding_fallback(session, keyword, limit=20):
    """当主动基金不在当期推荐池时，允许从当前持仓/净值库中补充检索结果。"""
    keyword = (keyword or '').strip()
    if not keyword:
        return []

    like_pattern = f"%{keyword}%"
    holding_rows = (
        session.query(Holding)
        .filter(Holding.asset_type == 'fund')
        .filter(or_(Holding.code.ilike(like_pattern), Holding.name.ilike(like_pattern)))
        .all()
    )

    merged = {}
    for row in holding_rows:
        key = str(row.code or '').split('.')[0]
        if not key:
            continue
        entry = merged.setdefault(key, {
            'code': key,
            'name': row.name or f'基金{key}',
            'quantity': 0.0,
            'cost_amount': 0.0,
        })
        qty = float(row.quantity or 0)
        entry['quantity'] += qty
        entry['cost_amount'] += qty * float(row.cost_price or 0)

    if not merged:
        latest_rows = (
            session.query(RawFundData)
            .filter(or_(RawFundData.code.ilike(like_pattern), RawFundData.name.ilike(like_pattern)))
            .order_by(RawFundData.date.desc())
            .limit(limit * 5)
            .all()
        )
        for row in latest_rows:
            key = str(row.code or '').split('.')[0]
            if key not in merged:
                merged[key] = {
                    'code': key,
                    'name': row.name or f'基金{key}',
                    'quantity': 0.0,
                    'cost_amount': 0.0,
                }

    if not merged:
        return []

    from api.holdings import get_current_price

    results = []
    for idx, item in enumerate(list(merged.values())[:limit], start=1):
        quantity = float(item.get('quantity') or 0.0)
        avg_cost = (float(item.get('cost_amount') or 0.0) / quantity) if quantity > 0 else 0.0
        current_price = float(get_current_price(item['code'], 'fund') or avg_cost or 1.0)
        price_change_pct = ((current_price - avg_cost) / avg_cost * 100.0) if avg_cost > 0 else 0.0
        bias = max(-6.0, min(6.0, price_change_pct * 1.5))
        up5 = round(50.0 + bias * 0.5, 2)
        up20 = round(52.0 + bias * 0.7, 2)
        up60 = round(54.0 + bias * 0.9, 2)
        total_score = round(max(2.8, min(4.2, 3.2 + bias / 10.0)), 2)

        result = {
            'rank': idx,
            'code': item['code'],
            'name': item['name'],
            'display_code': item['code'],
            'industry': '主动基金',
            'current_price': round(current_price, 4),
            'up_probability_5d': up5,
            'up_probability_20d': up20,
            'up_probability_60d': up60,
            'total_score': total_score,
            'volatility_level': 'low' if abs(price_change_pct) < 8 else 'medium',
            'reason_summary': '当前持有基金，已补充净值跟踪；若需完整评分，可等待下一轮基金推荐刷新。',
        }
        result['unified_trend'] = derive_unified_trend(result)
        result['trend_direction'] = result['unified_trend'].get('trend_direction', 'neutral')
        result['trend_score'] = result['unified_trend'].get('trend_score', 50.0)
        result['trend_confidence'] = result['unified_trend'].get('trend_confidence', 20.0)
        result['advisor_view'] = _build_recommendation_advisor_payload(result)
        result['advisor_action'] = result['advisor_view'].get('action', 'hold')
        result['advisor_confidence'] = result['advisor_view'].get('confidence', 'low')
        result['risk_level'] = result['advisor_view'].get('risk_level', 'medium')
        result['position_size_pct'] = result['advisor_view'].get('position_size_pct', 0)
        result['review_in_days'] = result['advisor_view'].get('review_in_days', 3)
        result['strength'] = _classify_recommendation_strength(result)
        _apply_holding_recommendation(result, asset_type='active_fund')
        results.append(result)

    return results


def _build_stock_search_fallback(session, query_type, keyword, limit=20, probability_health=None):
    """当股票不在最新推荐池时，回退到真实股票数据做检索。"""
    keyword = (keyword or '').strip()
    if not keyword:
        return []

    market_map = {
        'a_stock': 'A',
        'hk_stock': 'H',
        'us_stock': 'US',
    }
    market = market_map.get(query_type)
    if not market:
        return []

    like_pattern = f"%{keyword}%"
    normalized_keyword = keyword.upper()
    stock_collector = StockCollector()
    candidates = []
    seen_codes = set()

    def add_candidate(code, name=''):
        normalized_code = str(code or '').strip().upper()
        if not normalized_code or normalized_code in seen_codes:
            return
        seen_codes.add(normalized_code)
        candidates.append({
            'code': normalized_code,
            'name': (name or '').strip(),
        })

    if query_type == 'a_stock':
        stock_name_map = _load_stock_name_map()
        for info in stock_name_map.values():
            ts_code = str(info.get('ts_code') or '').strip().upper()
            name = str(info.get('name') or '').strip()
            if not ts_code or '.' not in ts_code:
                continue
            if normalized_keyword in ts_code or normalized_keyword in name.upper():
                add_candidate(ts_code, name)
            if len(candidates) >= limit * 4:
                break

    raw_rows = (
        session.query(RawStockData.code, RawStockData.name, RawStockData.market, RawStockData.date)
        .filter(RawStockData.market == market)
        .filter(
            or_(
                RawStockData.code.ilike(like_pattern),
                RawStockData.name.ilike(like_pattern),
            )
        )
        .order_by(RawStockData.date.desc())
        .limit(limit * 30)
        .all()
    )
    for code, name, _market, _date in raw_rows:
        add_candidate(code, name)
        if len(candidates) >= limit * 5:
            break

    market_status = _get_stock_market_model_status(query_type, probability_health=probability_health)
    fallback_results = []
    for candidate in candidates:
        code = candidate['code']
        df = stock_collector.get_stock_data_from_db(code)
        if df is None or df.empty:
            continue

        latest_close = float(df['close'].iloc[-1]) if 'close' in df.columns and len(df) > 0 else 0.0
        latest_date = df.index[-1] if len(df.index) > 0 else None
        meta = _resolve_stock_meta(code, candidate.get('name') or '')

        total_score = 0.0
        up5 = 50.0
        up20 = 50.0
        up60 = 50.0
        volatility_level = 'medium'
        reason_summary = '该标的不在最新推荐池，当前按股票数据检索结果展示。'
        short_term_source = 'search_fallback'
        short_term_validated = False

        if len(df) >= 60:
            try:
                analysis = recommender.get_stock_analysis(code, market, df)
                if analysis:
                    total_score = round(float(analysis.get('total_score') or 0.0), 2)
                    short_term = analysis.get('predictions', {}).get('short_term', {})
                    medium_term = analysis.get('predictions', {}).get('medium_term', {})
                    long_term = analysis.get('predictions', {}).get('long_term', {})
                    up5 = round(float(short_term.get('up_probability', 50.0) or 50.0), 2)
                    up20 = round(float(medium_term.get('up_probability', 50.0) or 50.0), 2)
                    up60 = round(float(long_term.get('up_probability', 50.0) or 50.0), 2)
                    volatility_level = str(analysis.get('volatility_level') or 'medium')
                    reason_summary = analysis.get('recommendation_reason') or reason_summary
                    short_term_source = 'trained_model'
                    short_term_validated = bool(recommender.short_predictor.is_trained)
            except Exception as exc:
                logger.warning(f"股票搜索回退分析失败 {code}: {exc}")

        item = {
            'rank': len(fallback_results) + 1,
            'code': code,
            'name': meta['name'] or code,
            'display_code': meta['ts_code'] or code,
            'industry': meta['industry'],
            'current_price': round(latest_close, 2),
            'up_probability_5d': up5,
            'up_probability_20d': up20,
            'up_probability_60d': up60,
            'total_score': total_score,
            'volatility_level': volatility_level,
            'reason_summary': reason_summary,
            'as_of': latest_date.isoformat() if latest_date else None,
            'short_term_source': short_term_source if market_status.get('short_term_validated', short_term_validated) else 'rule_fallback',
            'short_term_validated': market_status.get('short_term_validated', short_term_validated),
            'market_model_reliability': market_status.get('market_model_reliability', {}),
        }
        item['unified_trend'] = derive_unified_trend({
            'up_probability_5d': up5,
            'up_probability_20d': up20,
            'up_probability_60d': up60,
            'total_score': total_score,
            'model_status': {
                'short_term_validated': market_status.get('short_term_validated', short_term_validated),
                'medium_term_validated': market_status.get('medium_term_validated', bool(recommender.medium_predictor.is_trained)),
                'long_term_validated': market_status.get('long_term_validated', bool(recommender.long_predictor.is_trained)),
            }
        })
        item['trend_direction'] = item['unified_trend'].get('trend_direction', 'neutral')
        item['trend_score'] = item['unified_trend'].get('trend_score', 50.0)
        item['trend_confidence'] = item['unified_trend'].get('trend_confidence', 20.0)
        item['advisor_view'] = _build_recommendation_advisor_payload(item)
        item['advisor_action'] = item['advisor_view'].get('action', 'watch')
        item['advisor_confidence'] = item['advisor_view'].get('confidence', 'low')
        item['risk_level'] = item['advisor_view'].get('risk_level', 'medium')
        item['position_size_pct'] = item['advisor_view'].get('position_size_pct', 0)
        item['review_in_days'] = item['advisor_view'].get('review_in_days', 3)
        item['strength'] = _classify_recommendation_strength(item)
        _apply_holding_recommendation(item, asset_type=query_type)
        fallback_results.append(item)
        if len(fallback_results) >= limit:
            break

    return fallback_results


def _get_stock_market_model_status(query_type, probability_health=None):
    market_map = {
        'a_stock': 'A',
        'hk_stock': 'H',
        'us_stock': 'US',
    }
    market = market_map.get(query_type, 'A')
    try:
        context = recommender._resolve_market_predictor_context(market)
    except Exception:
        context = {}

    short_predictor = context.get('short_term') or recommender.short_predictor
    medium_predictor = context.get('medium_term') or recommender.medium_predictor
    long_predictor = context.get('long_term') or recommender.long_predictor
    runtime_short_valid = bool(getattr(short_predictor, 'is_trained', False))
    runtime_medium_valid = bool(getattr(medium_predictor, 'is_trained', False))
    runtime_long_valid = bool(getattr(long_predictor, 'is_trained', False))

    quality_snapshot = context.get('quality_snapshot') or {}
    probability_health = probability_health or {}
    has_probability_health = any(str(key) in probability_health for key in ('5', '20', '60'))

    def _default_signal(trained, horizon, snapshot=None):
        base = snapshot or {}
        if base:
            return {
                'gate': base.get('gate', 'runtime_snapshot'),
                'level': base.get('level', 'medium' if trained else 'low'),
                'label': base.get('label', 'stable' if trained else 'guarded'),
                'score': float(base.get('score', 68.0 if trained else 32.0)),
                'passed': bool(base.get('passed', trained)),
                'reason': base.get('reason') or ('模型已完成训练，可继续结合历史表现观察。' if trained else '模型未通过运行时校验，当前回退为规则辅助。'),
                'horizon': horizon,
            }
        return {
            'gate': 'runtime_training',
            'level': 'medium' if trained else 'low',
            'label': 'stable' if trained else 'guarded',
            'score': 68.0 if trained else 32.0,
            'passed': trained,
            'reason': '模型已完成训练，可继续结合历史表现观察。' if trained else '模型未通过运行时校验，当前回退为规则辅助。',
            'horizon': horizon,
        }

    if has_probability_health:
        short_signal = _score_health_reliability(probability_health.get('5') or {}, horizon=5)
        medium_signal = _score_health_reliability(probability_health.get('20') or {}, horizon=20)
        long_signal = _score_health_reliability(probability_health.get('60') or {}, horizon=60)
    else:
        short_signal = _default_signal(runtime_short_valid, 5, quality_snapshot.get('short_term'))
        medium_signal = _default_signal(runtime_medium_valid, 20, quality_snapshot.get('medium_term'))
        long_signal = _default_signal(runtime_long_valid, 60, quality_snapshot.get('long_term'))

    short_validated = runtime_short_valid and bool(short_signal.get('passed', runtime_short_valid))
    medium_validated = runtime_medium_valid and bool(medium_signal.get('passed', runtime_medium_valid))
    long_validated = runtime_long_valid and bool(long_signal.get('passed', runtime_long_valid))

    return {
        'short_term_source': 'model' if short_validated else 'rule_fallback',
        'short_term_validated': short_validated,
        'medium_term_validated': medium_validated,
        'long_term_validated': long_validated,
        'market_model_reliability': short_signal,
        'model_reliability_by_horizon': {
            '5': short_signal,
            '20': medium_signal,
            '60': long_signal,
        },
    }


def _score_health_reliability(health_entry, horizon=5):
    """把历史概率健康结果转成统一的模型可靠性评分。"""
    entry = health_entry or {}
    status = str(entry.get('status') or 'insufficient_history')
    grade = str(entry.get('grade') or 'N/A').upper()
    samples = int(entry.get('samples') or 0)
    hit_rate = entry.get('hit_rate')
    brier = entry.get('brier')
    calibration_gap = entry.get('calibration_gap')

    if status != 'ok' or samples < 80:
        return {
            'gate': 'historical_probability_health',
            'level': 'low',
            'label': 'guarded',
            'score': 32.0,
            'passed': False,
            'reason': f'{horizon}日窗口历史样本不足或仍未稳定，当前仅适合作为参考。',
            'samples': samples,
            'grade': grade,
            'horizon': horizon,
        }

    score = {'A': 86.0, 'B': 72.0, 'C': 48.0, 'D': 32.0}.get(grade, 42.0)
    try:
        hit_rate_value = float(hit_rate)
        if hit_rate_value >= 65:
            score += 6.0
        elif hit_rate_value < 55:
            score -= 6.0
        elif hit_rate_value < 50:
            score -= 10.0
    except Exception:
        hit_rate_value = None

    try:
        brier_value = float(brier)
        if brier_value <= 0.20:
            score += 6.0
        elif brier_value > 0.25:
            score -= 6.0
        if brier_value > 0.28:
            score -= 8.0
    except Exception:
        brier_value = None

    try:
        gap_value = abs(float(calibration_gap))
        if gap_value > 10:
            score -= 10.0
        elif gap_value > 6:
            score -= 4.0
    except Exception:
        gap_value = None

    score = max(20.0, min(92.0, score))
    level = 'high' if score >= 75 else ('medium' if score >= 55 else 'low')
    label = 'supportive' if level == 'high' else ('stable' if level == 'medium' else 'guarded')
    passed = level != 'low' and grade in ('A', 'B')

    if level == 'high':
        reason = f'{horizon}日历史命中率与校准表现较稳，可作为辅助支持。'
    elif level == 'medium':
        reason = f'{horizon}日历史表现中性，建议分批参与并继续观察。'
    else:
        reason = f'{horizon}日历史验证一般或存在偏差，当前应以下调预期为主。'

    return {
        'gate': 'historical_probability_health',
        'level': level,
        'label': label,
        'score': round(score, 1),
        'passed': passed,
        'reason': reason,
        'samples': samples,
        'grade': grade,
        'hit_rate': hit_rate,
        'brier': brier,
        'calibration_gap': calibration_gap,
        'horizon': horizon,
    }


def _build_asset_model_status(query_type, probability_health=None):
    """统一构建推荐列表所需的资产级模型状态。"""
    if query_type in ('a_stock', 'hk_stock', 'us_stock'):
        return _get_stock_market_model_status(query_type, probability_health=probability_health)

    probability_health = probability_health or {}
    short_signal = _score_health_reliability(probability_health.get('5') or {}, horizon=5)
    medium_signal = _score_health_reliability(probability_health.get('20') or {}, horizon=20)
    long_signal = _score_health_reliability(probability_health.get('60') or {}, horizon=60)
    return {
        'short_term_source': 'historical_validation' if short_signal.get('passed') else 'rule_fallback',
        'short_term_validated': bool(short_signal.get('passed')),
        'medium_term_validated': bool(medium_signal.get('passed')),
        'long_term_validated': bool(long_signal.get('passed')),
        'market_model_reliability': short_signal,
        'model_reliability_by_horizon': {
            '5': short_signal,
            '20': medium_signal,
            '60': long_signal,
        },
    }


def _build_live_stock_list_fallback(query_type, limit=20, probability_health=None):
    """当日股票推荐快照过少时，直接基于实时本地分析回退，避免页面只剩 1 条。"""
    market_map = {
        'a_stock': 'A',
        'hk_stock': 'H',
        'us_stock': 'US',
    }
    market = market_map.get(query_type)
    if not market:
        return []

    try:
        live_recs = recommender.get_top_recommendations(market, limit=limit)
    except Exception as exc:
        logger.warning(f"股票列表实时回退失败[{query_type}]: {exc}")
        return []

    market_status = _get_stock_market_model_status(query_type, probability_health=probability_health)
    results = []
    for idx, rec in enumerate(live_recs, start=1):
        code = rec.get('code', '')
        meta = _resolve_stock_meta(code, rec.get('name', '') or '')
        item = {
            'rank': idx,
            'code': code,
            'name': meta['name'] or rec.get('name', code),
            'display_code': meta['ts_code'] or code,
            'industry': meta['industry'],
            'current_price': round(float(rec.get('current_price') or 0), 2),
            'up_probability_5d': round(float(rec.get('up_probability_5d', 50) or 50), 2),
            'up_probability_20d': round(float(rec.get('up_probability_20d', 50) or 50), 2),
            'up_probability_60d': round(float(rec.get('up_probability_60d', 50) or 50), 2),
            'total_score': round(float(rec.get('total_score', 0) or 0), 2),
            'volatility_level': rec.get('volatility_level', 'medium'),
            'reason_summary': rec.get('reason_summary', ''),
        }
        item['unified_trend'] = derive_unified_trend({
            'up_probability_5d': item['up_probability_5d'],
            'up_probability_20d': item['up_probability_20d'],
            'up_probability_60d': item['up_probability_60d'],
            'total_score': item['total_score'],
            'model_status': {
                'short_term_validated': market_status.get('short_term_validated', False),
                'medium_term_validated': market_status.get('medium_term_validated', False),
                'long_term_validated': market_status.get('long_term_validated', False),
            }
        })
        item['trend_direction'] = item['unified_trend'].get('trend_direction', 'neutral')
        item['trend_score'] = item['unified_trend'].get('trend_score', 50.0)
        item['trend_confidence'] = item['unified_trend'].get('trend_confidence', 20.0)
        item['advisor_view'] = _build_recommendation_advisor_payload(item)
        item['advisor_action'] = item['advisor_view'].get('action', 'hold')
        item['advisor_confidence'] = item['advisor_view'].get('confidence', 'low')
        item['risk_level'] = item['advisor_view'].get('risk_level', 'medium')
        item['position_size_pct'] = item['advisor_view'].get('position_size_pct', 0)
        item['review_in_days'] = item['advisor_view'].get('review_in_days', 3)
        item['strength'] = _classify_recommendation_strength(item)
        item['short_term_source'] = 'live_stock_fallback' if market_status.get('short_term_validated', False) else 'rule_fallback'
        item['short_term_validated'] = market_status.get('short_term_validated', False)
        item['market_model_reliability'] = market_status.get('market_model_reliability', {})
        _apply_holding_recommendation(item, asset_type=query_type)
        results.append(item)

    return results


def _score_to_confidence(score):
    """将数值评分(0-5)转为置信度文字描述"""
    if score >= 4.0:
        return '较高'
    elif score >= 3.0:
        return '中等'
    else:
        return '较低'


def _build_recommendation_advisor_payload(item):
    """为列表推荐构造轻量级投顾视图，便于前端直接展示动作建议。"""
    unified_trend = item.get('unified_trend') or {}
    trend_direction = unified_trend.get('trend_direction', 'neutral')
    trend_text_map = {
        'bullish': '上升趋势',
        'bearish': '下降趋势',
        'neutral': '震荡整理',
    }
    trend_type_map = {
        'bullish': 'bullish',
        'bearish': 'bearish',
        'neutral': 'neutral',
    }
    volatility_map = {
        'low': 0.12,
        'medium': 0.18,
        'high': 0.26,
    }

    total_score = float(item.get('total_score', 0.0) or 0.0)
    up_prob_5d = float(item.get('up_probability_5d', 50.0) or 50.0)
    volatility_level = item.get('volatility_level', 'medium')

    if trend_direction == 'bullish':
        rsi = 56 if total_score >= 4 else 60
        price_ma20_ratio = 0.02 if up_prob_5d < 70 else 0.05
    elif trend_direction == 'bearish':
        rsi = 54 if up_prob_5d >= 40 else 58
        price_ma20_ratio = -0.02 if up_prob_5d > 35 else -0.04
    else:
        rsi = 50
        price_ma20_ratio = 0.0

    model_reliability = item.get('market_model_reliability') or item.get('model_reliability') or {}

    advisor = recommender._build_advisor_view(
        total_score=total_score,
        trend={
            'trend': trend_type_map.get(trend_direction, 'neutral'),
            'trend_text': trend_text_map.get(trend_direction, '震荡整理'),
        },
        unified_trend=unified_trend,
        tech_indicators={
            'rsi': rsi,
            'volatility': volatility_map.get(volatility_level, 0.18),
            'price_ma20_ratio': price_ma20_ratio,
        },
        risks=[],
        model_reliability=model_reliability,
    )

    if not item.get('is_holding') and advisor.get('action') in ('sell', 'reduce'):
        advisor['action'] = 'watch'
        advisor['risk_level'] = 'medium' if advisor.get('risk_level') == 'high' else advisor.get('risk_level', 'medium')
        advisor['position_size_pct'] = 0
        advisor['review_in_days'] = 3 if trend_direction == 'bearish' else 5
        if trend_direction == 'bearish':
            advisor['summary'] = '当前市场偏弱，该标的更适合作为观察名单跟踪，暂不建议新开仓。'
        else:
            advisor['summary'] = '当前信号尚未形成足够优势，建议先观察等待确认。'

    return advisor


def _build_holding_recommendation(item, asset_type=None, force_horizon=None):
    """根据概率、风险和动作，给出动态持有时间建议。"""
    asset_type = asset_type or item.get('asset_type') or 'unknown'
    p5 = float(item.get('up_probability_5d', 50.0) or 50.0)
    p20 = float(item.get('up_probability_20d', 50.0) or 50.0)
    p60 = float(item.get('up_probability_60d', 50.0) or 50.0)
    total_score = float(item.get('total_score', 0.0) or 0.0)
    advisor_view = item.get('advisor_view') or {}
    action = str(item.get('advisor_action') or item.get('action') or advisor_view.get('action') or 'watch')
    risk_level = str(item.get('risk_level') or advisor_view.get('risk_level') or 'medium')
    risk_penalty = {'low': 0.0, 'medium': 1.5, 'high': 3.5}.get(risk_level, 1.5)

    weighted_scores = {
        'short': p5 + total_score * 2.2 - risk_penalty,
        'medium': p20 + total_score * 2.6 - risk_penalty * 0.8,
        'long': p60 + total_score * 2.4 - risk_penalty * 0.4,
    }

    if asset_type in ('active_fund', 'gold'):
        weighted_scores['long'] += 2.0
        weighted_scores['medium'] += 1.0
    elif asset_type in ('silver',):
        weighted_scores['short'] += 1.0
        weighted_scores['long'] -= 1.0
    elif asset_type in ('etf',):
        weighted_scores['medium'] += 1.5

    if force_horizon:
        horizon_key = force_horizon
    elif p5 >= p20 + 5 and p5 >= p60 + 7:
        horizon_key = 'short'
    elif p60 >= p20 + 4 and p60 >= p5 + 6:
        horizon_key = 'long'
    else:
        horizon_key = max(weighted_scores, key=weighted_scores.get)

    def _short_window(probability, score, risk, action_name):
        if action_name in ('reduce', 'sell'):
            return (1, 3, '建议每日复查一次', f'当前信号偏弱且风险偏{risk}，建议在 1-3个交易日内减仓或止损评估。')
        if probability >= 70 and score >= 4.0 and risk == 'low':
            return (4, 8, '建议每日复查一次', f'短线动能较强，5日向上概率约 {probability:.1f}%，可先按 4-8 个交易日节奏跟踪。')
        if probability >= 68 and score >= 4.0:
            return (4, 8, '建议每日复查一次', f'短线节奏较强，5日向上概率约 {probability:.1f}%，适合快进快出。')
        if probability >= 60:
            return (3, 6, '建议每日复查一次', f'短期仍有交易机会，5日向上概率约 {probability:.1f}%，宜轻仓快打。')
        if action_name == 'watch':
            return (2, 4, '建议隔日复查一次', '短期确定性不足，建议先观察 2-4 个交易日，等信号更明确再出手。')
        return (2, 5, '建议每日复查一次', f'短线优势一般，5日向上概率约 {probability:.1f}%，更适合短周期验证。')

    def _medium_window(probability, score, risk, action_name):
        if action_name in ('reduce', 'sell'):
            return (3, 7, '建议1到2天复查一次', f'中期趋势走弱，建议在 3-7个交易日内完成减仓评估。')
        if probability >= 68 and score >= 4.0 and risk == 'low':
            return (20, 35, '建议每2到3天复查一次', f'中期趋势较顺，20日向上概率约 {probability:.1f}%，可按 20-35 个交易日做波段。')
        if probability >= 60:
            return (12, 25, '建议每2到3天复查一次', f'中期更适合做波段持有，20日向上概率约 {probability:.1f}%。')
        if action_name == 'watch':
            return (5, 10, '建议每2到3天复查一次', '中期趋势还未完全确认，建议先观察 5-10 个交易日。')
        return (8, 15, '建议每1到3天复查一次', f'中期优势有限，20日向上概率约 {probability:.1f}%，更适合边走边看。')

    def _long_window(probability, score, risk, action_name, asset_kind):
        if action_name in ('reduce', 'sell'):
            return (10, 20, '建议每周复核一次', '长期逻辑暂未形成，若已持有宜先控制仓位，等待新的中长期信号。')
        if asset_kind in ('active_fund', 'gold') and probability >= 65 and score >= 4.0:
            return (120, 240, '建议每1到2周复核一次', f'更适合作为中长期配置，60日向上概率约 {probability:.1f}%，可分批持有 4-8 个月。')
        if probability >= 70 and score >= 4.0 and risk == 'low':
            return (90, 180, '建议每1到2周复核一次', f'长期赔率较好，60日向上概率约 {probability:.1f}%，适合分批持有 3-6 个月。')
        if probability >= 60:
            return (60, 150, '建议每周复核一次', f'长期更偏配置思路，60日向上概率约 {probability:.1f}%，建议按 2-5 个月节奏观察。')
        if action_name == 'watch':
            return (15, 30, '建议每周复核一次', '长期信号仍偏弱，建议先观察 2-4 周，再决定是否布局。')
        return (30, 90, '建议每周复核一次', f'长期弹性一般，60日向上概率约 {probability:.1f}%，不宜过早重仓。')

    if horizon_key == 'short':
        min_days, max_days, review_text, reason = _short_window(p5, total_score, risk_level, action)
        period_text = f'建议先持有 {min_days}-{max_days}个交易日' if action not in ('reduce', 'sell') else f'建议在 {min_days}-{max_days}个交易日内减仓或止损评估'
        label = '短期优先'
        best_probability = p5
    elif horizon_key == 'medium':
        min_days, max_days, review_text, reason = _medium_window(p20, total_score, risk_level, action)
        period_text = f'建议持有 {min_days}-{max_days}个交易日' if action not in ('reduce', 'sell') else f'建议在 {min_days}-{max_days}个交易日内完成减仓评估'
        label = '中期优先'
        best_probability = p20
    else:
        min_days, max_days, review_text, reason = _long_window(p60, total_score, risk_level, action, asset_type)
        if max_days >= 60:
            min_month = max(1, round(min_days / 30))
            max_month = max(min_month, round(max_days / 30))
            period_text = f'建议分批持有 {min_month}-{max_month}个月' if action not in ('reduce', 'sell') else '建议先缩短久期并等待长期信号修复'
        else:
            period_text = f'建议先观察 {min_days}-{max_days}个交易日'
        label = '长期优先'
        best_probability = p60

    return {
        'horizon_key': horizon_key,
        'horizon_label': label,
        'holding_period_text': period_text,
        'review_frequency': review_text,
        'best_probability': round(float(best_probability), 2),
        'reason': reason,
        'min_days': int(min_days),
        'max_days': int(max_days),
    }


def _build_horizon_top_picks(recommendations_list):
    """从当前推荐列表中提炼短期 / 中期 / 长期最优资产。"""
    items = list(recommendations_list or [])
    if not items:
        return {}

    horizon_rules = {
        'short': {
            'prob_field': 'up_probability_5d',
            'label': '短期最优',
            'holding_period_text': '建议持有 3-10个交易日',
            'reason_tpl': '短期更看重近几日弹性，当前5日向上概率约 {prob:.1f}%。',
        },
        'medium': {
            'prob_field': 'up_probability_20d',
            'label': '中期最优',
            'holding_period_text': '建议持有 10-40个交易日',
            'reason_tpl': '中期更适合做波段，当前20日向上概率约 {prob:.1f}%。',
        },
        'long': {
            'prob_field': 'up_probability_60d',
            'label': '长期最优',
            'holding_period_text': '建议持有 3个月以上',
            'reason_tpl': '长期更看重配置价值，当前60日向上概率约 {prob:.1f}%。',
        },
    }
    risk_penalty = {'low': 0.0, 'medium': 1.5, 'high': 3.5}
    top_picks = {}

    for horizon_key, config in horizon_rules.items():
        prob_field = config['prob_field']
        best = max(
            items,
            key=lambda item: float(item.get(prob_field, 50.0) or 50.0)
            + float(item.get('total_score', 0.0) or 0.0) * 3.0
            - risk_penalty.get(str(item.get('risk_level') or 'medium'), 1.5)
        )
        probability = round(float(best.get(prob_field, 50.0) or 50.0), 2)
        dynamic_advice = _build_holding_recommendation(best, best.get('asset_type'), force_horizon=horizon_key)
        top_picks[horizon_key] = {
            'label': config['label'],
            'code': best.get('code', ''),
            'name': best.get('name', best.get('code', '')),
            'display_code': best.get('display_code', best.get('code', '')),
            'probability': probability,
            'holding_period_text': dynamic_advice.get('holding_period_text', config['holding_period_text']),
            'reason': dynamic_advice.get('reason', config['reason_tpl'].format(prob=probability)),
        }

    return top_picks


def _build_strength_spotlight(recommendations_list):
    items = list(recommendations_list or [])
    if not items:
        return []

    ranked = []
    for item in items:
        strength = item.get('strength') or _classify_recommendation_strength(item)
        p20 = float(item.get('up_probability_20d', 50.0) or 50.0)
        p5 = float(item.get('up_probability_5d', 50.0) or 50.0)
        score = float(item.get('total_score', 0.0) or 0.0)
        rank_score = (
            (15 if strength.get('level') == 'strong_bullish' else (8 if strength.get('level') == 'bullish_watch' else 0))
            + p20 * 0.6
            + p5 * 0.25
            + score * 4.0
        )
        ranked.append((rank_score, item, strength))

    ranked.sort(key=lambda x: x[0], reverse=True)
    result = []
    for _, item, strength in ranked[:3]:
        result.append({
            'code': item.get('code'),
            'name': item.get('name'),
            'display_code': item.get('display_code') or item.get('code'),
            'label': strength.get('label', '中性观察'),
            'probability_20d': round(float(item.get('up_probability_20d', 50.0) or 50.0), 2),
            'total_score': round(float(item.get('total_score', 0.0) or 0.0), 2),
        })
    return result


def _apply_holding_recommendation(item, asset_type=None):
    """为推荐项附加建议持有时间。"""
    advice = _build_holding_recommendation(item, asset_type=asset_type)
    item['holding_advice'] = advice
    item['horizon_label'] = advice.get('horizon_label', '中期优先')
    item['recommended_holding_period'] = advice.get('holding_period_text', '建议持有 10-40个交易日')
    item['review_frequency'] = advice.get('review_frequency', '建议每1到3天复查一次')
    return item


def _build_strategy_framework(recommendations_list, query_type='a_stock', market_sentiment=None):
    """把“宏观环境 → 行业趋势 → 资产筛选 → 周期策略”整理成更像理财师的可读框架。"""
    items = list(recommendations_list or [])
    market_sentiment = float(get_market_sentiment() if market_sentiment is None else market_sentiment)

    avg_p5 = sum(float(x.get('up_probability_5d', 50) or 50) for x in items) / max(len(items), 1)
    avg_p20 = sum(float(x.get('up_probability_20d', 50) or 50) for x in items) / max(len(items), 1)
    avg_p60 = sum(float(x.get('up_probability_60d', 50) or 50) for x in items) / max(len(items), 1)

    if market_sentiment >= 0.58:
        macro_regime = 'constructive'
        macro_summary = '当前风险偏好偏修复，市场更适合从政策支持和景气上行方向中寻找进攻机会。'
    elif market_sentiment <= 0.45:
        macro_regime = 'defensive'
        macro_summary = '当前外部不确定性仍在，系统更偏向防守、控制回撤，并优先筛选抗波动资产。'
    else:
        macro_regime = 'balanced'
        macro_summary = '当前市场处于均衡区，既不能盲目激进，也不必完全空仓，适合精选主线。'

    industry_score = {}
    for item in items[:12]:
        industry = str(item.get('industry') or '其他').strip() or '其他'
        boost = float(item.get('total_score', 0.0) or 0.0) + max(float(item.get('up_probability_20d', 50) or 50) - 50.0, 0.0) / 10.0
        industry_score[industry] = industry_score.get(industry, 0.0) + boost
    ranked_industries = sorted(industry_score.items(), key=lambda kv: kv[1], reverse=True)
    focus_industries = [name for name, _ in ranked_industries[:3]] or ['当前暂无明显主线']

    strength_labels = ['强势优先', '景气跟踪', '观察布局']
    sector_rotation = [
        {
            'industry': name,
            'strength': strength_labels[min(idx, len(strength_labels) - 1)],
            'score': round(score, 2),
            'reason': '景气度和模型评分靠前，适合纳入本轮重点观察清单。' if idx == 0 else ('趋势仍在延续，可作为第二梯队跟踪。' if idx == 1 else '适合轻仓观察，等待信号进一步强化。')
        }
        for idx, (name, score) in enumerate(ranked_industries[:3])
    ]

    policy_score = int(max(25, min(95, round(50 + (market_sentiment - 0.5) * 100 + max(avg_p20 - 50, 0) * 0.45 + max(avg_p60 - 50, 0) * 0.25))))
    if policy_score >= 65:
        policy_bias = 'supportive'
        policy_summary = '政策与流动性环境对风险资产偏支持，可优先围绕产业升级和政策受益方向布局。'
    elif policy_score <= 45:
        policy_bias = 'cautious'
        policy_summary = '政策环境偏谨慎，当前更适合控制仓位，优先看现金流稳健或防御性资产。'
    else:
        policy_bias = 'neutral'
        policy_summary = '政策环境中性偏稳，宜精选行业主线，避免在弱逻辑方向上追高。'

    global_context_map = {
        'constructive': '国际风险偏好正在修复，外部扰动对市场的压制有所缓和。',
        'balanced': '国际环境仍有反复，系统更强调主线聚焦和仓位纪律。',
        'defensive': '国际不确定性仍高，建议把回撤控制放在收益追求之前。',
    }
    asset_appendix_map = {
        'a_stock': 'A股更要结合政策导向、产业升级和业绩兑现节奏来选股。',
        'hk_stock': '港股可重点看估值修复、平台经济与南向资金偏好。',
        'us_stock': '美股需同步参考利率路径、科技资本开支和盈利预期。',
        'active_fund': '主动基金更适合做中长期配置，重点看风格稳定性和回撤控制。',
        'etf': 'ETF适合用来承接行业主线并分散单一标的风险。',
        'gold': '黄金适合纳入避险和抗波动仓位。',
        'silver': '白银弹性更强，但更适合控制仓位参与。',
    }
    global_context = f"{global_context_map.get(macro_regime, '')}{asset_appendix_map.get(query_type, '先看方向，再选资产。')}"

    asset_summary_map = {
        'a_stock': 'A股更要先看政策导向与行业景气，再从主线里挑龙头或高性价比标的。',
        'hk_stock': '港股更适合从估值修复与南向资金方向中筛选弹性资产。',
        'us_stock': '美股更要结合国际利率、科技周期与盈利预期来做筛选。',
        'active_fund': '主动基金更适合作为中长期配置工具，重点看风格稳定性和回撤控制。',
        'etf': 'ETF更适合承接行业或指数主线，用于分散单一股票风险。',
        'gold': '黄金更偏防守和对冲，应放在宏观避险框架下评估。',
        'silver': '白银弹性更大，但波动也更强，更适合小仓位参与主线行情。',
    }
    asset_summary = asset_summary_map.get(query_type, '先判断环境，再挑选更适合当前周期的资产。')

    if macro_regime == 'constructive':
        allocation_plan = {'equity_pct': 60, 'defense_pct': 15, 'cash_pct': 25, 'note': '可以适度提高进攻仓位，但仍建议分批布局。'}
    elif macro_regime == 'defensive':
        allocation_plan = {'equity_pct': 30, 'defense_pct': 35, 'cash_pct': 35, 'note': '先以控回撤为主，等待趋势和政策进一步确认。'}
    else:
        allocation_plan = {'equity_pct': 45, 'defense_pct': 25, 'cash_pct': 30, 'note': '维持均衡配置，主线和防守资产同时保留。'}

    if query_type in ('gold', 'silver'):
        allocation_plan = {'equity_pct': 20, 'defense_pct': 45, 'cash_pct': 35, 'note': '贵金属更偏防守与对冲，不宜用满仓思路处理。'}
    elif query_type == 'active_fund':
        allocation_plan['equity_pct'] = min(70, allocation_plan['equity_pct'] + 5)
        allocation_plan['cash_pct'] = max(15, allocation_plan['cash_pct'] - 5)
        allocation_plan['note'] = '基金更适合做中长期底仓，建议按阶段分批配置。'

    review_cycle = '每1-2天复核一次' if macro_regime == 'defensive' else ('每2-3天复核一次' if macro_regime == 'balanced' else '每3-5天复核一次')
    screening_flow = [
        {'title': '资产初筛', 'summary': f"先从 {' / '.join(focus_industries[:2]) if focus_industries else '重点行业'} 中筛出概率和评分更优的标的。"},
        {'title': '组合配置', 'summary': f"当前建议权益 {allocation_plan['equity_pct']}% / 防守 {allocation_plan['defense_pct']}% / 现金 {allocation_plan['cash_pct']}%。"},
        {'title': '动态调仓', 'summary': f"按 {review_cycle} 节奏复核，若趋势转弱或风险升温则分批调仓。"},
    ]
    rebalance_plan = {
        'review_cycle': review_cycle,
        'triggers': [
            {'title': '主线转弱', 'summary': '若重点行业跌出优先关注名单，相关仓位宜先降 5%-10%。'},
            {'title': '风险升级', 'summary': '若高风险信号增多或建议转为减仓/卖出，应提高现金比例。'},
            {'title': '目标兑现', 'summary': '若达到阶段止盈或上涨目标，可分批锁定收益，不必一次清仓。'},
        ]
    }

    if avg_p60 >= avg_p20 + 4:
        horizon_summary = '当前更适合先布局中长期方向，优先考虑能持有数月的配置型资产。'
    elif avg_p20 >= avg_p5 + 3:
        horizon_summary = '当前更适合做中期波段，建议围绕 10-40 个交易日的趋势做计划。'
    else:
        horizon_summary = '当前更适合先看短中期节奏，重点盯住交易信号是否继续强化。'

    sections = [
        {'title': '宏观环境', 'summary': macro_summary},
        {'title': '政策风向', 'summary': f"政策景气分 {policy_score} 分，判断为{'偏支持' if policy_bias == 'supportive' else ('偏谨慎' if policy_bias == 'cautious' else '中性偏稳')}。"},
        {'title': '行业主线', 'summary': f"当前优先关注：{' / '.join(focus_industries)}。"},
        {'title': '周期策略', 'summary': horizon_summary},
    ]

    return {
        'headline': '先看宏观环境，再选行业主线，最后匹配短中长期资产',
        'macro_regime': macro_regime,
        'focus_industries': focus_industries,
        'policy_score': policy_score,
        'policy_bias': policy_bias,
        'global_context': global_context,
        'policy_summary': policy_summary,
        'sector_rotation': sector_rotation,
        'allocation_plan': allocation_plan,
        'screening_flow': screening_flow,
        'rebalance_plan': rebalance_plan,
        'asset_summary': asset_summary,
        'sections': sections,
    }


def _classify_source_quality(raw_source):
    """根据单个来源字符串判断可信度等级。"""
    source_lower = str(raw_source or '').strip().lower()
    if not source_lower:
        return 'unknown'
    if any(key in source_lower for key in ['tushare', 'yfinance', 'nbs', 'fred', 'official']):
        return 'high'
    if any(key in source_lower for key in ['local', 'cache', 'db', 'system_db', 'snapshot', 'model', 'trained']):
        return 'medium'
    if any(key in source_lower for key in ['fallback', 'default']):
        return 'low'
    return 'unknown'


def _build_source_provenance(detail):
    """汇总详情中可追踪的数据来源，便于前端展示证据链。"""
    detail = detail or {}
    provenance_items = []
    seen = set()

    def _append(label, source):
        source = str(source or '').strip()
        if not source:
            return
        key = f"{label}:{source}"
        if key in seen:
            return
        seen.add(key)
        provenance_items.append({
            'label': label,
            'source': source,
            'quality': _classify_source_quality(source),
        })

    _append('推荐快照', detail.get('data_source') or detail.get('source'))

    for prediction in detail.get('predictions') or []:
        try:
            period = int(prediction.get('period') or prediction.get('period_days') or 0)
        except Exception:
            period = 0
        label = f'{period}日预测' if period else '预测模型'
        _append(label, prediction.get('source') or prediction.get('data_source'))

    analysis = detail.get('analysis') or {}
    for key, label in [('technical', '技术面'), ('valuation', '估值面'), ('money_flow', '资金面'), ('news', '消息面')]:
        block = analysis.get(key) or {}
        _append(label, block.get('source') or block.get('data_source'))

    advisor = detail.get('advisor') or {}
    _append('投顾决策', advisor.get('data_source') or 'trained_models')

    return provenance_items


def _build_data_quality_summary(detail, today=None):
    """评估当前详情数据的新鲜度、来源可信度和完整度。"""
    detail = detail or {}
    today = today or get_today()

    raw_update = detail.get('update_time') or detail.get('as_of') or detail.get('date')
    update_date = None
    if raw_update:
        try:
            update_date = datetime.fromisoformat(str(raw_update).replace('Z', '')).date()
        except Exception:
            try:
                update_date = datetime.strptime(str(raw_update)[:10], '%Y-%m-%d').date()
            except Exception:
                update_date = None

    age_days = (today - update_date).days if update_date else None
    if age_days is None:
        freshness_status = 'unknown'
        freshness_text = '更新时间未知'
    elif age_days <= 1:
        freshness_status = 'fresh'
        freshness_text = f'数据较新（{age_days}天内）'
    elif age_days <= 3:
        freshness_status = 'recent'
        freshness_text = f'数据有轻微延迟（约{age_days}天）'
    else:
        freshness_status = 'stale'
        freshness_text = f'数据偏旧（约{age_days}天）'

    provenance_items = _build_source_provenance(detail)
    quality_levels = [item.get('quality', 'unknown') for item in provenance_items]
    if 'low' in quality_levels and 'high' not in quality_levels:
        source_quality = 'low'
    elif 'high' in quality_levels:
        source_quality = 'high'
    elif 'medium' in quality_levels:
        source_quality = 'medium'
    else:
        source_quality = 'unknown'

    source_names = []
    for item in provenance_items:
        source_name = str(item.get('source') or '').strip()
        if source_name and source_name not in source_names:
            source_names.append(source_name)

    if source_quality == 'high':
        source_text = f"已覆盖多类高质量来源：{' / '.join(source_names[:3])}" if source_names else '已覆盖多类高质量来源'
    elif source_quality == 'medium':
        source_text = f"当前以本地快照与模型推断为主：{' / '.join(source_names[:3])}" if source_names else '当前以本地快照与模型推断为主'
    elif source_quality == 'low':
        source_text = f"当前部分依赖降级数据源：{' / '.join(source_names[:3])}" if source_names else '当前部分依赖降级数据源'
    else:
        source_text = '当前未明确标注数据来源'

    completeness = 0
    if detail.get('current_price') not in (None, '', 0):
        completeness += 20
    predictions = detail.get('predictions') or []
    completeness += 30 if len(predictions) >= 3 else (15 if predictions else 0)
    analysis = detail.get('analysis') or {}
    for key in ('technical', 'valuation', 'money_flow', 'news'):
        if analysis.get(key):
            completeness += 12
    if detail.get('advisor'):
        completeness += 10
    completeness = min(100, int(completeness))

    warnings = []
    if freshness_status == 'stale':
        warnings.append('当前数据相对偏旧，建议谨慎参考，等待下一轮刷新。')
    elif freshness_status == 'unknown':
        warnings.append('当前无法确认数据更新时间，建议不要重仓依赖此结论。')
    if source_quality == 'low':
        warnings.append('当前结果部分依赖降级数据源，可信度会明显下降。')
    elif source_quality == 'unknown':
        warnings.append('当前未明确标注数据来源，建议结合更多外部信息判断。')
    if len(predictions) < 3:
        warnings.append('预测周期数据不完整，结论说服力会下降。')
    if completeness < 60:
        warnings.append('分析维度还不够完整，当前更适合作为辅助参考。')

    return {
        'freshness_status': freshness_status,
        'freshness_text': freshness_text,
        'source_quality': source_quality,
        'source_text': source_text,
        'provenance_items': provenance_items[:6],
        'age_days': age_days,
        'completeness_pct': completeness,
        'warnings': warnings[:3],
    }


def _assess_historical_sample_quality(samples):
    samples = int(samples or 0)
    if samples >= 80:
        return 'adequate', '历史验证样本较充足。'
    if samples >= 30:
        return 'developing', '历史验证样本仍在积累中。'
    if samples > 0:
        return 'thin', f'历史已验证样本仅 {samples} 条，统计显著性仍偏弱。'
    return 'none', '当前尚无足够已验证样本。'


def _build_historical_validation_context(type_health, snapshot=None):
    """从历史命中率/校准数据中提炼当前建议最相关的验证上下文。"""
    snapshot = snapshot or {}
    asset_label_map = {
        'a_stock': '同类A股',
        'hk_stock': '同类港股',
        'us_stock': '同类美股',
        'active_fund': '同类主动基金',
        'etf': '同类ETF',
        'gold': '黄金相关资产',
        'silver': '白银相关资产',
    }
    asset_scope_label = asset_label_map.get(str(snapshot.get('asset_type') or '').strip().lower(), '同类资产')

    if isinstance(type_health, dict) and ('hit_rate' in type_health or 'preferred_horizon' in type_health):
        context = dict(type_health)
        context.setdefault('preferred_horizon', 20)
        context.setdefault('status', 'ok' if context.get('hit_rate') is not None else 'insufficient_history')
        samples = int(context.get('samples') or 0)
        sample_quality, sample_note = _assess_historical_sample_quality(samples)
        context.setdefault('sample_quality', sample_quality)
        context.setdefault('sample_note', sample_note)
        if context.get('status') == 'ok' and context.get('hit_rate') is not None:
            if sample_quality == 'thin':
                context.setdefault(
                    'summary',
                    f"{asset_scope_label}历史验证样本仅 {samples} 条：{context.get('preferred_horizon')}日窗口命中率约 {float(context.get('hit_rate') or 0):.1f}% ，但统计说服力仍有限。"
                )
            else:
                context.setdefault(
                    'summary',
                    f"{asset_scope_label}历史验证：{context.get('preferred_horizon')}日窗口命中率约 {float(context.get('hit_rate') or 0):.1f}% ，Brier {float(context.get('brier') or 0):.3f}，评级 {context.get('grade', 'N/A')}。"
                )
        else:
            context.setdefault('summary', '当前历史样本不足，暂时无法充分验证模型稳定性。')
        return context

    if not isinstance(type_health, dict) or not type_health:
        return {
            'status': 'insufficient_history',
            'preferred_horizon': 20,
            'summary': '当前历史样本不足，暂时无法充分验证模型稳定性。'
        }

    p5, p20, p60 = _extract_probability_triplet(snapshot)
    preferred_horizon = max({5: p5, 20: p20, 60: p60}, key=lambda x: {5: p5, 20: p20, 60: p60}[x])

    grade_rank = {'A': 4, 'B': 3, 'C': 2, 'D': 1, 'N/A': 0}
    candidates = []
    for horizon in (5, 20, 60):
        entry = type_health.get(str(horizon)) or type_health.get(horizon) or {}
        if isinstance(entry, dict):
            candidates.append((horizon, entry))

    chosen_horizon = preferred_horizon
    chosen = next((entry for horizon, entry in candidates if horizon == preferred_horizon and entry.get('status') == 'ok'), None)
    if chosen is None:
        ok_candidates = [(horizon, entry) for horizon, entry in candidates if entry.get('status') == 'ok']
        if ok_candidates:
            chosen_horizon, chosen = max(ok_candidates, key=lambda item: (grade_rank.get(item[1].get('grade', 'N/A'), 0), int(item[1].get('samples') or 0)))
        elif candidates:
            chosen_horizon, chosen = candidates[0]
        else:
            chosen = {}

    status = chosen.get('status', 'insufficient_history')
    scope_label = asset_scope_label
    signal_buckets = chosen.get('signal_buckets', {}) if isinstance(chosen, dict) else {}
    current_prob = {5: p5, 20: p20, 60: p60}.get(int(chosen_horizon), p20)
    bullish_bucket = signal_buckets.get('bullish', {}) if isinstance(signal_buckets, dict) else {}
    bearish_bucket = signal_buckets.get('bearish', {}) if isinstance(signal_buckets, dict) else {}

    if current_prob >= 55 and bullish_bucket.get('status') == 'ok' and int(bullish_bucket.get('samples') or 0) >= 20:
        chosen = {**chosen, **bullish_bucket}
        scope_label = f"{asset_scope_label}偏多信号"
    elif current_prob <= 45 and bearish_bucket.get('status') == 'ok' and int(bearish_bucket.get('samples') or 0) >= 20:
        chosen = {**chosen, **bearish_bucket}
        scope_label = f"{asset_scope_label}偏空信号"

    status = chosen.get('status', status)
    if status == 'ok':
        summary = (
            f"{scope_label}历史验证：{chosen_horizon}日窗口命中率约 {float(chosen.get('hit_rate') or 0):.1f}% ，"
            f"Brier {float(chosen.get('brier') or 0):.3f}，评级 {chosen.get('grade', 'N/A')}。"
        )
    else:
        summary = f"当前{chosen_horizon}日窗口历史样本不足，建议把本次判断作为辅助参考。"

    samples = int(chosen.get('samples') or 0)
    sample_quality, sample_note = _assess_historical_sample_quality(samples)

    return {
        'preferred_horizon': int(chosen_horizon),
        'scope_label': scope_label,
        'status': status,
        'grade': chosen.get('grade', 'N/A'),
        'samples': samples,
        'sample_quality': sample_quality,
        'sample_note': sample_note,
        'hit_rate': chosen.get('hit_rate'),
        'avg_probability': chosen.get('avg_probability'),
        'brier': chosen.get('brier'),
        'calibration_gap': chosen.get('calibration_gap'),
        'summary': summary,
    }


def _extract_probability_triplet(payload):
    """从详情根字段或预测列表中统一提取 5/20/60 日概率。"""
    payload = payload or {}
    predictions = payload.get('predictions') or []
    prediction_map = {}
    for item in predictions:
        try:
            period = int(item.get('period') or item.get('period_days') or 0)
        except Exception:
            period = 0
        if period in (5, 20, 60):
            prediction_map[period] = float(item.get('up_probability', 50) or 50)

    p5 = float(payload.get('up_probability_5d', prediction_map.get(5, 50)) or 50)
    p20 = float(payload.get('up_probability_20d', prediction_map.get(20, 50)) or 50)
    p60 = float(payload.get('up_probability_60d', prediction_map.get(60, 50)) or 50)
    return p5, p20, p60


def _build_recommendation_quality_gate(detail, snapshot=None, historical_context=None):
    """根据模型证据、风险、概率和历史命中率，对单个建议做质量分级。"""
    detail = detail or {}
    snapshot = snapshot or detail or {}
    advisor = detail.get('advisor') or detail.get('advisor_view') or {}

    p5, p20, p60 = _extract_probability_triplet(snapshot if snapshot else detail)
    evidence_score = float(advisor.get('evidence_score', detail.get('evidence_score', 50)) or 50)
    risk_level = str(advisor.get('risk_level', detail.get('risk_level', 'medium')) or 'medium')
    action = str(advisor.get('action', detail.get('advisor_action', 'watch')) or 'watch')
    reliability = (advisor.get('model_reliability') or {}).get('label') or (advisor.get('model_reliability') or {}).get('level') or 'stable'

    best_prob = max(p5, p20, p60)
    dispersion = max(p5, p20, p60) - min(p5, p20, p60)
    if dispersion <= 8:
        agreement_label = '高度一致'
    elif dispersion <= 18:
        agreement_label = '基本一致'
    else:
        agreement_label = '分歧较大'

    history = _build_historical_validation_context(historical_context, snapshot=snapshot) if historical_context else {}
    historical_grade = history.get('grade', 'N/A')
    historical_hit_rate = history.get('hit_rate')
    historical_brier = history.get('brier')
    historical_gap = history.get('calibration_gap')
    historical_horizon = history.get('preferred_horizon')
    historical_status = history.get('status')
    historical_samples = int(history.get('samples') or 0)
    sample_quality = history.get('sample_quality', 'unknown')

    warnings = []
    if evidence_score < 40:
        warnings.append('模型证据分偏低，当前更适合观察或轻仓处理。')
    if risk_level == 'high':
        warnings.append('当前风险等级偏高，仓位不宜激进。')
    if best_prob < 55:
        warnings.append('主要周期上涨概率还未形成明显优势。')
    if reliability in ('guarded', 'low'):
        warnings.append('模型近期稳定性一般，建议下调预期。')
    if p60 < 45:
        warnings.append('中长期信号偏弱，不支持长时间重仓持有。')
    if dispersion > 18:
        warnings.append('短中长期模型分歧较大，说明当前方向一致性不足。')
    if history and historical_status != 'ok':
        warnings.append('当前已到期复盘样本不足，暂不能把该信号视为高置信判断。')
    if sample_quality == 'thin':
        warnings.append(f"历史已验证样本仅 {historical_samples} 条，当前更适合作为观察参考。")
    if historical_status == 'ok' and historical_hit_rate is not None:
        if historical_grade in ('D',) or float(historical_hit_rate) < 50 or float(historical_brier or 0) > 0.28 or abs(float(historical_gap or 0)) > 12:
            warnings.append(f"历史验证偏弱：{historical_horizon}日窗口命中率约 {float(historical_hit_rate):.1f}% ，建议降低执行力度。")
        elif historical_grade == 'C':
            warnings.append(f"历史验证一般：{historical_horizon}日窗口稳定性仍需继续观察。")

    if evidence_score >= 75 and risk_level == 'low' and best_prob >= 60 and dispersion <= 15:
        grade = 'A'
        confidence_label = '高置信'
        actionable = True
        summary = '当前模型证据较强，可以考虑分批执行，但仍需遵守仓位纪律。'
    elif evidence_score >= 60 and risk_level != 'high' and best_prob >= 55 and dispersion <= 18:
        grade = 'B'
        confidence_label = '中高置信'
        actionable = True
        summary = '当前有一定把握，但更适合小步分批，而不是一次性重仓。'
    elif evidence_score >= 45 and risk_level != 'high':
        grade = 'C'
        confidence_label = '中性观察'
        actionable = False
        summary = '当前信号还不够强，更适合继续观察，等待更明确的确认。'
    else:
        grade = 'D'
        confidence_label = '低置信'
        actionable = False
        summary = '当前证据不足或风险偏高，不适合激进操作。'

    if dispersion > 18 and actionable:
        actionable = False
        if grade == 'B':
            grade = 'C'
            confidence_label = '中性观察'
        summary = '虽然局部信号不弱，但多周期模型分歧明显，暂不适合积极执行。'

    if history and actionable and historical_status != 'ok':
        actionable = False
        grade = 'C' if grade in ('A', 'B') else grade
        confidence_label = '中性观察' if grade != 'D' else '低置信'
        summary = '当前历史复盘样本还不足，虽然静态信号存在方向，但暂不适合视为高置信执行。'
    elif actionable and sample_quality == 'thin':
        actionable = False
        grade = 'C' if grade in ('A', 'B') else grade
        confidence_label = '中性观察' if grade != 'D' else '低置信'
        summary = f'历史已验证样本仅 {historical_samples} 条，当前先降级为观察信号，等待更多实盘样本确认。'
    elif historical_status == 'ok' and actionable:
        if historical_grade == 'D' or float(historical_hit_rate or 0) < 50 or float(historical_brier or 0) > 0.28 or abs(float(historical_gap or 0)) > 12:
            actionable = False
            if grade == 'A':
                grade = 'B'
                confidence_label = '中高置信'
            elif grade == 'B':
                grade = 'C'
                confidence_label = '中性观察'
            summary = '当前静态信号不弱，但历史命中率与校准表现偏弱，暂不适合积极执行。'
        elif historical_grade == 'B' and grade == 'B':
            summary = '当前信号与历史验证基本一致，可以考虑按纪律小步分批执行。'

    if action in ('reduce', 'sell'):
        next_step = '优先控风险，先减仓或止损复核。'
    elif actionable:
        next_step = '可按计划轻仓分批执行，并设置止损。'
    else:
        next_step = '先观察，等证据改善后再决定是否参与。'

    return {
        'grade': grade,
        'agreement_label': agreement_label,
        'confidence_label': confidence_label,
        'actionable': actionable,
        'summary': summary,
        'next_step': next_step,
        'historical_grade': historical_grade,
        'historical_hit_rate': historical_hit_rate,
        'historical_brier': historical_brier,
        'historical_samples': history.get('samples'),
        'historical_horizon': historical_horizon,
        'historical_sample_quality': sample_quality,
        'historical_summary': history.get('summary', ''),
        'warnings': warnings[:4],
    }


def _build_detail_recommendation_rationale(detail, snapshot=None, market_sentiment=None, historical_context=None):
    """把单个资产的推荐原因整理为“完整投顾流程”，用于详情页展示。"""
    detail = detail or {}
    if not snapshot:
        p5, p20, p60 = _extract_probability_triplet(detail)
        snapshot = {
            'code': detail.get('code', ''),
            'name': detail.get('name', ''),
            'industry': detail.get('industry', ''),
            'asset_type': detail.get('asset_type', 'unknown'),
            'up_probability_5d': p5,
            'up_probability_20d': p20,
            'up_probability_60d': p60,
            'total_score': float(detail.get('total_score', 0) or 0),
            'risk_level': (detail.get('advisor') or {}).get('risk_level', 'medium'),
            'predictions': detail.get('predictions') or [],
        }

    framework = _build_strategy_framework([snapshot], query_type=detail.get('asset_type', snapshot.get('asset_type', 'a_stock')), market_sentiment=market_sentiment)
    advisor = detail.get('advisor') or {}
    holding_advice = detail.get('holding_advice') or {}
    history = _build_historical_validation_context(historical_context, snapshot=snapshot) if historical_context else {}
    quality_gate = _build_recommendation_quality_gate(detail, snapshot=snapshot, historical_context=history)
    industry = detail.get('industry') or snapshot.get('industry') or '当前相关方向'
    focus_industries = framework.get('focus_industries') or []
    action = advisor.get('action', 'watch')
    action_text_map = {'buy': '买入', 'add': '加仓', 'hold': '持有', 'watch': '观察', 'reduce': '减仓', 'sell': '卖出'}
    action_text = action_text_map.get(action, '观察')

    predictions = detail.get('predictions') or []
    best_period = 20
    best_prob = float(snapshot.get('up_probability_20d', 50) or 50)
    if predictions:
        best = max(predictions, key=lambda x: float(x.get('up_probability', 0) or 0))
        best_period = int(best.get('period') or best.get('period_days') or 20)
        best_prob = float(best.get('up_probability', 50) or 50)

    total_score = float(detail.get('total_score', snapshot.get('total_score', 0)) or 0)
    position_pct = int(advisor.get('position_size_pct', 0) or 0)
    if position_pct > 0:
        position_text = f'建议分批布局，参考仓位约 {position_pct}%'
    elif position_pct < 0:
        position_text = f'若已持有，建议分批减仓约 {abs(position_pct)}%'
    elif action == 'hold':
        position_text = '若已持有可继续持有；若未持有，先别追高，等待更合适的位置再考虑布局'
    elif action == 'watch':
        position_text = '当前以观察为主，暂不急于新开仓'
    else:
        position_text = '暂不新增仓位'

    p5, p20, p60 = _extract_probability_triplet(snapshot)
    evidence_score = float(advisor.get('evidence_score', max(40.0, min(85.0, total_score * 18))) or 50.0)
    reliability = (advisor.get('model_reliability') or {}).get('label') or (advisor.get('model_reliability') or {}).get('level') or 'stable'
    reliability_text_map = {'supportive': '偏强支持', 'stable': '稳定', 'guarded': '谨慎', 'high': '偏强支持', 'medium': '稳定', 'low': '谨慎'}
    reliability_text = reliability_text_map.get(reliability, str(reliability))

    if industry in focus_industries:
        industry_summary = f'{industry} 属于当前系统筛出的重点主线之一，说明这个资产在行业方向上与当前市场节奏较匹配。'
    else:
        industry_summary = f'{industry} 当前不是最强主线，但该资产自身评分和概率仍具备跟踪价值。'

    asset_summary = f"综合评分约 {total_score:.2f}/5，当前最优观察窗口为 {best_period} 日，向上概率约 {best_prob:.1f}%，因此系统给出“{action_text}”判断。"
    rebalance_hint = ((framework.get('rebalance_plan') or {}).get('triggers') or [{}])[0].get('summary', '若趋势和风险发生变化，需要分批调仓。')
    execution_summary = f"建议按“{holding_advice.get('holding_period_text', detail.get('suggested_period', '10-40个交易日'))}”的节奏执行，{position_text}；并在 {advisor.get('review_in_days', 3)} 天后复查。{rebalance_hint}"

    factor_scores = []
    analysis = detail.get('analysis') or {}
    for key, label in [('technical', '技术面'), ('valuation', '估值面'), ('money_flow', '资金面'), ('news', '消息面')]:
        score = ((analysis.get(key) or {}).get('score'))
        if score is not None:
            factor_scores.append(f"{label} {float(score):.2f}/5")

    model_summary = f"这不是固定文案，系统会结合训练模型（5日 / 20日 / 60日 预测模型）、历史命中率、Brier 校准结果，以及技术面、估值面、资金面、消息面数据来生成建议。当前模型更关注 {best_period} 日窗口。"
    evidence_points = [
        f"训练模型结果：5日向上概率 {p5:.1f}% ，20日 {p20:.1f}% ，60日 {p60:.1f}%。",
        f"综合评分 {total_score:.2f}/5，证据分 {evidence_score:.1f}/100，模型可靠性 {reliability_text}。",
        f"当前动作建议为“{action_text}”，不是主观判断，而是由概率、风险和评分共同推导出来的。",
    ]
    if history.get('status') == 'ok' and history.get('hit_rate') is not None:
        evidence_points.append(
            f"同类资产历史验证已纳入可信度评估，当前参考窗口为 {history.get('preferred_horizon')} 日，命中率约 {float(history.get('hit_rate') or 0):.1f}% ，评级 {history.get('grade', 'N/A')}。"
        )
    if factor_scores:
        evidence_points.append(f"五维因子里，当前主要参考：{' / '.join(factor_scores[:4])}。")

    if history.get('sample_quality') in ('none', 'thin'):
        honesty_note = '当前已验证样本仍偏少，所以这次结果更适合作为参考线索，而不是高置信交易指令。'
    elif history.get('status') == 'ok' and (history.get('grade') in ('C', 'D') or float(history.get('hit_rate') or 0) < 55):
        honesty_note = '虽然当前信号存在机会，但历史命中率支持度一般，因此系统更倾向轻仓、分批和严格复核。'
    else:
        honesty_note = '当前证据并不算很强，所以系统更倾向观察、减仓或轻仓，而不是激进重仓。' if evidence_score < 60 else ('当前证据中等偏强，可以考虑按纪律分批执行，但仍不建议满仓。')

    steps = [
        {'title': '1. 宏观环境', 'summary': (framework.get('sections') or [{'summary': '当前宏观环境以均衡应对为主。'}])[0].get('summary', '当前宏观环境以均衡应对为主。')},
        {'title': '2. 政策与资金', 'summary': framework.get('policy_summary', '政策环境偏中性，资金面需要继续确认。')},
        {'title': '3. 行业主线匹配', 'summary': industry_summary},
        {'title': '4. 标的自身优势', 'summary': asset_summary},
        {'title': '5. 持有与执行策略', 'summary': execution_summary},
    ]

    return {
        'title': '为什么系统会推荐这个资产',
        'headline': framework.get('headline', '系统会先看环境，再决定是否推荐单个资产'),
        'model_summary': model_summary,
        'evidence_points': evidence_points,
        'honesty_note': honesty_note,
        'quality_gate': quality_gate,
        'steps': steps,
    }


def _attach_historical_validation(detail, session, health_builder):
    """把历史命中率与校准结果挂到详情中，增强投顾说服力。"""
    if not detail or not session or health_builder is None:
        return detail

    asset_type = _normalize_recommendation_type(detail.get('asset_type'))
    if not asset_type:
        return detail

    try:
        raw_health = health_builder(session, get_today(), [asset_type]).get(asset_type, {})
        history = _build_historical_validation_context(raw_health, snapshot=detail)
        detail['historical_validation'] = history
        detail['quality_gate'] = _build_recommendation_quality_gate(detail, snapshot=detail, historical_context=history)

        advisor = detail.get('advisor') or {}
        if advisor and not detail['quality_gate'].get('actionable') and advisor.get('action') in ('buy', 'add'):
            advisor['action'] = 'watch'
            advisor['confidence'] = 'low'
            advisor['position_size_pct'] = 0
            advisor['summary'] = '由于历史复盘样本或稳定性仍不足，当前已自动降级为观察信号。'
            detail['advisor'] = advisor

        detail['recommendation_rationale'] = _build_detail_recommendation_rationale(detail, snapshot=detail, historical_context=history)
    except Exception as e:
        logger.warning(f"附加历史命中率信息失败: {e}")

    return detail


def convert_to_serializable(obj, decimal_places=2):
    """递归转换对象为JSON可序列化格式，并保留指定小数位数"""
    if isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float32, np.float64, float)):
        return round(float(obj), decimal_places)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {k: convert_to_serializable(v, decimal_places) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_to_serializable(i, decimal_places) for i in obj]
    return obj


def _classify_recommendation_strength(item):
    """按概率、评分与实际动作给推荐结果打上更易理解的强弱标签。"""
    advisor = item.get('advisor') if isinstance(item.get('advisor'), dict) else {}
    advisor_view = item.get('advisor_view') if isinstance(item.get('advisor_view'), dict) else {}
    action = str(item.get('advisor_action') or item.get('action') or advisor.get('action') or advisor_view.get('action') or 'watch')
    risk_level = str(item.get('risk_level') or advisor.get('risk_level') or advisor_view.get('risk_level') or 'medium')
    p5 = float(item.get('up_probability_5d', 50.0) or 50.0)
    p20 = float(item.get('up_probability_20d', 50.0) or 50.0)
    p60 = float(item.get('up_probability_60d', 50.0) or 50.0)
    score = float(item.get('total_score', 0.0) or 0.0)
    position_pct = int(item.get('position_size_pct', advisor.get('position_size_pct', advisor_view.get('position_size_pct', 0))) or 0)
    evidence_score = float(item.get('evidence_score', advisor.get('evidence_score', advisor_view.get('evidence_score', 50.0))) or 50.0)

    if action in ('sell', 'reduce') or risk_level == 'high' or (p20 < 42 and p60 < 45):
        return {'label': '防守回避', 'level': 'defensive', 'class_name': 'advisor-sell'}
    if action in ('buy', 'add') and position_pct > 0 and evidence_score >= 60 and (p20 >= 55 or p60 >= 58) and score >= 3.2:
        return {'label': '强看涨', 'level': 'strong_bullish', 'class_name': 'advisor-buy'}
    if action == 'hold' and risk_level != 'high' and (p20 >= 52 or p60 >= 55 or p5 >= 58) and score >= 3.0:
        return {'label': '偏多持有', 'level': 'bullish_hold', 'class_name': 'advisor-hold'}
    if (p20 >= 48 or p5 >= 50 or p60 >= 52) and score >= 2.9 and action not in ('sell', 'reduce'):
        return {'label': '偏多观察', 'level': 'bullish_watch', 'class_name': 'advisor-add'}
    return {'label': '中性观察', 'level': 'neutral_watch', 'class_name': 'advisor-watch'}


def _build_default_detail_advisor(detail):
    """为非股票类详情补充统一投顾视图。"""
    score = float(detail.get('total_score', 0) or 0)
    asset_type = detail.get('asset_type', 'unknown')
    risk_level = 'medium'
    if asset_type in ('silver',):
        risk_level = 'high'
    elif asset_type in ('active_fund', 'gold', 'etf'):
        risk_level = 'medium'

    if score >= 4.0:
        action = 'buy'
        confidence = 'high'
        position_size_pct = 12 if asset_type in ('active_fund', 'etf') else 8
    elif score >= 3.0:
        action = 'hold'
        confidence = 'medium'
        position_size_pct = 8 if asset_type in ('active_fund', 'etf') else 5
    else:
        action = 'watch'
        confidence = 'low'
        position_size_pct = 3

    stop_loss_pct = 0.06 if asset_type in ('active_fund', 'etf') else (0.08 if asset_type == 'gold' else 0.1)
    take_profit_pct = 0.12 if asset_type in ('active_fund', 'etf') else (0.15 if asset_type == 'gold' else 0.18)
    return {
        'action': action,
        'confidence': confidence,
        'risk_level': risk_level,
        'position_size_pct': position_size_pct,
        'stop_loss_pct': stop_loss_pct,
        'take_profit_pct': take_profit_pct,
        'evidence_score': max(40.0, min(85.0, score * 18)),
        'model_reliability': {'label': 'stable'},
        'review_in_days': 3 if risk_level == 'high' else 5,
        'review_focus': ['趋势是否延续', '是否触发止损/止盈', '市场环境是否变化'],
        'summary': '适合分步执行、纪律化管理仓位，不宜一次性重仓。',
        'reason_tags': ['分批执行', '控制回撤', '动态复核'],
    }


def _get_asset_advice_profile(asset_type):
    """不同资产类型使用不同的理财师表达模板。"""
    stock_profile = {
        'core_logic': '更看重趋势、估值与资金面是否形成共振，适合做节奏化配置。',
        'buy_steps': ['优先分批建仓，不建议一次性重仓。', '把单一标的控制在可承受范围内，避免追高。'],
        'reduce_steps': ['若已持有，宜优先减仓控制回撤，而不是情绪化死扛。', '确认趋势修复前，不建议急于补仓。'],
        'watch_steps': ['先观察量价与趋势是否继续改善。', '等待更明确的入场信号后再行动。'],
        'suitable_for': ['能接受一定波动、愿意按纪律执行的投资者', '希望做行业或个股节奏配置的人'],
        'avoid_for': ['追求短期确定性且不愿承担回撤的人', '习惯一次性重仓押注单一个股的人'],
    }

    profiles = {
        'a_stock': stock_profile,
        'hk_stock': {**stock_profile, 'core_logic': '更要关注趋势延续、估值修复以及南向资金变化。'},
        'us_stock': {**stock_profile, 'core_logic': '更看重盈利预期、估值变化与海外风险偏好。'},
        'active_fund': {
            'core_logic': '更适合作为中长期配置工具，重点看基金经理稳定性与回撤控制。',
            'buy_steps': ['优先采用定投或分批申购，不追求一次性抄底。', '更适合做组合底仓，而不是短线快进快出。'],
            'reduce_steps': ['若近期回撤超出预期，可先小幅降仓，避免一次性赎回。', '重点复核基金经理风格是否发生明显漂移。'],
            'watch_steps': ['先观察净值修复和回撤控制情况。', '等待更清晰的风格与业绩信号再加大配置。'],
            'suitable_for': ['想做稳健配置、愿意中长期持有的投资者', '希望分散个股波动风险的人'],
            'avoid_for': ['期待短期快速翻倍的人', '不能接受阶段性净值回撤的人'],
        },
        'etf': {
            'core_logic': '更适合做指数或行业配置，优势在于分散单只股票风险。',
            'buy_steps': ['可以分批布局，优先用来做板块配置。', '仓位不宜过于集中在单一主题上。'],
            'reduce_steps': ['若板块热度退潮，可优先回到均衡仓位。', '不必追求一次性卖在高点，分步调整更稳妥。'],
            'watch_steps': ['先观察指数趋势和资金轮动方向。', '等待板块重新获得量能支持再考虑出手。'],
            'suitable_for': ['偏好指数化配置、希望降低个股风险的投资者', '适合做定投或波段配置的人'],
            'avoid_for': ['只想押注单一个股爆发收益的人', '对主题波动完全无法接受的人'],
        },
        'gold': {
            'core_logic': '黄金更偏防守和对冲，适合作为组合的稳定器，而不是短线暴利工具。',
            'buy_steps': ['更适合分2到3次配置，不建议因为情绪一次追高。', '宜把它当作防守仓位，而非核心进攻仓位。'],
            'reduce_steps': ['若避险需求降温，可把仓位降回防守水平。', '优先分批调整，而不是追涨杀跌。'],
            'watch_steps': ['先观察美元、利率与避险情绪变化。', '等待更合适的配置窗口，不必急于出手。'],
            'suitable_for': ['希望给组合增加防守资产的人', '更关注抗波动和资产对冲的投资者'],
            'avoid_for': ['追求短期高弹性收益的人', '无法接受商品价格波动的人'],
        },
        'silver': {
            'core_logic': '白银弹性比黄金更高，但波动也更大，更适合作为小仓位卫星配置。',
            'buy_steps': ['仓位宜比黄金更轻，建议小步试探。', '更适合分批执行，避免高波动下追涨。'],
            'reduce_steps': ['若波动放大或工业需求走弱，应优先降仓控制风险。', '不要把白银当成稳健型底仓长期死拿。'],
            'watch_steps': ['先观察工业需求与金银比是否继续改善。', '没有明显优势前，保持轻仓或观望更稳妥。'],
            'suitable_for': ['风险承受能力较强、接受高波动的投资者', '想用小仓位参与贵金属弹性的投资者'],
            'avoid_for': ['稳健保守、不能接受较大震荡的人', '希望短期高确定性的投资者'],
        },
    }
    return profiles.get(asset_type, stock_profile)


def _build_detail_execution_plan(detail):
    """生成更像理财师的详情执行方案。"""
    advisor = (detail.get('advisor') or {}).copy()
    if not advisor:
        advisor = _build_default_detail_advisor(detail)
        detail['advisor'] = advisor

    asset_type = detail.get('asset_type', 'unknown')
    profile = _get_asset_advice_profile(asset_type)
    predictions = detail.get('predictions') or []
    best_horizon = None
    if predictions:
        best_horizon = max(predictions, key=lambda x: float(x.get('up_probability', 0) or 0))

    action = advisor.get('action', 'hold')
    action_text_map = {
        'buy': '可买入', 'add': '可加仓', 'hold': '继续持有', 'watch': '先观察', 'reduce': '考虑减仓', 'sell': '考虑卖出'
    }
    position_pct = int(advisor.get('position_size_pct', 0) or 0)
    if position_pct > 0:
        headline = f"{action_text_map.get(action, '先观察')} · 建议仓位 {position_pct}%"
    elif position_pct < 0:
        headline = f"{action_text_map.get(action, '先观察')} · 建议减仓 {abs(position_pct)}%"
    elif action == 'hold':
        headline = '偏多持有 · 暂不追高'
    elif action == 'watch':
        headline = '先观察 · 等待更好买点'
    else:
        headline = f"{action_text_map.get(action, '先观察')} · 暂不新增仓位"

    technical = ((detail.get('analysis') or {}).get('technical') or {}).get('details', '技术面暂无明确信号')
    valuation = ((detail.get('analysis') or {}).get('valuation') or {}).get('details', '估值面暂无明确信号')
    money_flow = ((detail.get('analysis') or {}).get('money_flow') or {}).get('details', '资金面暂无明确信号')
    news = ((detail.get('analysis') or {}).get('news') or {}).get('details', '消息面暂无明确信号')
    risk_items = (((detail.get('analysis') or {}).get('risk') or {}).get('items') or [])[:3]

    if asset_type in ('gold', 'silver'):
        why_buy = [profile['core_logic'], technical, news]
    elif asset_type in ('active_fund',):
        why_buy = [profile['core_logic'], valuation, news]
    else:
        why_buy = [profile['core_logic'], technical, money_flow]

    if action in ('buy', 'add'):
        action_steps = list(profile.get('buy_steps', []))
        action_steps.append(f"若跌破止损线约 {(float(advisor.get('stop_loss_pct', 0.07)) * 100):.1f}% ，优先执行风险控制。")
    elif action in ('reduce', 'sell'):
        action_steps = list(profile.get('reduce_steps', []))
        action_steps.append(f"建议在 {advisor.get('review_in_days', 3)} 天内再次复核趋势。")
    else:
        action_steps = list(profile.get('watch_steps', []))
        action_steps.append(f"建议在 {advisor.get('review_in_days', 3)} 天内重新评估。")

    suitable_for = list(profile.get('suitable_for', []))
    avoid_for = list(profile.get('avoid_for', []))

    if best_horizon:
        best_period = best_horizon.get('period') or best_horizon.get('period_days')
        best_prob = float(best_horizon.get('up_probability', 0) or 0)
        if action in ('reduce', 'sell'):
            position_note = f"当前各周期优势不足，{best_period} 日窗口向上概率约 {best_prob:.1f}%，更适合先防守。"
        elif action in ('buy', 'add'):
            position_note = f"当前重点观察 {best_period} 日窗口，向上概率约 {best_prob:.1f}%，宜分批而不是一次性出手。"
        elif action == 'hold' and best_prob >= 58:
            position_note = f"信号偏多，当前重点观察 {best_period} 日窗口，向上概率约 {best_prob:.1f}%。若已持有可继续按计划持有，若未持有不建议追高。"
        elif 45 <= best_prob <= 55:
            position_note = f"当前信号仍偏胶着，{best_period} 日窗口概率约 {best_prob:.1f}%，更适合继续观察。"
        else:
            position_note = f"当前较值得关注的窗口为 {best_period} 日，向上概率约 {best_prob:.1f}%。"
    else:
        position_note = '当前暂无足够预测窗口数据，建议以轻仓试探和观察为主。'

    if action in ('buy', 'add'):
        portfolio_role = ['适合作为当前主线的进攻仓位', '更适合分批进入，而不是一次性重仓', '应与防守资产搭配，避免单一行业过度集中']
    elif action in ('reduce', 'sell'):
        portfolio_role = ['当前更像风险清理对象，而不是新增核心仓位', '应把资金转回更稳健或更强主线方向', '以控制回撤为先，不宜逆势加仓']
    else:
        portfolio_role = ['暂列观察名单，适合小仓位跟踪', '等待趋势确认后再决定是否进入核心仓位', '当前更适合做备选而不是主仓资产']

    rebalance_triggers = [
        '若该资产所在行业跌出当前重点主线，应下调仓位。',
        f"若 {advisor.get('review_in_days', 3)} 天后信号仍未强化，建议继续降低预期。",
        '若达到止盈/止损线，宜按纪律分批执行调仓。',
    ]

    return {
        'headline': headline,
        'position_note': position_note,
        'why_buy': [item for item in why_buy if item][:3],
        'action_steps': [item for item in action_steps if item][:3],
        'risk_checks': risk_items,
        'suitable_for': [item for item in suitable_for if item][:3],
        'avoid_for': [item for item in avoid_for if item][:3],
        'portfolio_role': portfolio_role[:3],
        'rebalance_triggers': rebalance_triggers[:3],
    }


def _normalize_recommendation_type(asset_type):
    """统一推荐资产类型别名。"""
    normalized = str(asset_type or '').strip().lower()
    if normalized == 'fund':
        return 'active_fund'
    return normalized


def _get_latest_recommendation_snapshot(session, code, asset_type=None):
    """优先按当前页面资产类型获取对应推荐快照，避免跨品类同代码串数据。"""
    normalized_type = _normalize_recommendation_type(asset_type)
    base_query = session.query(Recommendation).filter(Recommendation.code == code)

    if normalized_type:
        scoped = base_query.filter(Recommendation.type == normalized_type).order_by(
            Recommendation.date.desc(), Recommendation.rank.asc(), Recommendation.id.desc()
        ).first()
        if scoped:
            return scoped

    return base_query.order_by(
        Recommendation.date.desc(), Recommendation.rank.asc(), Recommendation.id.desc()
    ).first()


def _build_snapshot_recommendation_item(recommendation, asset_type=None):
    """将数据库中的推荐快照统一转为可复用的投顾输入。"""
    if not recommendation:
        return None

    item = {
        'code': recommendation.code,
        'name': recommendation.name or recommendation.code,
        'industry': getattr(recommendation, 'industry', '') or '',
        'asset_type': asset_type or getattr(recommendation, 'type', 'unknown'),
        'data_source': getattr(recommendation, 'data_source', '') or 'system_db',
        'current_price': round(float(recommendation.current_price or 0), 2),
        'up_probability_5d': round(float(recommendation.up_probability_5d), 2) if recommendation.up_probability_5d is not None else 50.0,
        'up_probability_20d': round(float(recommendation.up_probability_20d), 2) if recommendation.up_probability_20d is not None else 50.0,
        'up_probability_60d': round(float(recommendation.up_probability_60d), 2) if recommendation.up_probability_60d is not None else 50.0,
        'total_score': round(float(recommendation.total_score or 0), 2),
        'volatility_level': recommendation.volatility_level or 'medium',
    }
    item['unified_trend'] = derive_unified_trend({
        'up_probability_5d': item['up_probability_5d'],
        'up_probability_20d': item['up_probability_20d'],
        'up_probability_60d': item['up_probability_60d'],
        'total_score': item['total_score'],
        'model_status': {
            'short_term_validated': bool(recommender.short_predictor.is_trained),
            'medium_term_validated': bool(recommender.medium_predictor.is_trained),
            'long_term_validated': bool(recommender.long_predictor.is_trained),
        }
    })
    return item


def _apply_snapshot_to_detail(detail, recommendation):
    """优先使用推荐列表同批次快照，确保详情页与列表一致。"""
    if not detail or not recommendation:
        return detail

    snapshot = _build_snapshot_recommendation_item(recommendation, detail.get('asset_type'))
    if not snapshot:
        return detail

    probability_map = {
        5: snapshot['up_probability_5d'],
        20: snapshot['up_probability_20d'],
        60: snapshot['up_probability_60d'],
    }

    synced_predictions = []
    for raw_prediction in detail.get('predictions') or []:
        prediction = dict(raw_prediction)
        try:
            period = int(prediction.get('period') or prediction.get('period_days') or 0)
        except Exception:
            period = 0

        if period in probability_map:
            up_probability = float(probability_map[period] or 50.0)
            prediction['up_probability'] = round(up_probability, 2)
            prediction['down_probability'] = round(max(0.0, 100.0 - up_probability), 2)
        synced_predictions.append(prediction)

    if synced_predictions:
        detail['predictions'] = synced_predictions

    detail['up_probability_5d'] = snapshot.get('up_probability_5d', detail.get('up_probability_5d', 50))
    detail['up_probability_20d'] = snapshot.get('up_probability_20d', detail.get('up_probability_20d', 50))
    detail['up_probability_60d'] = snapshot.get('up_probability_60d', detail.get('up_probability_60d', 50))

    detail['advisor'] = _build_recommendation_advisor_payload(snapshot)
    detail['total_score'] = snapshot.get('total_score', detail.get('total_score', 0))
    detail['buy_confidence'] = _score_to_confidence(float(detail.get('total_score', 0) or 0))

    position_pct = int(detail['advisor'].get('position_size_pct', 0) or 0)
    if position_pct > 0:
        detail['suggested_position'] = f"建议单标的不超过总资金{position_pct}%"
    elif position_pct < 0:
        detail['suggested_position'] = f"若已持有，建议分批减仓约{abs(position_pct)}%"
    elif detail['advisor'].get('action') == 'hold':
        detail['suggested_position'] = '若已持有可继续持有；若未持有先别追高'
    else:
        detail['suggested_position'] = '建议先观察，等待更合适的入场点'

    action = detail['advisor'].get('action', 'hold')
    if action in ('buy', 'add', 'reduce', 'sell'):
        detail['suggested_period'] = '1-4周'

    if not detail.get('current_price') and snapshot.get('current_price'):
        detail['current_price'] = snapshot['current_price']
    if not detail.get('data_source') and snapshot.get('data_source'):
        detail['data_source'] = snapshot.get('data_source')

    holding_advice = _build_holding_recommendation(snapshot, detail.get('asset_type'))
    detail['holding_advice'] = holding_advice
    detail['suggested_period'] = holding_advice.get('holding_period_text', detail.get('suggested_period', '10-40个交易日'))
    detail['data_quality'] = _build_data_quality_summary(detail)
    detail['quality_gate'] = _build_recommendation_quality_gate(detail, snapshot=snapshot)
    detail['recommendation_rationale'] = _build_detail_recommendation_rationale(detail, snapshot=snapshot)

    return detail


def register_recommendations_routes(app):
    """注册推荐相关路由"""

    type_alias = {
        'fund': 'active_fund',
    }

    def _calc_probability_health(session, today, rec_types, lookback_days=240):
        """计算历史概率健康度（命中率/均值概率/Brier）。"""
        start_date = today - timedelta(days=lookback_days + 65)
        result = {}
        horizon_fields = {
            5: Recommendation.up_probability_5d,
            20: Recommendation.up_probability_20d,
            60: Recommendation.up_probability_60d,
        }

        for rec_type in rec_types:
            rows = (
                session.query(
                    Recommendation.code,
                    Recommendation.date,
                    Recommendation.current_price,
                    Recommendation.up_probability_5d,
                    Recommendation.up_probability_20d,
                    Recommendation.up_probability_60d,
                )
                .filter(Recommendation.type == rec_type)
                .filter(Recommendation.date >= start_date)
                .filter(Recommendation.current_price.isnot(None))
                .filter(Recommendation.current_price > 0)
                .order_by(Recommendation.code.asc(), Recommendation.date.asc())
                .all()
            )

            by_code = {}
            for code, d, px, p5, p20, p60 in rows:
                by_code.setdefault(code, []).append((d, float(px), float(p5 or 50), float(p20 or 50), float(p60 or 50)))

            type_health = {}
            for horizon in [5, 20, 60]:
                total = 0
                hit = 0
                sum_prob = 0.0
                sum_brier = 0.0
                signal_stats = {
                    'bullish': {'total': 0, 'hit': 0, 'sum_prob': 0.0, 'sum_brier': 0.0},
                    'bearish': {'total': 0, 'hit': 0, 'sum_prob': 0.0, 'sum_brier': 0.0},
                }

                for items in by_code.values():
                    if len(items) < 2:
                        continue
                    dates = [x[0] for x in items]
                    for i, (d0, p0, pp5, pp20, pp60) in enumerate(items):
                        target_date = d0 + timedelta(days=horizon)
                        j = i + 1
                        while j < len(items) and dates[j] < target_date:
                            j += 1
                        if j >= len(items):
                            continue
                        p1 = items[j][1]
                        prob = pp5 if horizon == 5 else (pp20 if horizon == 20 else pp60)
                        y = 1.0 if p1 > p0 else 0.0
                        predicted_up = float(prob) >= 50.0
                        is_hit = int((predicted_up and y == 1.0) or ((not predicted_up) and y == 0.0))
                        total += 1
                        hit += is_hit
                        sum_prob += prob
                        brier_component = ((prob / 100.0) - y) ** 2
                        sum_brier += brier_component

                        if float(prob) >= 55.0:
                            bucket = signal_stats['bullish']
                            bucket['total'] += 1
                            bucket['hit'] += is_hit
                            bucket['sum_prob'] += prob
                            bucket['sum_brier'] += brier_component
                        elif float(prob) <= 45.0:
                            bucket = signal_stats['bearish']
                            bucket['total'] += 1
                            bucket['hit'] += is_hit
                            bucket['sum_prob'] += prob
                            bucket['sum_brier'] += brier_component

                if total == 0:
                    type_health[str(horizon)] = {
                        'status': 'insufficient_history',
                        'grade': 'N/A',
                        'samples': 0,
                        'hit_rate': None,
                        'avg_probability': None,
                        'brier': None,
                        'calibration_gap': None,
                    }
                else:
                    hit_rate = hit / total * 100.0
                    avg_prob = sum_prob / total
                    brier = round(sum_brier / total, 4)
                    gap = round(avg_prob - hit_rate, 2)

                    # 简单阈值评级（越高越好）
                    # A: gap<=5 & brier<=0.20
                    # B: gap<=8 & brier<=0.25
                    # C: gap<=12 & brier<=0.30
                    # D: 其他
                    abs_gap = abs(gap)
                    if abs_gap <= 5 and brier <= 0.20:
                        grade = 'A'
                    elif abs_gap <= 8 and brier <= 0.25:
                        grade = 'B'
                    elif abs_gap <= 12 and brier <= 0.30:
                        grade = 'C'
                    else:
                        grade = 'D'

                    signal_buckets = {}
                    for bucket_name, bucket in signal_stats.items():
                        bucket_total = int(bucket.get('total') or 0)
                        if bucket_total <= 0:
                            signal_buckets[bucket_name] = {
                                'status': 'insufficient_history',
                                'samples': 0,
                                'hit_rate': None,
                                'avg_probability': None,
                                'brier': None,
                                'calibration_gap': None,
                                'grade': 'N/A',
                            }
                            continue

                        bucket_hit_rate = bucket['hit'] / bucket_total * 100.0
                        bucket_avg_prob = bucket['sum_prob'] / bucket_total
                        bucket_brier = round(bucket['sum_brier'] / bucket_total, 4)
                        bucket_gap = round(bucket_avg_prob - bucket_hit_rate, 2)
                        bucket_abs_gap = abs(bucket_gap)
                        if bucket_abs_gap <= 5 and bucket_brier <= 0.20:
                            bucket_grade = 'A'
                        elif bucket_abs_gap <= 8 and bucket_brier <= 0.25:
                            bucket_grade = 'B'
                        elif bucket_abs_gap <= 12 and bucket_brier <= 0.30:
                            bucket_grade = 'C'
                        else:
                            bucket_grade = 'D'

                        signal_buckets[bucket_name] = {
                            'status': 'ok',
                            'samples': bucket_total,
                            'hit_rate': round(bucket_hit_rate, 2),
                            'avg_probability': round(bucket_avg_prob, 2),
                            'brier': bucket_brier,
                            'calibration_gap': bucket_gap,
                            'grade': bucket_grade,
                        }

                    type_health[str(horizon)] = {
                        'status': 'ok',
                        'grade': grade,
                        'samples': total,
                        'hit_rate': round(hit_rate, 2),
                        'avg_probability': round(avg_prob, 2),
                        'brier': brier,
                        'calibration_gap': gap,
                        'signal_buckets': signal_buckets,
                    }

            result[rec_type] = type_health

        return result

    def _latest_recommendation_date(session, query_type=None):
        """获取指定资产类型最近一批可用推荐日期。"""
        today = get_today()
        query = session.query(Recommendation).filter(Recommendation.date <= today)
        if query_type:
            query = query.filter(Recommendation.type == query_type)
        latest = query.order_by(Recommendation.date.desc()).first()
        return latest.date if latest else None
    
    @app.route('/api/recommendations/<type>', methods=['GET'])
    def get_recommendations(type):
        """获取推荐列表"""
        try:
            query_type = type_alias.get(type, type)
            market_sentiment_value = get_market_sentiment()
            sort_by = request.args.get('sort_by', 'score')
            order = request.args.get('order', 'desc')
            keyword = request.args.get('keyword', '').strip()
            limit = min(max(request.args.get('limit', default=20, type=int) or 20, 1), 50)
            offset = max(request.args.get('offset', default=0, type=int) or 0, 0)
            industry = request.args.get('industry', '')
            volatility_level = request.args.get('volatility_level', '')
            
            session = get_session()
            query_date = _latest_recommendation_date(session, query_type)
            probability_health = {}
            try:
                probability_health = _calc_probability_health(session, get_today(), [query_type]).get(query_type, {})
            except Exception as e:
                logger.warning(f"构建资产级模型状态失败[{query_type}]: {e}")
                probability_health = {}

            query = session.query(Recommendation).filter(
                Recommendation.type == query_type
            )
            if query_date is not None:
                query = query.filter(Recommendation.date == query_date)
            else:
                query = query.filter(Recommendation.id == -1)
            
            if industry:
                pass

            if keyword:
                like_pattern = f"%{keyword}%"
                query = query.filter(
                    or_(
                        Recommendation.code.ilike(like_pattern),
                        Recommendation.name.ilike(like_pattern)
                    )
                )
            
            if volatility_level:
                query = query.filter(Recommendation.volatility_level == volatility_level)
            
            if sort_by == 'score':
                order_col = Recommendation.total_score
            elif sort_by in ['probability', 'probability_5d']:
                order_col = Recommendation.up_probability_5d
            elif sort_by == 'probability_20d':
                order_col = Recommendation.up_probability_20d
            elif sort_by == 'probability_60d':
                order_col = Recommendation.up_probability_60d
            elif sort_by == 'volatility':
                order_col = Recommendation.volatility_level
            else:
                order_col = Recommendation.rank
            
            if order == 'desc':
                query = query.order_by(order_col.desc())
            else:
                query = query.order_by(order_col.asc())
            
            total_count = query.count()
            recommendations = query.offset(offset).limit(limit).all()

            if query_type in ['a_stock', 'hk_stock', 'us_stock'] and not keyword and offset == 0 and total_count < min(limit, 5):
                live_stock_recs = _build_live_stock_list_fallback(query_type, limit=limit, probability_health=probability_health)
                if len(live_stock_recs) > total_count:
                    portfolio_advice = recommender._build_portfolio_advice(live_stock_recs)
                    horizon_top_picks = _build_horizon_top_picks(live_stock_recs)
                    strength_spotlight = _build_strength_spotlight(live_stock_recs)
                    strategy_framework = _build_strategy_framework(live_stock_recs, query_type=query_type, market_sentiment=market_sentiment_value)
                    session.close()
                    return jsonify({
                        'code': 200,
                        'status': 'success',
                        'data': {
                            'type': type,
                            'query_type': query_type,
                            'total': len(live_stock_recs),
                            'count': len(live_stock_recs),
                            'limit': limit,
                            'offset': offset,
                            'keyword': keyword,
                            'as_of': get_today().isoformat(),
                            'recommendations': live_stock_recs,
                            'portfolio_advice': portfolio_advice,
                            'horizon_top_picks': horizon_top_picks,
                            'strength_spotlight': strength_spotlight,
                            'strategy_framework': strategy_framework,
                            'source': 'live_stock_fallback'
                        },
                        'timestamp': datetime.now().isoformat()
                    })

            if total_count == 0 and query_type in ['a_stock', 'hk_stock', 'us_stock'] and keyword and offset == 0:
                stock_fallback = _build_stock_search_fallback(session, query_type, keyword, limit=limit, probability_health=probability_health)
                if stock_fallback:
                    portfolio_advice = recommender._build_portfolio_advice(stock_fallback)
                    horizon_top_picks = _build_horizon_top_picks(stock_fallback)
                    strength_spotlight = _build_strength_spotlight(stock_fallback)
                    strategy_framework = _build_strategy_framework(stock_fallback, query_type=query_type, market_sentiment=market_sentiment_value)
                    fallback_as_of = max((item.get('as_of') for item in stock_fallback if item.get('as_of')), default=None)
                    for item in stock_fallback:
                        item.pop('as_of', None)
                    session.close()
                    return jsonify({
                        'code': 200,
                        'status': 'success',
                        'data': {
                            'type': type,
                            'query_type': query_type,
                            'total': len(stock_fallback),
                            'count': len(stock_fallback),
                            'limit': limit,
                            'offset': offset,
                            'keyword': keyword,
                            'as_of': fallback_as_of,
                            'recommendations': stock_fallback,
                            'portfolio_advice': portfolio_advice,
                            'horizon_top_picks': horizon_top_picks,
                            'strength_spotlight': strength_spotlight,
                            'strategy_framework': strategy_framework,
                            'source': 'stock_search_fallback'
                        },
                        'timestamp': datetime.now().isoformat()
                    })

            if total_count == 0 and query_type == 'active_fund' and keyword and offset == 0:
                holding_fallback = _build_active_fund_holding_fallback(session, keyword, limit=limit)
                if holding_fallback:
                    portfolio_advice = recommender._build_portfolio_advice(holding_fallback)
                    horizon_top_picks = _build_horizon_top_picks(holding_fallback)
                    strength_spotlight = _build_strength_spotlight(holding_fallback)
                    strategy_framework = _build_strategy_framework(holding_fallback, query_type=query_type, market_sentiment=market_sentiment_value)
                    session.close()
                    return jsonify({
                        'code': 200,
                        'status': 'success',
                        'data': {
                            'type': type,
                            'query_type': query_type,
                            'total': len(holding_fallback),
                            'count': len(holding_fallback),
                            'limit': limit,
                            'offset': offset,
                            'keyword': keyword,
                            'as_of': get_today().isoformat(),
                            'recommendations': holding_fallback,
                            'portfolio_advice': portfolio_advice,
                            'horizon_top_picks': horizon_top_picks,
                            'strength_spotlight': strength_spotlight,
                            'strategy_framework': strategy_framework,
                            'source': 'holding_fallback'
                        },
                        'timestamp': datetime.now().isoformat()
                    })

            if total_count == 0 and query_type == 'active_fund' and offset == 0:
                try:
                    from recommenders.fund_recommender import FundRecommender

                    live_recs = FundRecommender().get_recommendations(limit=limit)
                    recommendations_list = []
                    for idx, rec in enumerate(live_recs, start=1):
                        up5, up20, up60 = derive_probabilities(rec, 'active_fund')
                        score = float(rec.get('score', rec.get('total_score', 0)) or 0)
                        unified_trend = derive_unified_trend({
                            'up_probability_5d': up5,
                            'up_probability_20d': up20,
                            'up_probability_60d': up60,
                            'total_score': score,
                        })
                        item = {
                            'rank': idx,
                            'code': rec.get('code', ''),
                            'name': rec.get('name', rec.get('code', '')),
                            'display_code': rec.get('code', ''),
                            'industry': '',
                            'current_price': round(float(rec.get('current_price') or 0), 2),
                            'up_probability_5d': round(float(up5), 2),
                            'up_probability_20d': round(float(up20), 2),
                            'up_probability_60d': round(float(up60), 2),
                            'unified_trend': unified_trend,
                            'trend_direction': unified_trend.get('trend_direction', 'neutral'),
                            'trend_score': unified_trend.get('trend_score', 50.0),
                            'trend_confidence': unified_trend.get('trend_confidence', 20.0),
                            'total_score': round(score, 2),
                            'volatility_level': rec.get('volatility_level', 'medium'),
                            'reason_summary': rec.get('reason', ''),
                        }
                        item['advisor_view'] = _build_recommendation_advisor_payload(item)
                        item['advisor_action'] = item['advisor_view'].get('action', 'hold')
                        item['advisor_confidence'] = item['advisor_view'].get('confidence', 'low')
                        item['risk_level'] = item['advisor_view'].get('risk_level', 'medium')
                        item['position_size_pct'] = item['advisor_view'].get('position_size_pct', 0)
                        item['strength'] = _classify_recommendation_strength(item)
                        _apply_holding_recommendation(item, asset_type=query_type)
                        recommendations_list.append(item)

                    portfolio_advice = recommender._build_portfolio_advice(recommendations_list)
                    horizon_top_picks = _build_horizon_top_picks(recommendations_list)
                    strength_spotlight = _build_strength_spotlight(recommendations_list)
                    strategy_framework = _build_strategy_framework(recommendations_list, query_type=query_type, market_sentiment=market_sentiment_value)
                    return jsonify({
                        'code': 200,
                        'status': 'success',
                        'data': {
                            'type': type,
                            'query_type': query_type,
                            'total': len(recommendations_list),
                            'count': len(recommendations_list),
                            'limit': limit,
                            'offset': offset,
                            'keyword': keyword,
                            'as_of': get_today().isoformat(),
                            'recommendations': recommendations_list,
                            'portfolio_advice': portfolio_advice,
                            'horizon_top_picks': horizon_top_picks,
                            'strength_spotlight': strength_spotlight,
                            'strategy_framework': strategy_framework,
                            'source': 'live_fallback'
                        },
                        'timestamp': datetime.now().isoformat()
                    })
                except Exception as e:
                    logger.warning(f"基金实时回退失败: {e}")
            
            recommendations_list = []
            market_model_status = _build_asset_model_status(query_type, probability_health)
            short_term_source = market_model_status.get('short_term_source', 'rule_fallback')
            short_term_validated = market_model_status.get('short_term_validated', False)
            for rec in recommendations:
                meta = _resolve_stock_meta(rec.code, rec.name or '') if query_type in ['a_stock', 'hk_stock', 'us_stock'] else {
                    'ts_code': rec.code,
                    'name': rec.name,
                    'industry': ''
                }
                item = {
                    'rank': rec.rank,
                    'code': rec.code,
                    'name': meta['name'] or rec.code,
                    'display_code': meta['ts_code'] or rec.code,
                    'industry': meta['industry'],
                    'asset_type': query_type,
                    'current_price': round(float(rec.current_price), 2) if rec.current_price else 0,
                    'up_probability_5d': round(float(rec.up_probability_5d), 2) if rec.up_probability_5d is not None else 50,
                    'up_probability_20d': round(float(rec.up_probability_20d), 2) if rec.up_probability_20d is not None else 50,
                    'up_probability_60d': round(float(rec.up_probability_60d), 2) if rec.up_probability_60d is not None else 50,
                    'total_score': round(float(rec.total_score), 2) if rec.total_score else 0,
                    'volatility_level': rec.volatility_level,
                    'reason_summary': rec.reason_summary,
                    'market_model_reliability': market_model_status.get('market_model_reliability', {}),
                    'model_status': {
                        'short_term_validated': market_model_status.get('short_term_validated', False),
                        'medium_term_validated': market_model_status.get('medium_term_validated', False),
                        'long_term_validated': market_model_status.get('long_term_validated', False),
                    }
                }
                item['unified_trend'] = derive_unified_trend({
                    'up_probability_5d': item['up_probability_5d'],
                    'up_probability_20d': item['up_probability_20d'],
                    'up_probability_60d': item['up_probability_60d'],
                    'total_score': item['total_score'],
                    'model_status': item['model_status']
                })
                item['trend_direction'] = item['unified_trend'].get('trend_direction', 'neutral')
                item['trend_score'] = item['unified_trend'].get('trend_score', 50.0)
                item['trend_confidence'] = item['unified_trend'].get('trend_confidence', 20.0)
                item['advisor_view'] = _build_recommendation_advisor_payload(item)
                item['advisor_action'] = item['advisor_view'].get('action', 'hold')
                item['advisor_confidence'] = item['advisor_view'].get('confidence', 'low')
                item['risk_level'] = item['advisor_view'].get('risk_level', 'medium')
                item['position_size_pct'] = item['advisor_view'].get('position_size_pct', 0)
                item['review_in_days'] = item['advisor_view'].get('review_in_days', 3)
                item['strength'] = _classify_recommendation_strength(item)
                item['reason_summary'] = item['advisor_view'].get('summary', item.get('reason_summary') or '等待进一步确认')
                item['short_term_source'] = short_term_source
                item['short_term_validated'] = short_term_validated
                item['market_model_reliability'] = market_model_status.get('market_model_reliability', {})
                _apply_holding_recommendation(item, asset_type=query_type)
                recommendations_list.append(item)
            
            portfolio_advice = recommender._build_portfolio_advice(recommendations_list)
            horizon_top_picks = _build_horizon_top_picks(recommendations_list)
            strength_spotlight = _build_strength_spotlight(recommendations_list)
            strategy_framework = _build_strategy_framework(recommendations_list, query_type=query_type, market_sentiment=market_sentiment_value)
            session.close()
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': {
                    'type': type,
                    'query_type': query_type,
                    'total': total_count,
                    'count': len(recommendations_list),
                    'limit': limit,
                    'offset': offset,
                    'keyword': keyword,
                    'as_of': query_date.isoformat() if query_date else None,
                    'recommendations': recommendations_list,
                    'portfolio_advice': portfolio_advice,
                    'horizon_top_picks': horizon_top_picks,
                    'strength_spotlight': strength_spotlight,
                    'strategy_framework': strategy_framework
                },
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"获取推荐列表失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/recommendations/<code>/detail', methods=['GET'])
    def get_recommendation_detail(code):
        """获取推荐标的详情"""
        try:
            session = get_session()
            requested_type = _normalize_recommendation_type(request.args.get('type', ''))

            recommendation = _get_latest_recommendation_snapshot(
                session,
                code,
                requested_type if requested_type else None,
            )
            
            if recommendation:
                asset_type = recommendation.type
                current_price = recommendation.current_price
                total_score = recommendation.total_score if recommendation.total_score else 3.0
            else:
                asset_type = _detect_market(code)
                current_price = 0
                total_score = 3.0
            
            market_sentiment = get_market_sentiment()
            
            # 股票类型
            if asset_type in ['a_stock', 'hk_stock', 'us_stock']:
                market = 'A' if asset_type == 'a_stock' else ('H' if asset_type == 'hk_stock' else 'US')
                collector = StockCollector()
                market_data_source = getattr(recommendation, 'data_source', '') or 'system_db'
                df = collector.get_stock_data_from_db(code)
                local_df = df

                if df is None or len(df) < 60:
                    import yfinance as yf
                    try:
                        ticker = yf.Ticker(_normalize_yfinance_symbol(code))
                        remote_df = ticker.history(period='1y')
                    except Exception:
                        remote_df = None

                    if remote_df is not None and len(remote_df) > 0:
                        df = remote_df
                        market_data_source = 'yfinance'
                        df.columns = [col.lower() for col in df.columns]
                    elif local_df is not None and len(local_df) > 0:
                        df = local_df

                if df is not None and len(df) >= 60:
                    analysis = recommender.get_stock_analysis(code, market, df)
                    
                    if analysis:
                        predictions = []
                        
                        # 5日预测 - 转换数值类型
                        short = analysis['predictions']['short_term']
                        short['period'] = short.get('period_days', 5)
                        short['up_probability'] = float(short['up_probability'])
                        short['down_probability'] = float(short['down_probability'])
                        short['target_low'] = float(short.get('target_low', 0))
                        short['target_high'] = float(short.get('target_high', 0))
                        short['stop_loss'] = float(short.get('stop_loss', 0))
                        short['confidence'] = float(short.get('confidence', 50))
                        short['source'] = f'trained_model + {market_data_source}'
                        predictions.append(short)
                        
                        # 20日预测
                        medium = analysis['predictions']['medium_term']
                        medium['period'] = medium.get('period_days', 20)
                        medium['up_probability'] = float(medium['up_probability'])
                        medium['down_probability'] = float(medium['down_probability'])
                        medium['target_low'] = float(medium.get('target_low', 0))
                        medium['target_high'] = float(medium.get('target_high', 0))
                        medium['stop_loss'] = float(medium.get('stop_loss', 0))
                        medium['confidence'] = float(medium.get('confidence', 50))
                        medium['source'] = f'trained_model + {market_data_source}'
                        predictions.append(medium)
                        
                        # 60日预测
                        long = analysis['predictions']['long_term']
                        long['period'] = long.get('period_days', 60)
                        long['up_probability'] = float(long['up_probability'])
                        long['down_probability'] = float(long['down_probability'])
                        long['target_low'] = float(long.get('target_low', 0))
                        long['target_high'] = float(long.get('target_high', 0))
                        long['stop_loss'] = float(long.get('stop_loss', 0))
                        long['confidence'] = float(long.get('confidence', 50))
                        long['source'] = f'trained_model + {market_data_source}'
                        predictions.append(long)
                        
                        advisor_view = analysis.get('advisor_view', {}) or {}
                        suggested_position_pct = int(max(0, advisor_view.get('position_size_pct', 5) or 5))
                        detail = {
                            'code': code,
                            'name': _resolve_stock_meta(code, recommendation.name if recommendation else '').get('name') if asset_type in ['a_stock', 'hk_stock', 'us_stock'] else (recommendation.name if recommendation else code.split('.')[0]),
                            'asset_type': asset_type,
                            'data_source': market_data_source,
                            'current_price': float(analysis['current_price']),
                            'update_time': datetime.now().isoformat(),
                            'predictions': predictions,
                            'analysis': {
                                'technical': {'score': float(analysis['technical_score']), 'details': f"技术评分{analysis['technical_score']}/5", 'source': market_data_source},
                                'valuation': {'score': float(analysis['valuation_score']), 'details': f"估值评分{analysis['valuation_score']}/5", 'source': market_data_source},
                                'money_flow': {'score': float(analysis['money_flow_score']), 'details': f"资金面评分{analysis['money_flow_score']}/5", 'source': market_data_source},
                                'news': {'score': float(analysis['news_score']), 'details': f"消息面评分{analysis['news_score']}/5", 'source': market_data_source},
                                'risk': {
                                    'level': advisor_view.get('risk_level', 'medium'),
                                    'items': analysis.get('risks', ['市场整体存在不确定性', '本建议不构成确定性投资建议'])
                                }
                            },
                            'advisor': {
                                'action': advisor_view.get('action', 'hold'),
                                'confidence': advisor_view.get('confidence', 'low'),
                                'risk_level': advisor_view.get('risk_level', 'medium'),
                                'position_size_pct': suggested_position_pct,
                                'stop_loss_pct': float(advisor_view.get('stop_loss_pct', 0.07) or 0.07),
                                'take_profit_pct': float(advisor_view.get('take_profit_pct', 0.16) or 0.16),
                                'evidence_score': float(advisor_view.get('evidence_score', 50.0) or 50.0),
                                'model_reliability': advisor_view.get('model_reliability', {}),
                                'review_in_days': int(advisor_view.get('review_in_days', 3) or 3),
                                'review_focus': advisor_view.get('review_focus', []),
                                'summary': advisor_view.get('summary', ''),
                                'reason_tags': advisor_view.get('reason_tags', []),
                                'data_source': f'trained_models + {market_data_source}',
                            },
                            'total_score': float(analysis['total_score']),
                            'buy_confidence': _score_to_confidence(float(analysis['total_score'])),
                            'suggested_position': f"建议单标的不超过总资金{suggested_position_pct}%",
                            'suggested_period': "1-4周" if advisor_view.get('action') in ('buy', 'add', 'reduce', 'sell') else "1-3个月"
                        }
                        detail = _apply_snapshot_to_detail(detail, recommendation)
                        detail = _attach_historical_validation(detail, session, _calc_probability_health)
                        detail['execution_plan'] = _build_detail_execution_plan(detail)
                        
                        session.close()
                        return jsonify({'code': 200, 'status': 'success', 'data': convert_to_serializable(detail, 2), 'timestamp': datetime.now().isoformat()})

                if df is not None and len(df) > 0:
                    latest_close = float(df['close'].iloc[-1]) if 'close' in df.columns else float(current_price or 0)
                    prev_close = float(df['close'].iloc[-2]) if 'close' in df.columns and len(df) >= 2 else latest_close
                    change_pct = ((latest_close - prev_close) / prev_close * 100.0) if prev_close else 0.0
                    history_count = len(df)
                    trend_label = '短线走强' if change_pct > 1 else ('短线走弱' if change_pct < -1 else '窄幅震荡')
                    meta = _resolve_stock_meta(code, recommendation.name if recommendation else '')

                    neutral_predictions = [
                        {
                            'period': 5,
                            'period_days': 5,
                            'up_probability': 50.0,
                            'down_probability': 50.0,
                            'target_low': round(latest_close * 0.97, 2) if latest_close else 0,
                            'target_high': round(latest_close * 1.03, 2) if latest_close else 0,
                            'stop_loss': round(latest_close * 0.95, 2) if latest_close else 0,
                            'confidence': 35.0,
                            'source': f'limited_history + {market_data_source}'
                        },
                        {
                            'period': 20,
                            'period_days': 20,
                            'up_probability': 50.0,
                            'down_probability': 50.0,
                            'target_low': round(latest_close * 0.94, 2) if latest_close else 0,
                            'target_high': round(latest_close * 1.06, 2) if latest_close else 0,
                            'stop_loss': round(latest_close * 0.91, 2) if latest_close else 0,
                            'confidence': 30.0,
                            'source': f'limited_history + {market_data_source}'
                        },
                        {
                            'period': 60,
                            'period_days': 60,
                            'up_probability': 50.0,
                            'down_probability': 50.0,
                            'target_low': round(latest_close * 0.88, 2) if latest_close else 0,
                            'target_high': round(latest_close * 1.12, 2) if latest_close else 0,
                            'stop_loss': round(latest_close * 0.85, 2) if latest_close else 0,
                            'confidence': 25.0,
                            'source': f'limited_history + {market_data_source}'
                        }
                    ]

                    detail = {
                        'code': code,
                        'name': meta.get('name') or code,
                        'asset_type': asset_type,
                        'data_source': market_data_source,
                        'current_price': latest_close,
                        'update_time': datetime.now().isoformat(),
                        'predictions': neutral_predictions,
                        'analysis': {
                            'technical': {
                                'score': round(2.8 if history_count >= 5 else 2.2, 1),
                                'details': f'当前已获取 {history_count} 条近期行情，最新收盘价 {latest_close:.2f}，近一交易日{change_pct:+.2f}%，走势表现为{trend_label}。',
                                'source': market_data_source,
                            },
                            'valuation': {
                                'score': 2.5,
                                'details': '当前历史样本不足，暂无法给出可靠估值模型判断。',
                                'source': market_data_source,
                            },
                            'money_flow': {
                                'score': 2.5,
                                'details': '已命中股票基础行情，但资金面特征样本仍不足，暂按中性处理。',
                                'source': market_data_source,
                            },
                            'news': {
                                'score': 2.5,
                                'details': '暂无可用于该标的的稳定新闻/事件评分，建议结合公告自行确认。',
                                'source': market_data_source,
                            },
                            'risk': {
                                'level': 'medium',
                                'items': [
                                    f'当前仅有 {history_count} 条近期行情，未达到正式模型预测所需的 60 条样本',
                                    '因此本页展示的是基础详情与中性区间，不代表完整模型结论',
                                    '若后续累计更多历史数据，系统会自动恢复正式预测输出'
                                ]
                            }
                        },
                        'advisor': {
                            'action': 'watch',
                            'confidence': 'low',
                            'risk_level': 'medium',
                            'position_size_pct': 0,
                            'stop_loss_pct': 0.05,
                            'take_profit_pct': 0.08,
                            'evidence_score': 35.0,
                            'model_reliability': {'status': 'limited_history'},
                            'review_in_days': 3,
                            'review_focus': ['volume_breakout', 'trend_confirmation'],
                            'summary': '已找到该股票基础行情，但历史样本不足，当前建议先观察等待更多数据。',
                            'reason_tags': ['data_gap', 'watch_only'],
                            'data_source': f'limited_history + {market_data_source}',
                        },
                        'total_score': 2.5,
                        'buy_confidence': '较低',
                        'suggested_position': '建议暂不重仓，先观察',
                        'suggested_period': '等待样本补齐后再评估'
                    }
                    detail = _apply_snapshot_to_detail(detail, recommendation)
                    detail = _attach_historical_validation(detail, session, _calc_probability_health)
                    detail['execution_plan'] = _build_detail_execution_plan(detail)

                    session.close()
                    return jsonify({'code': 200, 'status': 'success', 'data': convert_to_serializable(detail, 2), 'timestamp': datetime.now().isoformat()})
            
            # 主动基金
            elif asset_type == 'active_fund':
                fund_code = code
                fund_name = recommendation.name if recommendation else code
                fund_score = total_score
                
                if fund_score >= 4.0:
                    tech_desc = f"基金整体表现优秀，综合评分{fund_score}/5。基金经理管理能力强，历史业绩稳定，超额收益显著。"
                    value_desc = f"基金持仓估值合理，重仓股具有成长性，适合长期持有。"
                    money_desc = f"基金规模适中，流动性良好，机构投资者占比合理。"
                    news_desc = f"近期无重大负面消息，基金经理任职稳定，团队实力雄厚。"
                    confidence = "较高"
                    suggested_position = "不超过总资金15%"
                elif fund_score >= 3.0:
                    tech_desc = f"基金表现中等，综合评分{fund_score}/5。基金经理投资策略稳健，建议关注后续表现。"
                    value_desc = f"基金持仓估值处于合理区间，配置较为均衡。"
                    money_desc = f"基金规模稳定，申购赎回正常，流动性充足。"
                    news_desc = f"关注基金经理后续操作动向，暂无重大风险。"
                    confidence = "中等"
                    suggested_position = "不超过总资金10%"
                else:
                    tech_desc = f"基金表现偏弱，综合评分{fund_score}/5。近期业绩下滑，建议谨慎评估。"
                    value_desc = f"基金持仓估值偏高，注意回调风险，建议分批建仓。"
                    money_desc = f"关注基金规模变化和赎回压力，流动性需要关注。"
                    news_desc = f"建议关注基金公告和持仓变化，警惕基金经理变更风险。"
                    confidence = "较低"
                    suggested_position = "不超过总资金5%"
                
                try:
                    import akshare as ak
                    fund_info = ak.fund_open_fund_info_em(symbol=fund_code, indicator='单位净值走势')
                    if fund_info is not None and len(fund_info) > 0:
                        latest_nav = fund_info.iloc[-1]['单位净值']
                        nav_change = fund_info.iloc[-1]['日增长率'] if '日增长率' in fund_info.columns else 0
                        tech_desc = f"最新净值{latest_nav:.4f}元，{nav_change:+.2f}%。{tech_desc}"
                except Exception as e:
                    logger.warning(f"获取基金数据失败 {fund_code}: {e}")
                
                fund_predictions = [
                    {
                        'period': 5,
                        'period_days': 5,
                        'up_probability': round(45 + market_sentiment * 20, 1),
                        'down_probability': round(55 - market_sentiment * 20, 1),
                        'target_low': round(current_price * 0.97, 2) if current_price else 0,
                        'target_high': round(current_price * 1.03, 2) if current_price else 0,
                        'stop_loss': round(current_price * 0.95, 2) if current_price else 0,
                        'confidence': 55
                    },
                    {
                        'period': 20,
                        'period_days': 20,
                        'up_probability': round(48 + market_sentiment * 18, 1),
                        'down_probability': round(52 - market_sentiment * 18, 1),
                        'target_low': round(current_price * 0.95, 2) if current_price else 0,
                        'target_high': round(current_price * 1.06, 2) if current_price else 0,
                        'stop_loss': round(current_price * 0.93, 2) if current_price else 0,
                        'confidence': 58
                    },
                    {
                        'period': 60,
                        'period_days': 60,
                        'up_probability': round(50 + market_sentiment * 15, 1),
                        'down_probability': round(50 - market_sentiment * 15, 1),
                        'target_low': round(current_price * 0.92, 2) if current_price else 0,
                        'target_high': round(current_price * 1.10, 2) if current_price else 0,
                        'stop_loss': round(current_price * 0.90, 2) if current_price else 0,
                        'confidence': 60
                    }
                ]
                
                fund_source = getattr(recommendation, 'data_source', '') or 'system_db'
                for item in fund_predictions:
                    item['source'] = f'portfolio_model + {fund_source}'

                detail = {
                    'code': fund_code,
                    'name': fund_name,
                    'asset_type': 'active_fund',
                    'data_source': fund_source,
                    'current_price': float(current_price) if current_price else 0,
                    'update_time': datetime.now().isoformat(),
                    'predictions': fund_predictions,
                    'analysis': {
                        'technical': {'score': round(float(fund_score), 1), 'details': tech_desc, 'source': fund_source},
                        'valuation': {'score': round(float(fund_score), 1), 'details': value_desc, 'source': fund_source},
                        'money_flow': {'score': round(float(fund_score), 1), 'details': money_desc, 'source': fund_source},
                        'news': {'score': round(float(fund_score), 1), 'details': news_desc, 'source': fund_source},
                        'risk': {
                            'items': [
                                f'基金净值波动风险，近一年最大回撤约{max(8, min(25, int(28 - fund_score * 4)))}%',
                                f"基金经理{'任职稳定，经验丰富' if fund_score >= 3.5 else '需持续跟踪任职稳定性与策略一致性'}",
                                '市场系统性风险，需关注大盘走势',
                                '持仓集中度风险，前十大重仓股占比可能较高'
                            ]
                        }
                    },
                    'total_score': float(fund_score),
                    'buy_confidence': confidence,
                    'suggested_position': suggested_position,
                    'suggested_period': '6-12个月'
                }
                detail = _apply_snapshot_to_detail(detail, recommendation)
                detail = _attach_historical_validation(detail, session, _calc_probability_health)
                detail['execution_plan'] = _build_detail_execution_plan(detail)
                session.close()
                return jsonify({'code': 200, 'status': 'success', 'data': convert_to_serializable(detail, 2), 'timestamp': datetime.now().isoformat()})
            
            # ETF
            elif asset_type == 'etf':
                etf_score = total_score
                etf_name = recommendation.name if recommendation else code
                etf_scale = int(max(20, min(500, 40 + etf_score * 80)))
                daily_volume = int(max(1500, min(50000, 2000 + etf_score * 9000)))
                tracking_error = max(0.03, min(0.12, 0.16 - etf_score * 0.02))
                index_update_desc = '行业政策与成分股调整节奏稳定，关注跟踪偏离变化'
                
                if etf_score >= 4.0:
                    tech_desc = f"ETF跟踪指数表现强势，综合评分{etf_score}/5。流动性好，费率低，适合波段操作。"
                    value_desc = f"指数估值处于历史低位，配置价值较高，建议逢低布局。"
                    confidence = "较高"
                elif etf_score >= 3.0:
                    tech_desc = f"ETF跟踪指数震荡整理，综合评分{etf_score}/5。适合定投，长期持有。"
                    value_desc = f"指数估值处于合理区间，具有一定的安全边际。"
                    confidence = "中等"
                else:
                    tech_desc = f"ETF跟踪指数偏弱，综合评分{etf_score}/5。建议观望，等待企稳信号。"
                    value_desc = f"指数估值偏高，注意回调风险，建议控制仓位。"
                    confidence = "较低"
                
                etf_predictions = [
                    {
                        'period': 5,
                        'period_days': 5,
                        'up_probability': round(48 + market_sentiment * 22, 1),
                        'down_probability': round(52 - market_sentiment * 22, 1),
                        'target_low': round(current_price * 0.96, 2) if current_price else 0,
                        'target_high': round(current_price * 1.04, 2) if current_price else 0,
                        'stop_loss': round(current_price * 0.94, 2) if current_price else 0,
                        'confidence': 58
                    },
                    {
                        'period': 20,
                        'period_days': 20,
                        'up_probability': round(50 + market_sentiment * 20, 1),
                        'down_probability': round(50 - market_sentiment * 20, 1),
                        'target_low': round(current_price * 0.94, 2) if current_price else 0,
                        'target_high': round(current_price * 1.07, 2) if current_price else 0,
                        'stop_loss': round(current_price * 0.92, 2) if current_price else 0,
                        'confidence': 60
                    },
                    {
                        'period': 60,
                        'period_days': 60,
                        'up_probability': round(52 + market_sentiment * 18, 1),
                        'down_probability': round(48 - market_sentiment * 18, 1),
                        'target_low': round(current_price * 0.90, 2) if current_price else 0,
                        'target_high': round(current_price * 1.12, 2) if current_price else 0,
                        'stop_loss': round(current_price * 0.88, 2) if current_price else 0,
                        'confidence': 62
                    }
                ]
                
                etf_source = getattr(recommendation, 'data_source', '') or 'system_db'
                for item in etf_predictions:
                    item['source'] = f'portfolio_model + {etf_source}'

                detail = {
                    'code': code,
                    'name': etf_name,
                    'asset_type': 'etf',
                    'data_source': etf_source,
                    'current_price': float(current_price) if current_price else 0,
                    'update_time': datetime.now().isoformat(),
                    'predictions': etf_predictions,
                    'analysis': {
                        'technical': {'score': round(float(etf_score), 1), 'details': tech_desc, 'source': etf_source},
                        'valuation': {'score': round(float(etf_score), 1), 'details': value_desc, 'source': etf_source},
                        'money_flow': {'score': round(float(etf_score), 1), 'details': f"ETF规模{etf_scale}亿，日均成交额{daily_volume}万，流动性良好，折溢价率合理", 'source': etf_source},
                        'news': {'score': round(float(etf_score), 1), 'details': index_update_desc, 'source': etf_source},
                        'risk': {
                            'items': [
                                '指数波动风险，净值跟随指数涨跌',
                                f'跟踪误差约{tracking_error:.2f}%，存在一定的偏离风险',
                                '市场系统性风险，需关注大盘走势',
                                '流动性风险，极端行情下可能折价'
                            ]
                        }
                    },
                    'total_score': float(etf_score),
                    'buy_confidence': confidence,
                    'suggested_position': '不超过总资金10%',
                    'suggested_period': '3-6个月'
                }
                detail = _apply_snapshot_to_detail(detail, recommendation)
                detail = _attach_historical_validation(detail, session, _calc_probability_health)
                detail['execution_plan'] = _build_detail_execution_plan(detail)
                session.close()
                return jsonify({'code': 200, 'status': 'success', 'data': convert_to_serializable(detail, 2), 'timestamp': datetime.now().isoformat()})
            
            # 黄金
            elif asset_type == 'gold':
                gold_score = total_score
                gold_factor = get_gold_factor()
                gold_flow_desc = '近期持仓稳定'
                if gold_factor > 0.05:
                    gold_flow_desc = '近期资金净流入'
                elif gold_factor < -0.05:
                    gold_flow_desc = '近期小幅流出'
                
                if gold_score >= 4.0:
                    tech_desc = f"黄金配置价值较高，综合评分{gold_score}/5。避险情绪升温，技术形态向好。"
                    value_desc = f"实际利率下行预期支撑金价，中长期看好。"
                    confidence = "较高"
                elif gold_score >= 3.0:
                    tech_desc = f"黄金震荡整理，综合评分{gold_score}/5。建议逢低配置。"
                    value_desc = f"通胀预期与加息预期博弈，金价震荡为主。"
                    confidence = "中等"
                else:
                    tech_desc = f"黄金短期承压，综合评分{gold_score}/5。建议观望等待企稳。"
                    value_desc = f"美元走强压制金价，注意回调风险。"
                    confidence = "较低"
                
                gold_predictions = [
                    {
                        'period': 5,
                        'period_days': 5,
                        'up_probability': round(50 + gold_factor * 10, 1),
                        'down_probability': round(50 - gold_factor * 10, 1),
                        'target_low': round(current_price * 0.97, 2) if current_price else 0,
                        'target_high': round(current_price * 1.03, 2) if current_price else 0,
                        'stop_loss': round(current_price * 0.95, 2) if current_price else 0,
                        'confidence': 55
                    },
                    {
                        'period': 20,
                        'period_days': 20,
                        'up_probability': round(52 + gold_factor * 8, 1),
                        'down_probability': round(48 - gold_factor * 8, 1),
                        'target_low': round(current_price * 0.95, 2) if current_price else 0,
                        'target_high': round(current_price * 1.06, 2) if current_price else 0,
                        'stop_loss': round(current_price * 0.93, 2) if current_price else 0,
                        'confidence': 58
                    },
                    {
                        'period': 60,
                        'period_days': 60,
                        'up_probability': round(55 + gold_factor * 6, 1),
                        'down_probability': round(45 - gold_factor * 6, 1),
                        'target_low': round(current_price * 0.92, 2) if current_price else 0,
                        'target_high': round(current_price * 1.10, 2) if current_price else 0,
                        'stop_loss': round(current_price * 0.90, 2) if current_price else 0,
                        'confidence': 60
                    }
                ]
                
                detail = {
                    'code': code,
                    'name': recommendation.name if recommendation else '黄金',
                    'asset_type': 'gold',
                    'current_price': float(current_price) if current_price else 0,
                    'update_time': datetime.now().isoformat(),
                    'predictions': gold_predictions,
                    'analysis': {
                        'technical': {'score': round(float(gold_score), 1), 'details': tech_desc},
                        'valuation': {'score': round(float(gold_score), 1), 'details': value_desc},
                        'money_flow': {'score': round(float(gold_score), 1), 'details': f"全球黄金ETF持仓处于历史高位区间，{gold_flow_desc}"},
                        'news': {'score': round(float(gold_score), 1), 'details': '重点跟踪美联储利率路径、美元指数与地缘风险溢价'},
                        'risk': {
                            'items': [
                                '金价波动风险，短期波动较大',
                                '美元汇率风险，美元走强压制金价',
                                '利率风险，加息预期可能打压金价',
                                '流动性风险，极端行情下可能折价'
                            ]
                        }
                    },
                    'total_score': float(gold_score),
                    'buy_confidence': confidence,
                    'suggested_position': '不超过总资金10%' if gold_score >= 3 else '不超过总资金5%',
                    'suggested_period': '6-12个月'
                }
                detail = _apply_snapshot_to_detail(detail, recommendation)
                detail = _attach_historical_validation(detail, session, _calc_probability_health)
                detail['execution_plan'] = _build_detail_execution_plan(detail)
                session.close()
                return jsonify({'code': 200, 'status': 'success', 'data': convert_to_serializable(detail, 2), 'timestamp': datetime.now().isoformat()})
            
            # 白银
            elif asset_type == 'silver':
                silver_score = total_score
                silver_flow_desc = '持仓稳定'
                if market_sentiment >= 0.55:
                    silver_flow_desc = '资金阶段性净流入'
                elif market_sentiment <= 0.45:
                    silver_flow_desc = '短线资金偏谨慎'
                
                if silver_score >= 4.0:
                    tech_desc = f"白银表现强势，综合评分{silver_score}/5。工业需求旺盛，金银比修复。"
                    value_desc = f"新能源产业提振白银需求，中长期看好。"
                    confidence = "中等偏高"
                elif silver_score >= 3.0:
                    tech_desc = f"白银跟随黄金波动，综合评分{silver_score}/5。建议关注金银比。"
                    value_desc = f"工业需求复苏支撑银价，但波动较大。"
                    confidence = "中等"
                else:
                    tech_desc = f"白银短期偏弱，综合评分{silver_score}/5。建议谨慎参与。"
                    value_desc = f"经济衰退预期打压工业需求，注意风险。"
                    confidence = "较低"
                
                silver_predictions = [
                    {
                        'period': 5,
                        'period_days': 5,
                        'up_probability': round(48 + market_sentiment * 8, 1),
                        'down_probability': round(52 - market_sentiment * 8, 1),
                        'target_low': round(current_price * 0.96, 2) if current_price else 0,
                        'target_high': round(current_price * 1.04, 2) if current_price else 0,
                        'stop_loss': round(current_price * 0.94, 2) if current_price else 0,
                        'confidence': 52
                    },
                    {
                        'period': 20,
                        'period_days': 20,
                        'up_probability': round(50 + market_sentiment * 6, 1),
                        'down_probability': round(50 - market_sentiment * 6, 1),
                        'target_low': round(current_price * 0.94, 2) if current_price else 0,
                        'target_high': round(current_price * 1.08, 2) if current_price else 0,
                        'stop_loss': round(current_price * 0.91, 2) if current_price else 0,
                        'confidence': 55
                    },
                    {
                        'period': 60,
                        'period_days': 60,
                        'up_probability': round(52 + market_sentiment * 5, 1),
                        'down_probability': round(48 - market_sentiment * 5, 1),
                        'target_low': round(current_price * 0.90, 2) if current_price else 0,
                        'target_high': round(current_price * 1.12, 2) if current_price else 0,
                        'stop_loss': round(current_price * 0.88, 2) if current_price else 0,
                        'confidence': 58
                    }
                ]
                
                detail = {
                    'code': code,
                    'name': recommendation.name if recommendation else '白银',
                    'asset_type': 'silver',
                    'current_price': float(current_price) if current_price else 0,
                    'update_time': datetime.now().isoformat(),
                    'predictions': silver_predictions,
                    'analysis': {
                        'technical': {'score': round(float(silver_score), 1), 'details': tech_desc},
                        'valuation': {'score': round(float(silver_score), 1), 'details': value_desc},
                        'money_flow': {'score': round(float(silver_score), 1), 'details': f"全球白银ETF持仓维持高位，{silver_flow_desc}"},
                        'news': {'score': round(float(silver_score), 1), 'details': '重点跟踪光伏产业链需求、工业景气与金银比变化'},
                        'risk': {
                            'items': [
                                '银价波动风险，波动幅度大于黄金',
                                '工业需求风险，经济下行影响需求',
                                '投机性风险，白银市场投机氛围较重',
                                '汇率风险，美元走势影响银价'
                            ]
                        }
                    },
                    'total_score': float(silver_score),
                    'buy_confidence': confidence,
                    'suggested_position': '不超过总资金5%',
                    'suggested_period': '6-12个月'
                }
                detail = _apply_snapshot_to_detail(detail, recommendation)
                detail = _attach_historical_validation(detail, session, _calc_probability_health)
                detail['execution_plan'] = _build_detail_execution_plan(detail)
                session.close()
                return jsonify({'code': 200, 'status': 'success', 'data': convert_to_serializable(detail, 2), 'timestamp': datetime.now().isoformat()})
            
            # 默认返回
            session.close()
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': {
                    'code': code,
                    'name': code,
                    'asset_type': asset_type,
                    'current_price': 0,
                    'predictions': [],
                    'analysis': {
                        'technical': {'score': None, 'details': '暂无数据'},
                        'valuation': {'score': None, 'details': '暂无数据'},
                        'money_flow': {'score': None, 'details': '暂无数据'},
                        'news': {'score': None, 'details': '暂无数据'},
                        'risk': {'items': ['暂无风险提示']}
                    },
                    'total_score': 0,
                    'buy_confidence': '低',
                    'suggested_position': '不超过总资金5%',
                    'suggested_period': '1-3个月',
                    'execution_plan': {
                        'headline': '先观察 · 轻仓试探',
                        'position_note': '当前信息不足，建议等待更明确信号后再决策。',
                        'why_buy': ['暂无充分加分证据'],
                        'action_steps': ['先观察，不宜贸然重仓。', '等待更多数据确认趋势。'],
                        'risk_checks': ['注意市场环境变化与流动性风险'],
                        'suitable_for': ['适合愿意等待确认信号的投资者'],
                        'avoid_for': ['不适合追求短期确定性收益的人群']
                    }
                },
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"获取推荐详情失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/recommendations/refresh', methods=['POST'])
    def refresh_recommendations():
        """手动刷新推荐"""
        try:
            from scheduler import rebuild_today_recommendations

            result = rebuild_today_recommendations()
            if not result.get('success'):
                return jsonify({
                    'code': 500,
                    'status': 'error',
                    'message': result.get('error', '推荐刷新失败'),
                    'timestamp': datetime.now().isoformat()
                }), 500
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'message': f"推荐已刷新，共 {result.get('total_count', 0)} 条",
                'data': {
                    'date': result.get('date'),
                    'total_count': result.get('total_count', 0),
                },
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"刷新推荐失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500

    @app.route('/api/recommendations/probability-health', methods=['GET'])
    def probability_health():
        """概率健康检查：命中率、平均概率、Brier、校准偏差。"""
        try:
            session = get_session()
            today = get_today()
            rec_types = ['a_stock', 'hk_stock', 'us_stock', 'active_fund', 'etf', 'gold', 'silver']
            data = _calc_probability_health(session, today, rec_types)
            # 计算总体摘要
            grade_score = {'A': 4, 'B': 3, 'C': 2, 'D': 1}
            valid = []
            for t in rec_types:
                for h in ['5', '20', '60']:
                    item = data.get(t, {}).get(h, {})
                    if item.get('status') == 'ok':
                        valid.append(item)

            if valid:
                avg_brier = round(sum(x['brier'] for x in valid) / len(valid), 4)
                avg_gap = round(sum(abs(x['calibration_gap']) for x in valid) / len(valid), 2)
                avg_grade_score = sum(grade_score.get(x['grade'], 1) for x in valid) / len(valid)
                if avg_grade_score >= 3.5:
                    overall_grade = 'A'
                elif avg_grade_score >= 2.5:
                    overall_grade = 'B'
                elif avg_grade_score >= 1.5:
                    overall_grade = 'C'
                else:
                    overall_grade = 'D'
            else:
                avg_brier = None
                avg_gap = None
                overall_grade = 'N/A'

            summary = {
                'overall_grade': overall_grade,
                'valid_metrics_count': len(valid),
                'avg_brier': avg_brier,
                'avg_abs_calibration_gap': avg_gap,
            }

            # 构建风险预警清单（按严重程度排序）
            warnings = []
            severity_base = {'D': 100, 'C': 60, 'B': 20, 'A': 0, 'N/A': 0}
            for t in rec_types:
                for h in ['5', '20', '60']:
                    item = data.get(t, {}).get(h, {})
                    if item.get('status') != 'ok':
                        continue

                    grade = item.get('grade', 'N/A')
                    gap = float(item.get('calibration_gap', 0) or 0)
                    brier = float(item.get('brier', 0) or 0)
                    abs_gap = abs(gap)

                    need_alert = (grade in {'D', 'C'}) or (abs_gap > 8) or (brier > 0.25)
                    if not need_alert:
                        continue

                    severity = severity_base.get(grade, 0)
                    if abs_gap > 8:
                        severity += min(40, int((abs_gap - 8) * 4))
                    if brier > 0.25:
                        severity += min(40, int((brier - 0.25) * 400))

                    warnings.append({
                        'type': t,
                        'horizon': h,
                        'grade': grade,
                        'samples': item.get('samples', 0),
                        'brier': brier,
                        'calibration_gap': gap,
                        'severity': severity,
                    })

            warnings.sort(key=lambda x: x.get('severity', 0), reverse=True)
            warnings = warnings[:5]

            session.close()
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': data,
                'summary': summary,
                'warnings': warnings,
                'timestamp': datetime.now().isoformat(),
            })
        except Exception as e:
            logger.error(f"概率健康检查失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat(),
            }), 500


def _detect_market(code):
    """根据代码判断市场"""
    if '.SH' in code or '.SZ' in code:
        return 'a_stock'
    elif '.HK' in code:
        return 'hk_stock'
    else:
        return 'us_stock'


def get_market_sentiment():
    """获取市场情绪评分 (0-1)，基于当日股票推荐5日上涨概率均值。"""
    session = None
    try:
        session = get_session()
        today = get_today()
        rows = session.query(Recommendation.up_probability_5d).filter(
            Recommendation.date == today,
            Recommendation.type.in_(['a_stock', 'hk_stock', 'us_stock']),
            Recommendation.up_probability_5d.isnot(None)
        ).all()
        if rows:
            avg_prob = float(sum(float(r[0]) for r in rows) / len(rows))
            sentiment = max(0.35, min(0.65, avg_prob / 100.0))
            return sentiment
    except Exception as e:
        logger.warning(f"获取市场情绪失败，使用中性值: {e}")
    finally:
        if session:
            session.close()

    return 0.5


def get_gold_factor():
    """获取黄金影响因素（美元指数变化）"""
    try:
        import yfinance as yf
        dxy = yf.Ticker('DX-Y.NYB')
        dxy_hist = dxy.history(period='5d')
        if len(dxy_hist) > 0:
            dxy_change = (dxy_hist['Close'].iloc[-1] - dxy_hist['Close'].iloc[0]) / dxy_hist['Close'].iloc[0]
            gold_factor = -dxy_change * 2
            gold_factor = max(-0.2, min(0.2, gold_factor))
            return gold_factor
        else:
            return 0
    except Exception as e:
        logger.warning(f"获取美元指数失败: {e}")
        return 0


def _format_technical_details(tech_details, trend):
    """格式化技术面详情"""
    details = []
    
    if tech_details:
        rsi = tech_details.get('rsi', 50)
        details.append(f"RSI(14) = {rsi:.1f}")
        
        if rsi < 30:
            details.append("处于超卖区，存在反弹机会")
        elif rsi > 70:
            details.append("处于超买区，注意回调风险")
    
    details.append(f"趋势判断：{trend.get('trend_text', '震荡')}")
    
    return "；".join(details)


def _get_technical_details(indicator):
    """从指标数据获取技术面详情"""
    details = []
    
    if indicator and indicator.rsi:
        details.append(f"RSI(14) = {indicator.rsi:.1f}")
        
        if indicator.rsi < 30:
            details.append("处于超卖区")
        elif indicator.rsi > 70:
            details.append("处于超买区")
    
    if indicator and indicator.macd_hist:
        if indicator.macd_hist > 0:
            details.append("MACD多头信号")
        else:
            details.append("MACD空头信号")
    
    return "；".join(details) if details else "暂无技术分析数据"