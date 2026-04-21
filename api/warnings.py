"""
预警API - api/warnings.py
提供预警相关接口
"""

import sys
import os
import math
from datetime import datetime, timedelta
from flask import jsonify, request

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import get_session, Warning as WarningModel, Holding, Prediction, Recommendation, Review
from utils import get_logger, get_today

logger = get_logger(__name__)


def _safe_float(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _classify_future_direction(avg_up_probability):
    prob = _safe_float(avg_up_probability, 50.0)
    score = max(min(round((prob - 50.0) / 25.0, 2), 1.0), -1.0)

    if prob >= 60:
        direction = 'bullish'
        direction_text = '看涨'
    elif prob >= 53:
        direction = 'slightly_bullish'
        direction_text = '偏多'
    elif prob <= 40:
        direction = 'bearish'
        direction_text = '看跌'
    elif prob <= 47:
        direction = 'slightly_bearish'
        direction_text = '偏空'
    else:
        direction = 'neutral'
        direction_text = '震荡'

    return {
        'avg_up_probability': round(prob, 2),
        'score': score,
        'direction': direction,
        'direction_text': direction_text,
    }


def _normalize_code_key(code):
    code_text = str(code or '').strip().upper()
    if not code_text:
        return ''
    if code_text.endswith('.OF'):
        code_text = code_text[:-3]
    if '.' in code_text:
        code_text = code_text.split('.')[0]
    return code_text


def _infer_recommendation_type(asset_type, code):
    asset_type = str(asset_type or '').strip().lower()
    code_text = str(code or '').strip().upper()
    if asset_type == 'fund':
        return 'active_fund'
    if asset_type == 'etf':
        return 'etf'
    if asset_type == 'gold':
        return 'gold'
    if asset_type == 'silver':
        return 'silver'
    if code_text.endswith('.HK'):
        return 'hk_stock'
    if code_text.endswith('.SH') or code_text.endswith('.SZ'):
        return 'a_stock'
    return 'us_stock'


def _iter_future_trading_days(start_date, period_days):
    dates = []
    cursor = start_date
    target_days = max(int(period_days or 0), 1)
    while len(dates) < target_days:
        cursor += timedelta(days=1)
        if cursor.weekday() >= 5:
            continue
        dates.append(cursor)
    return dates


def _estimate_target_band(base_price, up_probability, period_days, prediction=None):
    if prediction is not None:
        low = _safe_float(getattr(prediction, 'target_low', None), None)
        high = _safe_float(getattr(prediction, 'target_high', None), None)
        if low is not None and high is not None and low > 0 and high > 0:
            return float(low), float(high)

    price = max(_safe_float(base_price, 1.0) or 1.0, 0.01)
    prob = max(5.0, min(95.0, _safe_float(up_probability, 50.0) or 50.0))
    bias = (prob - 50.0) / 50.0
    span = {5: 0.04, 20: 0.09, 60: 0.18}.get(int(period_days or 20), 0.09)

    target_high = price * (1 + span * (0.55 + max(bias, 0.0)))
    target_low = price * (1 - span * (0.45 + max(-bias, 0.0)))
    return round(target_low, 4), round(target_high, 4)


def _build_daily_path(base_price, target_low, target_high, up_probability, period_days, start_date):
    price = max(_safe_float(base_price, 1.0) or 1.0, 0.01)
    prob = max(5.0, min(95.0, _safe_float(up_probability, 50.0) or 50.0))
    bias = (prob - 50.0) / 50.0
    volatility = {5: 0.0035, 20: 0.005, 60: 0.007}.get(int(period_days or 20), 0.005)

    if bias >= 0:
        terminal_price = price + (target_high - price) * min(abs(bias), 1.0)
    else:
        terminal_price = price - (price - target_low) * min(abs(bias), 1.0)

    points = []
    last_price = price
    for index, path_date in enumerate(_iter_future_trading_days(start_date, period_days), start=1):
        progress = index / max(int(period_days or 1), 1)
        drift = (terminal_price - price) * (progress ** 0.92)
        wave = math.sin(progress * math.pi) * price * volatility * (0.4 + abs(bias))
        projected_price = price + drift + wave
        projected_price = max(target_low * 0.985, min(target_high * 1.015, projected_price))

        pct_change = ((projected_price - last_price) / last_price * 100.0) if last_price else 0.0
        if pct_change >= 0.08:
            direction = 'up'
            direction_text = '看涨'
        elif pct_change <= -0.08:
            direction = 'down'
            direction_text = '承压'
        else:
            direction = 'flat'
            direction_text = '震荡'

        trend_score = max(min(round((projected_price - price) / max(price * 0.08, 0.01), 3), 1.0), -1.0)
        confidence = max(50.0, min(95.0, round(55.0 + abs(prob - 50.0) * 0.7 + progress * 10.0, 1)))

        points.append({
            'day': index,
            'date': path_date.isoformat(),
            'price': round(projected_price, 4),
            'pct_change': round(pct_change, 3),
            'direction': direction,
            'direction_text': direction_text,
            'score': trend_score,
            'confidence': confidence,
        })
        last_price = projected_price

    return points


def _build_holding_paths(session):
    today = get_today()
    horizons = (5, 20, 60)

    raw_holdings = session.query(Holding).all()
    merged_holdings = {}
    for holding in raw_holdings:
        code = str(getattr(holding, 'code', '') or '').strip()
        if not code:
            continue
        asset_type = str(getattr(holding, 'asset_type', '') or 'stock').strip() or 'stock'
        key = (asset_type, _normalize_code_key(code))
        if key not in merged_holdings:
            merged_holdings[key] = {
                'code': code,
                'name': getattr(holding, 'name', code),
                'asset_type': asset_type,
                'quantity': float(getattr(holding, 'quantity', 0.0) or 0.0),
                'cost_amount': float(getattr(holding, 'cost_price', 0.0) or 0.0) * float(getattr(holding, 'quantity', 0.0) or 0.0),
            }
        else:
            merged_holdings[key]['quantity'] += float(getattr(holding, 'quantity', 0.0) or 0.0)
            merged_holdings[key]['cost_amount'] += float(getattr(holding, 'cost_price', 0.0) or 0.0) * float(getattr(holding, 'quantity', 0.0) or 0.0)

    prediction_rows = session.query(Prediction).filter(
        Prediction.period_days.in_(horizons)
    ).order_by(Prediction.date.desc(), Prediction.created_at.desc()).all()

    latest_prediction_map = {}
    for pred in prediction_rows:
        try:
            period = int(pred.period_days or 0)
        except Exception:
            continue
        if period not in horizons:
            continue
        normalized_code = _normalize_code_key(getattr(pred, 'code', ''))
        if not normalized_code:
            continue
        key = (normalized_code, period)
        if key not in latest_prediction_map:
            latest_prediction_map[key] = pred

    recommendation_rows = session.query(Recommendation).order_by(
        Recommendation.date.desc(), Recommendation.rank.asc()
    ).all()
    latest_recommendation_map = {}
    for rec in recommendation_rows:
        normalized_code = _normalize_code_key(getattr(rec, 'code', ''))
        if not normalized_code or normalized_code in latest_recommendation_map:
            continue
        latest_recommendation_map[normalized_code] = rec

    holding_options = []
    holding_paths = {f'{period}d': [] for period in horizons}

    for holding in merged_holdings.values():
        quantity = float(holding['quantity'] or 0.0)
        cost_based_price = round((float(holding['cost_amount']) / quantity), 4) if quantity > 0 else 1.0
        normalized_code = _normalize_code_key(holding['code'])
        rec = latest_recommendation_map.get(normalized_code)
        rec_type = _infer_recommendation_type(holding['asset_type'], holding['code'])
        if rec is not None and str(getattr(rec, 'type', '') or '').strip().lower() != rec_type:
            rec = None
        base_price = _safe_float(getattr(rec, 'current_price', None) if rec else None, cost_based_price) or cost_based_price

        holding_options.append({
            'code': holding['code'],
            'name': holding['name'],
            'asset_type': holding['asset_type'],
        })

        for period in horizons:
            pred = latest_prediction_map.get((normalized_code, period))
            if rec is not None:
                if period == 5:
                    up_probability = _safe_float(getattr(rec, 'up_probability_5d', None), None)
                elif period == 20:
                    up_probability = _safe_float(getattr(rec, 'up_probability_20d', None), None)
                else:
                    up_probability = _safe_float(getattr(rec, 'up_probability_60d', None), None)
            else:
                up_probability = None

            if up_probability is None and pred is not None and getattr(pred, 'up_probability', None) is not None:
                up_probability = _safe_float(pred.up_probability, 50.0)

            up_probability = up_probability if up_probability is not None else 50.0

            down_probability = round(100.0 - up_probability, 2)
            target_source = pred
            if target_source is None and rec is not None:
                target_source = type('RecTarget', (), {
                    'target_low': getattr(rec, f'target_low_{period}d', None),
                    'target_high': getattr(rec, f'target_high_{period}d', None),
                })()
            target_low, target_high = _estimate_target_band(base_price, up_probability, period, prediction=target_source)
            direction_meta = _classify_future_direction(up_probability)
            path = _build_daily_path(base_price, target_low, target_high, up_probability, period, today)

            up_days = sum(1 for item in path if item['direction'] == 'up')
            down_days = sum(1 for item in path if item['direction'] == 'down')
            flat_days = sum(1 for item in path if item['direction'] == 'flat')

            holding_paths[f'{period}d'].append({
                'code': holding['code'],
                'name': holding['name'],
                'asset_type': holding['asset_type'],
                'current_price': round(base_price, 4),
                'up_probability': round(up_probability, 2),
                'down_probability': down_probability,
                'direction': direction_meta['direction'],
                'direction_text': direction_meta['direction_text'],
                'target_low': round(target_low, 4),
                'target_high': round(target_high, 4),
                'up_days': up_days,
                'down_days': down_days,
                'flat_days': flat_days,
                'source_date': pred.date.isoformat() if pred and getattr(pred, 'date', None) else None,
                'path': path,
            })

    holding_options.sort(key=lambda item: (item.get('asset_type') or '', item.get('code') or ''))
    for key in holding_paths:
        holding_paths[key].sort(key=lambda item: (-(item.get('up_probability') or 0.0), item.get('code') or ''))

    return holding_options, holding_paths


def _build_holding_replays(session, limit=8):
    horizons = (5, 20, 60)
    raw_holdings = session.query(Holding).all()
    merged_holdings = {}
    for holding in raw_holdings:
        code = str(getattr(holding, 'code', '') or '').strip()
        if not code:
            continue
        asset_type = str(getattr(holding, 'asset_type', '') or 'stock').strip() or 'stock'
        key = (asset_type, _normalize_code_key(code))
        if key not in merged_holdings:
            merged_holdings[key] = {
                'code': code,
                'name': getattr(holding, 'name', code),
                'asset_type': asset_type,
            }

    prediction_rows = session.query(Prediction).filter(
        Prediction.period_days.in_(horizons)
    ).order_by(Prediction.date.desc(), Prediction.created_at.desc()).all()

    recommendation_rows = session.query(Recommendation).order_by(
        Recommendation.date.desc(), Recommendation.rank.asc()
    ).all()
    latest_recommendation_map = {}
    for rec in recommendation_rows:
        normalized_code = _normalize_code_key(getattr(rec, 'code', ''))
        if not normalized_code or normalized_code in latest_recommendation_map:
            continue
        latest_recommendation_map[normalized_code] = rec

    review_rows = session.query(Review).order_by(Review.reviewed_at.desc()).all()
    review_by_prediction = {
        int(getattr(review, 'prediction_id', 0) or 0): review
        for review in review_rows
        if getattr(review, 'prediction_id', None)
    }

    holding_replays = {f'{period}d': [] for period in horizons}
    for holding in merged_holdings.values():
        normalized_code = _normalize_code_key(holding['code'])
        rec = latest_recommendation_map.get(normalized_code)
        rec_type = _infer_recommendation_type(holding['asset_type'], holding['code'])
        if rec is not None and str(getattr(rec, 'type', '') or '').strip().lower() != rec_type:
            rec = None

        matched_predictions = [
            pred for pred in prediction_rows
            if _normalize_code_key(getattr(pred, 'code', '')) == normalized_code
        ]

        for period in horizons:
            deduped = []
            seen = set()
            for pred in matched_predictions:
                try:
                    pred_period = int(pred.period_days or 0)
                except Exception:
                    continue
                if pred_period != period:
                    continue
                dedupe_key = (getattr(pred, 'date', None), getattr(pred, 'expiry_date', None), pred_period)
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                deduped.append(pred)
                if len(deduped) >= limit:
                    break

            replay_items = []
            for pred in deduped:
                review = review_by_prediction.get(int(getattr(pred, 'id', 0) or 0))
                actual_return = _safe_float(getattr(pred, 'actual_return', None), None)
                if actual_return is None and review is not None:
                    actual_return = _safe_float(getattr(review, 'actual_return', None), None)

                is_direction_correct = getattr(pred, 'is_direction_correct', None)
                if is_direction_correct is None and review is not None:
                    is_direction_correct = getattr(review, 'is_direction_correct', None)

                if is_direction_correct is None:
                    status = 'pending'
                else:
                    status = 'reviewed'

                predicted_up_probability = _safe_float(getattr(pred, 'up_probability', None), None)
                if rec is not None and (status == 'pending' or predicted_up_probability is None or abs(float(predicted_up_probability or 50.0) - 50.0) < 0.01):
                    if period == 5:
                        predicted_up_probability = _safe_float(getattr(rec, 'up_probability_5d', None), predicted_up_probability)
                    elif period == 20:
                        predicted_up_probability = _safe_float(getattr(rec, 'up_probability_20d', None), predicted_up_probability)
                    else:
                        predicted_up_probability = _safe_float(getattr(rec, 'up_probability_60d', None), predicted_up_probability)
                predicted_up_probability = predicted_up_probability if predicted_up_probability is not None else 50.0
                direction_meta = _classify_future_direction(predicted_up_probability)

                if actual_return is None:
                    actual_direction = 'pending'
                    actual_direction_text = '待验证'
                elif actual_return > 0.3:
                    actual_direction = 'up'
                    actual_direction_text = '实际上涨'
                elif actual_return < -0.3:
                    actual_direction = 'down'
                    actual_direction_text = '实际下跌'
                else:
                    actual_direction = 'flat'
                    actual_direction_text = '实际震荡'

                replay_items.append({
                    'prediction_date': pred.date.isoformat() if getattr(pred, 'date', None) else None,
                    'expiry_date': pred.expiry_date.isoformat() if getattr(pred, 'expiry_date', None) else None,
                    'predicted_up_probability': round(predicted_up_probability, 2),
                    'predicted_direction': direction_meta['direction'],
                    'predicted_direction_text': direction_meta['direction_text'],
                    'actual_return': round(actual_return, 2) if actual_return is not None else None,
                    'actual_direction': actual_direction,
                    'actual_direction_text': actual_direction_text,
                    'is_direction_correct': bool(is_direction_correct) if is_direction_correct is not None else None,
                    'status': status,
                })

            reviewed_count = sum(1 for item in replay_items if item['status'] == 'reviewed')
            hit_count = sum(1 for item in replay_items if item['is_direction_correct'] is True)
            miss_count = sum(1 for item in replay_items if item['is_direction_correct'] is False)
            pending_count = sum(1 for item in replay_items if item['status'] == 'pending')

            holding_replays[f'{period}d'].append({
                'code': holding['code'],
                'name': holding['name'],
                'asset_type': holding['asset_type'],
                'reviewed_count': reviewed_count,
                'hit_count': hit_count,
                'miss_count': miss_count,
                'pending_count': pending_count,
                'replay': replay_items,
            })

    for key in holding_replays:
        holding_replays[key].sort(key=lambda item: (item.get('code') or ''))
    return holding_replays


def _build_warning_trend_payload(session, days):
    today = get_today()
    days = max(int(days or 30), 1)
    start_date = today - timedelta(days=days - 1)

    warning_rows = session.query(WarningModel.warning_time).filter(
        WarningModel.warning_time >= datetime.combine(start_date, datetime.min.time()),
        WarningModel.warning_time <= datetime.combine(today, datetime.max.time())
    ).all()
    warning_count_map = {}
    for (warning_time,) in warning_rows:
        day = warning_time.date() if hasattr(warning_time, 'date') else warning_time
        warning_count_map[day] = warning_count_map.get(day, 0) + 1

    prediction_rows = session.query(Prediction).filter(
        Prediction.date >= start_date,
        Prediction.date <= today,
        Prediction.period_days.in_([5, 20, 60])
    ).all()

    grouped_predictions = {}
    for pred in prediction_rows:
        try:
            period = int(pred.period_days or 0)
        except Exception:
            continue
        if period not in (5, 20, 60):
            continue
        grouped_predictions.setdefault((pred.date, period), []).append(pred)

    warning_counts = []
    future_trends = {f'{period}d': [] for period in (5, 20, 60)}

    for offset in range(days):
        current_date = start_date + timedelta(days=offset)
        warning_counts.append({
            'date': current_date.isoformat(),
            'count': int(warning_count_map.get(current_date, 0)),
        })

        for period in (5, 20, 60):
            items = grouped_predictions.get((current_date, period), [])
            if not items:
                future_trends[f'{period}d'].append({
                    'date': current_date.isoformat(),
                    'score': None,
                    'direction': 'no_data',
                    'direction_text': '无预测',
                    'avg_up_probability': None,
                    'sample_count': 0,
                    'validated_count': 0,
                    'correct_count': 0,
                    'accuracy': None,
                    'status': 'no_data',
                })
                continue

            avg_up_probability = sum(float(item.up_probability or 50.0) for item in items) / len(items)
            validated = [item for item in items if item.is_direction_correct is not None]
            validated_count = len(validated)
            correct_count = sum(1 for item in validated if item.is_direction_correct)
            accuracy = round((correct_count / validated_count) * 100, 1) if validated_count > 0 else None
            direction_meta = _classify_future_direction(avg_up_probability)

            future_trends[f'{period}d'].append({
                'date': current_date.isoformat(),
                'score': direction_meta['score'],
                'direction': direction_meta['direction'],
                'direction_text': direction_meta['direction_text'],
                'avg_up_probability': direction_meta['avg_up_probability'],
                'sample_count': len(items),
                'validated_count': validated_count,
                'correct_count': correct_count,
                'accuracy': accuracy,
                'status': 'validated' if validated_count > 0 else 'pending',
            })

    latest_outlook = []
    for period in (5, 20, 60):
        series = future_trends[f'{period}d']
        latest_item = next((item for item in reversed(series) if int(item.get('sample_count') or 0) > 0), None)
        if latest_item:
            latest_outlook.append({
                'period_days': period,
                'direction_text': latest_item.get('direction_text') or '无预测',
                'avg_up_probability': latest_item.get('avg_up_probability'),
                'status': latest_item.get('status') or 'pending',
                'accuracy': latest_item.get('accuracy'),
            })

    holding_options, holding_paths = _build_holding_paths(session)
    holding_replays = _build_holding_replays(session)

    return {
        'warning_counts': warning_counts,
        'future_trends': future_trends,
        'latest_outlook': latest_outlook,
        'holding_options': holding_options,
        'holding_paths': holding_paths,
        'holding_replays': holding_replays,
    }


def _build_warning_action(warning_type, level, suggestion=''):
    warning_type = str(warning_type or '').strip()
    suggestion = str(suggestion or '').strip()

    if level == 'high':
        action_label = '优先处理'
        review_in_hours = 4
    else:
        action_label = '重点观察'
        review_in_hours = 24

    if '死叉' in warning_type or '流出' in warning_type or '下跌' in warning_type or '破位' in warning_type:
        action_label = '减仓检查' if level == 'high' else '设提醒观察'
    elif '超买' in warning_type or '估值偏高' in warning_type:
        action_label = '分批止盈'
    elif '超卖' in warning_type:
        action_label = '等待企稳'
    elif '负面新闻' in warning_type:
        action_label = '复核逻辑'

    return {
        'action_label': action_label,
        'review_in_hours': review_in_hours,
        'advice': suggestion or ('建议立即复核仓位与止损线' if level == 'high' else '建议持续跟踪并等待确认')
    }


def _build_warning_summary(warnings_list, holding_codes):
    high_count = sum(1 for item in warnings_list if item.get('level') == 'high')
    medium_count = sum(1 for item in warnings_list if item.get('level') == 'medium')
    related_holdings_count = sum(1 for item in warnings_list if item.get('related_to_holding'))

    if high_count > 0:
        risk_status = 'high'
        advisor_summary = f'当前有 {high_count} 条高风险预警，建议先处理风险资产，再考虑新增仓位。'
        next_actions = [
            '优先检查高风险标的的止损位与仓位集中度。',
            '对于均线破位 / 死叉类信号，尽量避免逆势加仓。',
            '若为持仓标的，建议当天内至少复核一次。'
        ]
    elif medium_count > 0:
        risk_status = 'medium'
        advisor_summary = f'当前有 {medium_count} 条中风险预警，建议以观察与分批调整为主。'
        next_actions = [
            '设置价格提醒，等待确认信号是否持续。',
            '避免一次性大幅加仓，优先小步试探。',
            '结合推荐页和持仓页交叉验证。'
        ]
    else:
        risk_status = 'low'
        advisor_summary = '当前没有显著风险预警，可按既定计划执行。'
        next_actions = [
            '继续按计划跟踪重点资产。',
            '维持分散配置与纪律化止损。'
        ]

    return {
        'high': high_count,
        'medium': medium_count,
        'related_holdings_count': related_holdings_count,
        'risk_status': risk_status,
        'advisor_summary': advisor_summary,
        'next_actions': next_actions,
    }


def register_warnings_routes(app):
    """注册预警相关路由"""
    
    @app.route('/api/warnings/current', methods=['GET'])
    def get_current_warnings():
        """获取当前预警"""
        try:
            session = get_session()
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            
            warnings = session.query(WarningModel).filter(
                WarningModel.warning_time >= today_start
            ).order_by(WarningModel.warning_time.desc()).all()

            holding_codes = {
                str(code).strip().upper()
                for (code,) in session.query(Holding.code).all()
                if code
            }
            warnings_list = []
            for w in warnings:
                action_meta = _build_warning_action(w.warning_type, w.level, w.suggestion)
                code = str(w.code or '').strip()
                warnings_list.append({
                    'id': w.id,
                    'time': w.warning_time.strftime('%H:%M'),
                    'code': code,
                    'name': w.name,
                    'type': w.warning_type,
                    'level': w.level,
                    'message': w.message,
                    'suggestion': w.suggestion,
                    'action_label': action_meta['action_label'],
                    'review_in_hours': action_meta['review_in_hours'],
                    'related_to_holding': code.upper() in holding_codes,
                })

            summary = _build_warning_summary(warnings_list, holding_codes)
            session.close()
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': {
                    'total': len(warnings_list),
                    'high': summary['high'],
                    'medium': summary['medium'],
                    'related_holdings_count': summary['related_holdings_count'],
                    'risk_status': summary['risk_status'],
                    'advisor_summary': summary['advisor_summary'],
                    'next_actions': summary['next_actions'],
                    'warnings': warnings_list
                },
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"获取当前预警失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/warnings/history', methods=['GET'])
    def get_warning_history():
        """获取历史预警"""
        try:
            page = int(request.args.get('page', 1))
            size = int(request.args.get('size', 20))
            warning_type = request.args.get('type', '')
            start_date = request.args.get('start_date', '')
            end_date = request.args.get('end_date', '')
            
            session = get_session()
            query = session.query(WarningModel)
            
            if warning_type:
                query = query.filter(WarningModel.warning_type == warning_type)
            
            if start_date:
                query = query.filter(WarningModel.warning_time >= start_date)
            
            if end_date:
                query = query.filter(WarningModel.warning_time <= end_date + ' 23:59:59')
            
            total = query.count()
            offset = (page - 1) * size
            
            warnings = query.order_by(WarningModel.warning_time.desc()).offset(offset).limit(size).all()
            
            warnings_list = []
            for w in warnings:
                action_meta = _build_warning_action(w.warning_type, w.level, w.suggestion)
                warnings_list.append({
                    'id': w.id,
                    'date': w.warning_time.strftime('%Y-%m-%d'),
                    'time': w.warning_time.strftime('%H:%M:%S'),
                    'code': w.code,
                    'name': w.name,
                    'type': w.warning_type,
                    'level': w.level,
                    'message': w.message,
                    'suggestion': w.suggestion,
                    'action_label': action_meta['action_label'],
                    'is_sent': w.is_sent
                })
            
            session.close()
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': {
                    'total': total,
                    'page': page,
                    'size': size,
                    'items': warnings_list
                },
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"获取历史预警失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/warnings/stats', methods=['GET'])
    def get_warning_stats():
        """获取预警统计"""
        try:
            session = get_session()
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            
            current_count = session.query(WarningModel).filter(
                WarningModel.warning_time >= today_start
            ).count()
            
            today_new = session.query(WarningModel).filter(
                WarningModel.warning_time >= today_start
            ).count()
            
            total_count = session.query(WarningModel).count()
            
            session.close()
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': {
                    'current_count': current_count,
                    'today_new': today_new,
                    'total_count': total_count
                },
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"获取预警统计失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/warnings/trend', methods=['GET'])
    def get_warning_trend():
        """获取预警趋势与未来周期方向回看。"""
        try:
            days = int(request.args.get('days', 30))
            session = get_session()
            trend_payload = _build_warning_trend_payload(session, days)
            session.close()
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': trend_payload,
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"获取预警趋势失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/warnings/scan', methods=['POST'])
    def scan_warnings():
        """手动扫描预警"""
        try:
            from alerts.monitor import WarningMonitor
            monitor = WarningMonitor()
            warnings_count = monitor.scan_all_holdings()
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': {'warnings_count': warnings_count},
                'message': f'扫描完成，发现 {warnings_count} 条预警',
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"扫描预警失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500