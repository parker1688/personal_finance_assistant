"""
复盘API - api/reviews.py
提供复盘分析接口
"""

import sys
import os
import json
import pickle
import threading
from datetime import datetime, timedelta
from flask import jsonify, request

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import get_session, Review, Prediction, AccuracyStat, ModelVersion, RawFundData, DailyPrice, Holding
from predictors.model_manager import ModelManager
from config import MIN_MODEL_ACCURACY, MIN_MODEL_F1_SCORE, MIN_SHORT_HORIZON_AUC, MAX_SHORT_HORIZON_BRIER
from utils import get_logger, get_today

logger = get_logger(__name__)
_MODEL_MANAGER = ModelManager()
_REVIEW_SYNC_LOCK = threading.Lock()


def _ensure_due_reviews_current(force=False):
    """在页面查询前补做一次到期待复盘预测的追赶同步，避免列表长期停留在旧记录。"""
    session = None
    try:
        session = get_session()
        today = get_today()
        due_now_count = session.query(Prediction).filter(
            Prediction.expiry_date <= today,
            Prediction.is_expired == False
        ).count()
        reviewed_total = session.query(Prediction).filter(
            Prediction.is_direction_correct.isnot(None)
        ).count()
    except Exception as e:
        logger.warning(f"检查到期待复盘预测失败: {e}")
        due_now_count = 0
        reviewed_total = 0
    finally:
        if session:
            session.close()

    if (not force) and due_now_count <= 0 and reviewed_total >= 30:
        return 0

    if not _REVIEW_SYNC_LOCK.acquire(blocking=False):
        logger.info("复盘追赶同步已在执行中，跳过重复触发")
        return 0

    try:
        from reviews.reviewer import Reviewer

        reviewer = Reviewer()
        try:
            reviewed_count = reviewer.check_expired_predictions()
            logger.info(f"复盘API触发追赶同步，处理 {reviewed_count} 条到期预测")
            return int(reviewed_count)
        finally:
            reviewer.close()
    except Exception as e:
        logger.error(f"复盘API追赶同步失败: {e}", exc_info=True)
        return 0
    finally:
        _REVIEW_SYNC_LOCK.release()


def _safe_float(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _to_percent(value):
    """将0-1或0-100格式统一转为百分比值。"""
    v = _safe_float(value)
    if v is None:
        return None
    if 0 <= v <= 1:
        return round(v * 100, 2)
    return round(v, 2)


def _to_ratio(value):
    """将 0-1 或 0-100 指标统一转为 0-1。"""
    v = _safe_float(value)
    if v is None:
        return None
    if 0 <= v <= 1:
        return v
    if 1 < v <= 100:
        return v / 100.0
    return None


def _json_safe(value):
    """将 numpy/pandas 标量和嵌套对象递归转换为原生 JSON 安全类型。"""
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if hasattr(value, 'item'):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _load_runtime_model_metadata(period_days):
    runtime_files = {
        5: os.path.join('data', 'models', 'short_term_model.pkl'),
        20: os.path.join('data', 'models', 'medium_term_model.pkl'),
        60: os.path.join('data', 'models', 'long_term_model.pkl'),
    }
    path = runtime_files.get(period_days)
    if not path or not os.path.exists(path):
        return {}

    try:
        with open(path, 'rb') as f:
            data = pickle.load(f)
        return data.get('metadata', {}) if isinstance(data, dict) else {}
    except Exception as e:
        logger.warning(f"读取运行时模型元数据失败: period={period_days}, err={e}")
        return {}


def _load_short_term_training_reflection():
    path = os.path.join('data', 'models', 'short_term_training_reflection.json')
    if not os.path.exists(path):
        return {'history': [], 'latest': None}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {'history': [], 'latest': None}
        history = data.get('history', [])
        latest = data.get('latest')
        if not isinstance(history, list):
            history = []
        return {'history': history, 'latest': latest}
    except Exception as e:
        logger.warning(f"读取5日训练反思失败: {e}")
        return {'history': [], 'latest': None}


def _load_horizon_optimization_loop(period_days):
    horizon_key = {5: 'short_term', 20: 'medium_term', 60: 'long_term'}.get(int(period_days), f'{int(period_days)}d')
    path = os.path.join('data', 'models', f'{horizon_key}_optimization_loop.json')
    if not os.path.exists(path):
        return {
            'period_days': int(period_days),
            'status': 'not_started',
            'max_rounds': 0,
            'rounds_completed': 0,
            'target_accuracy': None,
            'target_f1': None,
            'best_accuracy': None,
            'best_f1': None,
            'latest_round': None,
            'history': []
        }
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        history = data.get('history') if isinstance(data, dict) else []
        if not isinstance(history, list):
            history = []
        latest_round = history[-1] if history else None
        best_accuracy = None
        best_f1 = None
        for item in history:
            metrics = (item or {}).get('metrics') or {}
            acc = _safe_float(metrics.get('accuracy'))
            f1 = _safe_float(metrics.get('f1'))
            if acc is not None and (best_accuracy is None or acc > best_accuracy):
                best_accuracy = acc
            if f1 is not None and (best_f1 is None or f1 > best_f1):
                best_f1 = f1
        return {
            'period_days': int(period_days),
            'status': data.get('status', 'unknown'),
            'max_rounds': int(data.get('max_rounds', 0) or 0),
            'rounds_completed': len(history),
            'target_accuracy': _safe_float(data.get('target_accuracy')),
            'target_f1': _safe_float(data.get('target_f1')),
            'best_accuracy': best_accuracy,
            'best_f1': best_f1,
            'latest_round': latest_round,
            'history': history[-6:],
            'updated_at': data.get('timestamp'),
        }
    except Exception as e:
        logger.warning(f"读取{period_days}日优化循环失败: {e}")
        return {
            'period_days': int(period_days),
            'status': 'error',
            'max_rounds': 0,
            'rounds_completed': 0,
            'target_accuracy': None,
            'target_f1': None,
            'best_accuracy': None,
            'best_f1': None,
            'latest_round': None,
            'history': []
        }


def _build_optimization_loops_payload():
    return {
        'short_term': _load_horizon_optimization_loop(5),
        'medium_term': _load_horizon_optimization_loop(20),
        'long_term': _load_horizon_optimization_loop(60),
    }


def _load_model_version_metadata(model_version):
    """合并数据库参数与模型文件内 metadata，确保监控口径完整。"""
    metadata = {}
    if model_version is None:
        return metadata

    raw_params = getattr(model_version, 'params', None)
    if raw_params:
        try:
            parsed = json.loads(raw_params)
            if isinstance(parsed, dict):
                metadata.update(parsed)
        except Exception:
            pass

    model_path = getattr(model_version, 'model_path', None)
    if model_path:
        try:
            path = model_path
            if not os.path.isabs(path):
                path = os.path.join(os.path.dirname(os.path.dirname(__file__)), path)
            if os.path.exists(path):
                with open(path, 'rb') as f:
                    payload = pickle.load(f)
                if isinstance(payload, dict) and isinstance(payload.get('metadata'), dict):
                    metadata.update(payload.get('metadata') or {})
        except Exception:
            pass

    if 'validation_accuracy' not in metadata:
        metadata['validation_accuracy'] = getattr(model_version, 'validation_accuracy', None)
    if 'train_data_count' not in metadata:
        metadata['train_data_count'] = getattr(model_version, 'train_data_count', None)
    return metadata


def _extract_version_metrics(model_version):
    """从模型版本记录提取评估指标(0-1口径)。"""
    metadata = _load_model_version_metadata(model_version)

    return {
        'accuracy': _to_ratio(metadata.get('validation_accuracy')),
        'f1': _to_ratio(metadata.get('validation_f1')),
        'auc': _to_ratio(metadata.get('validation_auc')),
        'brier': _safe_float(metadata.get('validation_brier')),
    }


def _calc_gate_status(period_days, metrics):
    accuracy = metrics.get('accuracy')
    f1 = metrics.get('f1')
    auc = metrics.get('auc')
    brier = metrics.get('brier')

    accuracy_ok = (accuracy is not None and float(accuracy) >= MIN_MODEL_ACCURACY)
    f1_ok = (f1 is not None and float(f1) >= MIN_MODEL_F1_SCORE)
    auc_ok = (auc is not None and float(auc) >= MIN_SHORT_HORIZON_AUC) if int(period_days) <= 5 else None
    brier_ok = (brier is not None and float(brier) <= MAX_SHORT_HORIZON_BRIER) if int(period_days) <= 5 else None

    passed, gate, reason = _MODEL_MANAGER.evaluate_validation_gate(period_days, metrics)
    return {
        'passed': bool(passed),
        'gate': gate,
        'reason': reason,
        'accuracy_ok': bool(accuracy_ok),
        'f1_ok': bool(f1_ok),
        'auc_ok': auc_ok,
        'brier_ok': brier_ok,
    }


def _normalize_asset_type(value):
    asset_type = str(value or '').strip().lower()
    if asset_type in ('fund', 'active_fund'):
        return 'active_fund'
    if asset_type in ('gold_silver', 'gold', 'silver'):
        return 'gold'
    return asset_type or 'unknown'


def _build_price_code_candidates(code, asset_type=None):
    code_text = str(code or '').strip().upper()
    if not code_text:
        return []

    candidates = {code_text}
    base = code_text.split('.')[0] if '.' in code_text else code_text
    if base:
        candidates.add(base)
        if str(asset_type or '').strip().lower() in ('fund', 'active_fund'):
            candidates.add(f'{base}.OF')
    return [item for item in sorted(candidates) if item]


def _prediction_has_real_price_source(session, prediction):
    """仅让有可追溯真实行情来源的预测进入准确率统计。"""
    try:
        code = str(getattr(prediction, 'code', '') or '').strip()
        if not code:
            return False

        asset_type = _normalize_asset_type(getattr(prediction, 'asset_type', None))
        relevant_dates = [d for d in [getattr(prediction, 'date', None), getattr(prediction, 'expiry_date', None)] if d]
        if not relevant_dates:
            return False

        code_candidates = _build_price_code_candidates(code, asset_type)
        if not code_candidates:
            return False

        fund_count = session.query(RawFundData).filter(
            RawFundData.code.in_(code_candidates),
            RawFundData.date.in_(relevant_dates)
        ).count()
        daily_count = session.query(DailyPrice).filter(
            DailyPrice.code.in_(code_candidates),
            DailyPrice.date.in_(relevant_dates)
        ).count()

        if asset_type in ('active_fund', 'etf'):
            return (fund_count + daily_count) > 0
        return daily_count > 0 or fund_count > 0
    except Exception as e:
        logger.warning(f"校验预测真实来源失败: {e}")
        return True


def _dedupe_predictions(preds):
    unique = {}
    for p in preds or []:
        key = (p.code, p.date, p.period_days, p.expiry_date)
        unique.setdefault(key, p)
    return list(unique.values())


def _normalize_holding_code(code):
    code_text = str(code or '').strip().upper()
    if not code_text:
        return ''
    if code_text.endswith('.OF'):
        code_text = code_text[:-3]
    return code_text.split('.')[0] if '.' in code_text else code_text


def _get_current_holding_code_keys(session):
    try:
        rows = session.query(Holding.code).all()
    except Exception:
        rows = []
    return {
        _normalize_holding_code(code)
        for (code,) in rows
        if _normalize_holding_code(code)
    }


def _is_current_holding_prediction(item, holding_keys):
    if not holding_keys:
        return True
    code = getattr(item, 'code', None) if not isinstance(item, dict) else item.get('code')
    return _normalize_holding_code(code) in holding_keys


def _load_validated_review_predictions(session, today, holding_keys, lookback_days=90):
    start_date = today - timedelta(days=max(30, int(lookback_days or 90)))
    validated_preds = session.query(Prediction).filter(
        Prediction.expiry_date >= start_date,
        Prediction.is_expired == True,
        Prediction.is_direction_correct.isnot(None)
    ).all()
    validated_preds = _dedupe_predictions([
        p for p in validated_preds if _prediction_has_real_price_source(session, p)
    ])
    holding_preds = [p for p in validated_preds if _is_current_holding_prediction(p, holding_keys)]
    return validated_preds, holding_preds


def _choose_review_sample_scope(all_preds, holding_preds):
    """优先使用当前持仓样本；仅在持仓完全没有真实已验证样本时才回退到全市场。"""
    if holding_preds:
        return holding_preds, 'current_holdings'
    if all_preds:
        return all_preds, 'broad_market'
    return holding_preds or all_preds, 'current_holdings'


def register_reviews_routes(app):
    """注册复盘相关路由"""
    
    @app.route('/api/reviews/accuracy', methods=['GET'])
    def get_accuracy_stats():
        """获取准确率统计"""
        try:
            _ensure_due_reviews_current()
            session = get_session()
            today = get_today()
            holding_keys = _get_current_holding_code_keys(session)
            
            # 优先展示当前持仓样本；若当前持仓在 20/60 日上仍过薄，则自动回退到更广的真实已验证样本。
            all_validated_preds, holding_validated_preds = _load_validated_review_predictions(session, today, holding_keys, lookback_days=90)
            validated_preds, sample_scope = _choose_review_sample_scope(all_validated_preds, holding_validated_preds)

            total_predictions = len(validated_preds)
            total_correct = sum(1 for p in validated_preds if p.is_direction_correct)
            overall_accuracy = (total_correct / total_predictions * 100) if total_predictions > 0 else 0

            # 按周期统计
            by_period = {}
            by_period_counts = {}
            for period in [5, 20, 60]:
                period_preds = [p for p in validated_preds if int(p.period_days or 0) == period]
                period_total = len(period_preds)
                period_correct = sum(1 for p in period_preds if p.is_direction_correct)
                by_period[f'{period}d'] = round((period_correct / period_total * 100), 1) if period_total > 0 else None
                by_period_counts[f'{period}d'] = int(period_total)

            # 按资产类型统计
            by_asset_type = {}
            by_asset_type_counts = {}
            asset_types = ['a_stock', 'hk_stock', 'us_stock', 'active_fund', 'etf', 'gold']
            for asset in asset_types:
                asset_preds = [p for p in validated_preds if _normalize_asset_type(p.asset_type) == asset]
                asset_total = len(asset_preds)
                asset_correct = sum(1 for p in asset_preds if p.is_direction_correct)
                by_asset_type[asset] = round((asset_correct / asset_total * 100), 1) if asset_total > 0 else None
                by_asset_type_counts[asset] = int(asset_total)

            # 按置信度统计
            conf_totals = {'high': 0, 'medium': 0, 'low': 0}
            conf_correct = {'high': 0, 'medium': 0, 'low': 0}
            for p in validated_preds:
                conf = p.confidence or 0
                if 0 <= conf <= 1:
                    conf *= 100
                level = 'high' if conf >= 70 else ('medium' if conf >= 50 else 'low')
                conf_totals[level] += 1
                if p.is_direction_correct:
                    conf_correct[level] += 1
            by_confidence = {
                lvl: round(conf_correct[lvl] / conf_totals[lvl] * 100, 1) if conf_totals[lvl] > 0 else None
                for lvl in ['high', 'medium', 'low']
            }
            by_confidence_counts = {lvl: int(conf_totals[lvl]) for lvl in ['high', 'medium', 'low']}
            
            # 最近复盘记录
            recent_reviews = session.query(Review).order_by(
                Review.reviewed_at.desc()
            ).all()
            
            recent_list = []
            for r in recent_reviews:
                pred = session.query(Prediction).filter(Prediction.id == r.prediction_id).first() if r.prediction_id else None
                if pred and not _prediction_has_real_price_source(session, pred):
                    continue
                if not _is_current_holding_prediction(pred or r, holding_keys):
                    continue
                predicted_direction = 'up' if r.predicted_up_prob > 50 else 'down'
                actual_direction = predicted_direction if r.is_direction_correct else ('down' if predicted_direction == 'up' else 'up')
                recent_list.append({
                    'expiry_date': pred.expiry_date.strftime('%Y-%m-%d') if pred and pred.expiry_date else (r.reviewed_at.strftime('%Y-%m-%d') if r.reviewed_at else ''),
                    'code': r.code,
                    'name': r.name or r.code,
                    'period_days': r.period_days,
                    'predicted_direction': predicted_direction,
                    'actual_direction': actual_direction,
                    'is_direction_correct': r.is_direction_correct,
                    'error_analysis': r.error_analysis or ''
                })

            deduped_recent_list = []
            seen_recent = set()
            for item in recent_list:
                key = (
                    item.get('code'),
                    item.get('period_days'),
                    item.get('predicted_direction'),
                    item.get('actual_direction'),
                    item.get('expiry_date'),
                )
                if key in seen_recent:
                    continue
                seen_recent.add(key)
                deduped_recent_list.append(item)
            recent_list = deduped_recent_list

            pending_validation_count = sum(
                1 for p in session.query(Prediction).filter(Prediction.is_direction_correct.is_(None)).all()
                if (sample_scope != 'current_holdings' or _is_current_holding_prediction(p, holding_keys))
            )
            
            session.close()
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': {
                    'overall_accuracy': round(overall_accuracy, 1),
                    'has_validated_data': bool(total_predictions > 0),
                    'pending_validation_count': int(pending_validation_count),
                    'by_period': by_period,
                    'by_period_counts': by_period_counts,
                    'by_asset_type': by_asset_type,
                    'by_asset_type_counts': by_asset_type_counts,
                    'by_confidence': by_confidence,
                    'by_confidence_counts': by_confidence_counts,
                    'recent_reviews': recent_list,
                    'validated_sample_count': int(total_predictions),
                    'sample_scope': sample_scope,
                    'holding_validated_sample_count': int(len(holding_validated_preds)),
                    'all_validated_sample_count': int(len(all_validated_preds)),
                    'data_authenticity': 'real_market_data_only'
                },
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"获取准确率统计失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/reviews/sync', methods=['POST'])
    def sync_reviews_now():
        """立即执行一次到期预测复盘与统计同步。"""
        try:
            from reviews.reviewer import Reviewer

            reviewer = Reviewer()
            reviewed_count = reviewer.check_expired_predictions()
            reviewer._update_accuracy_stats()
            reviewer.close()

            session = get_session()
            today = get_today()
            holding_keys = _get_current_holding_code_keys(session)
            due_now_count = sum(
                1 for p in session.query(Prediction).filter(
                    Prediction.expiry_date <= today,
                    Prediction.is_expired == False
                ).all()
                if _is_current_holding_prediction(p, holding_keys)
            )
            pending_validation_count = sum(
                1 for p in session.query(Prediction).filter(
                    Prediction.is_direction_correct.is_(None)
                ).all()
                if _is_current_holding_prediction(p, holding_keys)
            )
            validated_reviews = session.query(Review).count()
            accuracy_rows = session.query(AccuracyStat).count()
            session.close()

            return jsonify({
                'code': 200,
                'status': 'success',
                'data': {
                    'reviewed_count': int(reviewed_count),
                    'due_now_count': int(due_now_count),
                    'pending_validation_count': int(pending_validation_count),
                    'validated_reviews': int(validated_reviews),
                    'accuracy_stat_rows': int(accuracy_rows),
                    'message': f'已同步复盘 {int(reviewed_count)} 条到期预测'
                },
                'timestamp': datetime.now().isoformat()
            })

        except Exception as e:
            logger.error(f"手动同步复盘失败: {e}", exc_info=True)
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500

    @app.route('/api/reviews/trend', methods=['GET'])
    def get_accuracy_trend():
        """获取准确率趋势"""
        try:
            _ensure_due_reviews_current()
            days = int(request.args.get('days', 30))
            session = get_session()
            holding_keys = _get_current_holding_code_keys(session)
            
            trend_data = []
            today = get_today()
            
            for i in range(days):
                date = today - timedelta(days=i)
                day_preds = session.query(Prediction).filter(
                    Prediction.expiry_date == date,
                    Prediction.is_expired == True,
                    Prediction.is_direction_correct.isnot(None)
                ).all()
                day_preds = [
                    p for p in day_preds
                    if _prediction_has_real_price_source(session, p) and _is_current_holding_prediction(p, holding_keys)
                ]
                total = len(day_preds)
                correct = sum(1 for p in day_preds if p.is_direction_correct)
                accuracy = round((correct / total * 100), 1) if total > 0 else 0
                
                trend_data.append({
                    'date': date.isoformat(),
                    'accuracy': accuracy
                })
            
            trend_data.reverse()
            session.close()
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': trend_data,
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"获取准确率趋势失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/reviews/report', methods=['GET'])
    def get_review_report():
        """获取复盘报告（真实数据）"""
        try:
            _ensure_due_reviews_current()
            report_type = request.args.get('type', 'weekly')
            session = get_session()
            today = get_today()
            holding_keys = _get_current_holding_code_keys(session)

            if report_type == 'weekly':
                start_date = today - timedelta(days=7)
                period_label = '本周'
                title = "本周复盘统计报告"
            else:
                start_date = today - timedelta(days=30)
                period_label = '本月'
                title = "本月复盘统计报告"

            # 查询该时间段内已到期并验证的预测
            preds = session.query(Prediction).filter(
                Prediction.expiry_date >= start_date,
                Prediction.expiry_date <= today,
                Prediction.is_expired == True,
                Prediction.is_direction_correct.isnot(None)
            ).order_by(Prediction.created_at.desc()).all()

            unique_preds = {}
            for p in preds:
                if not _prediction_has_real_price_source(session, p):
                    continue
                if not _is_current_holding_prediction(p, holding_keys):
                    continue
                unique_preds.setdefault((p.code, p.date, p.period_days), p)
            preds = list(unique_preds.values())

            total = len(preds)
            correct = sum(1 for p in preds if p.is_direction_correct)
            accuracy = round(correct / total * 100, 1) if total > 0 else 0

            # 按周期统计
            period_lines = []
            for period in [5, 20, 60]:
                pp = [p for p in preds if p.period_days == period]
                pt = len(pp)
                pc = sum(1 for p in pp if p.is_direction_correct)
                if pt > 0:
                    acc = round(pc / pt * 100, 1)
                    period_lines.append(f"{period}日预测准确率：{acc}% ({pc}/{pt})")
                else:
                    period_lines.append(f"{period}日预测准确率：样本不足 (0/0)")

            # 按品类统计
            asset_map = {
                'a_stock': 'A股', 'hk_stock': '港股', 'us_stock': '美股',
                'active_fund': '主动基金', 'etf': 'ETF', 'gold': '贵金属'
            }
            asset_lines = []
            for asset_type, label in asset_map.items():
                ap = [p for p in preds if (p.asset_type or '') == asset_type]
                at = len(ap)
                ac = sum(1 for p in ap if p.is_direction_correct)
                if at > 0:
                    asset_lines.append(f"{label}准确率：{round(ac/at*100,1)}% ({ac}/{at})")

            # 最佳预测（方向正确且实际涨幅最大）
            best_preds = sorted(
                [p for p in preds if p.is_direction_correct and p.actual_return is not None],
                key=lambda p: abs(p.actual_return), reverse=True
            )[:3]
            best_lines = []
            for p in best_preds:
                predicted_dir = "上涨" if p.up_probability > 50 else "下跌"
                best_lines.append(
                    f"{p.name or p.code}：预测{predicted_dir}，实际{p.actual_return:+.1f}%"
                )

            # 最差预测（方向错误且实际反向幅度最大）
            worst_preds = sorted(
                [p for p in preds if not p.is_direction_correct and p.actual_return is not None],
                key=lambda p: abs(p.actual_return), reverse=True
            )[:3]
            worst_lines = []
            for p in worst_preds:
                predicted_dir = "上涨" if p.up_probability > 50 else "下跌"
                worst_lines.append(
                    f"{p.name or p.code}：预测{predicted_dir}，实际{p.actual_return:+.1f}%"
                )

            session.close()

            # 组装报告文本
            sep = "━" * 24
            content_lines = [
                f"📊 {title}（{start_date.isoformat()} 至 {today.isoformat()}）",
                "",
                f"{sep} 整体准确率 {sep}",
                f"{period_label}总预测数：{total}",
                f"正确数：{correct}",
                f"准确率：{accuracy}%",
            ]
            if total == 0:
                content_lines.append("\n（暂无已验证的预测数据）")
            else:
                if period_lines:
                    content_lines += ["", f"{sep} 按周期 {sep}"] + period_lines
                if asset_lines:
                    content_lines += ["", f"{sep} 按品类 {sep}"] + asset_lines
                if best_lines:
                    content_lines += ["", f"{sep} {period_label}最佳预测 {sep}"]
                    content_lines += [f"{i+1}. {line}" for i, line in enumerate(best_lines)]
                if worst_lines:
                    content_lines += ["", f"{sep} {period_label}最差预测 {sep}"]
                    content_lines += [f"{i+1}. {line}" for i, line in enumerate(worst_lines)]

            content = "\n".join(content_lines)

            return jsonify({
                'code': 200,
                'status': 'success',
                'data': {
                    'title': title,
                    'content': content
                },
                'timestamp': datetime.now().isoformat()
            })

        except Exception as e:
            logger.error(f"获取复盘报告失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/reviews/recent', methods=['GET'])
    def get_recent_reviews():
        """获取最近复盘记录"""
        try:
            _ensure_due_reviews_current()
            limit = int(request.args.get('limit', 20))
            session = get_session()
            holding_keys = _get_current_holding_code_keys(session)
            
            reviews = session.query(Review).order_by(
                Review.reviewed_at.desc()
            ).all()
            
            reviews_list = []
            for r in reviews:
                pred = session.query(Prediction).filter(Prediction.id == r.prediction_id).first() if r.prediction_id else None
                if pred and not _prediction_has_real_price_source(session, pred):
                    continue
                if not _is_current_holding_prediction(pred or r, holding_keys):
                    continue
                predicted_direction = 'up' if (r.predicted_up_prob or 0) > 50 else 'down'
                actual_direction = predicted_direction if r.is_direction_correct else ('down' if predicted_direction == 'up' else 'up')
                reviews_list.append({
                    'id': r.id,
                    'code': r.code,
                    'name': r.name or r.code,
                    'period_days': r.period_days,
                    'predicted_up_prob': r.predicted_up_prob,
                    'predicted_direction': predicted_direction,
                    'actual_direction': actual_direction,
                    'actual_return': r.actual_return,
                    'is_direction_correct': r.is_direction_correct,
                    'is_target_correct': r.is_target_correct,
                    'error_analysis': r.error_analysis or '',
                    'status': 'reviewed',
                    'reviewed_at': r.reviewed_at.strftime('%Y-%m-%d %H:%M') if r.reviewed_at else '',
                    'expiry_date': pred.expiry_date.strftime('%Y-%m-%d') if pred and pred.expiry_date else '',
                })

            if len(reviews_list) < limit:
                pending_preds = session.query(Prediction).filter(
                    Prediction.is_direction_correct.is_(None)
                ).order_by(
                    Prediction.created_at.desc()
                ).all()
                for p in pending_preds:
                    if not _is_current_holding_prediction(p, holding_keys):
                        continue
                    reviews_list.append({
                        'id': p.id,
                        'code': p.code,
                        'name': p.name or p.code,
                        'period_days': p.period_days,
                        'predicted_up_prob': p.up_probability,
                        'predicted_direction': 'up' if (p.up_probability or 0) > 50 else 'down',
                        'actual_direction': None,
                        'actual_return': None,
                        'is_direction_correct': None,
                        'is_target_correct': None,
                        'error_analysis': '待到期验证',
                        'status': 'pending',
                        'reviewed_at': p.created_at.strftime('%Y-%m-%d %H:%M') if p.created_at else '',
                        'expiry_date': p.expiry_date.strftime('%Y-%m-%d') if p.expiry_date else '',
                    })

            deduped_reviews = []
            seen = set()
            for item in reviews_list:
                key = (
                    item.get('status'),
                    item.get('code'),
                    item.get('period_days'),
                    item.get('predicted_direction'),
                    item.get('actual_direction'),
                    item.get('expiry_date') or item.get('reviewed_at') or '',
                )
                if key in seen:
                    continue
                seen.add(key)
                deduped_reviews.append(item)

            def _recent_sort_key(item):
                raw = item.get('reviewed_at') or item.get('expiry_date') or ''
                try:
                    if len(raw) == 10:
                        return datetime.fromisoformat(raw + ' 00:00:00')
                    return datetime.fromisoformat(raw)
                except Exception:
                    return datetime.min

            reviews_list = sorted(deduped_reviews, key=_recent_sort_key, reverse=True)[:limit]
            
            session.close()
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': reviews_list,
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"获取最近复盘失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
        
    #━━━━━━━━━━━━ 获取反思报告 ━━━━━━━━━━━━

    @app.route('/api/reviews/reflection', methods=['GET'])
    def get_reflection_report():
        """获取反思报告"""
        try:
            from reviews.reflection import ReflectionLearner
            learner = ReflectionLearner()
            report = learner.generate_reflection_report()
            analysis = learner.analyze_errors(days=30)
            learner.close()
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': {
                    'report': report,
                    'analysis': analysis
                },
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"获取反思报告失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500

    @app.route('/api/reviews/learning-status', methods=['GET'])
    def get_learning_status():
        """获取学习闭环状态（复盘→反思→重训）"""
        try:
            status_file = os.path.join('data', 'cache', 'learning_loop_status.json')
            if os.path.exists(status_file):
                with open(status_file, 'r', encoding='utf-8') as f:
                    status_data = json.load(f)
            else:
                status_data = {
                    'updated_at': datetime.now().isoformat(),
                    'last_review': {'status': 'idle', 'message': '暂无复盘记录'},
                    'last_reflection': {'has_adjustments': False, 'adjustments_count': 0, 'retrain_targets': []},
                    'last_retrain': {'status': 'idle', 'periods': [], 'results': {}}
                }

            # 附加最近学习洞察（最近3条）
            session = get_session()
            from models import LearningInsight
            insights = session.query(LearningInsight).order_by(
                LearningInsight.created_at.desc()
            ).limit(3).all()

            latest_insights = []
            for ins in insights:
                latest_insights.append({
                    'created_at': ins.created_at.isoformat() if ins.created_at else None,
                    'error_rate': ins.error_rate,
                    'dominant_pattern': ins.dominant_pattern,
                    'retrain_triggered': bool(ins.retrain_triggered),
                    'suggestion': ins.suggestion,
                    'total_analyzed': ins.total_analyzed,
                })
            session.close()

            status_data['latest_insights'] = latest_insights

            return jsonify({
                'code': 200,
                'status': 'success',
                'data': status_data,
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"获取学习闭环状态失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500

    @app.route('/api/reviews/model-evaluation', methods=['GET'])
    def get_model_evaluation_status():
        """获取模型训练评估状态（用于可信度日常监控）"""
        try:
            session = get_session()
            periods = [5, 20, 60]
            period_results = []

            for period in periods:
                latest = session.query(ModelVersion).filter(
                    ModelVersion.period_days == period
                ).order_by(ModelVersion.created_at.desc(), ModelVersion.id.desc()).first()

                history = session.query(ModelVersion).filter(
                    ModelVersion.period_days == period
                ).order_by(ModelVersion.created_at.desc(), ModelVersion.id.desc()).all()

                runtime_meta = _load_runtime_model_metadata(period)
                version_meta = _load_model_version_metadata(latest)
                merged_meta = dict(version_meta)
                merged_meta.update(runtime_meta or {})

                # 优先取运行时元数据，其次回退到模型文件 metadata 与数据库字段
                raw_metrics = {
                    'accuracy': _to_ratio(merged_meta.get('validation_accuracy', latest.validation_accuracy if latest else None)),
                    'f1': _to_ratio(merged_meta.get('validation_f1')),
                    'precision': _to_ratio(merged_meta.get('validation_precision')),
                    'recall': _to_ratio(merged_meta.get('validation_recall')),
                    'auc': _to_ratio(merged_meta.get('validation_auc')),
                    'brier': _safe_float(merged_meta.get('validation_brier')),
                }
                accuracy_pct = _to_percent(raw_metrics.get('accuracy'))
                f1_pct = _to_percent(raw_metrics.get('f1'))
                precision_pct = _to_percent(raw_metrics.get('precision'))
                recall_pct = _to_percent(raw_metrics.get('recall'))
                auc_pct = _to_percent(raw_metrics.get('auc'))
                brier = raw_metrics.get('brier')

                gate_status = _calc_gate_status(period, raw_metrics)
                quality_ok = gate_status['passed']

                # 计算最近一次通过时间和连续未通过次数
                last_passed_at = None
                consecutive_fail_count = 0
                for idx, ver in enumerate(history):
                    h_metrics = _extract_version_metrics(ver)
                    h_passed = _calc_gate_status(period, h_metrics)['passed']
                    if h_passed:
                        if idx == 0:
                            # 最新版本已通过，连续失败为0
                            consecutive_fail_count = 0
                        last_passed_at = ver.created_at.isoformat() if ver.created_at else None
                        break
                    if idx == consecutive_fail_count:
                        consecutive_fail_count += 1

                if not history:
                    consecutive_fail_count = 0

                period_results.append({
                    'period_days': period,
                    'version': latest.version if latest else None,
                    'train_date': latest.train_date.isoformat() if latest and latest.train_date else None,
                    'created_at': latest.created_at.isoformat() if latest and latest.created_at else None,
                    'is_active': bool(latest.is_active) if latest else False,
                    'metrics': {
                        'accuracy_pct': accuracy_pct,
                        'f1_pct': f1_pct,
                        'precision_pct': precision_pct,
                        'recall_pct': recall_pct,
                        'auc_pct': auc_pct,
                        'brier': round(brier, 4) if brier is not None else None,
                    },
                    'quality_gate': {
                        'min_accuracy_pct': round(MIN_MODEL_ACCURACY * 100, 2),
                        'min_f1_pct': round(MIN_MODEL_F1_SCORE * 100, 2),
                        'min_auc_pct': round(MIN_SHORT_HORIZON_AUC * 100, 2) if period <= 5 else None,
                        'max_brier': MAX_SHORT_HORIZON_BRIER if period <= 5 else None,
                        'accuracy_ok': gate_status['accuracy_ok'],
                        'f1_ok': gate_status['f1_ok'],
                        'auc_ok': gate_status['auc_ok'],
                        'brier_ok': gate_status['brier_ok'],
                        'gate': gate_status['gate'],
                        'gate_reason': gate_status['reason'],
                        'passed': quality_ok,
                    },
                    'last_passed_at': last_passed_at,
                    'consecutive_fail_count': consecutive_fail_count,
                    'train_data_count': latest.train_data_count if latest else None,
                    'status': 'ok' if quality_ok else ('insufficient_metrics' if raw_metrics['accuracy'] is None and raw_metrics['f1'] is None and raw_metrics['auc'] is None else 'below_threshold')
                })

            session.close()

            passed_count = sum(1 for item in period_results if item['quality_gate']['passed'])
            overall_status = 'healthy' if passed_count == len(period_results) else ('warning' if passed_count > 0 else 'risk')

            return jsonify({
                'code': 200,
                'status': 'success',
                'data': {
                    'overall_status': overall_status,
                    'passed_count': passed_count,
                    'total_models': len(period_results),
                    'updated_at': datetime.now().isoformat(),
                    'models': period_results,
                },
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"获取模型评估状态失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500

    @app.route('/api/reviews/short-term-training-reflection', methods=['GET'])
    def get_short_term_training_reflection():
        """获取5日训练反思与迭代趋势。"""
        try:
            data = _load_short_term_training_reflection()
            history = data.get('history') or []
            latest = data.get('latest')

            history_limit = request.args.get('limit', 10, type=int)
            if history_limit is None or history_limit <= 0:
                history_limit = 10

            history_tail = history[-history_limit:]
            pass_count = sum(1 for h in history if isinstance(h, dict) and h.get('passed'))

            return jsonify({
                'code': 200,
                'status': 'success',
                'data': {
                    'total_runs': len(history),
                    'passed_runs': pass_count,
                    'latest': latest,
                    'history': history_tail,
                },
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"获取5日训练反思失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/reviews/optimization-loops', methods=['GET'])
    def get_optimization_loops():
        """获取各周期训练→反思→优化闭环状态。"""
        try:
            data = _build_optimization_loops_payload()
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': data,
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"获取优化闭环状态失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500

    @app.route('/api/reviews/backtest-report', methods=['GET'])
    def get_backtest_report():
        """
        获取未来信号建议的准确性回测报告
        
        Query params:
            days_lookback: int (default=30) - 回看天数
        
        Returns:
            {
                'code': 200,
                'data': {
                    'report_date': datetime,
                    'analysis_period_days': int,
                    'take_profit_analysis': {...},
                    'add_signals_analysis': {...},
                    'overall_accuracy': float (%),
                    'recommendations': str,
                    'sample_results': [...]
                }
            }
        """
        try:
            _ensure_due_reviews_current()
            from reviews.backtest_validator import BacktestValidator
            
            days_lookback = request.args.get('days_lookback', 30, type=int)
            if days_lookback < 1 or days_lookback > 365:
                days_lookback = 30
            
            validator = BacktestValidator()
            report = validator.generate_backtest_report(days_lookback)
            validator.close()
            
            if 'error' in report:
                return jsonify({
                    'code': 500,
                    'status': 'error',
                    'message': report.get('error'),
                    'timestamp': datetime.now().isoformat()
                }), 500
            
            # 格式化报告数据
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': _json_safe({
                    'report_date': report['report_date'].isoformat(),
                    'analysis_period_days': int(report['analysis_period_days']),
                    'take_profit_analysis': {
                        'total_signals': int(report['take_profit_analysis']['total_signals']),
                        'targets_hit': int(report['take_profit_analysis']['targets_hit']),
                        'hit_rate': round(float(report['take_profit_analysis']['hit_rate']), 2),
                        'avg_profit_rate': round(float(report['take_profit_analysis']['avg_profit_rate']), 2),
                        'avg_days_to_target': round(float(report['take_profit_analysis']['avg_days_to_target']), 1),
                    },
                    'add_signals_analysis': {
                        'total_signals': int(report['add_signals_analysis']['total_signals']),
                        'profitable_signals': int(report['add_signals_analysis']['profitable_signals']),
                        'profitable_rate': round(float(report['add_signals_analysis']['profitable_rate']), 2),
                        'avg_price_move': round(float(report['add_signals_analysis']['avg_price_move']), 2),
                        'avg_entry_quality': round(float(report['add_signals_analysis']['avg_entry_quality']), 2),
                    },
                    'overall_accuracy': round(float(report['overall_accuracy']), 2),
                    'action_quality_summary': _json_safe(report.get('action_quality_summary', {})),
                    'recommendations': report['recommendations'],
                    'sample_take_profit': _json_safe(report.get('sample_take_profit_results', [])[:3]),
                    'sample_add_signals': _json_safe(report.get('sample_add_results', [])[:3]),
                }),
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"生成回测报告失败: {e}", exc_info=True)
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
