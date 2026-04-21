"""
模型监控API - api/model.py
提供模型状态查询、训练等接口
"""

import sys
import os
import json
import pickle
import hashlib
import subprocess
import threading
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from flask import jsonify, request, send_file
from werkzeug.utils import secure_filename

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import get_session, ModelVersion, Prediction
from utils import get_logger, get_today
from api.auth import require_admin_access, log_admin_audit

logger = get_logger(__name__)
TRAINING_PROGRESS_FILE = Path(__file__).resolve().parent.parent / 'data' / 'models' / 'training_progress.json'


def _is_pid_alive(pid):
    try:
        pid_val = int(pid)
        if pid_val <= 0:
            return False
        os.kill(pid_val, 0)
        return True
    except Exception:
        return False


def _load_training_progress(progress_file=None):
    """读取全量训练进度，供模型监控页展示实时进度条。"""
    default = {
        'status': 'idle',
        'total_steps': 0,
        'completed_steps': 0,
        'current_step': 0,
        'progress_percent': 0.0,
        'current_asset': None,
        'current_asset_type': None,
        'current_stage': 'idle',
        'message': '当前无训练任务',
        'started_at': None,
        'updated_at': None,
        'finished_at': None,
        'results': [],
    }

    path = Path(progress_file) if progress_file else TRAINING_PROGRESS_FILE
    if not path.exists() or not path.is_file():
        return default

    try:
        raw = json.loads(path.read_text(encoding='utf-8'))
        if not isinstance(raw, dict):
            return default
    except Exception:
        return default

    merged = {**default, **raw}
    try:
        merged['total_steps'] = int(merged.get('total_steps') or 0)
    except Exception:
        merged['total_steps'] = 0
    try:
        merged['completed_steps'] = int(merged.get('completed_steps') or 0)
    except Exception:
        merged['completed_steps'] = 0
    try:
        merged['current_step'] = int(merged.get('current_step') or 0)
    except Exception:
        merged['current_step'] = 0
    try:
        merged['progress_percent'] = float(merged.get('progress_percent') or 0.0)
    except Exception:
        merged['progress_percent'] = 0.0

    if merged['progress_percent'] <= 0 and merged['total_steps'] > 0:
        merged['progress_percent'] = round((merged['completed_steps'] / merged['total_steps']) * 100, 1)

    status = str(merged.get('status') or 'idle')
    pid = merged.get('pid')
    updated_at = _parse_datetime_value(merged.get('updated_at'))
    stale_running = status in {'running', 'starting'} and ((pid is not None and not _is_pid_alive(pid)) or (pid is None and updated_at is not None and (datetime.now() - updated_at) > timedelta(minutes=20)))
    if stale_running:
        merged['status'] = 'failed'
        merged['current_stage'] = 'stopped'
        merged['finished_at'] = merged.get('finished_at') or datetime.now().isoformat()
        merged['updated_at'] = datetime.now().isoformat()
        merged['message'] = f"训练进程已停止，请重新启动（上次资产：{merged.get('current_asset') or '未知'}）"
        try:
            path.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding='utf-8')
        except Exception:
            pass

    return merged


def _select_current_model_version(all_versions):
    """优先显式激活版本；若数据库未标记，则自动回退到最新可用版本。"""
    versions = [v for v in (all_versions or []) if v is not None]
    if not versions:
        return None, 'none'

    def _sort_key(item):
        train_date = getattr(item, 'train_date', None) or datetime.min.date()
        created_at = getattr(item, 'created_at', None) or datetime.min
        accuracy = getattr(item, 'validation_accuracy', None)
        accuracy = float(accuracy) if accuracy is not None else -1.0
        return (train_date, created_at, accuracy)

    active_versions = [v for v in versions if bool(getattr(v, 'is_active', False))]
    if active_versions:
        return sorted(active_versions, key=_sort_key, reverse=True)[0], 'explicit_active'
    return sorted(versions, key=_sort_key, reverse=True)[0], 'latest_fallback'


def _load_model_metadata(model_record):
    """优先从模型文件读取完整验证元数据，回退到数据库字段。"""
    metadata = {}
    if model_record is None:
        return metadata

    try:
        raw_params = getattr(model_record, 'params', None)
        if raw_params:
            parsed = json.loads(raw_params)
            if isinstance(parsed, dict):
                metadata.update(parsed)
    except Exception:
        pass

    model_path = getattr(model_record, 'model_path', None)
    if model_path:
        try:
            candidate = Path(model_path)
            if not candidate.is_absolute():
                candidate = Path(__file__).resolve().parent.parent / candidate
            if candidate.exists() and candidate.is_file():
                with open(candidate, 'rb') as f:
                    payload = pickle.load(f)
                if isinstance(payload, dict) and isinstance(payload.get('metadata'), dict):
                    metadata.update(payload.get('metadata') or {})
        except Exception:
            pass

    if 'validation_accuracy' not in metadata:
        metadata['validation_accuracy'] = getattr(model_record, 'validation_accuracy', None)
    if 'train_data_count' not in metadata:
        metadata['train_data_count'] = getattr(model_record, 'train_data_count', None)
    return metadata


def _resolve_review_coverage_status(reviewed, eligible_due, eligible_reviewed):
    if eligible_due > 0:
        pct = (eligible_reviewed / eligible_due) * 100
        if pct >= 95:
            return 'adequate'
        if pct >= 70:
            return 'limited'
        return 'thin'
    if reviewed >= 100:
        return 'adequate'
    if reviewed >= 30:
        return 'limited'
    return 'thin'


def _build_review_coverage_summary(session):
    """汇总实盘已复盘覆盖率，并补充“到期可复盘覆盖率”口径。"""
    empty_by_period = {
        '5': {
            'total': 0, 'reviewed': 0, 'pending': 0, 'coverage_pct': 0.0,
            'due': 0, 'reviewed_due': 0, 'due_pending': 0, 'due_coverage_pct': 0.0,
            'eligible_due': 0, 'eligible_reviewed': 0, 'eligible_due_pending': 0, 'eligible_due_coverage_pct': 0.0,
            'maturity_status': 'pending',
        },
        '20': {
            'total': 0, 'reviewed': 0, 'pending': 0, 'coverage_pct': 0.0,
            'due': 0, 'reviewed_due': 0, 'due_pending': 0, 'due_coverage_pct': 0.0,
            'eligible_due': 0, 'eligible_reviewed': 0, 'eligible_due_pending': 0, 'eligible_due_coverage_pct': 0.0,
            'maturity_status': 'pending',
        },
        '60': {
            'total': 0, 'reviewed': 0, 'pending': 0, 'coverage_pct': 0.0,
            'due': 0, 'reviewed_due': 0, 'due_pending': 0, 'due_coverage_pct': 0.0,
            'eligible_due': 0, 'eligible_reviewed': 0, 'eligible_due_pending': 0, 'eligible_due_coverage_pct': 0.0,
            'maturity_status': 'pending',
        },
    }

    try:
        from api.reviews import _prediction_has_real_price_source

        today = get_today()
        total = int(session.query(Prediction).count())
        reviewed = int(session.query(Prediction).filter(Prediction.is_direction_correct.isnot(None)).count())
        pending = max(total - reviewed, 0)

        due_predictions = session.query(Prediction).filter(Prediction.expiry_date <= today).all()
        reviewed_due = sum(1 for item in due_predictions if item.is_direction_correct is not None)

        price_source_cache = {}
        eligible_predictions = []
        for item in due_predictions:
            cache_key = (
                str(getattr(item, 'code', '') or '').upper(),
                getattr(item, 'date', None),
                getattr(item, 'expiry_date', None),
                str(getattr(item, 'asset_type', '') or '').lower(),
            )
            if cache_key not in price_source_cache:
                price_source_cache[cache_key] = bool(_prediction_has_real_price_source(session, item))
            if price_source_cache[cache_key]:
                eligible_predictions.append(item)

        eligible_due = len(eligible_predictions)
        eligible_reviewed = sum(1 for item in eligible_predictions if item.is_direction_correct is not None)

        by_period = {}
        for period in (5, 20, 60):
            p_total = int(session.query(Prediction).filter(Prediction.period_days == period).count())
            p_reviewed = int(session.query(Prediction).filter(Prediction.period_days == period, Prediction.is_direction_correct.isnot(None)).count())
            p_due_items = [item for item in due_predictions if int(item.period_days or 0) == period]
            p_due = len(p_due_items)
            p_reviewed_due = sum(1 for item in p_due_items if item.is_direction_correct is not None)
            p_eligible_items = [item for item in eligible_predictions if int(item.period_days or 0) == period]
            p_eligible_due = len(p_eligible_items)
            p_eligible_reviewed = sum(1 for item in p_eligible_items if item.is_direction_correct is not None)

            if p_due <= 0:
                maturity_status = 'pending'
            elif p_eligible_due <= 0:
                maturity_status = 'awaiting_data'
            elif p_eligible_reviewed >= p_eligible_due:
                maturity_status = 'complete'
            else:
                maturity_status = 'partial'

            by_period[str(period)] = {
                'total': p_total,
                'reviewed': p_reviewed,
                'pending': max(p_total - p_reviewed, 0),
                'coverage_pct': round((p_reviewed / p_total) * 100, 2) if p_total else 0.0,
                'due': p_due,
                'reviewed_due': p_reviewed_due,
                'due_pending': max(p_due - p_reviewed_due, 0),
                'due_coverage_pct': round((p_reviewed_due / p_due) * 100, 2) if p_due else 0.0,
                'eligible_due': p_eligible_due,
                'eligible_reviewed': p_eligible_reviewed,
                'eligible_due_pending': max(p_eligible_due - p_eligible_reviewed, 0),
                'eligible_due_coverage_pct': round((p_eligible_reviewed / p_eligible_due) * 100, 2) if p_eligible_due else 0.0,
                'maturity_status': maturity_status,
            }

        return {
            'total_predictions': total,
            'reviewed_predictions': reviewed,
            'pending_predictions': pending,
            'coverage_pct': round((reviewed / total) * 100, 2) if total else 0.0,
            'due_predictions': len(due_predictions),
            'reviewed_due_predictions': reviewed_due,
            'due_pending_predictions': max(len(due_predictions) - reviewed_due, 0),
            'due_coverage_pct': round((reviewed_due / len(due_predictions)) * 100, 2) if due_predictions else 0.0,
            'eligible_due_predictions': eligible_due,
            'eligible_reviewed_predictions': eligible_reviewed,
            'eligible_due_pending_predictions': max(eligible_due - eligible_reviewed, 0),
            'eligible_due_coverage_pct': round((eligible_reviewed / eligible_due) * 100, 2) if eligible_due else 0.0,
            'status': _resolve_review_coverage_status(reviewed, eligible_due, eligible_reviewed),
            'by_period': by_period,
        }
    except Exception:
        return {
            'total_predictions': 0,
            'reviewed_predictions': 0,
            'pending_predictions': 0,
            'coverage_pct': 0.0,
            'due_predictions': 0,
            'reviewed_due_predictions': 0,
            'due_pending_predictions': 0,
            'due_coverage_pct': 0.0,
            'eligible_due_predictions': 0,
            'eligible_reviewed_predictions': 0,
            'eligible_due_pending_predictions': 0,
            'eligible_due_coverage_pct': 0.0,
            'status': 'thin',
            'by_period': empty_by_period,
        }


def _metric_pct(value, digits=2):
    try:
        num = float(value)
        if num <= 1:
            num *= 100
        return round(num, digits)
    except Exception:
        return None


def _metric_num(value, digits=4):
    try:
        return round(float(value), digits)
    except Exception:
        return None


def _assess_overfit_risk(validation_value, train_value):
    """基于训练/验证差距给出是否可能过拟合的提示。"""
    if validation_value is None or train_value is None:
        return None, 'unknown'

    try:
        gap = round(float(train_value) - float(validation_value), 2)
    except Exception:
        return None, 'unknown'

    if gap >= 20:
        return gap, 'high'
    if gap >= 10:
        return gap, 'medium'
    return gap, 'low'


ASSET_LABEL_MAP = {
    'a_stock': 'A股',
    'hk_stock': '港股',
    'us_stock': '美股',
    'fund': '基金',
    'active_fund': '主动基金',
    'etf': 'ETF',
    'gold': '黄金',
    'silver': '白银',
}

ASSET_ORDER_MAP = {
    'a_stock': 1,
    'fund': 2,
    'active_fund': 2,
    'gold': 3,
    'silver': 4,
    'etf': 5,
    'hk_stock': 6,
    'us_stock': 7,
}

ASSET_MODEL_FILES = [
    ('short_term_model.pkl', 'a_stock', 5),
    ('medium_term_model.pkl', 'a_stock', 20),
    ('long_term_model.pkl', 'a_stock', 60),
    ('fund_model.pkl', 'fund', None),
    ('gold_short_term_model.pkl', 'gold', 5),
    ('gold_medium_term_model.pkl', 'gold', 20),
    ('gold_long_term_model.pkl', 'gold', 60),
    ('gold_model.pkl', 'gold', 5),
    ('silver_short_term_model.pkl', 'silver', 5),
    ('silver_medium_term_model.pkl', 'silver', 20),
    ('silver_long_term_model.pkl', 'silver', 60),
    ('silver_model.pkl', 'silver', 5),
    ('etf_short_term_model.pkl', 'etf', 5),
    ('etf_medium_term_model.pkl', 'etf', 20),
    ('etf_long_term_model.pkl', 'etf', 60),
    ('etf_model.pkl', 'etf', 20),
    ('hk_stock_short_term_model.pkl', 'hk_stock', 5),
    ('hk_stock_medium_term_model.pkl', 'hk_stock', 20),
    ('hk_stock_long_term_model.pkl', 'hk_stock', 60),
    ('us_stock_short_term_model.pkl', 'us_stock', 5),
    ('us_stock_medium_term_model.pkl', 'us_stock', 20),
    ('us_stock_long_term_model.pkl', 'us_stock', 60),
]


def _parse_datetime_value(value):
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    except Exception:
        return None


def _build_version_label(raw_version=None, train_date=None, file_path=None):
    if raw_version not in (None, ''):
        return str(raw_version)

    dt = _parse_datetime_value(train_date)
    if dt is not None:
        return f"v{dt.strftime('%Y%m%d_%H%M%S')}"

    if file_path is not None:
        try:
            return f"v{datetime.fromtimestamp(file_path.stat().st_mtime).strftime('%Y%m%d_%H%M%S')}"
        except Exception:
            pass

    return '--'


def _extract_feature_importance_from_payload(payload, topn=10):
    """从运行时模型载荷中提取特征重要性，兼容树模型和线性模型。"""
    try:
        if not isinstance(payload, dict):
            return []

        metadata = payload.get('metadata') or {}
        embedded = metadata.get('feature_importance') or metadata.get('top_features')
        if isinstance(embedded, dict):
            pairs = [(str(k), float(v)) for k, v in embedded.items()]
        elif isinstance(embedded, list):
            pairs = []
            for item in embedded:
                if not isinstance(item, dict):
                    continue
                name = item.get('feature') or item.get('name')
                score = item.get('importance')
                if score is None:
                    score = item.get('share')
                if name in (None, '') or score is None:
                    continue
                pairs.append((str(name), float(score)))
        else:
            pairs = []

        if not pairs:
            model = payload.get('model')
            scores = None
            if model is not None and hasattr(model, 'feature_importances_'):
                scores = np.asarray(model.feature_importances_, dtype=float).reshape(-1)
            elif model is not None and hasattr(model, 'coef_'):
                scores = np.abs(np.asarray(model.coef_, dtype=float)).reshape(-1)

            if scores is None or len(scores) == 0:
                return []

            feature_names = metadata.get('feature_columns') or payload.get('feature_columns') or metadata.get('feature_names')
            if not feature_names or len(feature_names) != len(scores):
                feature_names = [f'f{i+1}' for i in range(len(scores))]

            pairs = [
                (str(feature_names[i]), float(scores[i]))
                for i in range(len(scores))
                if np.isfinite(scores[i])
            ]

        pairs.sort(key=lambda x: x[1], reverse=True)
        return [
            {'feature': name, 'importance': round(score, 6)}
            for name, score in pairs[:max(1, int(topn))]
        ]
    except Exception:
        return []


def _load_feature_importance_for_display(current_model=None):
    """优先读取当前运行模型的真实特征重要性，避免图表空白。"""
    runtime_dir = Path(__file__).resolve().parent.parent / 'data' / 'models'
    candidate_paths = [
        runtime_dir / 'short_term_model.pkl',
        runtime_dir / 'medium_term_model.pkl',
        runtime_dir / 'long_term_model.pkl',
    ]

    if current_model is not None:
        try:
            raw_params = getattr(current_model, 'params', None)
            if raw_params:
                parsed = json.loads(raw_params)
                data = _extract_feature_importance_from_payload({'metadata': parsed})
                if data:
                    return data
        except Exception:
            pass

        model_path = getattr(current_model, 'model_path', None)
        if model_path:
            candidate = Path(model_path)
            if not candidate.is_absolute():
                candidate = Path(__file__).resolve().parent.parent / candidate
            candidate_paths.append(candidate)

    seen = set()
    for path in candidate_paths:
        try:
            path = Path(path)
            key = str(path.resolve())
            if key in seen or (not path.exists()) or (not path.is_file()):
                continue
            seen.add(key)
            with open(path, 'rb') as f:
                payload = pickle.load(f)
            data = _extract_feature_importance_from_payload(payload)
            if data:
                return data
        except Exception:
            continue
    return []


def _extract_payload_metadata(payload, fallback_period_days=None):
    merged = {}
    if isinstance(payload, dict):
        merged.update({k: v for k, v in payload.items() if k != 'metadata'})
        nested = payload.get('metadata')
        if isinstance(nested, dict):
            merged.update(nested or {})

    aliases = {
        'asset_type': ('asset_type',),
        'period_days': ('period_days',),
        'version': ('version',),
        'train_date': ('train_date', 'created_at'),
        'validation_accuracy': ('validation_accuracy', 'val_accuracy'),
        'train_accuracy': ('train_accuracy',),
        'validation_f1': ('validation_f1', 'val_f1'),
        'validation_auc': ('validation_auc', 'val_auc'),
        'validation_brier': ('validation_brier', 'val_brier'),
        'validation_score': ('validation_score', 'validation_r2', 'val_score'),
        'train_score': ('train_score',),
        'train_data_count': ('train_data_count', 'samples', 'sample_count'),
    }

    metadata = {}
    for field, keys in aliases.items():
        for key in keys:
            value = merged.get(key)
            if value not in (None, ''):
                metadata[field] = value
                break

    if metadata.get('period_days') in (None, '') and fallback_period_days is not None:
        metadata['period_days'] = fallback_period_days

    return metadata


def _load_asset_training_results(model_dir=None):
    """从运行时模型文件中汇总各资产最新训练结果。"""
    target_dir = Path(model_dir) if model_dir else Path(__file__).resolve().parent.parent / 'data' / 'models'
    results = []

    for file_name, default_asset_type, default_period in ASSET_MODEL_FILES:
        path = target_dir / file_name
        if not path.exists() or not path.is_file():
            continue

        try:
            with open(path, 'rb') as f:
                payload = pickle.load(f)
        except Exception:
            continue

        metadata = _extract_payload_metadata(payload, fallback_period_days=default_period)
        asset_type = metadata.get('asset_type') or default_asset_type
        asset_label = ASSET_LABEL_MAP.get(asset_type, str(asset_type or '未知资产'))

        try:
            period_days = int(metadata.get('period_days')) if metadata.get('period_days') not in (None, '') else None
        except Exception:
            period_days = default_period

        is_fund = asset_type in {'fund', 'active_fund'}
        metric_label = '历史回测R²' if is_fund else '历史回测准确率'
        metric_value = _metric_pct(metadata.get('validation_score') if is_fund else metadata.get('validation_accuracy'))
        train_metric_label = '样本内R²' if is_fund else '样本内准确率'
        train_metric_value = _metric_pct(metadata.get('train_score') if is_fund else metadata.get('train_accuracy'))
        generalization_gap, overfit_risk = _assess_overfit_risk(metric_value, train_metric_value)

        if metric_value is None:
            status = 'unknown'
        elif (is_fund and metric_value >= 80) or ((not is_fund) and metric_value >= 55):
            status = 'healthy'
        else:
            status = 'warning'

        if overfit_risk == 'high' and status == 'healthy':
            status = 'warning'

        train_date = metadata.get('train_date')
        if not train_date:
            try:
                train_date = datetime.fromtimestamp(path.stat().st_mtime).isoformat()
            except Exception:
                train_date = None

        version_label = _build_version_label(metadata.get('version'), train_date, path)

        results.append({
            'asset_type': asset_type,
            'asset_label': asset_label,
            'period_days': period_days,
            'horizon_label': f'{period_days}日' if period_days else '回归',
            'model_file': path.name,
            'version': version_label,
            'version_display': version_label,
            'metric_label': metric_label,
            'metric_value': metric_value,
            'train_metric_label': train_metric_label,
            'train_metric_value': train_metric_value,
            'generalization_gap': generalization_gap,
            'overfit_risk': overfit_risk,
            'train_date': str(train_date) if train_date else None,
            'status': status,
        })

    deduped = {}
    for item in results:
        key = (item.get('asset_type'), item.get('period_days'))
        previous = deduped.get(key)
        if previous is None or str(item.get('train_date') or '') >= str(previous.get('train_date') or ''):
            deduped[key] = item

    final_results = list(deduped.values())
    final_results.sort(
        key=lambda item: (
            ASSET_ORDER_MAP.get(item.get('asset_type'), 99),
            int(item.get('period_days') or 999),
            item.get('train_date') or '',
        )
    )
    return final_results


def _merge_asset_runtime_versions(asset_training_results, current_map=None):
    """优先使用运行中模型文件的真实版本，避免数据库版本滞后。"""
    merged = dict(current_map or {})
    for item in asset_training_results or []:
        if item.get('asset_type') != 'a_stock':
            continue
        try:
            period = int(item.get('period_days') or 0)
        except Exception:
            period = 0
        if period <= 0:
            continue
        version_value = item.get('version_display') or item.get('version')
        if version_value and version_value != '--':
            merged[period] = version_value

    summary = ' / '.join(
        f"{period}日:{merged[period]}"
        for period in sorted(merged)
        if merged.get(period) and merged.get(period) != '--'
    )
    return summary, merged


def _build_current_runtime_summary(version_rows, runtime_metadata_map=None):
    """构建 5/20/60 日当前使用版本摘要，避免只显示单一周期。"""
    grouped = {}
    for row in version_rows or []:
        period = int(row.get('period_days') or 0)
        if period <= 0:
            continue
        grouped.setdefault(period, []).append(row)

    runtime_metadata_map = runtime_metadata_map or {}
    current_map = {}
    for period, rows in grouped.items():
        rows = sorted(
            rows,
            key=lambda item: (item.get('train_date') or '', item.get('version') or ''),
            reverse=True,
        )
        runtime_meta = runtime_metadata_map.get(int(period or 0)) or {}
        runtime_version = runtime_meta.get('version')
        selected = next((item for item in rows if runtime_version and item.get('version') == runtime_version), None)
        if not selected:
            selected = rows[0] if rows else None
        current_map[int(period)] = (selected or {}).get('version') or runtime_version or '--'

    summary = ' / '.join(
        f"{period}日:{current_map[period]}"
        for period in sorted(current_map)
        if current_map.get(period) and current_map.get(period) != '--'
    )
    return summary, current_map


def _normalize_training_asset_type(value):
    raw = str(value or '').strip().lower()
    mapping = {
        'a': 'a_stock',
        'a_stock': 'a_stock',
        'stock': 'a_stock',
        'fund': 'fund',
        'active_fund': 'fund',
        'gold': 'gold',
        'silver': 'silver',
        'etf': 'etf',
        'hk': 'hk_stock',
        'hk_stock': 'hk_stock',
        'us': 'us_stock',
        'us_stock': 'us_stock',
    }
    return mapping.get(raw, raw)


def _build_training_launch_config(project_root, asset_type=None, period_days=None):
    script_path = project_root / 'scripts' / 'train_asset_suite.py'
    if not script_path.exists():
        raise FileNotFoundError(f'训练脚本不存在: {script_path}')

    normalized_asset = _normalize_training_asset_type(asset_type) if asset_type else None
    allowed_assets = {'a_stock', 'fund', 'gold', 'silver', 'etf', 'hk_stock', 'us_stock'}
    if normalized_asset and normalized_asset not in allowed_assets:
        raise ValueError(f'不支持的资产类型: {asset_type}')

    normalized_period = None
    if period_days not in (None, '', 0, '0'):
        normalized_period = int(period_days)
        if normalized_period not in (5, 20, 60):
            raise ValueError('仅支持 5/20/60 日模型训练')

    cmd = [sys.executable, '-u', str(script_path)]
    total_steps = 7
    task_name = 'full_asset_training'
    current_asset = '准备启动'
    message = '正在启动全量模型训练'

    if normalized_asset:
        asset_label = ASSET_LABEL_MAP.get(normalized_asset, normalized_asset)
        cmd.extend(['--only', normalized_asset])
        total_steps = 1
        current_asset = asset_label
        task_name = f'{normalized_asset}_training'
        message = f'正在启动 {asset_label} 模型训练'
        if normalized_period:
            cmd.extend(['--periods', str(normalized_period)])
            current_asset = f'{asset_label} {normalized_period}日'
            task_name = f'{normalized_asset}_{normalized_period}d_training'
            message = f'正在启动 {asset_label} {normalized_period}日模型训练'

    return {
        'command': cmd,
        'script_path': script_path,
        'total_steps': total_steps,
        'task_name': task_name,
        'current_asset': current_asset,
        'message': message,
        'asset_type': normalized_asset,
        'period_days': normalized_period,
    }


def _build_period_comparison(version_rows, runtime_metadata_map=None):
    grouped = {}
    for row in version_rows or []:
        grouped.setdefault(row.get('period_days'), []).append(row)

    runtime_metadata_map = runtime_metadata_map or {}
    comparisons = []
    for period_days, rows in grouped.items():
        rows = sorted(rows, key=lambda item: (item.get('train_date') or '', item.get('version') or ''), reverse=True)
        current = rows[0] if rows else None
        runtime_meta = runtime_metadata_map.get(int(period_days or 0))
        if runtime_meta:
            runtime_version = runtime_meta.get('version')
            matched = next((item for item in rows if item.get('version') == runtime_version), None)
            current = matched or {
                'version': runtime_version,
                'period_days': period_days,
                'validation_accuracy': _metric_pct(runtime_meta.get('validation_accuracy')),
                'validation_f1': _metric_pct(runtime_meta.get('validation_f1')),
                'validation_auc': _metric_pct(runtime_meta.get('validation_auc')),
                'validation_brier': _metric_num(runtime_meta.get('validation_brier')),
                'train_date': runtime_meta.get('train_date'),
            }
        previous = next((item for item in rows if current and item.get('version') != current.get('version')), None)
        if not current:
            continue

        deltas = {}
        if previous:
            for key in ('validation_accuracy', 'validation_f1', 'validation_auc', 'validation_brier'):
                cur_v = current.get(key)
                prev_v = previous.get(key)
                if cur_v is not None and prev_v is not None:
                    deltas[key] = round(float(cur_v) - float(prev_v), 4)

        status = 'flat'
        if deltas:
            acc_delta = float(deltas.get('validation_accuracy') or 0.0)
            f1_delta = float(deltas.get('validation_f1') or 0.0)
            auc_delta = float(deltas.get('validation_auc') or 0.0)
            brier_delta = float(deltas.get('validation_brier') or 0.0)
            if (acc_delta >= 0 and f1_delta >= 0 and auc_delta >= 0 and brier_delta <= 0):
                status = 'improved'
            elif (acc_delta < 0 or f1_delta < 0 or auc_delta < 0 or brier_delta > 0):
                status = 'weakened'

        comparisons.append({
            'period_days': period_days,
            'current_version': current.get('version'),
            'previous_version': previous.get('version') if previous else None,
            'current_accuracy': current.get('validation_accuracy'),
            'current_f1': current.get('validation_f1'),
            'current_auc': current.get('validation_auc'),
            'current_brier': current.get('validation_brier'),
            'delta_accuracy': deltas.get('validation_accuracy'),
            'delta_f1': deltas.get('validation_f1'),
            'delta_auc': deltas.get('validation_auc'),
            'delta_brier': deltas.get('validation_brier'),
            'status': status,
        })
    return sorted(comparisons, key=lambda item: int(item.get('period_days') or 0))


def _load_runtime_metadata_map():
    runtime_map = {}
    runtime_files = {
        5: Path(__file__).resolve().parent.parent / 'data' / 'models' / 'short_term_model.pkl',
        20: Path(__file__).resolve().parent.parent / 'data' / 'models' / 'medium_term_model.pkl',
        60: Path(__file__).resolve().parent.parent / 'data' / 'models' / 'long_term_model.pkl',
    }
    for period_days, path in runtime_files.items():
        try:
            if path.exists() and path.is_file():
                with open(path, 'rb') as f:
                    payload = pickle.load(f)
                runtime_meta = _extract_payload_metadata(payload, fallback_period_days=period_days)
                if runtime_meta:
                    runtime_map[int(period_days)] = runtime_meta
        except Exception:
            continue
    return runtime_map


def register_model_routes(app):
    """注册模型监控相关路由"""
    
    @app.route('/api/model/status', methods=['GET'])
    def get_model_status():
        """获取模型状态"""
        try:
            try:
                from api.reviews import _ensure_due_reviews_current
                _ensure_due_reviews_current(force=True)
            except Exception:
                pass

            session = get_session()

            all_versions = session.query(ModelVersion).order_by(
                ModelVersion.train_date.desc(),
                ModelVersion.created_at.desc()
            ).limit(200).all()
            current_model, current_version_source = _select_current_model_version(all_versions)

            runtime_metadata_map = _load_runtime_metadata_map()
            asset_training_results = _load_asset_training_results()
            training_progress = _load_training_progress()
            accuracy_trend = []
            recent_versions = []
            for v in all_versions:
                meta = _load_model_metadata(v)
                acc_pct = _metric_pct(meta.get('validation_accuracy'))
                f1_pct = _metric_pct(meta.get('validation_f1'))
                auc_pct = _metric_pct(meta.get('validation_auc'))
                brier = _metric_num(meta.get('validation_brier'))
                asset_type = meta.get('asset_type') or 'a_stock'
                asset_label = ASSET_LABEL_MAP.get(asset_type, str(asset_type or '未知资产'))

                trend_date = v.train_date.isoformat() if v.train_date else None
                accuracy_trend.append({
                    'version': f"{v.version} · {v.period_days}日",
                    'asset_type': asset_type,
                    'asset_label': asset_label,
                    'accuracy': acc_pct or 0,
                    'period_days': v.period_days,
                    'train_date': trend_date,
                    'day_label': str(v.train_date.day) if v.train_date else '--',
                })
                recent_versions.append({
                    'version': v.version,
                    'model_type': v.model_type,
                    'period_days': v.period_days,
                    'train_date': v.train_date.isoformat() if v.train_date else None,
                    'validation_accuracy': acc_pct,
                    'validation_f1': f1_pct,
                    'validation_auc': auc_pct,
                    'validation_brier': brier,
                    'train_data_count': meta.get('train_data_count') or v.train_data_count,
                    'is_active': bool(v.is_active),
                    'is_current': False,
                })
            period_comparison = _build_period_comparison(recent_versions, runtime_metadata_map)
            runtime_versions_summary, current_period_versions = _build_current_runtime_summary(recent_versions, runtime_metadata_map)
            asset_runtime_summary, current_period_versions = _merge_asset_runtime_versions(asset_training_results, current_period_versions)
            if asset_runtime_summary:
                runtime_versions_summary = asset_runtime_summary
            for item in recent_versions:
                period = int(item.get('period_days') or 0)
                item['is_current'] = current_period_versions.get(period) == item.get('version')

            feature_importance = _load_feature_importance_for_display(current_model)

            latest_train_dt = None
            for candidate in [
                current_model.train_date if current_model else None,
                *[_parse_datetime_value(item.get('train_date')) for item in asset_training_results],
            ]:
                parsed = _parse_datetime_value(candidate)
                if parsed and (latest_train_dt is None or parsed > latest_train_dt):
                    latest_train_dt = parsed

            review_coverage = _build_review_coverage_summary(session)

            asset_training_summary = {
                'total_models': len(asset_training_results),
                'healthy_models': sum(1 for item in asset_training_results if item.get('status') == 'healthy'),
                'latest_train_date': latest_train_dt.isoformat() if latest_train_dt else None,
                'asset_categories': len({item.get('asset_type') for item in asset_training_results if item.get('asset_type')}),
                'reviewed_predictions': review_coverage.get('reviewed_predictions', 0),
                'total_predictions': review_coverage.get('total_predictions', 0),
                'review_coverage_pct': review_coverage.get('coverage_pct', 0.0),
                'due_predictions': review_coverage.get('due_predictions', 0),
                'reviewed_due_predictions': review_coverage.get('reviewed_due_predictions', 0),
                'due_coverage_pct': review_coverage.get('due_coverage_pct', 0.0),
                'eligible_due_predictions': review_coverage.get('eligible_due_predictions', 0),
                'eligible_reviewed_predictions': review_coverage.get('eligible_reviewed_predictions', 0),
                'eligible_due_coverage_pct': review_coverage.get('eligible_due_coverage_pct', 0.0),
                'review_status': review_coverage.get('status', 'thin'),
            }

            # 当前表结构未存储完整混淆矩阵与误差分布，避免返回伪造数据。
            confusion_matrix = None
            error_distribution = []
            
            session.close()
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': {
                    'current_version': runtime_versions_summary or (current_model.version if current_model else None),
                    'current_period_versions': current_period_versions,
                    'current_version_source': 'runtime_assets' if asset_runtime_summary else ('period_summary' if runtime_versions_summary else current_version_source),
                    'last_train_date': latest_train_dt.isoformat() if latest_train_dt else (current_model.train_date.isoformat() if current_model and current_model.train_date else None),
                    'validation_accuracy': current_model.validation_accuracy if current_model else None,
                    'train_data_count': current_model.train_data_count if current_model else None,
                    'asset_training_results': asset_training_results,
                    'asset_training_summary': asset_training_summary,
                    'review_coverage': review_coverage,
                    'training_progress': training_progress,
                    'accuracy_trend': accuracy_trend,
                    'recent_versions': recent_versions,
                    'feature_importance': feature_importance,
                    'period_comparison': period_comparison,
                    'confusion_matrix': confusion_matrix,
                    'error_distribution': error_distribution,
                    'diagnostics_available': {
                        'feature_importance': len(feature_importance) > 0,
                        'confusion_matrix': False,
                        'error_distribution': False,
                    }
                },
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"获取模型状态失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/model/train', methods=['POST'])
    @require_admin_access(action='model.train')
    def train_model():
        """手动触发模型训练，支持全量或单模型重训。"""
        try:
            progress = _load_training_progress()
            last_update = _parse_datetime_value(progress.get('updated_at'))
            is_recent_running = progress.get('status') in {'running', 'starting'} and (
                last_update is None or (datetime.now() - last_update) < timedelta(minutes=30)
            )
            if is_recent_running:
                return jsonify({
                    'code': 200,
                    'status': 'success',
                    'message': '已有训练任务在执行中',
                    'data': progress,
                    'timestamp': datetime.now().isoformat()
                })

            payload = request.get_json(silent=True) or {}
            asset_type = payload.get('asset_type') or request.form.get('asset_type')
            period_days = payload.get('period_days') or request.form.get('period_days')

            project_root = Path(__file__).resolve().parent.parent
            launch_config = _build_training_launch_config(project_root, asset_type=asset_type, period_days=period_days)

            TRAINING_PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
            TRAINING_PROGRESS_FILE.write_text(json.dumps({
                'status': 'starting',
                'total_steps': launch_config['total_steps'],
                'completed_steps': 0,
                'current_step': 0,
                'progress_percent': 0.0,
                'current_asset': launch_config['current_asset'],
                'current_stage': 'prepare',
                'message': launch_config['message'],
                'started_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat(),
                'results': [],
            }, ensure_ascii=False, indent=2), encoding='utf-8')

            log_path = project_root / 'logs' / 'model_training.log'
            log_path.parent.mkdir(parents=True, exist_ok=True)
            env = dict(os.environ)
            env['PYTHONUNBUFFERED'] = '1'
            with open(log_path, 'a', encoding='utf-8', buffering=1) as log_file:
                process = subprocess.Popen(
                    launch_config['command'],
                    cwd=str(project_root),
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    env=env,
                )

            TRAINING_PROGRESS_FILE.write_text(json.dumps({
                'status': 'running',
                'total_steps': launch_config['total_steps'],
                'completed_steps': 0,
                'current_step': 0,
                'progress_percent': 0.0,
                'current_asset': launch_config['current_asset'],
                'current_stage': 'prepare',
                'message': launch_config['message'],
                'started_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat(),
                'pid': process.pid,
                'results': [],
            }, ensure_ascii=False, indent=2), encoding='utf-8')

            logger.info(f"模型训练任务已启动: pid={process.pid}, task={launch_config['task_name']}")
            log_admin_audit('model.train', 'success', f"task={launch_config['task_name']} pid={process.pid}")
            return jsonify({
                'code': 200,
                'status': 'success',
                'message': ('单模型训练已启动，将在后台执行' if launch_config['asset_type'] else '全量模型训练已启动，将在后台执行'),
                'data': {
                    'task_name': launch_config['task_name'],
                    'pid': process.pid,
                    'started_at': datetime.now().isoformat(),
                    'asset_type': launch_config['asset_type'],
                    'period_days': launch_config['period_days'],
                },
                'timestamp': datetime.now().isoformat()
            })

        except ValueError as e:
            return jsonify({
                'code': 400,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 400
        except Exception as e:
            logger.error(f"启动模型训练失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/model/export', methods=['GET'])
    @require_admin_access(action='model.export')
    def export_model():
        """导出模型"""
        session = None
        try:
            project_root = Path(__file__).resolve().parent.parent
            model_dir = project_root / 'data' / 'models'

            # 优先导出数据库中当前激活模型对应的文件。
            session = get_session()
            active_model = session.query(ModelVersion).filter(
                ModelVersion.is_active == True
            ).order_by(ModelVersion.train_date.desc()).first()

            model_file_path = None
            if active_model and active_model.model_path:
                candidate = Path(active_model.model_path)
                if not candidate.is_absolute():
                    candidate = project_root / candidate
                if candidate.exists() and candidate.is_file():
                    model_file_path = candidate

            # 回退：选择 models 目录中最近更新的 pkl 文件。
            if model_file_path is None:
                pkl_files = sorted(
                    model_dir.glob('*.pkl'),
                    key=lambda p: p.stat().st_mtime,
                    reverse=True,
                )
                if pkl_files:
                    model_file_path = pkl_files[0]

            if model_file_path is None:
                return jsonify({
                    'code': 404,
                    'status': 'error',
                    'message': '未找到可导出的模型文件',
                    'timestamp': datetime.now().isoformat()
                }), 404

            logger.info(f"导出模型文件: {model_file_path}")
            return send_file(
                str(model_file_path),
                mimetype='application/octet-stream',
                as_attachment=True,
                download_name=model_file_path.name,
            )
            
        except Exception as e:
            logger.error(f"导出模型失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
        finally:
            if session is not None:
                session.close()
    
    @app.route('/api/model/import', methods=['POST'])
    @require_admin_access(action='model.import')
    def import_model():
        """导入模型"""
        try:
            if 'model_file' not in request.files:
                return jsonify({
                    'code': 400,
                    'status': 'error',
                    'message': '未上传模型文件',
                    'timestamp': datetime.now().isoformat()
                }), 400
            
            file = request.files['model_file']
            if file.filename == '':
                return jsonify({
                    'code': 400,
                    'status': 'error',
                    'message': '文件名为空',
                    'timestamp': datetime.now().isoformat()
                }), 400
            
            safe_name = secure_filename(file.filename or '')
            if not safe_name:
                return jsonify({
                    'code': 400,
                    'status': 'error',
                    'message': '文件名非法',
                    'timestamp': datetime.now().isoformat()
                }), 400

            allowed_suffixes = {'.pkl', '.joblib'}
            suffix = Path(safe_name).suffix.lower()
            if suffix not in allowed_suffixes:
                return jsonify({
                    'code': 400,
                    'status': 'error',
                    'message': '仅支持导入 .pkl 或 .joblib 模型文件',
                    'timestamp': datetime.now().isoformat()
                }), 400

            max_import_mb = max(1, int(os.environ.get('MAX_MODEL_IMPORT_MB', '20')))
            max_size_bytes = max_import_mb * 1024 * 1024
            try:
                current_pos = file.stream.tell()
                file.stream.seek(0, os.SEEK_END)
                file_size = int(file.stream.tell() or 0)
                file.stream.seek(current_pos)
            except Exception:
                file_size = int(getattr(file, 'content_length', 0) or 0)

            if file_size > max_size_bytes:
                log_admin_audit('model.import', 'rejected', f"filename={safe_name} reason=file_too_large size={file_size}", level='WARNING')
                return jsonify({
                    'code': 400,
                    'status': 'error',
                    'message': f'模型文件过大，限制为 {max_import_mb}MB',
                    'timestamp': datetime.now().isoformat()
                }), 400

            # 保存模型文件
            model_dir = Path(os.path.dirname(os.path.dirname(__file__))) / 'data' / 'models'
            model_dir.mkdir(parents=True, exist_ok=True)

            stored_name = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{safe_name}"
            filepath = model_dir / stored_name
            file.save(str(filepath))

            sha256 = hashlib.sha256(filepath.read_bytes()).hexdigest()[:16]
            log_admin_audit('model.import', 'success', f"filename={stored_name} size={file_size} sha256={sha256}")
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'message': f'模型已导入: {stored_name}',
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"导入模型失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500