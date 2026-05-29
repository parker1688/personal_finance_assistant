"""
仪表盘API - api/dashboard.py
提供仪表盘汇总数据接口
"""

import sys
import os
import json
from datetime import datetime, timedelta
from flask import jsonify, request
import numpy as np
try:
    import tushare as ts
except ImportError:
    ts = None
from sqlalchemy import text, func, case

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import get_session, Warning, Holding, HoldingSnapshot, Recommendation, AccuracyStat, ModelVersion, Prediction
from utils import get_logger, get_today, SimpleCache
from predictors.model_manager import ModelManager

logger = get_logger(__name__)
market_temp_cache = SimpleCache(ttl=300)
dashboard_insight_cache = SimpleCache(ttl=180)
_MODEL_MANAGER = ModelManager()
_MIN_ACTION_BACKTEST_SAMPLE_SIZE = 20


def _build_runtime_model_catalog():
    return [
        {'asset_type': 'a_stock', 'label': 'A股', 'period_days': 5, 'path': os.path.join('data', 'models', 'short_term_model.pkl')},
        {'asset_type': 'a_stock', 'label': 'A股', 'period_days': 20, 'path': os.path.join('data', 'models', 'medium_term_model.pkl')},
        {'asset_type': 'a_stock', 'label': 'A股', 'period_days': 60, 'path': os.path.join('data', 'models', 'long_term_model.pkl')},
        {'asset_type': 'hk_stock', 'label': '港股', 'period_days': 5, 'path': os.path.join('data', 'models', 'hk_stock_short_term_model.pkl')},
        {'asset_type': 'hk_stock', 'label': '港股', 'period_days': 20, 'path': os.path.join('data', 'models', 'hk_stock_medium_term_model.pkl')},
        {'asset_type': 'hk_stock', 'label': '港股', 'period_days': 60, 'path': os.path.join('data', 'models', 'hk_stock_long_term_model.pkl')},
        {'asset_type': 'us_stock', 'label': '美股', 'period_days': 5, 'path': os.path.join('data', 'models', 'us_stock_short_term_model.pkl')},
        {'asset_type': 'us_stock', 'label': '美股', 'period_days': 20, 'path': os.path.join('data', 'models', 'us_stock_medium_term_model.pkl')},
        {'asset_type': 'us_stock', 'label': '美股', 'period_days': 60, 'path': os.path.join('data', 'models', 'us_stock_long_term_model.pkl')},
        {'asset_type': 'active_fund', 'label': '主动基金', 'period_days': 5, 'path': os.path.join('data', 'models', 'fund_short_term_model.pkl')},
        {'asset_type': 'active_fund', 'label': '主动基金', 'period_days': 20, 'path': os.path.join('data', 'models', 'fund_medium_term_model.pkl')},
        {'asset_type': 'active_fund', 'label': '主动基金', 'period_days': 60, 'path': os.path.join('data', 'models', 'fund_long_term_model.pkl')},
        {'asset_type': 'etf', 'label': 'ETF', 'period_days': 5, 'path': os.path.join('data', 'models', 'etf_short_term_model.pkl')},
        {'asset_type': 'etf', 'label': 'ETF', 'period_days': 20, 'path': os.path.join('data', 'models', 'etf_medium_term_model.pkl')},
        {'asset_type': 'etf', 'label': 'ETF', 'period_days': 60, 'path': os.path.join('data', 'models', 'etf_long_term_model.pkl')},
        {'asset_type': 'gold', 'label': '黄金', 'period_days': 5, 'path': os.path.join('data', 'models', 'gold_short_term_model.pkl')},
        {'asset_type': 'gold', 'label': '黄金', 'period_days': 20, 'path': os.path.join('data', 'models', 'gold_medium_term_model.pkl')},
        {'asset_type': 'gold', 'label': '黄金', 'period_days': 60, 'path': os.path.join('data', 'models', 'gold_long_term_model.pkl')},
        {'asset_type': 'silver', 'label': '白银', 'period_days': 5, 'path': os.path.join('data', 'models', 'silver_short_term_model.pkl')},
        {'asset_type': 'silver', 'label': '白银', 'period_days': 20, 'path': os.path.join('data', 'models', 'silver_medium_term_model.pkl')},
        {'asset_type': 'silver', 'label': '白银', 'period_days': 60, 'path': os.path.join('data', 'models', 'silver_long_term_model.pkl')},
    ]


def _get_model_health_overview():
    model_rows = []
    for item in _build_runtime_model_catalog():
        runtime_bundle = _MODEL_MANAGER.load_runtime_model_bundle(item['path'], period_days=item['period_days'], allow_legacy=None)
        metadata = runtime_bundle.get('metadata') or {}
        model_rows.append({
            'asset_type': item['asset_type'],
            'asset_label': item['label'],
            'label': f"{item['label']}{item['period_days']}日",
            'period_days': item['period_days'],
            'passed': bool(runtime_bundle.get('loaded')),
            'gate': runtime_bundle.get('gate', 'failed'),
            'version': metadata.get('version') or os.path.basename(item['path']),
            'accuracy': _safe_float(metadata.get('validation_accuracy'), None),
            'f1': _safe_float(metadata.get('validation_f1'), None),
            'auc': _safe_float(metadata.get('validation_auc'), None),
            'brier': _safe_float(metadata.get('validation_brier'), None),
            'reason': runtime_bundle.get('reason', ''),
        })

    passed_count = sum(1 for item in model_rows if item['passed'])
    asset_groups = []
    for asset_type in ['a_stock', 'hk_stock', 'us_stock', 'active_fund', 'etf', 'gold', 'silver']:
        rows = [item for item in model_rows if item['asset_type'] == asset_type]
        if not rows:
            continue
        asset_groups.append({
            'asset_type': asset_type,
            'label': rows[0]['asset_label'],
            'passed_count': sum(1 for item in rows if item['passed']),
            'total_models': len(rows),
            'status': 'healthy' if all(item['passed'] for item in rows) else ('warning' if any(item['passed'] for item in rows) else 'risk'),
        })

    return {
        'overall_status': 'healthy' if passed_count == len(model_rows) else ('warning' if passed_count > 0 else 'risk'),
        'passed_count': passed_count,
        'total_models': len(model_rows),
        'asset_groups': asset_groups,
        'models': model_rows,
    }


def _build_portfolio_overview(session, holdings, high_warnings, medium_warnings, low_warnings):
    latest_snapshot_date = session.query(func.max(HoldingSnapshot.snapshot_date)).scalar()
    snapshot_values = {}
    if latest_snapshot_date:
        snapshot_rows = session.query(
            HoldingSnapshot.holding_id,
            HoldingSnapshot.market_value,
        ).filter(
            HoldingSnapshot.snapshot_date == latest_snapshot_date
        ).all()
        snapshot_values = {
            int(holding_id): max(0.0, _safe_float(market_value))
            for holding_id, market_value in snapshot_rows
            if holding_id is not None
        }

    holding_values = []
    snapshot_covered = 0
    for holding in holdings:
        snapshot_value = snapshot_values.get(int(holding.id))
        if snapshot_value is not None and snapshot_value > 0:
            holding_values.append(snapshot_value)
            snapshot_covered += 1
        else:
            holding_values.append(max(0.0, _safe_float(holding.cost_price) * _safe_float(holding.quantity)))

    holding_count = len(holdings)
    total_holding_value = round(sum(holding_values), 2)
    concentration_ratio_pct = round((max(holding_values) / total_holding_value * 100), 1) if total_holding_value > 0 and holding_values else 0.0
    portfolio_risk = 'high' if concentration_ratio_pct >= 45 or high_warnings >= 3 else ('medium' if concentration_ratio_pct >= 25 or high_warnings >= 1 or medium_warnings >= 1 else 'low')
    portfolio_stance = 'defensive' if portfolio_risk == 'high' else ('balanced' if portfolio_risk == 'medium' else 'constructive')
    recommended_cash_ratio_pct = 40 if portfolio_risk == 'high' else (25 if portfolio_risk == 'medium' else 15)
    risk_label = {'high': '较高', 'medium': '中等', 'low': '较低'}.get(portfolio_risk, '中等')
    health_score = 88
    if holding_count <= 2 and holding_count > 0:
        health_score -= 10
    if concentration_ratio_pct >= 45:
        health_score -= 22
    elif concentration_ratio_pct >= 25:
        health_score -= 10
    if high_warnings >= 1:
        health_score -= min(18, high_warnings * 6)
    elif medium_warnings >= 2:
        health_score -= 6
    health_score = max(30, min(95, int(health_score))) if holding_count > 0 else 80

    if holding_count == 0:
        summary = '当前无持仓，建议先以观察和分散建仓为主。'
    else:
        basis_label = '最新快照市值' if snapshot_covered == holding_count and holding_count > 0 else ('快照/成本混合口径' if snapshot_covered > 0 else '成本口径')
        snapshot_suffix = f'（基于 {latest_snapshot_date.isoformat()} 快照）' if latest_snapshot_date and snapshot_covered > 0 else ''
        summary = f'当前组合{concentration_ratio_pct:.1f}%集中于单一资产，整体风险{risk_label}，建议保留约{recommended_cash_ratio_pct}%现金。估值口径：{basis_label}{snapshot_suffix}'

    return {
        'holding_count': holding_count,
        'total_value': total_holding_value,
        'concentration_ratio_pct': concentration_ratio_pct,
        'health_score': health_score,
        'overall_risk': portfolio_risk,
        'stance': portfolio_stance,
        'recommended_cash_ratio_pct': recommended_cash_ratio_pct,
        'valuation_basis': 'snapshot' if snapshot_covered == holding_count and holding_count > 0 else ('mixed' if snapshot_covered > 0 else 'cost'),
        'snapshot_coverage': snapshot_covered,
        'snapshot_date': latest_snapshot_date.isoformat() if latest_snapshot_date and snapshot_covered > 0 else None,
        'low_warning_count': int(low_warnings or 0),
        'summary': summary,
    }


def _get_cached_market_temperature(session):
    """获取带缓存的市场温度，避免短时间重复调用外部API。"""
    cached = market_temp_cache.get('market_temperature')
    if cached:
        return cached

    temp = _calculate_market_temperature(session)
    market_temp_cache.set('market_temperature', temp)
    return temp


def _get_latest_recommendation_date(session, today=None):
    """获取最近一批可用推荐日期。"""
    if today is None:
        today = get_today()
    latest = session.query(Recommendation).filter(Recommendation.date <= today).order_by(Recommendation.date.desc()).first()
    return latest.date if latest else None


def _safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _get_cached_action_backtest_summary():
    cached = dashboard_insight_cache.get('action_backtest_summary')
    if cached:
        return cached

    summary = {
        'overall_accuracy': 0.0,
        'overall_grade': 'N/A',
        'take_profit_grade': 'N/A',
        'add_signal_grade': 'N/A',
        'has_action_samples': False,
        'is_reliable': False,
        'sample_size': 0,
        'sample_note': '暂无动作回测数据',
        'recommendation': '暂无动作回测数据'
    }
    try:
        from reviews.backtest_validator import BacktestValidator
        validator = BacktestValidator()
        report = validator.generate_backtest_report(30)
        validator.close()
        if isinstance(report, dict) and 'error' not in report:
            grade_info = report.get('action_quality_summary', {}) or {}
            summary = {
                'overall_accuracy': round(_safe_float(report.get('overall_accuracy'), 0.0), 2),
                'overall_grade': grade_info.get('overall_grade', 'N/A'),
                'take_profit_grade': grade_info.get('take_profit_grade', 'N/A'),
                'add_signal_grade': grade_info.get('add_signal_grade', 'N/A'),
                'has_action_samples': bool(grade_info.get('has_action_samples', False)),
                'sample_size': int(grade_info.get('sample_size', 0) or 0),
                'is_reliable': int(grade_info.get('sample_size', 0) or 0) >= _MIN_ACTION_BACKTEST_SAMPLE_SIZE,
                'sample_note': (
                    f"样本 {int(grade_info.get('sample_size', 0) or 0)} 条，统计稳定性有限"
                    if int(grade_info.get('sample_size', 0) or 0) < _MIN_ACTION_BACKTEST_SAMPLE_SIZE
                    else f"样本 {int(grade_info.get('sample_size', 0) or 0) } 条，统计稳定性较好"
                ),
                'recommendation': (
                    '动作回测样本仍偏少，暂不建议据此高权重决策。'
                    if int(grade_info.get('sample_size', 0) or 0) < _MIN_ACTION_BACKTEST_SAMPLE_SIZE
                    else report.get('recommendations', '暂无动作回测数据')
                )
            }
    except Exception as e:
        logger.warning(f"计算动作回测摘要失败: {e}")

    dashboard_insight_cache.set('action_backtest_summary', summary)
    return summary


def _build_advisor_brief(market_temp_detail, portfolio_overview, model_health, action_backtest, warning_stats, pending_validation_count):
    """构建更像理财顾问的首页摘要。"""
    temp = _safe_float((market_temp_detail or {}).get('temperature'), 50.0)
    risk = (portfolio_overview or {}).get('overall_risk', 'medium')
    stance = (portfolio_overview or {}).get('stance', 'balanced')
    high_warn = int((warning_stats or {}).get('high', 0) or 0)
    total_warn = int((warning_stats or {}).get('total', 0) or 0)

    if risk == 'high' or high_warn >= 2:
        headline = '今日建议：先防守，先处理风险'
    elif temp <= 40:
        headline = '今日建议：市场偏冷，可分批布局'
    elif temp >= 60:
        headline = '今日建议：市场偏热，控制追高'
    else:
        headline = '今日建议：均衡配置，精选机会'

    bullets = []
    if temp <= 40:
        bullets.append('权益性价比较高，可优先关注高质量资产并分批建仓。')
    elif temp >= 60:
        bullets.append('权益估值性价比下降，建议降低激进仓位，避免追涨。')
    else:
        bullets.append('市场处于中性区间，适合均衡配置与耐心筛选。')

    if risk == 'high':
        bullets.append(f"当前组合风险偏高，建议现金保留约 {portfolio_overview.get('recommended_cash_ratio_pct', 25)}%。")
    else:
        bullets.append(f"当前组合整体风险可控，建议现金保留约 {portfolio_overview.get('recommended_cash_ratio_pct', 25)}%。")

    if high_warn > 0:
        bullets.append(f"今日存在 {high_warn} 条高风险预警，优先复核止损线与仓位集中度。")
    elif total_warn > 0:
        bullets.append(f"今日共有 {total_warn} 条预警，建议盘中关注波动与回撤。")
    else:
        bullets.append('当前没有显著风险预警，可按计划执行。')

    if (model_health or {}).get('overall_status') != 'healthy':
        bullets.append('模型状态仍需观察，建议降低单笔仓位并缩短复查周期。')
    elif not (action_backtest or {}).get('has_action_samples'):
        bullets.append(f"动作回测样本仍在积累中，当前仍有 {int(pending_validation_count or 0)} 条待验证预测。")
    elif not (action_backtest or {}).get('is_reliable'):
        bullets.append(f"动作回测样本仅 {int((action_backtest or {}).get('sample_size', 0) or 0)} 条，暂不建议据此高权重决策。")
    else:
        bullets.append(f"动作回测评级 {(action_backtest or {}).get('overall_grade', 'N/A')}，可作为辅助决策参考。")

    return {
        'headline': headline,
        'stance': stance,
        'risk': risk,
        'bullets': bullets[:4],
        'market_source': (market_temp_detail or {}).get('data_source', 'FALLBACK'),
        'formula': (market_temp_detail or {}).get('calculation_formula', ''),
    }


def _build_advisor_workflow(today_recommendations, portfolio_overview, warning_stats, pending_validation_count, validated_prediction_count, overall_accuracy, action_backtest):
    """构建“预测前 → 持有中 → 到期复盘”的理财师工作流摘要。"""
    today_recommendations = today_recommendations or {}
    portfolio_overview = portfolio_overview or {}
    warning_stats = warning_stats or {}
    action_backtest = action_backtest or {}

    total_recommendations = int(today_recommendations.get('total', 0) or 0)
    holding_count = int(portfolio_overview.get('holding_count', 0) or 0)
    risk = str(portfolio_overview.get('overall_risk', 'medium') or 'medium')
    high_warn = int(warning_stats.get('high', 0) or 0)
    medium_warn = int(warning_stats.get('medium', 0) or 0)
    pending_validation_count = int(pending_validation_count or 0)
    validated_prediction_count = int(validated_prediction_count or 0)
    overall_accuracy_value = None if overall_accuracy is None else round(float(overall_accuracy), 1)

    pre_trade_status = 'good' if total_recommendations > 0 else 'idle'
    monitor_status = 'warning' if (risk == 'high' or high_warn > 0) else ('active' if holding_count > 0 else 'idle')
    review_status = 'good' if validated_prediction_count > 0 else ('active' if pending_validation_count > 0 else 'idle')

    if total_recommendations > 0:
        pre_trade_action = '先看短期机会，再结合中期趋势与长期估值做分批布局。'
    else:
        pre_trade_action = '当前可先观察市场温度与下一批推荐快照。'

    if holding_count > 0:
        monitor_action = '短期建议每日复查；中期建议每1到3天更新；长期建议每周复核一次。'
    else:
        monitor_action = '当前暂无持仓，可先建立观察池与候选仓位。'

    if validated_prediction_count > 0 and overall_accuracy_value is not None:
        review_action = f'已形成 {validated_prediction_count} 条验收样本，当前整体准确率约 {overall_accuracy_value}%。'
    elif pending_validation_count > 0:
        review_action = f'已有 {pending_validation_count} 条预测在等待到期验证，系统会持续累计样本。'
    else:
        review_action = '当前暂无到期样本，后续会自动进入复盘验收。'

    return {
        'headline': 'AI 理财师工作流：预测前 → 持有中 → 到期复盘',
        'stages': [
            {
                'key': 'pre_trade',
                'title': '预测前',
                'subtitle': '先筛选，再决定是否买入',
                'status': pre_trade_status,
                'metrics': [
                    {'label': '候选资产', 'value': f'{total_recommendations} 个'},
                    {'label': '观察窗口', 'value': '短期 3-10日 / 中期 10-40日 / 长期 3个月+'},
                ],
                'next_action': pre_trade_action,
            },
            {
                'key': 'monitoring',
                'title': '持有中',
                'subtitle': '持续观察并动态修正',
                'status': monitor_status,
                'metrics': [
                    {'label': '跟踪持仓', 'value': f'{holding_count} 项'},
                    {'label': '风险预警', 'value': f'{high_warn} 高风险 / {medium_warn} 中风险'},
                ],
                'next_action': monitor_action,
            },
            {
                'key': 'review',
                'title': '到期复盘',
                'subtitle': '看方向、看概率、看收益',
                'status': review_status,
                'metrics': [
                    {'label': '已验收', 'value': f'{validated_prediction_count} 条'},
                    {'label': '待验收', 'value': f'{pending_validation_count} 条'},
                ],
                'next_action': review_action,
                'grade': action_backtest.get('overall_grade', 'N/A') if action_backtest.get('is_reliable') else '样本不足',
            },
        ]
    }


def register_dashboard_routes(app):
    """注册仪表盘相关路由"""
    
    @app.route('/api/dashboard/summary', methods=['GET'])
    def get_dashboard_summary():
        """
        获取仪表盘汇总数据
        GET /api/dashboard/summary
        """
        session = None
        try:
            session = get_session()
            today = get_today()
            today_start = datetime.now().replace(hour=0, minute=0, second=0)
            
            # 单次聚合查询预警统计，减少多次 count 往返
            warning_agg = session.query(
                func.count(Warning.id),
                func.sum(case((Warning.level == 'high', 1), else_=0)),
                func.sum(case((Warning.level == 'medium', 1), else_=0)),
                func.sum(case((Warning.level == 'low', 1), else_=0)),
            ).filter(
                Warning.warning_time >= today_start
            ).one()
            today_warnings = int(warning_agg[0] or 0)
            high_warnings = int(warning_agg[1] or 0)
            medium_warnings = int(warning_agg[2] or 0)
            low_warnings = int(warning_agg[3] or 0)
            
            # 获取最近一批可用推荐数量（避免当天未刷新时页面空白）
            recommendation_batch_date = _get_latest_recommendation_date(session, today=today)
            type_counts = {}
            if recommendation_batch_date:
                type_rows = session.query(
                    Recommendation.type,
                    func.count(Recommendation.id)
                ).filter(
                    Recommendation.date == recommendation_batch_date
                ).group_by(
                    Recommendation.type
                ).all()
                type_counts = {str(t): int(c or 0) for t, c in type_rows}

            today_recommendations = int(sum(type_counts.values()))
            a_stock_count = int(type_counts.get('a_stock', 0))
            hk_stock_count = int(type_counts.get('hk_stock', 0))
            us_stock_count = int(type_counts.get('us_stock', 0))
            active_fund_count = int(type_counts.get('active_fund', 0))
            etf_count = int(type_counts.get('etf', 0))
            gold_count = int(type_counts.get('gold', 0))
            silver_count = int(type_counts.get('silver', 0))
            
            # 获取整体准确率（最近30天）
            thirty_days_ago = today - timedelta(days=30)
            accuracy_totals = session.query(
                func.sum(AccuracyStat.total_count),
                func.sum(AccuracyStat.correct_count)
            ).filter(
                AccuracyStat.stat_date >= thirty_days_ago
            ).one()

            total_predictions = int(accuracy_totals[0] or 0)
            total_correct = int(accuracy_totals[1] or 0)
            if total_predictions > 0:
                overall_accuracy = (total_correct / total_predictions * 100)
            else:
                overall_accuracy = None

            validated_prediction_count = session.query(Prediction).filter(
                Prediction.is_expired == True,
                Prediction.is_direction_correct.isnot(None)
            ).count()
            pending_validation_count = session.query(Prediction).filter(
                Prediction.is_direction_correct.is_(None)
            ).count()
            
            # 获取准确率趋势（最近7天）
            accuracy_trend = []
            trend_start = today - timedelta(days=6)
            trend_rows = session.query(
                AccuracyStat.stat_date,
                func.sum(AccuracyStat.total_count),
                func.sum(AccuracyStat.correct_count)
            ).filter(
                AccuracyStat.stat_date >= trend_start,
                AccuracyStat.stat_date <= today
            ).group_by(
                AccuracyStat.stat_date
            ).all()
            trend_map = {
                day: {
                    'total': int(total or 0),
                    'correct': int(correct or 0)
                }
                for day, total, correct in trend_rows
            }
            for i in range(7):
                date = trend_start + timedelta(days=i)
                bucket = trend_map.get(date, {'total': 0, 'correct': 0})
                day_total = int(bucket['total'])
                day_correct = int(bucket['correct'])
                day_accuracy = round(day_correct / day_total * 100, 1) if day_total > 0 else None
                accuracy_trend.append({
                    'date': date.isoformat(),
                    'accuracy': day_accuracy,
                    'sample_count': day_total,
                })
            
            # 获取最近预警
            recent_warnings = session.query(Warning).order_by(
                Warning.warning_time.desc()
            ).limit(5).all()
            
            recent_warnings_list = []
            for w in recent_warnings:
                recent_warnings_list.append({
                    'time': w.warning_time.strftime('%m-%d %H:%M'),
                    'code': w.code,
                    'name': w.name,
                    'type': w.warning_type,
                    'level': w.level,
                    'message': w.message
                })
            
            # 组合概览
            holdings = session.query(Holding).all()
            portfolio_overview = _build_portfolio_overview(session, holdings, high_warnings, medium_warnings, low_warnings)

            # 模型健康概览
            model_health = _get_model_health_overview()

            # 动作回测摘要
            action_backtest = _get_cached_action_backtest_summary()

            # 计算市场温度（简化版）
            market_temperature = _get_cached_market_temperature(session)

            warning_summary = {
                'total': today_warnings,
                'high': high_warnings,
                'medium': medium_warnings,
                'low': low_warnings,
            }
            advisor_brief = _build_advisor_brief(
                market_temperature,
                portfolio_overview,
                model_health,
                action_backtest,
                warning_summary,
                pending_validation_count,
            )
            advisor_workflow = _build_advisor_workflow(
                {
                    'total': today_recommendations,
                    'a_stock': a_stock_count,
                    'hk_stock': hk_stock_count,
                    'us_stock': us_stock_count,
                    'active_fund': active_fund_count,
                    'etf': etf_count,
                    'gold': gold_count,
                    'silver': silver_count,
                },
                portfolio_overview,
                warning_summary,
                pending_validation_count,
                validated_prediction_count,
                overall_accuracy,
                action_backtest,
            )
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': {
                    'market_temperature': market_temperature['temperature'],
                    'market_interpretation': market_temperature['interpretation'],
                    'market_temperature_detail': market_temperature,
                    'today_warnings': {
                        'total': today_warnings,
                        'high': high_warnings,
                        'medium': medium_warnings,
                        'low': low_warnings,
                    },
                    'today_recommendations': {
                        'total': today_recommendations,
                        'a_stock': a_stock_count,
                        'hk_stock': hk_stock_count,
                        'us_stock': us_stock_count,
                        'active_fund': active_fund_count,
                        'etf': etf_count,
                        'gold': gold_count,
                        'silver': silver_count
                    },
                    'overall_accuracy': round(overall_accuracy, 1) if overall_accuracy is not None else None,
                    'has_review_data': bool(validated_prediction_count > 0),
                    'pending_validation_count': int(pending_validation_count),
                    'recommendation_batch_date': recommendation_batch_date.isoformat() if recommendation_batch_date else None,
                    'accuracy_trend': accuracy_trend,
                    'recent_warnings': recent_warnings_list,
                    'portfolio_overview': portfolio_overview,
                    'model_health': model_health,
                    'action_backtest': action_backtest,
                    'advisor_brief': advisor_brief,
                    'advisor_workflow': advisor_workflow
                },
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"获取仪表盘数据失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
        finally:
            if session:
                session.close()
    
    @app.route('/api/market/temperature', methods=['GET'])
    def get_market_temperature():
        """获取市场温度"""
        session = None
        try:
            session = get_session()
            temperature = _get_cached_market_temperature(session)
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': temperature,
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"获取市场温度失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
        finally:
            if session:
                session.close()
    
    @app.route('/api/health', methods=['GET'])
    def dashboard_health_check():
        """兼容健康检查入口（建议优先使用 /health）。"""
        session = None
        try:
            session = get_session()
            session.execute(text('SELECT 1'))
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': {
                    'status': 'healthy',
                    'deprecated': True,
                    'recommended_endpoint': '/health',
                    'timestamp': datetime.now().isoformat()
                }
            })
        except Exception as e:
            logger.warning(f"/api/health 检查失败: {e}")
            return jsonify({
                'code': 503,
                'status': 'error',
                'message': str(e),
                'data': {
                    'status': 'degraded',
                    'deprecated': True,
                    'recommended_endpoint': '/health',
                    'timestamp': datetime.now().isoformat()
                }
            }), 503
        finally:
            if session:
                session.close()


def _calculate_market_temperature(session):
    """
    计算市场温度 - 基于股债性价比
    
    理论公式:
        股债收益率差 (Equity Risk Premium) = E/P - 国债收益率
        
        当差 > 3%: 股票更便宜，温度低（30-40）
        当差 = 2%: 均衡，温度中性（45-55）  
        当差 < 1%: 股票更贵，温度高（70-100）
    
    映射到0-100:
        温度 = 50 + (差值 - 2%) × 500
    
    返回: 稳定、可复现的温度值 (已移除随机数)
    """
    import pandas as pd
    
    try:
        # 兼容不同 tushare 版本: 优先使用 pro_api，失败后再尝试旧接口
        try:
            pro = ts.pro_api()
        except Exception:
            pro = ts.pro_connect()

        # 1. HS300估值: 使用 index_dailybasic 的真实 PE/PE_TTM
        hs300_pe = None
        pe_trade_date = None
        pe_col_used = None
        try:
            pe_df = pro.index_dailybasic(
                ts_code='000300.SH',
                limit=30,
                fields='ts_code,trade_date,pe,pe_ttm'
            )
            if pe_df is not None and not pe_df.empty:
                for _, row in pe_df.iterrows():
                    for cand in ('pe_ttm', 'pe'):
                        val = pd.to_numeric(pd.Series([row.get(cand)]), errors='coerce').iloc[0]
                        if pd.notna(val) and float(val) > 0:
                            hs300_pe = float(val)
                            pe_trade_date = str(row.get('trade_date', ''))
                            pe_col_used = cand
                            break
                    if hs300_pe is not None:
                        break
        except Exception as e:
            logger.debug(f"从 index_dailybasic 获取HS300 PE失败: {e}")

        if hs300_pe is None or hs300_pe <= 0:
            logger.warning("无法从API获取HS300 PE，使用保守估计 PE=13.5")
            hs300_pe = 13.5

        hs300_ep = 1.0 / hs300_pe * 100  # E/P (%)

        # 2. 中国10Y国债收益率: 使用 yc_cb 中债国债收益率曲线(10年)
        bond_yield = None
        bond_trade_date = None
        bond_term = None
        bond_source = 'TuShare yc_cb'
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=45)).strftime('%Y%m%d')

        try:
            yc_df = pro.yc_cb(
                ts_code='1001.CB',
                start_date=start_date,
                end_date=end_date,
                fields='trade_date,curve_term,yield'
            )
            if yc_df is not None and not yc_df.empty:
                yc_df = yc_df.copy()
                yc_df['curve_term'] = pd.to_numeric(yc_df['curve_term'], errors='coerce')
                yc_df['yield'] = pd.to_numeric(yc_df['yield'], errors='coerce')
                yc_df = yc_df.dropna(subset=['curve_term', 'yield'])
                if not yc_df.empty:
                    latest_day = str(yc_df['trade_date'].max())
                    latest_df = yc_df[yc_df['trade_date'] == latest_day].copy()
                    if not latest_df.empty:
                        latest_df['term_diff'] = (latest_df['curve_term'] - 10.0).abs()
                        best = latest_df.sort_values('term_diff').iloc[0]
                        y = float(best['yield'])
                        if 0 < y < 15:
                            bond_yield = y
                            bond_trade_date = latest_day
                            bond_term = float(best['curve_term'])
        except Exception as e:
            logger.debug(f"从 yc_cb 获取10Y收益率失败: {e}")

        # 备用: 使用 Shibor 1Y 作为利率代理（真实数据，但非10Y）
        if bond_yield is None:
            try:
                shibor_df = pro.shibor(start_date=start_date, end_date=end_date)
                if shibor_df is not None and not shibor_df.empty and '1y' in shibor_df.columns:
                    shibor_df = shibor_df.copy()
                    shibor_df['1y'] = pd.to_numeric(shibor_df['1y'], errors='coerce')
                    shibor_df = shibor_df.dropna(subset=['1y'])
                    if not shibor_df.empty:
                        row = shibor_df.sort_values('date', ascending=False).iloc[0]
                        y = float(row['1y'])
                        if 0 < y < 15:
                            bond_yield = y
                            bond_trade_date = str(row.get('date', ''))
                            bond_term = 1.0
                            bond_source = 'TuShare shibor(1Y proxy)'
            except Exception as e:
                logger.debug(f"从 shibor 获取1Y利率失败: {e}")

        if bond_yield is None:
            logger.warning("无法从API获取利率，使用备用值 2.85%")
            bond_yield = 2.85
            bond_source = 'FALLBACK'
        
        # 3. 计算股债性价比差
        equity_premium = hs300_ep - bond_yield
        
        # 4. 映射到温度0-100
        # 用平滑S曲线代替线性裁剪，避免长期显示0°C或100°C。
        # 股债溢价越高，权益越便宜，温度越低；反之越高。
        centered_premium = equity_premium - 2.5
        temperature = 50 - 38 * np.tanh(centered_premium / 1.6)
        temperature = round(max(0, min(100, temperature)), 1)
        
        # 5. 生成解释文本
        interpretation = _generate_market_interpretation(
            temperature, hs300_pe, hs300_ep, bond_yield, equity_premium
        )
        
        logger.info(
            "市场温度计算: PE=%.2f(%s,%s), E/P=%.2f%%, Bond=%.2f%%(%s,term=%s), Premium=%.2f%%",
            hs300_pe,
            pe_col_used or 'fallback',
            pe_trade_date or '-',
            hs300_ep,
            bond_yield,
            bond_trade_date or '-',
            f"{bond_term:.2f}Y" if bond_term is not None else '-',
            equity_premium,
        )
        
        return {
            'temperature': round(temperature, 1),
            'pe_ratio': round(hs300_pe, 2),
            'equity_earnings_yield': round(hs300_ep, 2),
            'bond_yield': round(bond_yield, 2),
            'equity_premium': round(equity_premium, 2),
            'interpretation': interpretation,
            'calculation_formula': f'T = 50 - 38 × tanh((ERP {equity_premium:.2f}% - 2.50%) / 1.60)',
            'timestamp': datetime.now().isoformat(),
            'pe_trade_date': pe_trade_date,
            'pe_source': f"TuShare index_dailybasic.{pe_col_used}" if pe_col_used else 'FALLBACK',
            'bond_trade_date': bond_trade_date,
            'bond_term_years': round(float(bond_term), 2) if bond_term is not None else None,
            'data_source': f'TuShare: index_dailybasic + {bond_source}'
        }
        
    except Exception as e:
        logger.error(f"计算市场温度异常: {e}")
        return _default_market_temperature()

def _generate_market_interpretation(temperature, pe, ep, bond_yield, premium):
    """根据温度生成解释"""
    
    if temperature < 25:
        level = "极度冷"
        action = "强烈建议加大权益配置"
    elif temperature < 40:
        level = "偏冷"
        action = "建议增加权益配置"
    elif temperature < 45:
        level = "中性偏冷"
        action = "可适度增加权益配置"
    elif temperature < 55:
        level = "中性"
        action = "股债均衡配置"
    elif temperature < 60:
        level = "中性偏热"
        action = "可适度降低权益配置"
    elif temperature < 75:
        level = "偏热"
        action = "建议降低权益配置"
    else:
        level = "极度热"
        action = "建议大幅降低权益配置"
    
    return (f"市场{level}({temperature:.0f}°C): "
            f"沪深300 PE={pe:.1f}(E/P={ep:.2f}%), 10Y国债={bond_yield:.2f}%, "
            f"股债溢价={premium:.2f}%. {action}")

def _default_market_temperature():
    """降级的默认温度 (中性)"""
    return {
        'temperature': 50.0,
        'interpretation': '市场中性。数据获取中断，返回默认中性温度',
        'data_source': 'FALLBACK',
        'timestamp': datetime.now().isoformat()
    }