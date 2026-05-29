"""
模拟交易员 - 全链路验证指标计算 - trader/validator.py
计算 AI 预测准确率、推荐有效性、复盘识别率、组合收益指标
"""

import sys
import os
from datetime import date, timedelta
from collections import Counter, defaultdict
from sqlalchemy import func

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import (
    get_session,
    Prediction,
    Recommendation,
    Review,
    SimulatedTrade,
    SimulatedDailyPnl,
    SimulatedDecisionLog,
)
from utils import get_logger

logger = get_logger(__name__)

DEFAULT_TRADER_ID = 'default'


def compute_validation_report(trader_id: str = DEFAULT_TRADER_ID) -> dict:
    """
    计算全链路验证报告，返回结构化 dict。
    """
    session = get_session()
    try:
        loss_analysis = _loss_trade_analysis(session, trader_id)
        return {
            'prediction_accuracy': _prediction_accuracy(session),
            'recommendation_effectiveness': _recommendation_effectiveness(session, trader_id),
            'review_identification_rate': _review_identification_rate(session, trader_id),
            'portfolio_metrics': _portfolio_metrics(session, trader_id),
            'ai_scorecard': _ai_scorecard(session, trader_id),
            'loss_trade_analysis': loss_analysis,
            'retraining_suggestions': _build_retraining_suggestions(loss_analysis),
            'review_tasks': _build_review_tasks(loss_analysis),
            'generated_at': date.today().isoformat(),
        }
    finally:
        session.close()


# ------------------------------------------------------------------
# 1. AI 预测模型准确率
# ------------------------------------------------------------------

def _prediction_accuracy(session) -> dict:
    """
    预测方向准确率：is_direction_correct=True 的比例
    按周期（5/20/60）分组统计
    """
    result = {}
    for period in (5, 20, 60):
        total = (
            session.query(func.count(Prediction.id))
            .filter(
                Prediction.period_days == period,
                Prediction.is_expired == True,
                Prediction.is_direction_correct.isnot(None),
            )
            .scalar() or 0
        )
        correct = (
            session.query(func.count(Prediction.id))
            .filter(
                Prediction.period_days == period,
                Prediction.is_expired == True,
                Prediction.is_direction_correct == True,
            )
            .scalar() or 0
        )
        result[f'{period}d'] = {
            'total': total,
            'correct': correct,
            'accuracy_pct': round(correct / total * 100, 2) if total > 0 else None,
        }
    return result


# ------------------------------------------------------------------
# 2. 推荐模型有效性（通过模拟交易流水验证）
# ------------------------------------------------------------------

def _recommendation_effectiveness(session, trader_id: str) -> dict:
    """
    已平仓交易中：盈利笔数/总笔数（胜率），平均持有收益率
    """
    sells = (
        session.query(SimulatedTrade)
        .filter_by(trader_id=trader_id, action='sell')
        .filter(SimulatedTrade.price > 0)  # 已成交
        .all()
    )
    if not sells:
        return {'win_rate_pct': None, 'avg_return_pct': None, 'total_closed': 0}

    wins = [t for t in sells if (t.pnl or 0) > 0]
    returns = [t.pnl_pct for t in sells if t.pnl_pct is not None]

    return {
        'total_closed': len(sells),
        'wins': len(wins),
        'win_rate_pct': round(len(wins) / len(sells) * 100, 2),
        'avg_return_pct': round(sum(returns) / len(returns), 2) if returns else None,
        'total_pnl': round(sum(t.pnl or 0 for t in sells), 2),
    }


# ------------------------------------------------------------------
# 3. 复盘模型识别率
# ------------------------------------------------------------------

def _review_identification_rate(session, trader_id: str) -> dict:
    """
    亏损平仓的交易中，有多少在 Review 表中有记录（且识别为失败）
    """
    loss_sells = (
        session.query(SimulatedTrade)
        .filter_by(trader_id=trader_id, action='sell')
        .filter(SimulatedTrade.price > 0, SimulatedTrade.pnl < 0)
        .all()
    )
    if not loss_sells:
        return {'loss_trades': 0, 'reviewed': 0, 'review_coverage_pct': None}

    reviewed = 0
    for trade in loss_sells:
        # 找到对应 code 在同期的 Review 记录
        rev = (
            session.query(Review)
            .filter(
                Review.code == trade.code,
                Review.is_direction_correct == False,
            )
            .first()
        )
        if rev:
            reviewed += 1

    return {
        'loss_trades': len(loss_sells),
        'reviewed': reviewed,
        'review_coverage_pct': round(reviewed / len(loss_sells) * 100, 2),
    }


# ------------------------------------------------------------------
# 4. 组合整体指标
# ------------------------------------------------------------------

def _portfolio_metrics(session, trader_id: str) -> dict:
    """
    总收益率、最大回撤、夏普比率（简化版）、当前净值
    """
    rows = (
        session.query(SimulatedDailyPnl)
        .filter_by(trader_id=trader_id)
        .order_by(SimulatedDailyPnl.pnl_date.asc())
        .all()
    )
    if not rows:
        return {
            'total_return_pct': None,
            'max_drawdown_pct': None,
            'sharpe_ratio': None,
            'current_total_value': None,
            'days_running': 0,
        }

    first = rows[0]
    last = rows[-1]

    total_return = last.total_return or 0.0
    max_dd = max((r.max_drawdown or 0.0) for r in rows)

    # 简化夏普：年化收益 / 年化波动率（假设无风险利率 2%）
    daily_returns = [r.daily_return or 0.0 for r in rows if r.daily_return is not None]
    sharpe = None
    if len(daily_returns) >= 20:
        import statistics
        avg_daily = statistics.mean(daily_returns)
        std_daily = statistics.stdev(daily_returns)
        if std_daily > 0:
            # 年化（约 250 个交易日）
            annual_return = avg_daily * 250
            annual_std = std_daily * (250 ** 0.5)
            sharpe = round((annual_return - 2.0) / annual_std, 3)

    return {
        'current_total_value': round(last.total_value, 2),
        'total_return_pct': round(total_return, 2),
        'max_drawdown_pct': round(max_dd, 2),
        'sharpe_ratio': sharpe,
        'days_running': (last.pnl_date - first.pnl_date).days + 1,
        'start_date': first.pnl_date.isoformat(),
        'latest_date': last.pnl_date.isoformat(),
    }


def _ai_scorecard(session, trader_id: str) -> dict:
    """交易员对AI信号的评分卡（采纳率/一致率/坏信号率）"""
    decisions = (
        session.query(SimulatedDecisionLog)
        .filter_by(trader_id=trader_id)
        .all()
    )
    if not decisions:
        return {
            'signals_total': 0,
            'adoption_rate_pct': None,
            'alignment_rate_pct': None,
            'bad_signal_rate_pct': None,
        }

    ai_buy_signals = [d for d in decisions if (d.recommended_action or '').lower() == 'buy']
    adopted = [d for d in ai_buy_signals if (d.final_action or '').lower() == 'buy']
    aligned = [d for d in decisions if (d.recommended_action or '').lower() == (d.final_action or '').lower()]

    closed_sells = (
        session.query(SimulatedTrade)
        .filter_by(trader_id=trader_id, action='sell')
        .filter(SimulatedTrade.price > 0)
        .all()
    )
    bad_sells = [t for t in closed_sells if (t.pnl or 0.0) < 0]

    return {
        'signals_total': len(decisions),
        'ai_buy_signals': len(ai_buy_signals),
        'adopted_buys': len(adopted),
        'adoption_rate_pct': round(len(adopted) / len(ai_buy_signals) * 100, 2) if ai_buy_signals else None,
        'alignment_rate_pct': round(len(aligned) / len(decisions) * 100, 2) if decisions else None,
        'bad_signal_rate_pct': round(len(bad_sells) / len(closed_sells) * 100, 2) if closed_sells else None,
    }


def _loss_trade_analysis(session, trader_id: str) -> dict:
    """对亏损交易进行归因分类，为训练和复盘闭环提供输入。"""
    loss_sells = (
        session.query(SimulatedTrade)
        .filter_by(trader_id=trader_id, action='sell')
        .filter(SimulatedTrade.price > 0, SimulatedTrade.pnl < 0)
        .order_by(SimulatedTrade.trade_date.desc())
        .all()
    )
    if not loss_sells:
        return {
            'total_loss_trades': 0,
            'total_loss_amount': 0.0,
            'category_summary': [],
            'asset_summary': [],
            'cases': [],
        }

    category_counter = Counter()
    asset_counter = Counter()
    asset_loss_amount = defaultdict(float)
    cases = []

    for trade in loss_sells:
        decision = (
            session.query(SimulatedDecisionLog)
            .filter_by(trader_id=trader_id, code=trade.code)
            .filter(SimulatedDecisionLog.signal_date == trade.signal_date)
            .order_by(SimulatedDecisionLog.created_at.desc())
            .first()
        )
        recommendation = None
        if trade.source_recommendation_id:
            recommendation = session.query(Recommendation).filter_by(id=trade.source_recommendation_id).first()
        if recommendation is None:
            recommendation = (
                session.query(Recommendation)
                .filter(Recommendation.code == trade.code, Recommendation.date == trade.signal_date)
                .order_by(Recommendation.rank.asc())
                .first()
            )
        review = (
            session.query(Review)
            .filter(Review.code == trade.code)
            .order_by(Review.reviewed_at.desc())
            .first()
        )

        classification = _classify_loss_trade(trade, decision, recommendation, review)
        category = classification['category']
        category_counter[category] += 1
        asset_key = trade.asset_type or 'unknown'
        asset_counter[asset_key] += 1
        asset_loss_amount[asset_key] += abs(float(trade.pnl or 0.0))

        cases.append({
            'trade_id': trade.id,
            'code': trade.code,
            'name': trade.name,
            'asset_type': trade.asset_type,
            'trade_date': trade.trade_date.isoformat() if trade.trade_date else None,
            'signal_date': trade.signal_date.isoformat() if trade.signal_date else None,
            'trigger': trade.trigger,
            'pnl': round(float(trade.pnl or 0.0), 2),
            'pnl_pct': round(float(trade.pnl_pct or 0.0), 2) if trade.pnl_pct is not None else None,
            'category': category,
            'category_label': classification['label'],
            'reason': classification['reason'],
            'recommendation_score': round(float(recommendation.total_score), 2) if recommendation and recommendation.total_score is not None else None,
            'decision_score': round(float(decision.decision_score), 4) if decision and decision.decision_score is not None else None,
            'pred_score': round(float(decision.pred_score), 4) if decision and decision.pred_score is not None else None,
            'risk_score': round(float(decision.risk_score), 4) if decision and decision.risk_score is not None else None,
            'portfolio_score': round(float(decision.portfolio_score), 4) if decision and decision.portfolio_score is not None else None,
            'review_score': round(float(review.review_score), 2) if review and review.review_score is not None else None,
        })

    total_loss_amount = round(sum(abs(float(t.pnl or 0.0)) for t in loss_sells), 2)
    category_summary = [
        {
            'category': category,
            'label': _loss_category_label(category),
            'count': count,
            'ratio_pct': round(count / len(loss_sells) * 100, 2),
        }
        for category, count in category_counter.most_common()
    ]
    asset_summary = [
        {
            'asset_type': asset_type,
            'count': count,
            'loss_amount': round(asset_loss_amount[asset_type], 2),
        }
        for asset_type, count in asset_counter.most_common()
    ]

    cases.sort(key=lambda item: abs(item['pnl']), reverse=True)
    return {
        'total_loss_trades': len(loss_sells),
        'total_loss_amount': total_loss_amount,
        'category_summary': category_summary,
        'asset_summary': asset_summary,
        'cases': cases,
    }


def _classify_loss_trade(trade, decision, recommendation, review) -> dict:
    """将单笔亏损交易归因为模型、推荐、风控或时机问题。"""
    pnl_pct = float(trade.pnl_pct or 0.0)
    pred_score = float(decision.pred_score) if decision and decision.pred_score is not None else None
    rec_score = float(decision.rec_score) if decision and decision.rec_score is not None else None
    risk_score = float(decision.risk_score) if decision and decision.risk_score is not None else None
    rec_total = float(recommendation.total_score) if recommendation and recommendation.total_score is not None else None
    review_wrong = bool(review and review.is_direction_correct is False)

    if review_wrong or (pred_score is not None and pred_score < 0.45):
        return {
            'category': 'model_misprediction',
            'label': _loss_category_label('model_misprediction'),
            'reason': '预测方向或概率判断偏差，模型信号本身可信度不足。',
        }

    if trade.trigger in ('timeout', 'score_drop') and pnl_pct <= -5:
        return {
            'category': 'timing_deviation',
            'label': _loss_category_label('timing_deviation'),
            'reason': '信号方向可能并非完全错误，但进出时机偏晚，导致收益兑现失败。',
        }

    if trade.trigger != 'stop_loss' and ((risk_score is not None and risk_score < 0.45) or pnl_pct <= -8):
        return {
            'category': 'risk_control_failure',
            'label': _loss_category_label('risk_control_failure'),
            'reason': '风险约束未能及时拦截回撤，止损/退出机制偏弱。',
        }

    if (rec_total is not None and rec_total >= 0.70) or (rec_score is not None and rec_score >= 0.70):
        return {
            'category': 'recommendation_bias',
            'label': _loss_category_label('recommendation_bias'),
            'reason': '推荐评分偏高但后续兑现较差，说明推荐排序或打分存在偏置。',
        }

    return {
        'category': 'timing_deviation',
        'label': _loss_category_label('timing_deviation'),
        'reason': '主要表现为交易时机偏差，需结合入场窗口与退出节奏复盘。',
    }


def _loss_category_label(category: str) -> str:
    mapping = {
        'model_misprediction': '模型误判',
        'recommendation_bias': '推荐偏置',
        'risk_control_failure': '风控不足',
        'timing_deviation': '时机偏差',
    }
    return mapping.get(category, category)


def _build_retraining_suggestions(loss_analysis: dict) -> list[dict]:
    """基于亏损归因生成可执行的重训建议。"""
    cases = loss_analysis.get('cases', [])
    if not cases:
        return []

    grouped = defaultdict(list)
    for case in cases:
        grouped[(case['category'], case['asset_type'])].append(case)

    suggestions = []
    for (category, asset_type), items in grouped.items():
        loss_amount = sum(abs(float(item['pnl'] or 0.0)) for item in items)
        priority = 'high' if len(items) >= 3 or loss_amount >= 10000 else 'medium' if len(items) >= 2 or loss_amount >= 3000 else 'low'
        if category == 'model_misprediction':
            action = '补充方向标签与波动特征，优先重训 5d/20d 预测模型'
        elif category == 'recommendation_bias':
            action = '回看推荐打分权重，降低高分低兑现样本的排序权重'
        elif category == 'risk_control_failure':
            action = '重训止损/退出阈值参数，并增加高波动样本约束'
        else:
            action = '优化入场窗口和持有时长特征，增加时机类样本复盘'

        suggestions.append({
            'category': category,
            'category_label': _loss_category_label(category),
            'asset_type': asset_type,
            'trade_count': len(items),
            'loss_amount': round(loss_amount, 2),
            'priority': priority,
            'target_periods': ['5d', '20d'] if category in ('model_misprediction', 'timing_deviation') else ['20d', '60d'],
            'action': action,
        })

    suggestions.sort(key=lambda item: ({'high': 0, 'medium': 1, 'low': 2}[item['priority']], -item['loss_amount']))
    return suggestions[:8]


def _build_review_tasks(loss_analysis: dict) -> list[dict]:
    """根据亏损贡献生成待复盘任务。"""
    tasks = []
    for idx, case in enumerate(loss_analysis.get('cases', [])[:10], start=1):
        severity = 'high' if abs(float(case['pnl'] or 0.0)) >= 5000 else 'medium' if abs(float(case['pnl'] or 0.0)) >= 1500 else 'low'
        tasks.append({
            'rank': idx,
            'code': case['code'],
            'name': case['name'],
            'asset_type': case['asset_type'],
            'trade_date': case['trade_date'],
            'loss_amount': case['pnl'],
            'loss_pct': case['pnl_pct'],
            'category': case['category'],
            'category_label': case['category_label'],
            'severity': severity,
            'task_title': f"复盘 {case['code']} {case['category_label']}",
            'task_focus': case['reason'],
        })
    return tasks


def compute_ai_scorecard(trader_id: str = DEFAULT_TRADER_ID) -> dict:
    """对外暴露AI评分卡"""
    session = get_session()
    try:
        return _ai_scorecard(session, trader_id)
    finally:
        session.close()
