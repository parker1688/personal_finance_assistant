"""
反思学习模块 - reviews/reflection.py
分析错误预测，识别模式，持久化学习洞察，在错误率超阈值时触发重训建议
"""

import numpy as np
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import get_session, Prediction, Review, LearningInsight
from utils import get_logger

logger = get_logger(__name__)

# 错误模式 → 改进建议
_PATTERN_SUGGESTIONS = {
    'overconfident':  '高置信度信号中仍有大量错误，建议增加交叉验证过滤机制，或提高输出阈值至80%以上',
    'low_confidence': '低置信度信号错误率高，建议屏蔽置信度<55%的推荐，只输出高质量信号',
    'high_volatility': '高波动行情模型失效明显，建议增加波动率感知特征，高波动期间收窄仓位或暂停预测',
    'trend_reversal':  '趋势反转识别不足，建议引入多周期趋势一致性验证，或增加均值回归因子',
    'small_error':     '方向判断接近无效（涨跌幅极小），建议增设预测门槛，不预测震荡行情',
    'news_impact':     '突发事件导致预测失效，建议接入实时新闻情感分析，重大事件前后降低风险暴露',
    'no_error':        '当前复盘样本未发现显著错误，继续积累更多已到期样本并保持监控即可',
    'other':           '建议回溯分析历史特征工程，增加因子多样性或调整模型超参数',
}


class ReflectionLearner:
    """反思学习器 - 分析预测错误并持久化学习洞察"""

    def __init__(self, session=None):
        self.session = session or get_session()
        self._owns_session = session is None

    # ──────────────────────────────────────────────────────────────────
    # 1. 错误分析
    # ──────────────────────────────────────────────────────────────────
    def analyze_errors(self, days=30):
        """分析最近 N 天的到期预测，返回多维误差统计"""
        cutoff_date = datetime.now().date() - timedelta(days=days)

        predictions = self.session.query(Prediction).filter(
            Prediction.expiry_date >= cutoff_date,
            Prediction.is_expired == True,
            Prediction.is_direction_correct.isnot(None)
        ).all()
        pending_predictions = self.session.query(Prediction).filter(
            Prediction.expiry_date >= cutoff_date,
            Prediction.is_expired == False
        ).all()

        empty = {
            'total_predictions': 0,
            'total_errors': 0,
            'error_rate': 0,
            'by_period': {5: 0, 20: 0, 60: 0},
            'reviewed_by_period': {5: 0, 20: 0, 60: 0},
            'pending_by_period': {5: 0, 20: 0, 60: 0},
            'by_asset': {},
            'by_confidence': {'high': 0, 'medium': 0, 'low': 0},
            'error_patterns': {
                'overconfident': 0,
                'low_confidence': 0,
                'high_volatility': 0,
                'trend_reversal': 0,
                'small_error': 0,
                'news_impact': 0,
                'other': 0,
            },
            'status_note': '暂无已到期预测样本'
        }
        if not predictions and not pending_predictions:
            return empty

        total = len(predictions)
        errors = [p for p in predictions if not p.is_direction_correct]
        total_errors = len(errors)

        result = {
            'total_predictions': total,
            'total_errors': total_errors,
            'error_rate': total_errors / total * 100 if total > 0 else 0,
            'by_period': {5: 0, 20: 0, 60: 0},
            'reviewed_by_period': {5: 0, 20: 0, 60: 0},
            'pending_by_period': {5: 0, 20: 0, 60: 0},
            'by_asset': {},
            'by_confidence': {'high': 0, 'medium': 0, 'low': 0},
            'error_patterns': {k: 0 for k in empty['error_patterns']},
            'status_note': '',
        }

        for pred in predictions:
            if pred.period_days in result['reviewed_by_period']:
                result['reviewed_by_period'][pred.period_days] += 1

        for pred in pending_predictions:
            if pred.period_days in result['pending_by_period']:
                result['pending_by_period'][pred.period_days] += 1

        for pred in errors:
            if pred.period_days in result['by_period']:
                result['by_period'][pred.period_days] += 1

            asset = pred.asset_type or 'unknown'
            result['by_asset'][asset] = result['by_asset'].get(asset, 0) + 1

            conf = pred.confidence or 0
            result['by_confidence']['high' if conf >= 70 else ('medium' if conf >= 50 else 'low')] += 1

            pattern = self._identify_error_pattern(pred)
            result['error_patterns'][pattern] = result['error_patterns'].get(pattern, 0) + 1

        if total == 0 and sum(result['pending_by_period'].values()) > 0:
            result['status_note'] = '当前预测大多尚未到期，复盘样本仍在积累'
        elif total > 0 and total_errors == 0:
            result['status_note'] = '最近30天复盘样本暂无方向错误'
        elif total_errors > 0:
            result['status_note'] = f'最近30天共有 {total_errors} 条错误样本可供分析'

        return result

    # ──────────────────────────────────────────────────────────────────
    # 2. 错误模式识别（增强版）
    # ──────────────────────────────────────────────────────────────────
    def _identify_error_pattern(self, prediction):
        """
        将单条错误预测归类为以下模式之一：
          overconfident  | 置信度≥75 却预测错误
          low_confidence | 置信度<55
          news_impact    | 复盘记录含新闻/事件关键词
          high_volatility| |实际涨跌幅| > 8%
          trend_reversal | 3% < |实际涨跌幅| ≤ 8%
          small_error    | |实际涨跌幅| ≤ 3%（接近无方向震荡）
          other          | 其他
        """
        conf = prediction.confidence or 0

        # 过度自信
        if conf >= 75:
            return 'overconfident'

        # 低置信度
        if conf < 55:
            return 'low_confidence'

        # 查对应复盘记录
        review = self.session.query(Review).filter(
            Review.prediction_id == prediction.id
        ).first()

        # 新闻/事件冲击
        if review and review.error_analysis:
            news_kws = ['突发', '政策', '消息', '公告', '事件', '监管', '停牌', '利空', '利好', '黑天鹅']
            if any(kw in review.error_analysis for kw in news_kws):
                return 'news_impact'

        # 取实际涨跌幅（优先用复盘记录）
        actual_ret = None
        if review and review.actual_return is not None:
            actual_ret = review.actual_return
        elif prediction.actual_return is not None:
            actual_ret = prediction.actual_return

        if actual_ret is not None:
            abs_ret = abs(actual_ret)
            if abs_ret > 8:
                return 'high_volatility'
            if abs_ret > 3:
                return 'trend_reversal'
            return 'small_error'

        return 'other'

    # ──────────────────────────────────────────────────────────────────
    # 3. 重训需求检测
    # ──────────────────────────────────────────────────────────────────
    def check_retrain_needed(self, error_rate_threshold=40.0):
        """
        检查是否需要重训，返回 (period_days, asset_type) 元组列表。
        触发条件：
          - 全局错误率 ≥ threshold
          - 某周期错误数占总错误的 50% 以上（且样本≥10）
          - 某资产类型的错误率 ≥ threshold（且样本≥10）
        """
        analysis = self.analyze_errors(days=30)
        retrain_targets = []

        if analysis['error_rate'] >= error_rate_threshold:
            retrain_targets.append(('all', 'all'))

        total_errors = analysis['total_errors']
        for period, count in analysis['by_period'].items():
            if total_errors >= 10 and count / total_errors >= 0.5:
                retrain_targets.append((period, 'all'))

        cutoff = datetime.now().date() - timedelta(days=30)
        for asset, err_count in analysis['by_asset'].items():
            total_asset = self.session.query(Prediction).filter(
                Prediction.asset_type == asset,
                Prediction.expiry_date >= cutoff,
                Prediction.is_expired == True,
                Prediction.is_direction_correct.isnot(None)
            ).count()
            if total_asset >= 10 and err_count / total_asset * 100 >= error_rate_threshold:
                retrain_targets.append(('all', asset))

        return retrain_targets

    # ──────────────────────────────────────────────────────────────────
    # 4. 持久化学习洞察
    # ──────────────────────────────────────────────────────────────────
    def save_insight(self, analysis, days_analyzed=30):
        """将本次分析结果写入 learning_insights 表"""
        try:
            if analysis['total_predictions'] == 0:
                return

            patterns = analysis['error_patterns']
            if analysis['total_errors'] <= 0:
                dominant_pattern = 'no_error'
                dominant_count = 0
                pattern_ratio = 0
            else:
                dominant_pattern = max(patterns, key=patterns.get) if patterns else 'other'
                dominant_count = patterns.get(dominant_pattern, 0)
                pattern_ratio = dominant_count / analysis['total_errors'] * 100

            retrain_targets = self.check_retrain_needed()
            retrain_triggered = len(retrain_targets) > 0

            suggestion = _PATTERN_SUGGESTIONS.get(dominant_pattern, _PATTERN_SUGGESTIONS['other'])
            if retrain_triggered:
                targets_str = '、'.join(
                    [f"{'全局' if p=='all' else f'{p}日'}/{'全量' if a=='all' else a}" for p, a in retrain_targets]
                )
                suggestion += f"；⚠️ 建议重训：{targets_str}"

            insight = LearningInsight(
                period_days=None,
                asset_type='all',
                error_rate=round(analysis['error_rate'], 2),
                total_analyzed=analysis['total_predictions'],
                dominant_pattern=dominant_pattern,
                pattern_ratio=round(pattern_ratio, 2),
                suggestion=suggestion,
                retrain_triggered=retrain_triggered,
                days_analyzed=days_analyzed,
            )
            self.session.add(insight)
            self.session.commit()
            logger.info(f"已保存学习洞察: 错误率={analysis['error_rate']:.1f}%, 主要模式={dominant_pattern}, 重训={retrain_triggered}")
        except Exception as e:
            self.session.rollback()
            logger.error(f"保存学习洞察失败: {e}")

    def get_historical_insights(self, limit=5):
        """取最近几条历史学习洞察"""
        try:
            return self.session.query(LearningInsight).order_by(
                LearningInsight.created_at.desc()
            ).limit(limit).all()
        except Exception as e:
            logger.error(f"获取历史洞察失败: {e}")
            return []

    # ──────────────────────────────────────────────────────────────────
    # 5. 模型权重调整建议（兼容旧接口）
    # ──────────────────────────────────────────────────────────────────
    def update_model_weights(self):
        """根据误差分析输出调整建议（供报告调用）"""
        analysis = self.analyze_errors(days=30)
        if analysis['total_errors'] == 0:
            return {}

        adjustments = {}
        patterns = analysis['error_patterns']
        total_errors = analysis['total_errors']

        for pattern, count in patterns.items():
            if count > total_errors * 0.3:
                adjustments[pattern] = {
                    'action': 'review',
                    'suggestion': _PATTERN_SUGGESTIONS.get(pattern, _PATTERN_SUGGESTIONS['other'])
                }
                logger.warning(f"错误模式 [{pattern}] 占比过高: {count/total_errors:.1%}")

        for period, count in analysis['by_period'].items():
            if count > total_errors * 0.4:
                adjustments[f'period_{period}d'] = {
                    'action': 'retrain',
                    'suggestion': f'{period}日预测错误较多，建议重新训练对应模型'
                }

        return adjustments

    # ──────────────────────────────────────────────────────────────────
    # 6. 报告生成
    # ──────────────────────────────────────────────────────────────────
    def generate_reflection_report(self):
        """生成反思报告（含历史洞察和重训建议）"""
        analysis = self.analyze_errors(days=30)

        pending_total = sum((analysis.get('pending_by_period') or {}).values())
        if analysis['total_predictions'] == 0 and pending_total == 0:
            return "暂无预测数据，无法生成反思报告"
        if analysis['total_predictions'] == 0:
            pending_lines = ' / '.join(
                [f"{period}日待到期 {count} 条" for period, count in (analysis.get('pending_by_period') or {}).items()]
            )
            return f"当前暂无已到期预测可复盘，错误图为空属正常。{pending_lines}"

        # 持久化本次洞察
        self.save_insight(analysis, days_analyzed=30)

        retrain_targets = self.check_retrain_needed()
        retrain_flag = "⚠️  建议重训" if retrain_targets else "✅ 模型正常"

        lines = [
            "╔══════════════════════════════════════════════════════════════╗",
            "║                   📊 预测反思学习报告                        ║",
            "╠══════════════════════════════════════════════════════════════╣",
            f"║  统计周期：最近30天              状态: {retrain_flag}",
            f"║  总预测数：{analysis['total_predictions']}    错误数：{analysis['total_errors']}    "
            f"错误率：{analysis['error_rate']:.1f}%",
            "╠══════════════════════════════════════════════════════════════╣",
            "║  按周期错误分布:",
        ]
        for period, count in analysis['by_period'].items():
            pct = count / analysis['total_errors'] * 100 if analysis['total_errors'] > 0 else 0
            reviewed_count = (analysis.get('reviewed_by_period') or {}).get(period, 0)
            pending_count = (analysis.get('pending_by_period') or {}).get(period, 0)
            lines.append(f"║    {period}日预测：错误 {count} 次 ({pct:.1f}%) | 已复盘 {reviewed_count} 条 | 待到期 {pending_count} 条")

        lines += [
            "╠══════════════════════════════════════════════════════════════╣",
            "║  按置信度错误分布:",
        ]
        level_map = {'high': '高(≥70)', 'medium': '中(50-70)', 'low': '低(<50)'}
        for lvl, count in analysis['by_confidence'].items():
            lines.append(f"║    {level_map[lvl]}：{count} 次")

        lines += [
            "╠══════════════════════════════════════════════════════════════╣",
            "║  错误模式分析:",
        ]
        sorted_patterns = sorted(analysis['error_patterns'].items(), key=lambda x: -x[1])
        if analysis['total_errors'] == 0:
            lines.append("║    当前暂无错误样本，错误模式图为空属正常")
        for pattern, count in sorted_patterns:
            if count > 0:
                pct = count / analysis['total_errors'] * 100 if analysis['total_errors'] > 0 else 0
                lines.append(f"║    {pattern}：{count} 次 ({pct:.1f}%)")

        lines += [
            "╠══════════════════════════════════════════════════════════════╣",
            "║  改进建议:",
        ]
        adjustments = self.update_model_weights()
        if adjustments:
            for adj in adjustments.values():
                # 换行显示长建议
                s = adj['suggestion']
                lines.append(f"║    • {s[:60]}")
                if len(s) > 60:
                    lines.append(f"║      {s[60:]}")
        else:
            lines.append("║    • 当前误差分布均衡，继续监控")

        if retrain_targets:
            lines += [
                "╠══════════════════════════════════════════════════════════════╣",
                "║  ⚠️  重训建议:",
            ]
            for period, asset in retrain_targets:
                if period == 'all' and asset == 'all':
                    lines.append("║    • 全局错误率超阈值，建议重训所有模型")
                elif period != 'all':
                    lines.append(f"║    • {period}日预测误差集中，建议重训该周期模型")
                else:
                    lines.append(f"║    • {asset} 类型错误率高，建议重训对应品类模型")

        # 历史学习记录（最近3条，跳过本次刚保存的）
        historical = self.get_historical_insights(limit=4)
        if len(historical) > 1:
            lines += [
                "╠══════════════════════════════════════════════════════════════╣",
                "║  历史学习记录（近3次）:",
            ]
            for ins in historical[1:4]:
                date_str = ins.created_at.strftime('%Y-%m-%d')
                short_sug = ins.suggestion[:50] + '…' if len(ins.suggestion) > 50 else ins.suggestion
                lines.append(f"║    [{date_str}] 错误率:{ins.error_rate:.1f}% 主模式:{ins.dominant_pattern}")
                lines.append(f"║      → {short_sug}")

        lines.append("╚══════════════════════════════════════════════════════════════╝")
        return "\n".join(lines)

    def close(self):
        if self._owns_session and self.session is not None:
            self.session.close()


if __name__ == '__main__':
    learner = ReflectionLearner()
    report = learner.generate_reflection_report()
    print(report)
    learner.close()

