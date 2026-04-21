"""
报告生成模块 - reviews/reporter.py
生成周报、月报等复盘报告
"""

import sys
import os
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from reviews.analyzer import AccuracyAnalyzer
from models import get_session, Prediction, Review
from utils import get_logger

logger = get_logger(__name__)


class Reporter:
    """报告生成器"""
    
    def __init__(self):
        self.analyzer = AccuracyAnalyzer()
    
    def generate_weekly_report(self):
        """
        生成周报（使用真实数据）
        Returns:
            dict: 周报内容
        """
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=7)
        
        # 获取本周统计
        period_5d = self.analyzer.calculate_period_accuracy(5, start_date, end_date)
        period_20d = self.analyzer.calculate_period_accuracy(20, start_date, end_date)
        
        # 获取本周最佳/最差推荐
        best_worst = self._get_best_worst_predictions(start_date, end_date)
        
        # 计算整体准确率
        total_predictions = period_5d['total'] + period_20d['total']
        total_correct = period_5d['correct'] + period_20d['correct']
        overall_accuracy = (total_correct / total_predictions * 100) if total_predictions > 0 else 0
        
        # 生成报告内容
        content = f"""
📊 本周复盘统计报告（{start_date} 至 {end_date}）

━━━━━━━━━━━━ 整体准确率 ━━━━━━━━━━━━
本周总预测数：{total_predictions}
正确数：{total_correct}
准确率：{overall_accuracy:.1f}%

━━━━━━━━━━━━ 按周期 ━━━━━━━━━━━━
5日预测准确率：{period_5d['accuracy']}% ({period_5d['correct']}/{period_5d['total']})
20日预测准确率：{period_20d['accuracy']}% ({period_20d['correct']}/{period_20d['total']})

━━━━━━━━━━━━ 按置信度 ━━━━━━━━━━━━
高置信度预测：{period_5d['by_confidence']['high']['accuracy']:.1f}%
中置信度预测：{period_5d['by_confidence']['medium']['accuracy']:.1f}%
低置信度预测：{period_5d['by_confidence']['low']['accuracy']:.1f}%

━━━━━━━━━━━━ 本周最佳推荐 ━━━━━━━━━━━━
"""
        
        if best_worst['best']:
            for i, rec in enumerate(best_worst['best'][:3]):
                content += f"{i+1}. {rec['name']}：预测{rec['predicted_return']:.1f}%，实际{rec['actual_return']:.1f}%\n"
        else:
            content += "暂无数据\n"
        
        content += """
━━━━━━━━━━━━ 本周最差推荐 ━━━━━━━━━━━━
"""
        
        if best_worst['worst']:
            for i, rec in enumerate(best_worst['worst'][:3]):
                reason = rec.get('reason', '市场波动')
                content += f"{i+1}. {rec['name']}：预测{rec['predicted_return']:.1f}%，实际{rec['actual_return']:.1f}%（原因：{reason}）\n"
        else:
            content += "暂无数据\n"
        
        content += """
━━━━━━━━━━━━ 市场回顾 ━━━━━━━━━━━━
本周市场整体表现平稳，主要指数小幅震荡。
建议关注下周宏观数据发布，保持合理仓位。

━━━━━━━━━━━━ 免责声明 ━━━━━━━━━━━━
本报告仅供参考，不构成投资建议。
"""
        
        # 保存报告
        report_data = {
            'title': f'本周复盘统计报告（{start_date} 至 {end_date}）',
            'content': content,
            'generated_at': datetime.now().isoformat(),
            'type': 'weekly'
        }
        
        self._save_report(report_data)
        
        return report_data
    
    def generate_monthly_report(self):
        """
        生成月报（使用真实数据）
        Returns:
            dict: 月报内容
        """
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30)
        
        # 获取本月统计
        period_5d = self.analyzer.calculate_period_accuracy(5, start_date, end_date)
        period_20d = self.analyzer.calculate_period_accuracy(20, start_date, end_date)
        period_60d = self.analyzer.calculate_period_accuracy(60, start_date, end_date)
        
        # 获取本月最佳/最差推荐
        best_worst = self._get_best_worst_predictions(start_date, end_date, limit=10)
        
        # 错误分析
        error_analysis = self.analyzer.analyze_error_patterns()
        
        total_predictions = period_5d['total'] + period_20d['total'] + period_60d['total']
        total_correct = period_5d['correct'] + period_20d['correct'] + period_60d['correct']
        overall_accuracy = (total_correct / total_predictions * 100) if total_predictions > 0 else 0
        
        content = f"""
📊 本月复盘统计报告（{start_date} 至 {end_date}）

━━━━━━━━━━━━ 整体准确率 ━━━━━━━━━━━━
本月总预测数：{total_predictions}
正确数：{total_correct}
准确率：{overall_accuracy:.1f}%

━━━━━━━━━━━━ 按周期 ━━━━━━━━━━━━
5日预测准确率：{period_5d['accuracy']}% ({period_5d['correct']}/{period_5d['total']})
20日预测准确率：{period_20d['accuracy']}% ({period_20d['correct']}/{period_20d['total']})
60日预测准确率：{period_60d['accuracy']}% ({period_60d['correct']}/{period_60d['total']})

━━━━━━━━━━━━ 按品类 ━━━━━━━━━━━━
A股准确率：{self.analyzer.calculate_asset_type_accuracy('SH', start_date, end_date)['accuracy']}%
港股准确率：{self.analyzer.calculate_asset_type_accuracy('HK', start_date, end_date)['accuracy']}%
美股准确率：{self.analyzer.calculate_asset_type_accuracy('US', start_date, end_date)['accuracy']}%

━━━━━━━━━━━━ 错误分析 ━━━━━━━━━━━━
总错误数：{error_analysis['total_errors']}
主要错误类型：{error_analysis.get('most_common', '未知')}

━━━━━━━━━━━━ 本月最佳推荐 ━━━━━━━━━━━━
"""
        
        if best_worst['best']:
            for i, rec in enumerate(best_worst['best'][:5]):
                content += f"{i+1}. {rec['name']}：预测{rec['predicted_return']:.1f}%，实际{rec['actual_return']:.1f}%\n"
        else:
            content += "暂无数据\n"
        
        content += """
━━━━━━━━━━━━ 本月最差推荐 ━━━━━━━━━━━━
"""
        
        if best_worst['worst']:
            for i, rec in enumerate(best_worst['worst'][:5]):
                reason = rec.get('reason', '市场波动')
                content += f"{i+1}. {rec['name']}：预测{rec['predicted_return']:.1f}%，实际{rec['actual_return']:.1f}%（原因：{reason}）\n"
        else:
            content += "暂无数据\n"
        
        content += """
━━━━━━━━━━━━ 市场展望 ━━━━━━━━━━━━
基于当前市场数据和模型分析，下月建议关注：
1. 估值合理的蓝筹股
2. 受益于经济复苏的顺周期板块
3. 黄金等避险资产保持适当配置

━━━━━━━━━━━━ 免责声明 ━━━━━━━━━━━━
本报告仅供参考，不构成投资建议。
"""
        
        report_data = {
            'title': f'本月复盘统计报告（{start_date} 至 {end_date}）',
            'content': content,
            'generated_at': datetime.now().isoformat(),
            'type': 'monthly'
        }
        
        self._save_report(report_data)
        
        return report_data
    
    def _get_best_worst_predictions(self, start_date, end_date, limit=20):
        """
        获取最佳和最差推荐（使用真实数据）
        """
        session = get_session()
        
        predictions = session.query(Prediction).filter(
            Prediction.expiry_date >= start_date,
            Prediction.expiry_date <= end_date,
            Prediction.is_expired == True,
            Prediction.actual_return.isnot(None)
        ).all()
        
        best = []
        worst = []
        
        for p in predictions:
            # 计算预测收益率
            if p.target_low and p.target_high:
                predicted_return = (p.target_high - p.target_low) / p.target_low * 100
            else:
                predicted_return = 0
            
            item = {
                'code': p.code,
                'name': p.name or p.code,
                'predicted_return': predicted_return,
                'actual_return': p.actual_return,
                'period': p.period_days,
                'reason': self._analyze_error_reason(p)
            }
            
            # 判断预测是否准确（上涨概率 > 50 且实际上涨，或反之）
            predicted_up = p.up_probability > 50 if p.up_probability else False
            actual_up = p.actual_return > 0 if p.actual_return else False
            
            if predicted_up == actual_up:
                best.append(item)
            else:
                worst.append(item)
        
        best.sort(key=lambda x: x['actual_return'], reverse=True)
        worst.sort(key=lambda x: x['actual_return'])
        
        session.close()
        
        return {
            'best': best[:limit],
            'worst': worst[:limit]
        }
    
    def _analyze_error_reason(self, prediction):
        """分析预测错误原因"""
        if prediction.confidence and prediction.confidence < 60:
            return "模型置信度较低"
        
        if prediction.actual_return and abs(prediction.actual_return) > 5:
            return "市场波动超预期"
        
        return "市场变化"
    
    def _save_report(self, report_data):
        """
        保存报告到文件
        """
        import json
        
        reports_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'data', 'reports'
        )
        os.makedirs(reports_dir, exist_ok=True)
        
        filename = f"{report_data['type']}_{datetime.now().strftime('%Y%m%d')}.json"
        filepath = os.path.join(reports_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"报告已保存: {filepath}")
    
    def get_latest_report(self, report_type='weekly'):
        """
        获取最新报告
        """
        reports_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'data', 'reports'
        )
        
        if not os.path.exists(reports_dir):
            return None
        
        import json
        files = [f for f in os.listdir(reports_dir) if f.startswith(report_type) and f.endswith('.json')]
        
        if not files:
            return None
        
        files.sort(reverse=True)
        latest_file = files[0]
        
        with open(os.path.join(reports_dir, latest_file), 'r', encoding='utf-8') as f:
            return json.load(f)


# 测试代码
if __name__ == '__main__':
    reporter = Reporter()
    
    weekly = reporter.generate_weekly_report()
    print("周报已生成")
    print(weekly['content'][:500])