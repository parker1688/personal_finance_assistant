"""
复盘分析模块 - reviews/analyzer.py
分析预测准确率、误差等 - 优化版
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from functools import lru_cache

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import get_session, Prediction, Review, AccuracyStat
from utils import get_logger

logger = get_logger(__name__)


class AccuracyAnalyzer:
    """准确率分析器（优化版）"""
    
    def __init__(self):
        self.session = get_session()
        self._cache = {}
    
    def calculate_period_accuracy(self, period_days, start_date=None, end_date=None):
        """
        计算指定周期的准确率
        Args:
            period_days: 预测周期
            start_date: 开始日期
            end_date: 结束日期
        Returns:
            dict: 准确率统计
        """
        # 构建缓存键
        cache_key = f"period_{period_days}_{start_date}_{end_date}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        query = self.session.query(Prediction).filter(
            Prediction.period_days == period_days,
            Prediction.is_expired == True,
            Prediction.is_direction_correct.isnot(None)
        )
        
        if start_date:
            query = query.filter(Prediction.expiry_date >= start_date)
        if end_date:
            query = query.filter(Prediction.expiry_date <= end_date)
        
        predictions = query.all()
        
        if not predictions:
            result = {
                'total': 0,
                'correct': 0,
                'accuracy': 0,
                'by_confidence': {},
                'by_asset_type': {}
            }
            self._cache[cache_key] = result
            return result
        
        total = len(predictions)
        correct = sum(1 for p in predictions if p.is_direction_correct)
        accuracy = (correct / total * 100) if total > 0 else 0
        
        # 按置信度分组统计
        by_confidence = {
            'high': {'total': 0, 'correct': 0},
            'medium': {'total': 0, 'correct': 0},
            'low': {'total': 0, 'correct': 0}
        }
        
        # 按资产类型分组统计（使用新增的asset_type字段）
        by_asset_type = {}
        
        for p in predictions:
            # 置信度分组
            if p.confidence and p.confidence >= 70:
                group = 'high'
            elif p.confidence and p.confidence >= 50:
                group = 'medium'
            else:
                group = 'low'
            
            by_confidence[group]['total'] += 1
            if p.is_direction_correct:
                by_confidence[group]['correct'] += 1
            
            # 资产类型分组
            asset_type = getattr(p, 'asset_type', self._detect_asset_type(p.code))
            if asset_type not in by_asset_type:
                by_asset_type[asset_type] = {'total': 0, 'correct': 0}
            by_asset_type[asset_type]['total'] += 1
            if p.is_direction_correct:
                by_asset_type[asset_type]['correct'] += 1
        
        # 计算各分组准确率
        for group in by_confidence:
            total_g = by_confidence[group]['total']
            if total_g > 0:
                by_confidence[group]['accuracy'] = by_confidence[group]['correct'] / total_g * 100
            else:
                by_confidence[group]['accuracy'] = 0
        
        for asset_type in by_asset_type:
            total_a = by_asset_type[asset_type]['total']
            if total_a > 0:
                by_asset_type[asset_type]['accuracy'] = by_asset_type[asset_type]['correct'] / total_a * 100
            else:
                by_asset_type[asset_type]['accuracy'] = 0
        
        result = {
            'total': total,
            'correct': correct,
            'accuracy': round(accuracy, 1),
            'by_confidence': by_confidence,
            'by_asset_type': by_asset_type
        }
        
        self._cache[cache_key] = result
        return result
    
    def _detect_asset_type(self, code):
        """根据代码判断资产类型"""
        code = code.upper()
        if code.endswith(('.SH', '.SZ')):
            return 'a_stock'
        elif code.endswith('.HK'):
            return 'hk_stock'
        elif code.isalpha() and len(code) <= 5:
            return 'us_stock'
        elif code.startswith(('51', '15', '16')) and len(code) <= 6:
            return 'etf'
        elif code.isdigit() and len(code) == 6:
            return 'fund'
        else:
            return 'other'
    
    def calculate_asset_type_accuracy(self, asset_type, start_date=None, end_date=None):
        """
        计算指定资产类型的准确率（使用asset_type字段）
        """
        cache_key = f"asset_{asset_type}_{start_date}_{end_date}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        query = self.session.query(Prediction).filter(
            Prediction.asset_type == asset_type,
            Prediction.is_expired == True,
            Prediction.is_direction_correct.isnot(None)
        )
        
        if start_date:
            query = query.filter(Prediction.expiry_date >= start_date)
        if end_date:
            query = query.filter(Prediction.expiry_date <= end_date)
        
        predictions = query.all()
        
        if not predictions:
            result = {'total': 0, 'correct': 0, 'accuracy': 0}
            self._cache[cache_key] = result
            return result
        
        total = len(predictions)
        correct = sum(1 for p in predictions if p.is_direction_correct)
        accuracy = (correct / total * 100) if total > 0 else 0
        
        result = {
            'total': total,
            'correct': correct,
            'accuracy': round(accuracy, 1)
        }
        
        self._cache[cache_key] = result
        return result
    
    def calculate_trend_accuracy(self, days=30):
        """
        计算趋势准确率（按日期）
        Returns:
            list: 每日准确率趋势
        """
        trend = []
        
        for i in range(days):
            date = datetime.now().date() - timedelta(days=i)
            start_date = date
            end_date = date
            
            predictions = self.session.query(Prediction).filter(
                Prediction.expiry_date == date,
                Prediction.is_expired == True,
                Prediction.is_direction_correct.isnot(None)
            ).all()
            
            total = len(predictions)
            correct = sum(1 for p in predictions if p.is_direction_correct)
            accuracy = (correct / total * 100) if total > 0 else 0
            
            trend.append({
                'date': date.isoformat(),
                'total': total,
                'correct': correct,
                'accuracy': round(accuracy, 1)
            })
        
        trend.reverse()
        return trend
    
    def analyze_error_patterns(self, limit=50):
        """
        分析错误模式（基于实际数据特征）
        Returns:
            dict: 错误模式分析
        """
        predictions = self.session.query(Prediction).filter(
            Prediction.is_expired == True,
            Prediction.is_direction_correct == False
        ).order_by(Prediction.expiry_date.desc()).limit(limit).all()
        
        error_patterns = {
            'trend_reversal': 0,      # 趋势反转
            'high_volatility': 0,      # 高波动
            'low_confidence': 0,       # 低置信度
            'gap_open': 0,             # 跳空开盘
            'news_impact': 0,          # 新闻影响
            'other': 0
        }
        
        for p in predictions:
            pattern = self._identify_error_pattern_by_features(p)
            error_patterns[pattern] += 1
        
        return {
            'total_errors': len(predictions),
            'patterns': error_patterns,
            'most_common': max(error_patterns, key=error_patterns.get) if error_patterns else None
        }
    
    def _identify_error_pattern_by_features(self, prediction):
        """
        基于特征识别错误模式
        """
        # 低置信度
        if prediction.confidence and prediction.confidence < 60:
            return 'low_confidence'
        
        # 高波动（通过实际收益率判断）
        if prediction.actual_return and abs(prediction.actual_return) > 5:
            return 'high_volatility'
        
        # 趋势反转（通过复盘记录判断）
        review = self.session.query(Review).filter(
            Review.prediction_id == prediction.id
        ).first()
        
        if review and '趋势反转' in (review.error_analysis or ''):
            return 'trend_reversal'
        
        if review and '跳空' in (review.error_analysis or ''):
            return 'gap_open'
        
        if review and '新闻' in (review.error_analysis or ''):
            return 'news_impact'
        
        return 'other'
    
    def get_performance_summary(self):
        """
        获取性能摘要
        Returns:
            dict: 性能摘要
        """
        # 整体准确率
        overall = self.calculate_period_accuracy(0)
        
        # 各周期准确率
        period_5d = self.calculate_period_accuracy(5)
        period_20d = self.calculate_period_accuracy(20)
        period_60d = self.calculate_period_accuracy(60)
        
        # 趋势
        trend = self.calculate_trend_accuracy(30)
        
        # 错误分析
        errors = self.analyze_error_patterns()
        
        # 计算最近30天的平均准确率
        recent_accuracy = 0
        if trend:
            recent_accuracies = [t['accuracy'] for t in trend[-7:] if t['total'] > 0]
            recent_accuracy = sum(recent_accuracies) / len(recent_accuracies) if recent_accuracies else 0
        
        return {
            'overall_accuracy': overall['accuracy'],
            'period_5d_accuracy': period_5d['accuracy'],
            'period_20d_accuracy': period_20d['accuracy'],
            'period_60d_accuracy': period_60d['accuracy'],
            'recent_7d_accuracy': round(recent_accuracy, 1),
            'trend': trend,
            'error_analysis': errors,
            'last_updated': datetime.now().isoformat()
        }
    
    def update_accuracy_stats(self, date=None):
        """
        更新准确率统计表
        """
        if date is None:
            date = datetime.now().date()
        
        try:
            # 计算当日各周期准确率
            for period in [5, 20, 60]:
                period_stats = self.calculate_period_accuracy(period, date, date)
                
                # 更新数据库
                stat = self.session.query(AccuracyStat).filter(
                    AccuracyStat.stat_date == date,
                    AccuracyStat.period_days == period
                ).first()
                
                if stat:
                    stat.total_count = period_stats['total']
                    stat.correct_count = period_stats['correct']
                    stat.accuracy = period_stats['accuracy']
                else:
                    stat = AccuracyStat(
                        stat_date=date,
                        period_days=period,
                        asset_type='all',
                        total_count=period_stats['total'],
                        correct_count=period_stats['correct'],
                        accuracy=period_stats['accuracy']
                    )
                    self.session.add(stat)
            
            self.session.commit()
            logger.info(f"准确率统计已更新: {date}")
            
            # 清空缓存
            self._cache.clear()
            
        except Exception as e:
            logger.error(f"更新准确率统计失败: {e}")
            self.session.rollback()
    
    def clear_cache(self):
        """清空缓存"""
        self._cache.clear()
    
    def close(self):
        """关闭数据库连接"""
        self.session.close()


# 测试代码
if __name__ == '__main__':
    analyzer = AccuracyAnalyzer()
    summary = analyzer.get_performance_summary()
    print("性能摘要:")
    for key, value in summary.items():
        if key != 'trend':
            print(f"  {key}: {value}")
    analyzer.close()