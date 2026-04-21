#!/usr/bin/env python3
"""
回测脚本 - 对历史数据做预测并验证准确率（使用训练好的模型）
修复版：正确加载所有采集的数据文件
"""

import sys
import os
import pickle
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from tqdm import tqdm

sys.path.append(str(Path(__file__).resolve().parent.parent))

from collectors.stock_collector import StockCollector
from predictors.short_term import ShortTermPredictor
from predictors.medium_term import MediumTermPredictor
from predictors.long_term import LongTermPredictor
from models import get_session, Prediction, Review, AccuracyStat
from utils import get_logger
from utils.data_loader import get_data_loader

logger = get_logger(__name__)


class BacktestEngine:
    """回测引擎（使用训练好的模型）- 修复版"""
    
    def __init__(self):
        self.short_predictor = ShortTermPredictor()
        self.medium_predictor = MediumTermPredictor()
        self.long_predictor = LongTermPredictor()
        self.collector = StockCollector()
        self.session = get_session()
        self.data_loader = None
        
        # 加载训练好的模型
        self._load_models()
        
        # 加载辅助数据
        self._load_auxiliary_data()
    
    def _load_models(self):
        """加载训练好的模型"""
        from config import MODELS_DIR
        
        short_model_path = MODELS_DIR / 'short_term_model.pkl'
        if short_model_path.exists():
            with open(short_model_path, 'rb') as f:
                model_data = pickle.load(f)
                self.short_predictor.model = model_data.get('model')
                self.short_predictor.is_trained = True
                logger.info("✅ 已加载5日预测模型")
        
        medium_model_path = MODELS_DIR / 'medium_term_model.pkl'
        if medium_model_path.exists():
            with open(medium_model_path, 'rb') as f:
                model_data = pickle.load(f)
                self.medium_predictor.model = model_data.get('model')
                self.medium_predictor.is_trained = True
                logger.info("✅ 已加载20日预测模型")
    
    def _load_auxiliary_data(self):
        """加载辅助数据（使用统一数据加载器）"""
        try:
            self.data_loader = get_data_loader(force_reload=False)
            data = self.data_loader.data
            
            if data.get('moneyflow') is not None:
                self.fund_df = data['moneyflow']
                logger.info(f"资金流向数据: {len(self.fund_df)} 条")
            else:
                logger.warning("资金流向数据未找到")
                self.fund_df = None
            
            if data.get('north_money') is not None:
                self.north_df = data['north_money']
                logger.info(f"北向资金数据: {len(self.north_df)} 条")
            else:
                logger.warning("北向资金数据未找到")
                self.north_df = None
            
            if data.get('valuation') is not None:
                self.valuation_df = data['valuation']
                logger.info(f"估值数据: {len(self.valuation_df)} 条")
            else:
                logger.warning("估值数据未找到")
                self.valuation_df = None
            
        except Exception as e:
            logger.warning(f"加载辅助数据失败: {e}")
            self.fund_df = None
            self.north_df = None
            self.valuation_df = None
    
    def _get_auxiliary_features(self, code: str, cur_date) -> Dict[str, float]:
        """获取辅助特征（资金流向、北向资金、估值数据）"""
        features = {
            'net_mf_amount': 0,
            'north_money': 0,
            'cpi': 0,
            'pmi': 50,
            'shibor_1w': 0,
            'pe': 0,
            'pb': 0
        }
        
        if self.data_loader is None:
            return features
        
        # 资金流向
        moneyflow = self.data_loader.get_moneyflow_for_stock(code, cur_date)
        if moneyflow:
            features['net_mf_amount'] = moneyflow.get('net_mf_amount', 0) / 1e8
        
        # 北向资金
        north_money = self.data_loader.get_north_money_for_date(cur_date)
        if north_money:
            features['north_money'] = north_money / 1e8
        
        # 估值数据
        valuation = self.data_loader.get_valuation_for_stock(code, cur_date)
        if valuation:
            features['pe'] = valuation.get('pe', 0) or 0
            features['pb'] = valuation.get('pb', 0) or 0
        
        return features
    
    def backtest_stock(self, code: str, start_date, end_date) -> Optional[Dict]:
        """对单只股票进行回测"""
        # 使用数据加载器获取数据
        df = self.data_loader.get_stock_with_all_features(code, start_date, end_date) if self.data_loader else None
        
        if df is None or len(df) < 100:
            # 备用：从collector获取
            df = self.collector.get_stock_data_from_db(code, start_date, end_date)
            if df is None or len(df) < 100:
                return None
        
        code_num = code.replace('.SH', '').replace('.SZ', '').lstrip('0')
        
        results = {
            'code': code,
            'name': code,
            'total_predictions': 0,
            'correct_predictions': 0,
            'by_period': {5: {'total': 0, 'correct': 0}}
        }
        
        dates = df.index
        for i in tqdm(range(60, len(dates) - 5), desc=f"回测 {code}", leave=False):
            window_df = df.iloc[:i+1]
            cur_date = dates[i]
            
            # 获取辅助特征
            aux_features = self._get_auxiliary_features(code_num, cur_date)
            
            try:
                X = self.short_predictor.prepare_features(window_df)
                if X is not None:
                    # 合并辅助特征
                    X_dict = X.iloc[0].to_dict()
                    X_dict.update(aux_features)
                    
                    if self.short_predictor.is_trained and self.short_predictor.model is not None:
                        feature_order = list(X_dict.keys())
                        X_array = np.array([[float(X_dict[f]) for f in feature_order]])
                        prob = self.short_predictor.model.predict_proba(X_array)[0]
                        up_prob = prob[1] * 100
                    else:
                        up_prob = 50
                    
                    actual_return = (df.iloc[i+5]['close'] - df.iloc[i]['close']) / df.iloc[i]['close']
                    is_correct = (up_prob > 50) == (actual_return > 0)
                    
                    results['by_period'][5]['total'] += 1
                    if is_correct:
                        results['by_period'][5]['correct'] += 1
                        results['correct_predictions'] += 1
                    results['total_predictions'] += 1
            except Exception as e:
                continue
        
        return results
    
    def run_backtest(self, stock_list: List[str], start_date, end_date) -> Optional[Dict]:
        """运行完整回测"""
        all_results = []
        
        for code in tqdm(stock_list, desc="回测进度"):
            result = self.backtest_stock(code, start_date, end_date)
            if result and result['total_predictions'] > 0:
                all_results.append(result)
        
        if not all_results:
            return None
        
        summary = {
            'total_predictions': sum(r['total_predictions'] for r in all_results),
            'correct_predictions': sum(r['correct_predictions'] for r in all_results),
            'by_period': {5: {'total': 0, 'correct': 0}}
        }
        
        for r in all_results:
            for period in [5]:
                summary['by_period'][period]['total'] += r['by_period'][period]['total']
                summary['by_period'][period]['correct'] += r['by_period'][period]['correct']
        
        summary['overall_accuracy'] = summary['correct_predictions'] / summary['total_predictions'] * 100 if summary['total_predictions'] > 0 else 0
        for period in [5]:
            period_total = summary['by_period'][period]['total']
            if period_total > 0:
                summary['by_period'][period]['accuracy'] = summary['by_period'][period]['correct'] / period_total * 100
            else:
                summary['by_period'][period]['accuracy'] = 0
        
        return summary
    
    def save_results_to_db(self, summary: Dict) -> None:
        """保存回测结果到数据库"""
        today = datetime.now().date()
        
        stat = self.session.query(AccuracyStat).filter(
            AccuracyStat.stat_date == today,
            AccuracyStat.asset_type == 'backtest'
        ).first()
        
        if stat:
            stat.total_count = summary['total_predictions']
            stat.correct_count = summary['correct_predictions']
            stat.accuracy = summary['overall_accuracy']
        else:
            stat = AccuracyStat(
                stat_date=today,
                period_days=0,
                asset_type='backtest',
                total_count=summary['total_predictions'],
                correct_count=summary['correct_predictions'],
                accuracy=summary['overall_accuracy']
            )
            self.session.add(stat)
        
        for period in [5]:
            period_stat = self.session.query(AccuracyStat).filter(
                AccuracyStat.stat_date == today,
                AccuracyStat.period_days == period,
                AccuracyStat.asset_type == 'backtest'
            ).first()
            
            if period_stat:
                period_stat.total_count = summary['by_period'][period]['total']
                period_stat.correct_count = summary['by_period'][period]['correct']
                period_stat.accuracy = summary['by_period'][period]['accuracy']
            else:
                period_stat = AccuracyStat(
                    stat_date=today,
                    period_days=period,
                    asset_type='backtest',
                    total_count=summary['by_period'][period]['total'],
                    correct_count=summary['by_period'][period]['correct'],
                    accuracy=summary['by_period'][period]['accuracy']
                )
                self.session.add(period_stat)
        
        self.session.commit()
        logger.info("回测结果已保存到数据库")
    
    def close(self):
        """关闭资源"""
        self.session.close()


def get_all_stocks_with_data() -> List[str]:
    """获取所有有数据的股票"""
    from config import HISTORICAL_A_STOCK_FILE
    
    try:
        if HISTORICAL_A_STOCK_FILE.exists():
            price_df = pd.read_csv(HISTORICAL_A_STOCK_FILE)
            stocks = price_df['code'].unique().tolist()
            logger.info(f"从价格数据获取到 {len(stocks)} 只股票")
            return stocks[:100]  # 取前100只测试
    except Exception as e:
        logger.error(f"获取股票列表失败: {e}")
    
    # 备用：使用预设股票
    return ['000333.SZ', '002415.SZ', '000858.SZ', '300750.SZ', '002475.SZ']


if __name__ == '__main__':
    # 获取所有有数据的股票
    test_stocks = get_all_stocks_with_data()
    
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=180)
    
    print("=" * 60)
    print("开始回测（使用训练好的模型）")
    print("=" * 60)
    print(f"股票数量: {len(test_stocks)} 只")
    print(f"时间范围: {start_date} 至 {end_date}")
    print()
    
    engine = BacktestEngine()
    summary = engine.run_backtest(test_stocks, start_date, end_date)
    
    if summary:
        print("\n" + "=" * 60)
        print("回测结果")
        print("=" * 60)
        print(f"总预测次数: {summary['total_predictions']}")
        print(f"正确次数: {summary['correct_predictions']}")
        print(f"整体准确率: {summary['overall_accuracy']:.2f}%")
        print()
        print("按周期统计:")
        print(f"  5日预测: {summary['by_period'][5]['accuracy']:.2f}% ({summary['by_period'][5]['correct']}/{summary['by_period'][5]['total']})")
        
        # 保存结果
        engine.save_results_to_db(summary)
    else:
        print("❌ 无回测结果")
    
    engine.close()