#!/usr/bin/env python3
"""
优化版训练脚本 - 使用所有采集的数据训练预测模型
基于数据文件: historical_*.csv, daily_basic.csv, moneyflow_all.csv, north_money_all.csv
"""

import sys
import os
import pickle
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, List
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

# 添加项目根目录到路径
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import (
    MODELS_DIR, DATA_DIR, XGBOOST_PARAMS, TRAIN_TEST_SPLIT,
    PREDICTION_THRESHOLD, PREDICTION_PERIODS,
    SHORT_TERM_MODEL_FILE, MEDIUM_TERM_MODEL_FILE, LONG_TERM_MODEL_FILE,
    GOLD_MODEL_FILE, SILVER_MODEL_FILE, TRAINING_STATS_FILE,
    HISTORICAL_A_STOCK_FILE, HISTORICAL_HK_STOCK_FILE, HISTORICAL_US_STOCK_FILE,
    DAILY_BASIC_FILE, MONEYFLOW_FILE, NORTH_MONEY_FILE
)
from indicators.feature_extractor import FeatureExtractor, get_feature_extractor
from indicators.technical import TechnicalIndicator
from utils import get_logger, ensure_dir
from utils.data_loader import get_data_loader

logger = get_logger(__name__)

# 确保模型目录存在
ensure_dir(MODELS_DIR)


class ModelTrainer:
    """模型训练器（优化版 - 使用所有采集数据）"""
    
    def __init__(self, threshold: Optional[float] = None):
        self.threshold = threshold if threshold is not None else PREDICTION_THRESHOLD
        self.feature_extractor = get_feature_extractor()
        self.technical = TechnicalIndicator()
        self.training_stats: Dict[str, Any] = {}
        self.data_loader = None
    
    def load_all_data(self) -> Dict[str, Optional[pd.DataFrame]]:
        """加载所有采集的数据"""
        self.data_loader = get_data_loader(force_reload=True)
        return self.data_loader.data
    
    def get_external_features(self, code: str, date) -> Dict[str, float]:
        """获取外部特征（估值、资金流向、北向资金）"""
        features = {
            'net_mf_amount': 0,
            'north_money': 0,
            'pe': 0,
            'pb': 0,
            'pe_percentile': 50
        }
        
        if self.data_loader is None:
            return features
        
        # 获取估值数据
        valuation = self.data_loader.get_valuation_for_stock(code, date)
        if valuation:
            features['pe'] = valuation.get('pe', 0) or 0
            features['pb'] = valuation.get('pb', 0) or 0
        
        # 获取资金流向
        moneyflow = self.data_loader.get_moneyflow_for_stock(code, date)
        if moneyflow:
            features['net_mf_amount'] = moneyflow.get('net_mf_amount', 0) / 1e8  # 转换为亿
        
        # 获取北向资金
        north_money = self.data_loader.get_north_money_for_date(date)
        if north_money:
            features['north_money'] = north_money / 1e8  # 转换为亿
        
        return features
    
    def prepare_training_data_with_features(self, df: pd.DataFrame, period_days: int = 5) -> Tuple[Optional[pd.DataFrame], Optional[pd.Series]]:
        """
        准备训练数据（包含外部特征）
        Args:
            df: 历史数据DataFrame（需要包含code列）
            period_days: 预测周期
        Returns:
            tuple: (X, y) 特征和标签
        """
        if df is None or len(df) < 60 + period_days:
            return None, None
        
        codes = df['code'].unique()
        X_list = []
        y_list = []
        
        for code in tqdm(codes, desc=f"准备{period_days}日数据", leave=False):
            code_df = df[df['code'] == code].copy()
            code_df = code_df.set_index('date').sort_index()
            
            if len(code_df) < 60 + period_days:
                continue
            
            close = code_df['close'].values
            
            # 滑动窗口
            for i in range(60, len(code_df) - period_days, 5):
                window_df = code_df.iloc[:i+1]
                current_date = window_df.index[-1]
                
                # 获取外部特征
                external_features = self.get_external_features(code, current_date)
                
                # 使用统一特征提取器
                features = self.feature_extractor.extract_features_from_df(window_df, external_features)
                
                if features is not None:
                    # 计算标签
                    future_return = (close[i + period_days] - close[i]) / close[i]
                    label = 1 if future_return > self.threshold else 0
                    
                    X_list.append(features.iloc[0].to_dict())
                    y_list.append(label)
        
        if not X_list:
            return None, None
        
        X = pd.DataFrame(X_list)[self.feature_extractor.get_feature_columns()]
        y = pd.Series(y_list)
        
        return X, y
    
    def train_model(self, X: Optional[pd.DataFrame], y: Optional[pd.Series], 
                    model_name: str, period_days: int) -> Optional[Dict]:
        """训练模型"""
        print(f"\n{'='*60}")
        print(f"训练 {model_name} ({period_days}日预测)")
        print(f"{'='*60}")
        
        if X is None or len(X) < 100:
            print(f"  ❌ 数据不足: {len(X) if X is not None else 0} 个样本")
            return None
        
        print(f"  ✅ 样本数: {len(X)}")
        print(f"  ✅ 特征数: {len(X.columns)}")
        
        # 检查正负样本比例
        pos_ratio = y.mean()
        print(f"  正样本比例: {pos_ratio:.2%}")
        
        # 如果样本不平衡，使用类别权重
        if pos_ratio < 0.3 or pos_ratio > 0.7:
            print(f"  ⚠️ 样本不平衡，将使用类别权重")
            scale_pos_weight = (1 - pos_ratio) / pos_ratio if pos_ratio > 0 else 1
        else:
            scale_pos_weight = 1
        
        # 划分训练集和验证集
        split_idx = int(len(X) * TRAIN_TEST_SPLIT)
        X_train, X_val = X[:split_idx], X[split_idx:]
        y_train, y_val = y[:split_idx], y[split_idx:]
        
        print(f"  训练集: {len(X_train)} 样本")
        print(f"  验证集: {len(X_val)} 样本")
        
        # 训练模型
        print("  训练XGBoost模型...")
        
        try:
            import xgboost as xgb
            
            params = XGBOOST_PARAMS.copy()
            if scale_pos_weight != 1:
                params['scale_pos_weight'] = scale_pos_weight
            
            model = xgb.XGBClassifier(
                **params,
                eval_metric='logloss',
                use_label_encoder=False
            )
            
            model.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                verbose=False
            )
            
            # 评估
            train_acc = model.score(X_train, y_train)
            val_acc = model.score(X_val, y_val)
            
            print(f"  📊 训练准确率: {train_acc:.2%}")
            print(f"  📊 验证准确率: {val_acc:.2%}")
            
            # 保存模型
            model_path = MODELS_DIR / f"{model_name}_model.pkl"
            
            model_data = {
                'model': model,
                'feature_columns': list(X.columns),
                'train_accuracy': train_acc,
                'val_accuracy': val_acc,
                'train_date': datetime.now().isoformat(),
                'period_days': period_days,
                'threshold': self.threshold,
                'pos_ratio': pos_ratio
            }
            
            with open(model_path, 'wb') as f:
                pickle.dump(model_data, f)
            
            print(f"  ✅ 模型已保存: {model_path}")
            
            # 记录训练统计
            self.training_stats[model_name] = {
                'val_accuracy': val_acc,
                'train_accuracy': train_acc,
                'samples': len(X_train),
                'features': len(X.columns),
                'period_days': period_days,
                'threshold': self.threshold,
                'pos_ratio': pos_ratio,
                'train_date': datetime.now().isoformat()
            }
            
            return model_data
            
        except ImportError:
            print("  ⚠️ XGBoost未安装，使用RandomForest替代")
            return self._train_random_forest(X_train, X_val, y_train, y_val, model_name, period_days)
        except Exception as e:
            print(f"  ❌ 训练失败: {e}")
            return None
    
    def _train_random_forest(self, X_train: pd.DataFrame, X_val: pd.DataFrame,
                              y_train: pd.Series, y_val: pd.Series,
                              model_name: str, period_days: int) -> Optional[Dict]:
        """使用随机森林训练"""
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.preprocessing import StandardScaler
        
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_val_scaled = scaler.transform(X_val)
        
        model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            min_samples_split=5,
            class_weight='balanced',
            random_state=42,
            n_jobs=-1
        )
        
        model.fit(X_train_scaled, y_train)
        
        train_acc = model.score(X_train_scaled, y_train)
        val_acc = model.score(X_val_scaled, y_val)
        
        print(f"  📊 训练准确率: {train_acc:.2%}")
        print(f"  📊 验证准确率: {val_acc:.2%}")
        
        model_path = MODELS_DIR / f"{model_name}_model.pkl"
        
        model_data = {
            'model': model,
            'scaler': scaler,
            'feature_columns': list(X_train.columns),
            'train_accuracy': train_acc,
            'val_accuracy': val_acc,
            'train_date': datetime.now().isoformat(),
            'period_days': period_days,
            'threshold': self.threshold
        }
        
        with open(model_path, 'wb') as f:
            pickle.dump(model_data, f)
        
        print(f"  ✅ 模型已保存: {model_path}")
        
        self.training_stats[model_name] = {
            'val_accuracy': val_acc,
            'train_accuracy': train_acc,
            'samples': len(X_train),
            'features': len(X_train.columns),
            'period_days': period_days,
            'threshold': self.threshold,
            'train_date': datetime.now().isoformat()
        }
        
        return model_data
    
    def train_precious_metal_model(self, symbol: str, model_name: str, 
                                    period_days: int = 5) -> Optional[Dict]:
        """训练贵金属预测模型"""
        print(f"\n{'='*60}")
        print(f"训练{model_name}模型 ({period_days}日)")
        print(f"{'='*60}")
        
        try:
            import yfinance as yf
            
            print(f"  从 yfinance 获取 {symbol} 数据...")
            ticker = yf.Ticker(symbol)
            df = ticker.history(period='2y')
            
            if len(df) < 100:
                print(f"  ❌ 数据不足: {len(df)} 条")
                return None
            
            df = df.reset_index()
            df.columns = [c.lower() for c in df.columns]
            df['code'] = model_name.upper()
            
            print(f"  ✅ {model_name}数据: {len(df)} 条")
            
            # 准备数据
            X, y = self.prepare_training_data_with_features(df, period_days)
            
            if X is None or len(X) < 50:
                print(f"  ❌ 训练数据不足")
                return None
            
            print(f"  ✅ 样本数: {len(X)}")
            
            # 训练
            from sklearn.ensemble import RandomForestClassifier
            from sklearn.preprocessing import StandardScaler
            
            split_idx = int(len(X) * TRAIN_TEST_SPLIT)
            X_train, X_val = X[:split_idx], X[split_idx:]
            y_train, y_val = y[:split_idx], y[split_idx:]
            
            scaler = StandardScaler()
            X_train_scaled = scaler.fit_transform(X_train)
            X_val_scaled = scaler.transform(X_val)
            
            model = RandomForestClassifier(
                n_estimators=100,
                max_depth=8,
                class_weight='balanced',
                random_state=42,
                n_jobs=-1
            )
            model.fit(X_train_scaled, y_train)
            
            train_acc = model.score(X_train_scaled, y_train)
            val_acc = model.score(X_val_scaled, y_val)
            
            print(f"  📊 训练准确率: {train_acc:.2%}")
            print(f"  📊 验证准确率: {val_acc:.2%}")
            
            # 保存
            model_path = MODELS_DIR / f"{model_name}_model.pkl"
            model_data = {
                'model': model,
                'scaler': scaler,
                'feature_columns': list(X.columns),
                'train_accuracy': train_acc,
                'val_accuracy': val_acc,
                'train_date': datetime.now().isoformat()
            }
            
            with open(model_path, 'wb') as f:
                pickle.dump(model_data, f)
            
            print(f"  ✅ {model_name}模型已保存: {model_path}")
            
            self.training_stats[model_name] = {
                'val_accuracy': val_acc,
                'train_accuracy': train_acc,
                'samples': len(X_train),
                'train_date': datetime.now().isoformat()
            }
            
            return model_data
            
        except Exception as e:
            print(f"  ❌ {model_name}模型训练失败: {e}")
            return None
    
    def print_summary(self) -> None:
        """打印训练摘要"""
        print("\n" + "=" * 60)
        print("训练结果汇总")
        print("=" * 60)
        
        if not self.training_stats:
            print("  ❌ 无成功训练的模型")
            return
        
        for name, stats in self.training_stats.items():
            val_acc = stats.get('val_accuracy', 0)
            samples = stats.get('samples', 0)
            features = stats.get('features', 0)
            print(f"  ✅ {name}: 验证准确率 {val_acc:.2%} (样本数: {samples}, 特征数: {features})")
    
    def save_training_stats(self) -> None:
        """保存训练统计"""
        import json
        existing_stats = {}
        if TRAINING_STATS_FILE.exists():
            try:
                with open(TRAINING_STATS_FILE, 'r', encoding='utf-8') as f:
                    existing_stats = json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        
        existing_stats[datetime.now().strftime('%Y%m%d_%H%M%S')] = self.training_stats
        
        with open(TRAINING_STATS_FILE, 'w', encoding='utf-8') as f:
            json.dump(existing_stats, f, indent=2, ensure_ascii=False)
        
        logger.info(f"训练统计已保存: {TRAINING_STATS_FILE}")
    
    def train_all(self, periods: Optional[List[int]] = None, train_gold: bool = True) -> Dict[str, Any]:
        """
        训练所有模型
        Args:
            periods: 要训练的周期列表，默认使用配置中的 PREDICTION_PERIODS
            train_gold: 是否训练贵金属模型
        Returns:
            dict: 训练结果
        """
        if periods is None:
            periods = PREDICTION_PERIODS
        
        # 加载所有数据
        print("\n" + "=" * 60)
        print("加载采集数据")
        print("=" * 60)
        
        all_data = self.load_all_data()
        stock_df = all_data.get('stock')
        
        if stock_df is None or len(stock_df) < 1000:
            print("\n❌ 没有足够的历史数据，请先运行数据采集脚本")
            print("   python3 scripts/collect_historical_data.py --years 2")
            return {'success': False, 'error': 'No historical data'}
        
        print(f"\n📊 股票数据: {len(stock_df)} 条, {stock_df['code'].nunique()} 只股票")
        
        # 显示其他数据统计
        if all_data.get('valuation') is not None:
            print(f"📊 估值数据: {len(all_data['valuation'])} 条")
        if all_data.get('moneyflow') is not None:
            print(f"📊 资金流向: {len(all_data['moneyflow'])} 条")
        if all_data.get('north_money') is not None:
            print(f"📊 北向资金: {len(all_data['north_money'])} 条")
        
        results = {'success': True, 'models': {}}
        
        # 训练各周期模型
        period_names = {
            5: 'short_term',
            20: 'medium_term',
            60: 'long_term'
        }
        
        for period in periods:
            name = period_names.get(period, f'{period}d')
            X, y = self.prepare_training_data_with_features(stock_df, period)
            model_data = self.train_model(X, y, name, period)
            results['models'][name] = model_data is not None
        
        # 训练贵金属模型
        if train_gold:
            gold_result = self.train_precious_metal_model('GC=F', 'gold', 5)
            silver_result = self.train_precious_metal_model('SI=F', 'silver', 5)
            results['models']['gold'] = gold_result is not None
            results['models']['silver'] = silver_result is not None
        
        # 打印摘要并保存统计
        self.print_summary()
        self.save_training_stats()
        
        return results


def main():
    parser = argparse.ArgumentParser(description='AI理财助手模型训练')
    parser.add_argument('--periods', type=str, default='5,20,60',
                        help='训练周期，逗号分隔 (5,20,60)')
    parser.add_argument('--threshold', type=float, default=None,
                        help='上涨阈值，默认使用配置')
    parser.add_argument('--no-gold', action='store_true', help='跳过贵金属训练')
    parser.add_argument('--force-reload', action='store_true', help='强制重新加载数据')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("AI理财助手 - 模型训练（使用所有采集数据）")
    print("=" * 60)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 解析周期
    periods = [int(p.strip()) for p in args.periods.split(',')]
    print(f"训练周期: {periods}")
    print(f"上涨阈值: {args.threshold if args.threshold else '使用默认配置'}")
    
    trainer = ModelTrainer(threshold=args.threshold)
    
    # 如果强制重新加载，先重置数据加载器
    if args.force_reload:
        import utils.data_loader as data_loader_module
        # 重置模块级别的全局变量
        if hasattr(data_loader_module, '_data_loader'):
            data_loader_module._data_loader = None
    
    results = trainer.train_all(periods=periods, train_gold=not args.no_gold)
    
    print("\n" + "=" * 60)
    print(f"训练完成: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"模型保存在: {MODELS_DIR}")
    print("=" * 60)


if __name__ == '__main__':
    main()