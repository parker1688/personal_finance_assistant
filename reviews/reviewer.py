"""
复盘执行模块 - reviews/reviewer.py
检查到期预测并执行复盘 - 优化版
"""

import sys
import os
import json
from datetime import datetime, timedelta
from functools import lru_cache

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import get_session, Prediction, Review, DailyPrice, RawStockData, RawFundData, AccuracyStat, PredictionAccuracy, Recommendation
from collectors.stock_collector import StockCollector
from utils import get_logger, get_today, get_cached_price, set_cached_price, retry

logger = get_logger(__name__)


class Reviewer:
    """复盘执行器（优化版）"""
    
    def __init__(self):
        self.session = get_session()
        self.collector = StockCollector()
        self._price_cache = {}  # 内存缓存
        self._learning_status_file = os.path.join('data', 'cache', 'learning_loop_status.json')

    def _default_learning_status(self):
        return {
            'updated_at': datetime.now().isoformat(),
            'last_review': {
                'time': None,
                'reviewed_count': 0,
                'status': 'idle',
                'message': '暂无复盘记录'
            },
            'last_reflection': {
                'time': None,
                'has_adjustments': False,
                'adjustments_count': 0,
                'retrain_targets': []
            },
            'last_retrain': {
                'time': None,
                'status': 'idle',
                'periods': [],
                'results': {},
                'error': None
            }
        }

    def _load_learning_status(self):
        os.makedirs(os.path.dirname(self._learning_status_file), exist_ok=True)
        if not os.path.exists(self._learning_status_file):
            return self._default_learning_status()

        try:
            with open(self._learning_status_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            default = self._default_learning_status()
            default.update(data)
            return default
        except Exception as e:
            logger.warning(f"读取学习闭环状态失败，使用默认状态: {e}")
            return self._default_learning_status()

    def _save_learning_status(self, status):
        status['updated_at'] = datetime.now().isoformat()
        os.makedirs(os.path.dirname(self._learning_status_file), exist_ok=True)
        with open(self._learning_status_file, 'w', encoding='utf-8') as f:
            json.dump(status, f, ensure_ascii=False, indent=2)

    def _update_learning_status(self, section, payload):
        status = self._load_learning_status()
        if section not in status:
            status[section] = {}
        status[section].update(payload)
        self._save_learning_status(status)
    
    def _count_reviewed_predictions(self):
        try:
            return int(self.session.query(Prediction).filter(Prediction.is_direction_correct.isnot(None)).count())
        except Exception:
            return 0

    def _build_code_variants(self, code):
        raw = str(code or '').strip().upper()
        variants = []
        for item in [raw, raw.split('.')[0] if raw else '']:
            if item and item not in variants:
                variants.append(item)
        return variants

    def _get_due_review_gap(self):
        try:
            today = get_today()
            total_due = int(self.session.query(Prediction).filter(Prediction.expiry_date <= today).count())
            reviewed_due = int(self.session.query(Prediction).filter(Prediction.expiry_date <= today, Prediction.is_direction_correct.isnot(None)).count())
            return {
                'total_due': total_due,
                'reviewed_due': reviewed_due,
                'missing_due': max(total_due - reviewed_due, 0),
                'coverage_pct': round((reviewed_due / total_due) * 100, 2) if total_due else 100.0,
            }
        except Exception:
            return {'total_due': 0, 'reviewed_due': 0, 'missing_due': 0, 'coverage_pct': 100.0}

    def seed_historical_review_samples(self, lookback_days=180, rank_limit=10, max_new_predictions=1500):
        """从历史推荐快照补种可复盘样本，优先提升真实验证覆盖率。"""
        try:
            today = get_today()
            start_date = today - timedelta(days=max(30, int(lookback_days or 180)))
            existing_keys = {
                (code, rec_date, int(period or 0))
                for code, rec_date, period in self.session.query(
                    Prediction.code, Prediction.date, Prediction.period_days
                ).filter(Prediction.date >= start_date).all()
            }

            recommendations = self.session.query(Recommendation).filter(
                Recommendation.date >= start_date,
                Recommendation.date < today,
                Recommendation.rank <= max(1, int(rank_limit or 10)),
                Recommendation.current_price.isnot(None),
                Recommendation.current_price > 0,
            ).order_by(Recommendation.date.asc(), Recommendation.rank.asc()).all()

            horizon_fields = {
                5: ('up_probability_5d', 'target_low_5d', 'target_high_5d', 'stop_loss_5d'),
                20: ('up_probability_20d', 'target_low_20d', 'target_high_20d', 'stop_loss_20d'),
                60: ('up_probability_60d', 'target_low_60d', 'target_high_60d', 'stop_loss_60d'),
            }
            created = 0
            candidates = []
            priority_map = {60: 0, 20: 1, 5: 2}

            for rec in recommendations:
                asset_type = str(getattr(rec, 'type', '') or 'unknown')
                current_price = float(getattr(rec, 'current_price', 0) or 0)
                if current_price <= 0:
                    continue
                for period_days, field_names in horizon_fields.items():
                    expiry_date = rec.date + timedelta(days=period_days)
                    if expiry_date > today:
                        continue

                    dedupe_key = (rec.code, rec.date, period_days)
                    if dedupe_key in existing_keys:
                        continue

                    start_price = self._get_local_price_only(rec.code, rec.date)
                    expiry_price = self._get_local_price_only(rec.code, expiry_date)
                    if start_price is None or expiry_price is None:
                        continue

                    up_field, low_field, high_field, stop_field = field_names
                    up_probability = getattr(rec, up_field, None)
                    if up_probability is None:
                        continue

                    candidates.append((priority_map.get(period_days, 9), expiry_date, rec, asset_type, period_days, field_names, current_price, up_probability))

            candidates.sort(key=lambda item: (item[0], item[1], getattr(item[2], 'rank', 9999)))

            for _priority, expiry_date, rec, asset_type, period_days, field_names, current_price, up_probability in candidates:
                if created >= max_new_predictions:
                    break

                dedupe_key = (rec.code, rec.date, period_days)
                if dedupe_key in existing_keys:
                    continue

                up_field, low_field, high_field, stop_field = field_names
                target_low = getattr(rec, low_field, None)
                target_high = getattr(rec, high_field, None)
                stop_loss = getattr(rec, stop_field, None)
                if target_low is None:
                    target_low = round(current_price * (0.97 if period_days == 5 else (0.92 if period_days == 20 else 0.88)), 4)
                if target_high is None:
                    target_high = round(current_price * (1.03 if period_days == 5 else (1.08 if period_days == 20 else 1.15)), 4)
                if stop_loss is None:
                    stop_loss = round(current_price * (0.95 if period_days == 5 else (0.90 if period_days == 20 else 0.85)), 4)

                confidence = max(35.0, min(90.0, 45.0 + abs(float(up_probability or 50.0) - 50.0) * 1.4 + float(getattr(rec, 'total_score', 0) or 0) * 4.0))
                prediction = Prediction(
                    code=rec.code,
                    name=getattr(rec, 'name', None),
                    asset_type=asset_type,
                    date=rec.date,
                    period_days=period_days,
                    up_probability=float(up_probability or 50.0),
                    down_probability=max(0.0, 100.0 - float(up_probability or 50.0)),
                    target_low=float(target_low or 0),
                    target_high=float(target_high or 0),
                    confidence=float(confidence),
                    stop_loss=float(stop_loss or 0),
                    expiry_date=expiry_date,
                    is_expired=False,
                    created_at=datetime.now(),
                )
                self.session.add(prediction)
                existing_keys.add(dedupe_key)
                created += 1

            if created > 0:
                self.session.commit()
                logger.info(f"已从历史推荐快照补种 {created} 条待复盘样本")
            return int(created)
        except Exception as e:
            logger.error(f"补种历史复盘样本失败: {e}", exc_info=True)
            self.session.rollback()
            return 0

    def check_expired_predictions(self):
        """
        检查到期的预测并执行复盘
        Returns:
            int: 复盘数量
        """
        try:
            today = get_today()
            reviewed_total = self._count_reviewed_predictions()
            due_gap = self._get_due_review_gap()
            seeded_count = 0

            # 先消化现有到期待复盘队列，避免在 backlog 尚未清空时继续扩容分母。
            should_seed_history = (
                due_gap.get('missing_due', 0) <= 0 and reviewed_total < 100
            )
            if should_seed_history:
                seeded_limit = min(1200, max(120, 300 if reviewed_total < 30 else 150))
                seeded_count = self.seed_historical_review_samples(max_new_predictions=seeded_limit)
                if seeded_count:
                    logger.info(f"已自动补充历史待复盘样本 {seeded_count} 条，准备继续执行验收")
            
            # 查找今天到期的预测（包括之前未过期的）
            expired_predictions = self.session.query(Prediction).filter(
                Prediction.expiry_date <= today,
                Prediction.is_expired == False
            ).all()
            
            if not expired_predictions:
                logger.info("无到期预测")
                self._update_learning_status('last_review', {
                    'time': datetime.now().isoformat(),
                    'reviewed_count': 0,
                    'seeded_count': int(seeded_count),
                    'status': 'checked',
                    'message': '训练完成后已检查复盘，当前无到期预测'
                })
                return 0
            
            logger.info(f"发现 {len(expired_predictions)} 条到期预测，开始复盘...")
            reviewed_count = 0
            
            for pred in expired_predictions:
                try:
                    # 获取实际价格（带缓存）
                    actual_price = self._get_actual_price_with_cache(pred.code, pred.expiry_date)
                    
                    if actual_price is None:
                        logger.warning(f"无法获取 {pred.code} 在 {pred.expiry_date} 的价格，跳过")
                        continue
                    
                    # 计算实际收益率（基于预测低点）
                    if pred.target_low and pred.target_low > 0:
                        actual_return = (actual_price - pred.target_low) / pred.target_low * 100
                    else:
                        actual_return = 0
                    
                    # 判断方向是否正确
                    predicted_up = pred.up_probability > 50 if pred.up_probability else False
                    # 计算起始价格（从预测日期获取）
                    start_price = self._get_actual_price_with_cache(pred.code, pred.date)
                    if start_price:
                        actual_up = actual_price > start_price
                    else:
                        actual_up = actual_price > pred.target_low if pred.target_low else False
                    
                    is_direction_correct = predicted_up == actual_up
                    
                    # 判断是否在目标区间内
                    is_target_correct = False
                    if pred.target_low and pred.target_high:
                        is_target_correct = pred.target_low <= actual_price <= pred.target_high
                    
                    # 计算预测误差百分比
                    if pred.target_low and pred.target_high:
                        predicted_center = (pred.target_low + pred.target_high) / 2
                        error_percentage = abs(actual_price - predicted_center) / predicted_center * 100
                    else:
                        error_percentage = 100
                    
                    # 误差分析（增强版）
                    error_analysis = self._analyze_error_enhanced(
                        pred, actual_price, actual_return, 
                        is_direction_correct, is_target_correct
                    )
                    
                    # 计算复盘评分
                    review_score = self._calculate_review_score(
                        is_direction_correct, 
                        is_target_correct,
                        error_percentage
                    )
                    
                    # 创建复盘记录
                    review = Review(
                        prediction_id=pred.id,
                        code=pred.code,
                        name=pred.name,
                        period_days=pred.period_days,
                        predicted_up_prob=pred.up_probability,
                        predicted_target_low=pred.target_low,
                        predicted_target_high=pred.target_high,
                        actual_price=actual_price,
                        actual_return=actual_return,
                        is_direction_correct=is_direction_correct,
                        is_target_correct=is_target_correct,
                        error_analysis=error_analysis,
                        review_score=review_score
                    )
                    
                    self.session.add(review)
                    
                    # 更新预测记录
                    pred.is_expired = True
                    pred.actual_price = actual_price
                    pred.actual_return = actual_return
                    pred.is_direction_correct = is_direction_correct
                    pred.is_target_correct = is_target_correct
                    
                    reviewed_count += 1
                    logger.info(
                        f"复盘完成: {pred.code}({pred.period_days}日) - "
                        f"方向{'正确' if is_direction_correct else '错误'}, "
                        f"目标{'达成' if is_target_correct else '未达成'}, "
                        f"评分: {review_score}"
                    )
                    
                except Exception as e:
                    logger.error(f"复盘 {pred.code} 失败: {e}", exc_info=True)
                    continue
            
            # 提交所有更改
            self.session.commit()
            
            # 更新准确率统计
            if reviewed_count > 0:
                self._update_accuracy_stats()
            
            logger.info(f"复盘完成，共处理 {reviewed_count} 条预测")

            self._update_learning_status('last_review', {
                'time': datetime.now().isoformat(),
                'reviewed_count': reviewed_count,
                'seeded_count': int(seeded_count),
                'status': 'success',
                'message': f'复盘完成，处理 {reviewed_count} 条预测'
            })
            
            # 每天复盘后触发反思学习
            if reviewed_count > 0:
                try:
                    from reviews.reflection import ReflectionLearner
                    learner = ReflectionLearner(session=self.session)
                    adjustments = learner.update_model_weights()
                    if adjustments:
                        logger.info(f"反思学习建议: {adjustments}")

                    # 闭环：根据反思结果自动触发定向重训
                    retrain_targets = learner.check_retrain_needed()
                    self._update_learning_status('last_reflection', {
                        'time': datetime.now().isoformat(),
                        'has_adjustments': bool(adjustments),
                        'adjustments_count': len(adjustments) if adjustments else 0,
                        'retrain_targets': [
                            {'period': p, 'asset': a} for p, a in retrain_targets
                        ]
                    })
                    if retrain_targets and reviewed_count <= 100:
                        self._auto_retrain_from_reflection(retrain_targets)
                    elif retrain_targets:
                        logger.info("本轮主要用于批量补齐实盘复盘样本，已暂缓自动重训以避免影响在线服务")
                    learner.close()
                except Exception as e:
                    logger.error(f"反思学习失败: {e}")
            
            return reviewed_count
            
        except Exception as e:
            logger.error(f"检查到期预测失败: {e}", exc_info=True)
            self.session.rollback()
            self._update_learning_status('last_review', {
                'time': datetime.now().isoformat(),
                'status': 'failed',
                'message': f'复盘失败: {type(e).__name__}'
            })
            return 0

    def _auto_retrain_from_reflection(self, retrain_targets):
        """根据反思学习目标自动触发模型重训。"""
        try:
            from predictors.model_trainer import ModelTrainer

            # 将反思目标映射为预测周期
            periods = set()
            for period, _asset in retrain_targets:
                if period == 'all':
                    periods.update([5, 20, 60])
                elif isinstance(period, int) and period in (5, 20, 60):
                    periods.add(period)

            if not periods:
                logger.info("反思学习未命中可重训周期，跳过自动重训")
                self._update_learning_status('last_retrain', {
                    'time': datetime.now().isoformat(),
                    'status': 'skipped',
                    'periods': [],
                    'results': {},
                    'error': None
                })
                return

            logger.warning(f"触发自动重训（反思闭环）: 目标周期={sorted(periods)}")
            self._update_learning_status('last_retrain', {
                'time': datetime.now().isoformat(),
                'status': 'running',
                'periods': sorted(periods),
                'results': {},
                'error': None
            })
            trainer = ModelTrainer()
            results = trainer.train_all_models(target_periods=sorted(periods))
            logger.warning(f"自动重训完成（反思闭环）: {results}")
            self._update_learning_status('last_retrain', {
                'time': datetime.now().isoformat(),
                'status': 'success',
                'periods': sorted(periods),
                'results': results,
                'error': None
            })

        except Exception as e:
            logger.error(f"自动重训失败（反思闭环）: {e}", exc_info=True)
            self._update_learning_status('last_retrain', {
                'time': datetime.now().isoformat(),
                'status': 'failed',
                'error': str(e)
            })
    
    def _get_local_price_only(self, code, target_date):
        """仅使用本地数据库价格，允许匹配最近交易日，避免在批量回填时走外部慢查询。"""
        try:
            if isinstance(target_date, datetime):
                target_date = target_date.date()

            code_variants = self._build_code_variants(code)
            start_date = target_date - timedelta(days=7)
            end_date = target_date + timedelta(days=7)

            def _pick_best(rows, value_field):
                candidates = []
                for row in rows:
                    value = getattr(row, value_field, None)
                    row_date = getattr(row, 'date', None)
                    if value is None or row_date is None:
                        continue
                    day_gap = abs((row_date - target_date).days)
                    prefer_future = 1 if row_date > target_date else 0
                    candidates.append((day_gap, prefer_future, row_date, float(value)))
                if not candidates:
                    return None
                candidates.sort(key=lambda item: (item[0], item[1], item[2]))
                return candidates[0][3]

            price_rows = self.session.query(DailyPrice).filter(
                DailyPrice.code.in_(code_variants),
                DailyPrice.date >= start_date,
                DailyPrice.date <= end_date,
            ).all()
            best = _pick_best(price_rows, 'close')
            if best is not None:
                return best

            stock_rows = self.session.query(RawStockData).filter(
                RawStockData.code.in_(code_variants),
                RawStockData.date >= start_date,
                RawStockData.date <= end_date,
            ).all()
            best = _pick_best(stock_rows, 'close')
            if best is not None:
                return best

            fund_rows = self.session.query(RawFundData).filter(
                RawFundData.code.in_(code_variants),
                RawFundData.date >= start_date,
                RawFundData.date <= end_date,
            ).all()
            best = _pick_best(fund_rows, 'nav')
            if best is not None:
                return best
        except Exception:
            return None
        return None

    def _get_actual_price_with_cache(self, code, target_date):
        """
        获取实际价格（带缓存）
        Args:
            code: 股票代码
            target_date: 日期
        Returns:
            float: 实际价格
        """
        # 确保target_date是date对象
        if isinstance(target_date, datetime):
            target_date = target_date.date()
        
        # 检查内存缓存
        cache_key = f"{code}_{target_date}"
        if cache_key in self._price_cache:
            return self._price_cache[cache_key]
        
        # 检查文件缓存
        cached_price = get_cached_price(code, target_date)
        if cached_price is not None:
            self._price_cache[cache_key] = cached_price
            return cached_price
        
        # 获取价格
        price = self._get_actual_price(code, target_date)
        
        if price is not None:
            # 保存到缓存
            self._price_cache[cache_key] = price
            set_cached_price(code, target_date, price)
        
        return price
    
    @retry(max_attempts=3, delay=1)
    def _get_actual_price(self, code, target_date):
        """
        获取指定日期的实际价格（带重试）
        """
        try:
            import pandas as pd
            import yfinance as yf
            from utils import get_asset_type_from_code

            code = str(code).strip()
            asset_type = get_asset_type_from_code(code)

            local_price = self._get_local_price_only(code, target_date)
            if local_price is not None:
                return float(local_price)
            
            upper_code = code.upper()

            # 方法3: 基金优先走本地净值补采，避免误走股票通道导致超时与误判。
            if asset_type == 'fund' or upper_code.endswith('.OF'):
                try:
                    from collectors.fund_collector import FundCollector
                    normalized_code = upper_code.split('.')[0]
                    lookback_days = max(30, min(365, abs((get_today() - target_date).days) + 30))
                    fund_collector = FundCollector()
                    fund_collector.collect_fund_nav(normalized_code, days=lookback_days)
                    local_price = self._get_local_price_only(normalized_code, target_date)
                    if local_price is not None:
                        return float(local_price)
                except Exception as e:
                    logger.warning(f"基金 {code} 本地净值补采失败: {e}")

                logger.warning(f"基金 {upper_code.split('.')[0]} 缺少可验证历史净值，跳过本次复盘")
                return None

            # 方法4: 从 yfinance 获取历史数据（含黄金/白银映射）
            candidate_symbols = [code]
            if upper_code == 'XAUUSD':
                candidate_symbols.extend(['GC=F'])
            elif upper_code == 'XAGUSD':
                candidate_symbols.extend(['SI=F'])

            start_date = target_date - timedelta(days=5)
            end_date = target_date + timedelta(days=5)
            target_datetime = pd.to_datetime(target_date)

            for symbol in list(dict.fromkeys(candidate_symbols)):
                try:
                    ticker = yf.Ticker(symbol)
                    df = ticker.history(start=start_date, end=end_date)
                    if df is None or df.empty:
                        continue

                    df.index = pd.to_datetime(df.index).tz_localize(None)

                    if target_datetime in df.index:
                        price = float(df.loc[target_datetime, 'Close'])
                        self._save_price_to_db(code, target_date, price)
                        return price

                    df['date_diff'] = abs(df.index - target_datetime)
                    closest_idx = df['date_diff'].idxmin()
                    if df.loc[closest_idx, 'date_diff'] <= pd.Timedelta(days=3):
                        price = float(df.loc[closest_idx, 'Close'])
                        self._save_price_to_db(code, target_date, price)
                        return price
                except Exception:
                    continue

            # 方法5: 严格模式下不再使用临时抓取兜底，避免把不可复现数据记入准确率
            return None
            
        except Exception as e:
            logger.error(f"获取实际价格失败 {code}: {e}")
            return None
    
    def _save_price_to_db(self, code, date, price):
        """保存价格到数据库"""
        try:
            # 保存到DailyPrice表
            existing = self.session.query(DailyPrice).filter(
                DailyPrice.code == code,
                DailyPrice.date == date
            ).first()
            
            if not existing:
                daily_price = DailyPrice(
                    code=code,
                    date=date,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=0
                )
                self.session.add(daily_price)
                self.session.commit()
        except Exception as e:
            logger.warning(f"保存价格到数据库失败: {e}")
    
    def _analyze_error_enhanced(self, prediction, actual_price, actual_return, 
                                is_direction_correct, is_target_correct):
        """
        增强版错误分析
        """
        if is_direction_correct and is_target_correct:
            return "预测准确，方向与目标区间均正确"
        
        error_reasons = []
        
        # 方向错误分析
        if not is_direction_correct:
            if prediction.up_probability > 50:
                error_reasons.append("预测上涨但实际下跌")
            else:
                error_reasons.append("预测下跌但实际上涨")
            
            # 添加可能的原因
            if actual_return < -5:
                error_reasons.append("市场大幅下跌超出预期")
            elif actual_return > 5:
                error_reasons.append("市场强劲上涨超出预期")
            elif abs(actual_return) < 1:
                error_reasons.append("市场横盘震荡，方向不明显")
            else:
                error_reasons.append("市场波动方向与预测相反")
        
        # 目标区间错误分析
        if not is_target_correct and prediction.target_low and prediction.target_high:
            if actual_price < prediction.target_low:
                deviation = (prediction.target_low - actual_price) / prediction.target_low * 100
                error_reasons.append(f"实际价格低于预测低点 {prediction.target_low:.2f} (偏差{deviation:.1f}%)")
            elif actual_price > prediction.target_high:
                deviation = (actual_price - prediction.target_high) / prediction.target_high * 100
                error_reasons.append(f"实际价格高于预测高点 {prediction.target_high:.2f} (偏差{deviation:.1f}%)")
        
        # 添加置信度影响
        if prediction.confidence and prediction.confidence < 60:
            error_reasons.append(f"预测置信度较低({prediction.confidence:.0f}%)")
        elif prediction.confidence and prediction.confidence > 80:
            error_reasons.append(f"高置信度预测失败({prediction.confidence:.0f}%)，需重点分析")
        
        # 添加周期信息
        error_reasons.append(f"预测周期{prediction.period_days}日")
        
        return "；".join(error_reasons) if error_reasons else "预测偏差较大"
    
    def _calculate_review_score(self, is_direction_correct, is_target_correct, error_percentage=None):
        """
        计算复盘评分（优化版）
        """
        score = 0
        
        # 方向正确得60分
        if is_direction_correct:
            score += 60
        
        # 目标正确得30分
        if is_target_correct:
            score += 30
        
        # 误差小额外加分
        if error_percentage is not None:
            if error_percentage < 3:
                score += 10
            elif error_percentage < 5:
                score += 7
            elif error_percentage < 10:
                score += 5
            elif error_percentage < 15:
                score += 3
        
        return min(100, score)
    
    def _update_accuracy_stats(self):
        """
        更新预测准确率统计，并同步前端使用的 accuracy_stats 表。
        """
        try:
            from utils import get_asset_type_from_code

            predictions = self.session.query(Prediction).filter(
                Prediction.is_expired == True,
                Prediction.is_direction_correct.isnot(None)
            ).all()

            unique_predictions = {}
            for p in sorted(predictions, key=lambda x: ((x.created_at or datetime.min), x.id or 0), reverse=True):
                key = (p.code, int(p.period_days or 0), p.expiry_date)
                unique_predictions.setdefault(key, p)
            predictions = list(unique_predictions.values())

            if not predictions:
                logger.info("暂无已验证预测，跳过准确率统计更新")
                return

            # 1) 汇总 prediction_accuracy（按周期总览）
            for period in [5, 20, 60]:
                period_predictions = [p for p in predictions if int(p.period_days or 0) == period]
                total = len(period_predictions)
                if total == 0:
                    continue

                correct = sum(1 for p in period_predictions if p.is_direction_correct)
                accuracy = round(correct / total * 100, 2)

                errors = []
                for p in period_predictions:
                    if p.actual_price and p.target_low and p.target_high:
                        predicted_center = (p.target_low + p.target_high) / 2
                        if predicted_center:
                            errors.append(abs(p.actual_price - predicted_center) / predicted_center * 100)
                    elif p.actual_price and p.target_low:
                        errors.append(abs(p.actual_price - p.target_low) / p.target_low * 100)

                avg_error = round(sum(errors) / len(errors), 2) if errors else 0.0

                stats = self.session.query(PredictionAccuracy).filter(
                    PredictionAccuracy.period_days == period
                ).first()

                if stats:
                    stats.total_predictions = total
                    stats.correct_predictions = correct
                    stats.accuracy = accuracy
                    stats.avg_error = avg_error
                    stats.last_updated = datetime.now()
                else:
                    self.session.add(PredictionAccuracy(
                        period_days=period,
                        total_predictions=total,
                        correct_predictions=correct,
                        accuracy=accuracy,
                        avg_error=avg_error
                    ))

            # 2) 汇总 accuracy_stats（按到期日 / 周期 / 资产类型），供前端页面直接读取
            grouped_stats = {}
            for p in predictions:
                period = int(p.period_days or 0)
                if period not in (5, 20, 60):
                    continue

                stat_date = p.expiry_date or get_today()
                inferred_asset_type = get_asset_type_from_code(p.code)
                asset_type = p.asset_type or inferred_asset_type or 'unknown'
                if asset_type == 'fund':
                    asset_type = 'active_fund'
                elif asset_type == 'stock' and inferred_asset_type in ('fund', 'gold', 'silver', 'etf'):
                    asset_type = 'active_fund' if inferred_asset_type == 'fund' else inferred_asset_type

                for target_asset in ('all', asset_type):
                    key = (stat_date, period, target_asset)
                    bucket = grouped_stats.setdefault(key, {'total': 0, 'correct': 0})
                    bucket['total'] += 1
                    if p.is_direction_correct:
                        bucket['correct'] += 1

            for (stat_date, period, asset_type), bucket in grouped_stats.items():
                total = int(bucket['total'])
                correct = int(bucket['correct'])
                accuracy = round(correct / total * 100, 2) if total > 0 else 0.0

                stat = self.session.query(AccuracyStat).filter(
                    AccuracyStat.stat_date == stat_date,
                    AccuracyStat.period_days == period,
                    AccuracyStat.asset_type == asset_type
                ).first()

                if stat:
                    stat.total_count = total
                    stat.correct_count = correct
                    stat.accuracy = accuracy
                else:
                    self.session.add(AccuracyStat(
                        stat_date=stat_date,
                        period_days=period,
                        asset_type=asset_type,
                        total_count=total,
                        correct_count=correct,
                        accuracy=accuracy
                    ))

            self.session.commit()
            logger.info("预测准确率统计已同步更新")

        except Exception as e:
            logger.error(f"更新准确率统计失败: {e}", exc_info=True)
            self.session.rollback()
    
    def get_review_statistics(self, days=30):
        """
        获取复盘统计
        """
        try:
            start_date = datetime.now().date() - timedelta(days=days)
            
            reviews = self.session.query(Review).filter(
                Review.reviewed_at >= start_date
            ).all()
            
            total = len(reviews)
            if total == 0:
                return {
                    'total': 0,
                    'correct': 0,
                    'accuracy': 0,
                    'by_period': {},
                    'by_asset': {}
                }
            
            correct = sum(1 for r in reviews if r.is_direction_correct)
            target_correct = sum(1 for r in reviews if r.is_target_correct)
            avg_score = sum(r.review_score for r in reviews) / total if total > 0 else 0
            
            # 按周期统计
            by_period = {}
            for period in [5, 20, 60]:
                period_reviews = [r for r in reviews if r.period_days == period]
                period_total = len(period_reviews)
                period_correct = sum(1 for r in period_reviews if r.is_direction_correct)
                by_period[period] = {
                    'total': period_total,
                    'correct': period_correct,
                    'accuracy': round(period_correct / period_total * 100, 1) if period_total > 0 else 0,
                    'avg_score': round(sum(r.review_score for r in period_reviews) / period_total, 1) if period_total > 0 else 0
                }
            
            # 按资产类型统计
            by_asset = {}
            for review in reviews:
                asset_type = self._detect_asset_type(review.code)
                if asset_type not in by_asset:
                    by_asset[asset_type] = {'total': 0, 'correct': 0}
                by_asset[asset_type]['total'] += 1
                if review.is_direction_correct:
                    by_asset[asset_type]['correct'] += 1
            
            # 计算各资产准确率
            for asset in by_asset:
                if by_asset[asset]['total'] > 0:
                    by_asset[asset]['accuracy'] = round(
                        by_asset[asset]['correct'] / by_asset[asset]['total'] * 100, 1
                    )
                else:
                    by_asset[asset]['accuracy'] = 0
            
            return {
                'total': total,
                'correct': correct,
                'target_correct': target_correct,
                'accuracy': round(correct / total * 100, 1),
                'avg_score': round(avg_score, 1),
                'by_period': by_period,
                'by_asset': by_asset,
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"获取复盘统计失败: {e}")
            return {}
    
    def _detect_asset_type(self, code):
        """根据代码判断资产类型"""
        code = code.upper()
        if code.endswith(('.SH', '.SZ')):
            return 'A股'
        elif code.endswith('.HK'):
            return '港股'
        elif code.isalpha() and len(code) <= 5:
            return '美股'
        else:
            return '其他'
    
    def get_pending_reviews(self):
        """
        获取待复盘的预测
        """
        try:
            today = get_today()
            pending = self.session.query(Prediction).filter(
                Prediction.expiry_date <= today,
                Prediction.is_expired == False
            ).all()
            return pending
        except Exception as e:
            logger.error(f"获取待复盘预测失败: {e}")
            return []
    
    def clear_cache(self):
        """清空价格缓存"""
        self._price_cache.clear()
    
    def close(self):
        """关闭数据库连接"""
        try:
            self.session.close()
        except Exception as e:
            logger.error(f"关闭数据库连接失败: {e}")


# 测试代码
if __name__ == '__main__':
    reviewer = Reviewer()
    
    # 检查并执行复盘
    count = reviewer.check_expired_predictions()
    print(f"复盘完成: {count} 条")
    
    # 获取统计信息
    stats = reviewer.get_review_statistics(30)
    print(f"\n统计信息:")
    print(f"  总预测数: {stats.get('total', 0)}")
    print(f"  正确数: {stats.get('correct', 0)}")
    print(f"  准确率: {stats.get('accuracy', 0)}%")
    print(f"  平均评分: {stats.get('avg_score', 0)}")
    
    reviewer.close()