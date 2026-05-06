"""
黄金白银推荐引擎 - recommenders/gold_recommender.py
推荐黄金和白银投资标的
"""

import sys
import os
from datetime import datetime
import yfinance as yf
import pandas as pd
import numpy as np
import pickle

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from recommenders.base_recommender import BaseRecommender
from utils import get_logger

logger = get_logger(__name__)


class GoldRecommender(BaseRecommender):
    """黄金白银推荐引擎"""
    
    def __init__(self):
        super().__init__()
        self.gold_pool = self._get_gold_pool()
        self.silver_pool = self._get_silver_pool()
        # 黄金 ML
        self._ml_model = None
        self._ml_scaler = None
        self._ml_feature_cols = None
        self._ml_model_loaded = False
        self._ml_decision_threshold = 0.50
        self._ml_calibrator = None
        self._ml_calibration_method = 'none'
        self._ml_gate_passed = False
        self._gold_price_snap = None
    
    def get_asset_type(self):
        return "gold_silver"

    # ──────────────────────────────────────────────────────────────────
    # ML 模型接入 (gold_short_term_model.pkl)
    # ──────────────────────────────────────────────────────────────────
    @staticmethod
    def _apply_calibrator(method, calibrator, proba):
        p = np.clip(float(proba), 1e-6, 1 - 1e-6)
        if calibrator is None or method == 'none':
            return p
        try:
            if method == 'platt':
                return float(np.clip(calibrator.predict_proba([[p]])[0][1], 1e-6, 1 - 1e-6))
            if method == 'isotonic':
                return float(np.clip(calibrator.predict([p])[0], 1e-6, 1 - 1e-6))
        except Exception:
            return p
        return p

    def _load_gold_ml_model(self):
        """延迟加载 gold_short_term_model.pkl，gate 未通过则跳过 ML。"""
        if self._ml_model_loaded:
            return self._ml_model is not None and self._ml_gate_passed
        self._ml_model_loaded = True
        try:
            model_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                'data', 'models', 'gold_short_term_model.pkl'
            )
            if not os.path.exists(model_path):
                logger.warning('gold_short_term_model.pkl 不存在，ML评分将跳过')
                return False
            with open(model_path, 'rb') as f:
                payload = pickle.load(f)
            gate_passed = bool(payload.get('validation_passed', True))
            if not gate_passed:
                gate_name = payload.get('validation_gate', 'unknown')
                gate_reason = payload.get('validation_reason', '')
                logger.warning(
                    f'gold_short_term_model.pkl 未通过 validation gate ({gate_name}): {gate_reason}，ML评分将跳过'
                )
                return False
            self._ml_model = payload.get('model')
            self._ml_scaler = payload.get('scaler')
            self._ml_feature_cols = payload.get('feature_columns', [])
            self._ml_decision_threshold = float(payload.get('decision_threshold', 0.50))
            self._ml_calibrator = payload.get('calibrator', None)
            self._ml_calibration_method = payload.get('calibration_method', 'none')
            self._ml_gate_passed = True
            logger.info(
                f'gold_short_term_model.pkl 已加载: {len(self._ml_feature_cols)} 特征, '
                f'val_acc={payload.get("val_accuracy")}, threshold={self._ml_decision_threshold:.2f}, '
                f'cal={self._ml_calibration_method}, gate_passed={gate_passed}'
            )
            return True
        except Exception as e:
            logger.warning(f'gold_short_term_model.pkl 加载失败: {e}')
            return False

    def _compute_gold_price_snapshot(self):
        """从 data/gold_prices.csv 计算技术指标快照，只执行一次。"""
        if self._gold_price_snap is not None:
            return self._gold_price_snap
        snap = {
            'rsi': 50.0, 'macd_hist': 0.0,
            'price_ma5_ratio': 0.0, 'price_ma20_ratio': 0.0, 'price_ma60_ratio': 0.0,
            'volume_ratio': 1.0,
            'volatility': 0.01, 'volatility_5d': 0.01,
            'return_5d': 0.0, 'return_10d': 0.0, 'return_20d': 0.0, 'return_60d': 0.0,
            'momentum_accel': 0.0,
            'price_position': 0.5, 'channel_position': 0.5, 'bb_position': 0.5,
            'atr_ratio': 0.01, 'uptrend_strength': 0.5,
        }
        try:
            csv_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                'data', 'gold_prices.csv'
            )
            if not os.path.exists(csv_path):
                self._gold_price_snap = snap
                return snap
            df = pd.read_csv(csv_path)
            df = df.sort_values('date').reset_index(drop=True)
            close = df['close'].values.astype(float)
            high  = df['high'].values.astype(float)
            low   = df['low'].values.astype(float)
            if len(close) < 65:
                self._gold_price_snap = snap
                return snap
            def safe_ret(n):
                return (close[-1] - close[-n-1]) / close[-n-1] if close[-n-1] > 0 else 0.0
            snap['return_5d']  = safe_ret(5)
            snap['return_10d'] = safe_ret(10)
            snap['return_20d'] = safe_ret(20)
            snap['price_ma5_ratio']  = close[-1] / np.mean(close[-5:])  if np.mean(close[-5:])  > 0 else 1.0
            snap['price_ma20_ratio'] = close[-1] / np.mean(close[-20:]) if np.mean(close[-20:]) > 0 else 1.0
            snap['price_ma60_ratio'] = close[-1] / np.mean(close[-60:]) if np.mean(close[-60:]) > 0 else 1.0
            rets = np.diff(close[-21:]) / (close[-21:-1] + 1e-9)
            snap['volatility']    = float(np.std(rets) * np.sqrt(252))
            rets5 = np.diff(close[-6:]) / (close[-6:-1] + 1e-9)
            snap['volatility_5d'] = float(np.std(rets5) * np.sqrt(252)) if len(rets5) > 1 else snap['volatility']
            delta = np.diff(close[-15:])
            gain = np.where(delta > 0, delta, 0.0)
            loss = np.where(delta < 0, -delta, 0.0)
            avg_gain = np.mean(gain[-14:]) if len(gain) >= 14 else np.mean(gain)
            avg_loss = np.mean(loss[-14:]) if len(loss) >= 14 else np.mean(loss)
            snap['rsi'] = 100.0 - 100.0 / (1.0 + avg_gain / (avg_loss + 1e-9))
            def ema(arr, n):
                k = 2.0 / (n + 1)
                e = float(arr[0])
                for v in arr[1:]:
                    e = v * k + e * (1 - k)
                return e
            tail = close[-40:] if len(close) >= 40 else close
            snap['macd_hist'] = (ema(tail, 12) - ema(tail, 26)) * 0.2
            look = close[-252:] if len(close) >= 252 else close
            lo, hi = float(np.min(look)), float(np.max(look))
            snap['price_position'] = (close[-1] - lo) / (hi - lo + 1e-9)
            h20 = float(np.max(high[-20:]))
            l20 = float(np.min(low[-20:]))
            snap['channel_position'] = (close[-1] - l20) / (h20 - l20 + 1e-9)
            # 布林带位置
            bb_mid = float(np.mean(close[-20:]))
            bb_std = float(np.std(close[-20:]) + 1e-9)
            snap['bb_position'] = (close[-1] - (bb_mid - 2*bb_std)) / (4*bb_std + 1e-9)
            # ATR 比率
            if len(close) >= 15:
                tr_arr = np.maximum(
                    high[-14:] - low[-14:],
                    np.maximum(
                        np.abs(high[-14:] - close[-15:-1]),
                        np.abs(low[-14:]  - close[-15:-1])
                    )
                )
                snap['atr_ratio'] = float(np.mean(tr_arr)) / (close[-1] + 1e-9)
            # 收益率动量
            snap['return_60d'] = (close[-1] - close[-61]) / close[-61] if len(close) >= 61 else 0.0
            snap['momentum_accel'] = snap['return_5d'] - snap['return_10d'] / 2.0
            ma5  = np.mean(close[-5:])
            ma10 = np.mean(close[-10:])
            ma20 = np.mean(close[-20:])
            ma60 = np.mean(close[-60:])
            snap['uptrend_strength'] = sum([ma5 > ma10, ma10 > ma20, ma20 > ma60]) / 3.0
        except Exception as e:
            logger.warning(f'黄金技术指标计算失败: {e}')
        self._gold_price_snap = snap
        return snap

    def _build_gold_ml_features(self, item):
        """为单个黄金标的构建特征向量（按 feature_columns 动态对齐）。"""
        if not self._ml_feature_cols:
            return None
        tech = self._compute_gold_price_snapshot()
        vec = [tech.get(col, 0.0) for col in self._ml_feature_cols]
        return pd.DataFrame([vec], columns=self._ml_feature_cols)

    def _ml_score_metal(self, item):
        """返回黄金/白银 ML 短期上涨概率 [0,1]（已校准），gate 未通过则返回 None。"""
        code = str(item.get('code', '')).lower()
        name = str(item.get('name', '')).lower()
        is_silver = 'silver' in code or 'silver' in name or '白银' in name
        if is_silver:
            return self._ml_score_silver(item)
        if not self._load_gold_ml_model():
            return None
        try:
            X = self._build_gold_ml_features(item)
            if X is None:
                return None
            if self._ml_scaler is not None:
                X_arr = self._ml_scaler.transform(X)
            else:
                X_arr = X.values
            if hasattr(self._ml_model, 'predict_proba'):
                raw_prob = float(self._ml_model.predict_proba(X_arr)[0][1])
            else:
                raw_prob = float(self._ml_model.predict(X_arr)[0])
            prob = self._apply_calibrator(self._ml_calibration_method, self._ml_calibrator, raw_prob)
            return max(0.0, min(1.0, prob))
        except Exception as e:
            logger.debug(f'黄金ML评分异常 {item.get("code")}: {e}')
            return None

    def _ml_score_silver(self, item):
        """用 silver_short_term_model.pkl 推断白银上涨概率（含 gate 强制 + 校准器）。"""
        if not hasattr(self, '_silver_ml_model'):
            self._silver_ml_model = None
            self._silver_ml_scaler = None
            self._silver_ml_feature_cols = None
            self._silver_ml_loaded = False
            self._silver_ml_gate_passed = False
            self._silver_ml_decision_threshold = 0.50
            self._silver_ml_calibrator = None
            self._silver_ml_calibration_method = 'none'
        if not self._silver_ml_loaded:
            self._silver_ml_loaded = True
            try:
                path = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    'data', 'models', 'silver_short_term_model.pkl'
                )
                if os.path.exists(path):
                    with open(path, 'rb') as f:
                        p = pickle.load(f)
                    gate_passed = bool(p.get('validation_passed', True))
                    if not gate_passed:
                        logger.warning(
                            f'silver_short_term_model.pkl 未通过 gate ({p.get("validation_gate")}), 白银ML跳过'
                        )
                    else:
                        self._silver_ml_model = p.get('model')
                        self._silver_ml_scaler = p.get('scaler')
                        self._silver_ml_feature_cols = p.get('feature_columns', [])
                        self._silver_ml_gate_passed = True
                        self._silver_ml_decision_threshold = float(p.get('decision_threshold', 0.50))
                        self._silver_ml_calibrator = p.get('calibrator', None)
                        self._silver_ml_calibration_method = p.get('calibration_method', 'none')
                        logger.info(
                            f'silver_short_term_model.pkl 已加载, threshold={self._silver_ml_decision_threshold:.2f}, '
                            f'cal={self._silver_ml_calibration_method}'
                        )
            except Exception as e:
                logger.warning(f'silver_short_term_model.pkl 加载失败: {e}')
        if self._silver_ml_model is None:
            return None
        try:
            tech = self._compute_gold_price_snapshot()   # 近似用黄金行情替代
            vec = [tech.get(col, 0.0) for col in self._silver_ml_feature_cols]
            X = pd.DataFrame([vec], columns=self._silver_ml_feature_cols)
            if self._silver_ml_scaler is not None:
                X_arr = self._silver_ml_scaler.transform(X)
            else:
                X_arr = X.values
            raw_prob = float(self._silver_ml_model.predict_proba(X_arr)[0][1])
            prob = self._apply_calibrator(
                self._silver_ml_calibration_method, self._silver_ml_calibrator, raw_prob
            )
            return max(0.0, min(1.0, prob))
        except Exception as e:
            logger.debug(f'白银ML评分异常: {e}')
            return None

    def _fuse_ml_score(self, rule_score, ml_prob):
        """规则评分 70% + ML 概率映射分 30%。"""
        if ml_prob is None:
            return rule_score
        ml_score = 1.0 + ml_prob * 4.0
        return max(1.0, min(5.0, round(0.7 * rule_score + 0.3 * ml_score, 2)))

    def _fetch_last_close(self, ticker, periods=('5d', '1mo')):
        """抓取ticker最近有效收盘价，支持多周期回退。"""
        for period in periods:
            try:
                df = yf.Ticker(ticker).history(period=period)
                if df is None or df.empty or 'Close' not in df.columns:
                    continue
                close = pd.to_numeric(df['Close'], errors='coerce').dropna()
                if not close.empty:
                    return float(close.iloc[-1])
            except Exception:
                continue
        return None
    
    def _get_gold_pool(self):
        """获取真实黄金标的池（扩展版）。"""
        now = datetime.now().isoformat()
        candidates = [
            ('GC=F', 'COMEX黄金期货', '期货', 'USD/oz', 0.00),
            ('GLD', 'SPDR Gold Trust', 'ETF', 'USD', 0.40),
            ('IAU', 'iShares Gold Trust', 'ETF', 'USD', 0.25),
            ('GLDM', 'SPDR Gold MiniShares', 'ETF', 'USD', 0.10),
            ('SGOL', 'abrdn Physical Gold Shares ETF', 'ETF', 'USD', 0.17),
        ]

        result = []
        seen = set()
        for code, name, asset_kind, currency, fee in candidates:
            price = self._fetch_last_close(code)
            if price is None and code == 'GC=F':
                price = self._fetch_last_close('XAUUSD=X')
            if price is None or code in seen:
                continue
            seen.add(code)
            result.append({
                'code': code,
                'name': name,
                'type': asset_kind,
                'currency': currency,
                'fee': fee,
                'price': float(price),
                'last_update': now,
                'data_source': 'yfinance'
            })

        if not result:
            logger.error("❌ 获取黄金池失败: 所有数据源不可用")

        return result
    
    def _get_silver_pool(self):
        """获取真实白银标的池（扩展版）。"""
        now = datetime.now().isoformat()
        candidates = [
            ('SI=F', 'COMEX白银期货', '期货', 'USD/oz', 0.00),
            ('SLV', 'iShares Silver Trust', 'ETF', 'USD', 0.50),
            ('SIVR', 'abrdn Physical Silver Shares ETF', 'ETF', 'USD', 0.30),
            ('PSLV', 'Sprott Physical Silver Trust', 'ETF', 'USD', 0.45),
        ]

        result = []
        seen = set()
        for code, name, asset_kind, currency, fee in candidates:
            price = self._fetch_last_close(code)
            if price is None and code == 'SI=F':
                price = self._fetch_last_close('XAGUSD=X')
            if price is None or code in seen:
                continue
            seen.add(code)
            result.append({
                'code': code,
                'name': name,
                'type': asset_kind,
                'currency': currency,
                'fee': fee,
                'price': float(price),
                'last_update': now,
                'data_source': 'yfinance'
            })

        if not result:
            logger.error("❌ 获取白银池失败: 所有数据源不可用")

        return result
    
    def _get_real_risk_metrics(self):
        """
        获取真实风险指标 (增强版)

        修复说明:
        - 原来: yf.Ticker('DXY') — 该 ticker 已从 Yahoo Finance 下架
        - 现在: yf.Ticker('DX-Y.NYB') — ICE US Dollar Index (正确 ticker)
        - 新增: ^TNX 10年期美债收益率 (实际利率代理)
        - 新增: 黄金/白银价格动量 (相对20日/60日均线)
        """
        result = {
            'dxy': 103.0,      # 默认中性
            'vix': 20.0,
            'tnx': 4.5,        # 10年期收益率 (%)
            'dxy_neutral': 103.0,
            'tnx_neutral': 4.5,
            'gold_vs_ma20': 0.0,   # 黄金价格 vs 20日均线 (%)
            'gold_vs_ma60': 0.0,
            'silver_vs_ma20': 0.0,
            'silver_vs_ma60': 0.0,
            'timestamp': datetime.now().isoformat(),
        }

        # 1. 美元指数 — DX-Y.NYB 是正确 ticker
        for dxy_ticker in ['DX-Y.NYB', 'UUP']:   # UUP 作备用
            try:
                d = yf.Ticker(dxy_ticker).history(period='1y')
                if not d.empty:
                    result['dxy'] = float(d['Close'].iloc[-1])
                    med = float(d['Close'].dropna().median())
                    if med > 0:
                        result['dxy_neutral'] = med
                    break
            except Exception:
                continue

        # 2. VIX
        try:
            v = yf.Ticker('^VIX').history(period='5d')
            if not v.empty:
                result['vix'] = float(v['Close'].iloc[-1])
        except Exception as e:
            logger.warning(f"VIX获取失败: {e}")

        # 3. 10年期美债收益率 (实际利率代理: 利率低利好贵金属)
        try:
            t = yf.Ticker('^TNX').history(period='1y')
            if not t.empty:
                result['tnx'] = float(t['Close'].iloc[-1])
                med = float(t['Close'].dropna().median())
                if med > 0:
                    result['tnx_neutral'] = med
        except Exception as e:
            logger.warning(f"TNX获取失败: {e}")

        # 4. 价格动量 — 2个月历史数据计算MA
        for ticker, gold_key, silver_key in [
            ('GC=F', 'gold', None),
            ('SI=F', None, 'silver'),
        ]:
            try:
                hist = yf.Ticker(ticker).history(period='3mo')
                if len(hist) < 20:
                    continue
                cur  = float(hist['Close'].iloc[-1])
                ma20 = float(hist['Close'].tail(20).mean())
                ma60 = float(hist['Close'].tail(60).mean()) if len(hist) >= 60 else ma20
                vs20 = (cur - ma20) / ma20 * 100
                vs60 = (cur - ma60) / ma60 * 100
                if gold_key:
                    result['gold_vs_ma20'] = vs20
                    result['gold_vs_ma60'] = vs60
                else:
                    result['silver_vs_ma20'] = vs20
                    result['silver_vs_ma60'] = vs60
            except Exception as e:
                logger.warning(f"{ticker}动量获取失败: {e}")

        logger.info(
            f"风险指标: DXY={result['dxy']:.1f}(中性{result['dxy_neutral']:.1f}), "
            f"VIX={result['vix']:.1f}, TNX={result['tnx']:.2f}%(中性{result['tnx_neutral']:.2f}), "
            f"gold_vs_ma20={result['gold_vs_ma20']:.1f}%"
        )
        return result

    def _calculate_precious_metal_score(self, item, metal_type='gold', indicators=None):
        """
        计算贵金属评分 (增强版)

        评分维度与权重:
          美元走势 (DXY)           25% — 弱美元利好贵金属
          避险情绪 (VIX)           20% — 高恐慌利好贵金属
          实际利率 (TNX)           20% — 低利率利好贵金属
          价格动量 (vs MA20/MA60)  25% — 趋势跟随
          品种/标的属性            10% — 费率/期货/ETF 差异
        """
        if indicators is None:
            indicators = self._get_real_risk_metrics()

        try:
            score = 3.0
            dxy    = indicators['dxy']
            vix    = indicators['vix']
            tnx    = indicators['tnx']
            dxy_neutral = indicators.get('dxy_neutral', 103.0)
            tnx_neutral = indicators.get('tnx_neutral', 4.5)

            # ── 1. 美元指数 (25%) ──────────────────────
            # 使用近1年中位数作为动态中性点，减少不同宏观阶段下的偏差
            dxy_delta = (dxy_neutral - dxy) * 0.05
            score += max(-0.60, min(0.60, dxy_delta))

            # ── 2. VIX 避险情绪 (20%) ──────────────────
            if   vix > 35: score += 0.50
            elif vix > 28: score += 0.35
            elif vix > 22: score += 0.20
            elif vix > 17: score += 0.05
            elif vix < 13: score -= 0.20
            else:          score -= 0.05

            # ── 3. 实际利率/10年期收益率 (20%) ─────────
            # 利率高 → 持有贵金属机会成本高 → 利空
            tnx_delta = (tnx_neutral - tnx) * 0.10
            score += max(-0.50, min(0.50, tnx_delta))

            # ── 4. 价格动量 (25%) ──────────────────────
            if metal_type == 'gold':
                vs20 = indicators['gold_vs_ma20']
                vs60 = indicators['gold_vs_ma60']
            else:
                vs20 = indicators['silver_vs_ma20']
                vs60 = indicators['silver_vs_ma60']

            # MA20 动量 (15%)
            if   vs20 >  5: score += 0.35
            elif vs20 >  2: score += 0.20
            elif vs20 >  0: score += 0.05
            elif vs20 < -5: score -= 0.35
            elif vs20 < -2: score -= 0.20
            else:           score -= 0.05

            # MA60 趋势确认 (10%)
            if   vs60 >  3: score += 0.20
            elif vs60 >  0: score += 0.05
            elif vs60 < -3: score -= 0.20
            else:           score -= 0.05

            # ── 5. 品种/标的属性 (10%) ─────────────────
            if metal_type == 'silver':
                score -= 0.10   # 白银工业属性较强, 避险略逊
            if item['type'] == '期货':
                score += 0.08   # 期货直接追踪现货, 无额外管理成本
            elif item['type'] == 'ETF':
                if   item['fee'] <= 0.25: score += 0.10
                elif item['fee'] <= 0.50: score += 0.00   # 中性
                elif item['fee'] >  0.60: score -= 0.10

            # 最终分数限制在1-5
            final_score = max(1.0, min(5.0, round(score, 2)))
            
            return final_score
            
        except Exception as e:
            logger.warning(f"计算贵金属评分异常: {e}")
            return 3.0
    
    def get_gold_recommendations(self, limit=None):
        """获取黄金推荐"""
        self.gold_pool = self._get_gold_pool()
        if not self.gold_pool:
            logger.warning("⚠️ 黄金池为空")
            return []

        indicators = self._get_real_risk_metrics()
        recommendations = []

        for item in self.gold_pool:
            try:
                score = self._calculate_precious_metal_score(item, 'gold', indicators)
                ml_prob = self._ml_score_metal(item)
                score = self._fuse_ml_score(score, ml_prob)
                reason = self._generate_metal_reason('silver', indicators)
                if ml_prob is not None:
                    reason += f'; ML短期上涨概率{ml_prob:.1%}'
                score = self._fuse_ml_score(score, ml_prob)
                reason = self._generate_metal_reason('gold', indicators)
                if ml_prob is not None:
                    reason += f'; ML短期上涨概率{ml_prob:.1%}'
                recommendations.append({
                    'code':          item['code'],
                    'name':          item['name'],
                    'type':          item['type'],
                    'currency':      item['currency'],
                    'reason': reason,
                    'ml_score': round(ml_prob, 4) if ml_prob is not None else None,
                    'current_price': item['price'],
                    'fee':           item['fee'],
                    'reason':        reason,
                    'ml_score':      round(ml_prob, 4) if ml_prob is not None else None,
                    'data_source':   'yfinance',
                    'update_time':   item.get('last_update', datetime.now().isoformat()),
                })
            except Exception as e:
                logger.warning(f"处理黄金{item.get('code')}异常: {e}")

        recommendations = self.sort_by_score(recommendations)
        recommendations = self.add_rank(recommendations)
        if limit is None or int(limit or 0) <= 0:
            return recommendations
        return recommendations[:limit]

    def get_silver_recommendations(self, limit=None):
        """获取白银推荐"""
        self.silver_pool = self._get_silver_pool()
        if not self.silver_pool:
            logger.warning("⚠️ 白银池为空")
            return []

        indicators = self._get_real_risk_metrics()
        recommendations = []

        for item in self.silver_pool:
            try:
                score = self._calculate_precious_metal_score(item, 'silver', indicators)
                
                rec = {
                    'code': item['code'],
                    'name': item['name'],
                    'type': item['type'],
                    'currency': item['currency'],
                    'score': score,
                    'current_price': item['price'],
                    'fee': item['fee'],
                    'reason': self._generate_metal_reason('silver', indicators),
                    'data_source': 'yfinance',
                    'update_time': item.get('last_update', datetime.now().isoformat())
                }
                recommendations.append(rec)
                
            except Exception as e:
                logger.warning(f"处理白银{item.get('code')}异常: {e}")
                continue
        
        recommendations = self.sort_by_score(recommendations)
        recommendations = self.add_rank(recommendations)
        if limit is None or int(limit or 0) <= 0:
            return recommendations
        return recommendations[:limit]
    
    def _generate_metal_reason(self, metal_type, indicators):
        """根据真实指标生成推荐理由 (增强版)"""
        if not indicators:
            return "数据获取中..."

        reasons = []
        vix  = indicators.get('vix',  20.0)
        dxy  = indicators.get('dxy', 103.0)
        tnx  = indicators.get('tnx',   4.5)
        vs20_key = f"{metal_type}_vs_ma20"
        vs20 = indicators.get(vs20_key, 0.0)

        if vix > 25:
            reasons.append(f"VIX={vix:.1f}(高恐慌避险)")
        if dxy < 100:
            reasons.append(f"美元指数{dxy:.1f}(偏弱利好)")
        elif dxy > 106:
            reasons.append(f"美元指数{dxy:.1f}(偏强利空)")
        if tnx < 3.8:
            reasons.append(f"10年期收益率{tnx:.2f}%低")
        elif tnx > 5.0:
            reasons.append(f"10年期收益率{tnx:.2f}%高")
        if vs20 > 2:
            reasons.append(f"价格高于MA20 {vs20:.1f}%")
        elif vs20 < -2:
            reasons.append(f"价格低于MA20 {abs(vs20):.1f}%")

        return "; ".join(reasons) if reasons else "持中立态度"
    
    def get_recommendations(self, limit=20):
        """获取所有贵金属推荐"""
        gold_recs = self.get_gold_recommendations(limit // 2)
        silver_recs = self.get_silver_recommendations(limit // 2)
        return gold_recs + silver_recs