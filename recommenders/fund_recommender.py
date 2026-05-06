"""
基金推荐引擎 - recommenders/fund_recommender.py
推荐主动管理型基金 (增强版: 使用真实NAV表现数据驱动评分)
"""

import sys
import os
import json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import pickle
try:
    import tushare as ts
except ImportError:
    ts = None

from models import get_session, Holding, RawFundData

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from recommenders.base_recommender import BaseRecommender
from config import TUSHARE_TOKEN, FUND_POOL_CACHE_FILE
from utils import get_logger

logger = get_logger(__name__)


class FundRecommender(BaseRecommender):
    """主动基金推荐引擎 (增强版)"""

    def __init__(self):
        super().__init__()
        self._pro = self._init_pro() if ts is not None else None
        self._nav_perf = {}   # code -> {'ret_1m': float, 'ret_3m': float}
        self._risk_metrics = {}  # code -> {'vol_90d': float, 'mdd_180d': float, 'rar_3m': float}
        self.fund_pool = self._get_fund_pool()
        self._ml_model = None          # fund_model.pkl
        self._ml_feature_cols = None
        self._ml_model_loaded = False
        self._ml_decision_threshold = 0.50
        self._ml_calibrator = None
        self._ml_calibration_method = 'none'
        self._ml_gate_passed = False
        self._fund_nav_cache = {}      # code -> sorted nav Series (for ML features)

    # ──────────────────────────────────────────────────────────────────
    # ML 模型接入 (fund_model.pkl)
    # ──────────────────────────────────────────────────────────────────
    @staticmethod
    def _apply_calibrator(method, calibrator, proba):
        import numpy as np
        p = np.clip(float(proba), 1e-6, 1 - 1e-6)
        if calibrator is None or method == 'none':
            return p
        try:
            if method == 'platt':
                return float(np.clip(
                    calibrator.predict_proba([[p]])[0][1], 1e-6, 1 - 1e-6
                ))
            if method == 'isotonic':
                return float(np.clip(calibrator.predict([p])[0], 1e-6, 1 - 1e-6))
        except Exception:
            return p
        return p

    def _load_ml_model(self):
        """延迟加载 fund_model.pkl，只加载一次。gate 未通过则跳过 ML。"""
        if self._ml_model_loaded:
            return self._ml_model is not None and self._ml_gate_passed
        self._ml_model_loaded = True
        try:
            model_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                'data', 'models', 'fund_model.pkl'
            )
            if not os.path.exists(model_path):
                logger.warning('fund_model.pkl 不存在，ML评分将跳过')
                return False
            with open(model_path, 'rb') as f:
                payload = pickle.load(f)
            gate_passed = bool(payload.get('validation_passed', True))
            if not gate_passed:
                gate_name = payload.get('validation_gate', 'unknown')
                gate_reason = payload.get('validation_reason', '')
                logger.warning(
                    f'fund_model.pkl 未通过 validation gate ({gate_name}): {gate_reason}，ML评分将跳过'
                )
                return False
            self._ml_model = payload.get('model')
            self._ml_feature_cols = payload.get('feature_columns', [])
            self._ml_decision_threshold = float(payload.get('decision_threshold', 0.50))
            self._ml_calibrator = payload.get('calibrator', None)
            self._ml_calibration_method = payload.get('calibration_method', 'none')
            self._ml_gate_passed = True
            logger.info(
                f'fund_model.pkl 已加载: {len(self._ml_feature_cols)} 特征, '
                f'threshold={self._ml_decision_threshold:.2f}, '
                f'cal={self._ml_calibration_method}, '
                f'gate_passed={gate_passed}'
            )
            return True
        except Exception as e:
            logger.warning(f'fund_model.pkl 加载失败: {e}')
            return False

    def _load_fund_nav_series(self, code):
        """从 RawFundData 读取单只基金的完整净值序列。"""
        if code in self._fund_nav_cache:
            return self._fund_nav_cache[code]
        session = None
        try:
            raw = str(code or '').strip().upper()
            base = raw[:-3] if raw.endswith('.OF') else raw.split('.')[0]
            variants = [c for c in [raw, base, f'{base}.OF'] if c]
            session = get_session()
            rows = (
                session.query(RawFundData)
                .filter(RawFundData.code.in_(variants))
                .order_by(RawFundData.date.asc())
                .all()
            )
            data = []
            for row in rows:
                if not row.date or row.nav is None:
                    continue
                nav = float(row.nav or 0)
                if nav <= 0:
                    continue
                data.append((pd.Timestamp(row.date), nav))
            if not data:
                self._fund_nav_cache[code] = None
                return None
            df = pd.DataFrame(data, columns=['date', 'nav']).drop_duplicates(subset=['date']).sort_values('date').reset_index(drop=True)
            self._fund_nav_cache[code] = df
            return df
        except Exception as e:
            logger.debug(f'基金历史净值读取失败 {code}: {e}')
            self._fund_nav_cache[code] = None
            return None
        finally:
            if session:
                session.close()

    def _get_macro_snapshot(self):
        """从本地 CSV 读取最新 CPI/PMI/SHIBOR，读取失败时回退硬编码。"""
        if hasattr(self, '_macro_snap'):
            return self._macro_snap
        snap = {'cpi_yoy': 1.0, 'pmi': 50.0, 'shibor_1w': 1.42, 'shibor_1m': 1.44}
        data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
        try:
            cpi_path = os.path.join(data_dir, 'macro_cpi.csv')
            if os.path.exists(cpi_path):
                df_cpi = pd.read_csv(cpi_path)
                df_cpi = df_cpi.dropna(subset=['nt_yoy']).sort_values('month')
                if len(df_cpi):
                    snap['cpi_yoy'] = float(df_cpi['nt_yoy'].iloc[-1])
        except Exception:
            pass
        try:
            pmi_path = os.path.join(data_dir, 'macro_pmi.csv')
            if os.path.exists(pmi_path):
                df_pmi = pd.read_csv(pmi_path)
                df_pmi = df_pmi.dropna(subset=['manufacturing_pmi']).sort_values('month')
                if len(df_pmi):
                    snap['pmi'] = float(df_pmi['manufacturing_pmi'].iloc[-1])
        except Exception:
            pass
        try:
            shibor_path = os.path.join(data_dir, 'macro_shibor.csv')
            if os.path.exists(shibor_path):
                df_sh = pd.read_csv(shibor_path).sort_values('date')
                if len(df_sh):
                    snap['shibor_1w'] = float(df_sh['1w'].iloc[-1])
                    snap['shibor_1m'] = float(df_sh['1m'].iloc[-1])
        except Exception:
            pass
        self._macro_snap = snap
        return snap

    def _get_market_snapshot(self):
        """对齐 train_fund.py 的市场情绪默认快照。"""
        if hasattr(self, '_mkt_snap'):
            return self._mkt_snap
        snap = {
            'mkt_ret_5d': 0.0,
            'mkt_ret_20d': 0.0,
            'mkt_vol_20d': 0.20,
            'mkt_up_trend': 0.5,
        }
        try:
            etf_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                'data', 'historical_etf.csv'
            )
            if os.path.exists(etf_path):
                etf = pd.read_csv(etf_path)
                etf = etf[etf['code'] == '510300.SH'].sort_values('date')
                if len(etf) >= 21:
                    close = etf['close'].values.astype(float)
                    snap['mkt_ret_5d'] = (close[-1] - close[-6]) / close[-6] if close[-6] > 0 else 0.0
                    snap['mkt_ret_20d'] = (close[-1] - close[-21]) / close[-21] if close[-21] > 0 else 0.0
                    rets = np.diff(close[-21:]) / (close[-21:-1] + 1e-9)
                    snap['mkt_vol_20d'] = float(np.std(rets) * np.sqrt(252))
                    ma20 = float(np.mean(close[-21:]))
                    snap['mkt_up_trend'] = 1.0 if close[-1] > ma20 else 0.0
        except Exception:
            pass
        self._mkt_snap = snap
        return snap

    def _build_ml_features(self, code, nav_perf, risk_metrics):
        """按 train_fund.py extract_features 的同构逻辑构建最新一期 23 维特征。"""
        if not self._ml_feature_cols:
            return None

        series = self._load_fund_nav_series(code)
        if series is None or len(series) < 31:
            return None

        nav_values = series['nav'].values.astype(float)
        i = len(nav_values) - 1
        prev_nav = nav_values[:i]
        returns = np.diff(prev_nav) / (prev_nav[:-1] + 1e-9)
        feat = {}

        feat['return_5d'] = (nav_values[i] - nav_values[i - 5]) / nav_values[i - 5] if nav_values[i - 5] > 0 else 0.0
        feat['return_10d'] = (nav_values[i] - nav_values[i - 10]) / nav_values[i - 10] if nav_values[i - 10] > 0 else 0.0
        feat['return_20d'] = (nav_values[i] - nav_values[i - 20]) / nav_values[i - 20] if nav_values[i - 20] > 0 else 0.0
        feat['return_30d'] = (nav_values[i] - nav_values[i - 30]) / nav_values[i - 30] if nav_values[i - 30] > 0 else 0.0
        feat['volatility_5d'] = float(np.std(returns[-5:]) * np.sqrt(252)) if len(returns) >= 5 else 0.0
        feat['volatility_10d'] = float(np.std(returns[-10:]) * np.sqrt(252)) if len(returns) >= 10 else 0.0
        feat['volatility_20d'] = float(np.std(returns[-20:]) * np.sqrt(252)) if len(returns) >= 20 else 0.0
        avg_ret = float(np.mean(returns[-20:])) if len(returns) >= 20 else 0.0
        std_ret = float(np.std(returns[-20:])) if len(returns) >= 20 else 1.0
        feat['sharpe_ratio'] = (avg_ret - 0.00005) / (std_ret + 1e-6) if std_ret > 0 else 0.0
        recent = returns[-30:] if len(returns) >= 30 else returns
        cumulative = np.cumprod(1 + np.clip(recent, -0.5, 0.5))
        running_max = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - running_max) / (running_max + 1e-9)
        feat['max_drawdown'] = float(np.min(drawdown)) if len(drawdown) > 0 else 0.0
        positive_cnt = int(np.sum(returns[-20:] > 0)) if len(returns) >= 20 else 0
        feat['positive_days_ratio'] = positive_cnt / min(20, max(len(returns), 1))
        ma10 = float(np.mean(nav_values[i - 10:i + 1])) if i >= 10 else float(nav_values[i])
        ma20 = float(np.mean(nav_values[i - 20:i + 1])) if i >= 20 else float(nav_values[i])
        feat['nav_ma10_ratio'] = (nav_values[i] / ma10 - 1) if ma10 > 0 else 0.0
        feat['nav_ma20_ratio'] = (nav_values[i] / ma20 - 1) if ma20 > 0 else 0.0

        macro = self._get_macro_snapshot()
        feat['cpi_yoy'] = macro['cpi_yoy']
        feat['pmi'] = macro['pmi']
        feat['shibor_1w'] = macro['shibor_1w']
        feat['shibor_1m'] = macro['shibor_1m']
        feat.update(self._get_market_snapshot())

        ref_date = pd.Timestamp(series['date'].iloc[-1])
        month = int(ref_date.month)
        feat['month_sin'] = float(np.sin(2 * np.pi * month / 12))
        feat['month_cos'] = float(np.cos(2 * np.pi * month / 12))
        feat['is_q1'] = 1.0 if month in (1, 2, 3) else 0.0

        row = [feat.get(col, 0.0) for col in self._ml_feature_cols]
        return pd.DataFrame([row], columns=self._ml_feature_cols)

    def _ml_score_fund(self, code, nav_perf, risk_metrics):
        """
        返回 ML 模型预测的"上涨概率" [0, 1]（已校准），失败时返回 None。
        gate 未通过则直接返回 None。
        """
        if not self._load_ml_model():
            return None
        try:
            X = self._build_ml_features(code, nav_perf, risk_metrics)
            if X is None:
                return None
            if hasattr(self._ml_model, 'predict_proba'):
                raw_prob = float(self._ml_model.predict_proba(X)[0][1])
            else:
                raw_prob = float(self._ml_model.predict(X)[0])
            prob = self._apply_calibrator(
                self._ml_calibration_method, self._ml_calibrator, raw_prob
            )
            return max(0.0, min(1.0, prob))
        except Exception as e:
            logger.debug(f'ML评分异常 {code}: {e}')
            return None

    def get_asset_type(self):
        return "active_fund"

    def _get_cached_or_local_fund_pool(self, limit=100):
        """优先使用缓存和本地净值库中的真实基金池，避免退回到极小默认池。"""
        result = []
        seen = set()

        def _append(code, name=''):
            base_code = str(code or '').strip().upper().split('.')[0]
            if not base_code or base_code in seen:
                return
            seen.add(base_code)
            result.append({
                'code': base_code,
                'name': name or f'基金{base_code}',
                'mgmt_fee': 1.2,
                'inception_date': '',
                'last_update': datetime.now().isoformat(),
                'data_source': 'local_cache',
            })

        try:
            cache_file = str(FUND_POOL_CACHE_FILE)
            if os.path.exists(cache_file):
                with open(cache_file, 'r', encoding='utf-8') as f:
                    payload = json.load(f)
                for item in (payload.get('funds') or []):
                    _append(item.get('code'), item.get('name'))
        except Exception as e:
            logger.warning(f"读取基金缓存池失败: {e}")

        session = None
        try:
            session = get_session()
            holding_funds = session.query(Holding).filter(Holding.asset_type == 'fund').all()
            for row in holding_funds:
                _append(row.code, row.name)

            db_codes = (
                session.query(RawFundData.code, RawFundData.name)
                .distinct(RawFundData.code)
                .all()
            )
            for code, name in db_codes:
                _append(code, name)
        except Exception as e:
            logger.warning(f"读取本地基金池失败: {e}")
        finally:
            if session:
                session.close()

        return result[:limit]

    def _get_fallback_fund_pool(self, limit=100):
        """当主数据源不足时，优先退回本地真实基金数据，再用极小默认池补足。"""
        default_pool = [
            {'code': '110011', 'name': '易方达中小盘'},
            {'code': '519069', 'name': '汇添富价值精选'},
            {'code': '163402', 'name': '兴全趋势投资'},
            {'code': '260108', 'name': '景顺长城新兴成长'},
            {'code': '161005', 'name': '富国天惠成长'},
            {'code': '110003', 'name': '易方达上证50'},
            {'code': '160706', 'name': '嘉实沪深300'},
            {'code': '110020', 'name': '易方达中证500'},
            {'code': '000191', 'name': '富国信用债'},
            {'code': '110017', 'name': '易方达增强回报'},
        ]

        result = []
        seen = set()

        def _append(code, name='', source='fallback'):
            base_code = str(code or '').strip().upper().split('.')[0]
            if not base_code or base_code in seen:
                return
            seen.add(base_code)
            result.append({
                'code': base_code,
                'name': name or f'基金{base_code}',
                'mgmt_fee': 1.2,
                'inception_date': '',
                'last_update': datetime.now().isoformat(),
                'data_source': source,
            })

        local_pool = self._get_cached_or_local_fund_pool(limit=limit)
        for item in local_pool:
            _append(item.get('code'), item.get('name'), item.get('data_source', 'local_cache'))

        if len(result) < min(limit, 20):
            for item in default_pool:
                _append(item.get('code'), item.get('name'), 'fallback')

        logger.info(f"使用回退基金池: {len(result[:limit])} 只")
        return result[:limit]

    def _init_pro(self):
        """初始化 TuShare Pro 连接"""
        try:
            if hasattr(ts, 'pro_connect'):
                return ts.pro_connect()
            ts.set_token(TUSHARE_TOKEN)
            return ts.pro_api()
        except Exception as e:
            logger.error(f"TuShare Pro初始化失败: {e}")
            return None

    def _find_latest_nav_date(self, start_daysback=2, max_search=10):
        """往前查找最近一个有NAV数据的交易日 (跳过节假日/周末)"""
        today = datetime.now()
        for i in range(start_daysback, start_daysback + max_search):
            d = (today - timedelta(days=i)).strftime('%Y%m%d')
            try:
                df = self._pro.fund_nav(nav_date=d, fields='ts_code')
                if df is not None and not df.empty:
                    logger.info(f"最近NAV交易日: {d} ({len(df)}只)")
                    return d
            except Exception:
                pass
        return None

    def _find_nav_date_near(self, base_date, max_search=10):
        """从指定日期向前回退，找到最近一个有NAV数据的交易日。"""
        for i in range(0, max_search + 1):
            d = (base_date - timedelta(days=i)).strftime('%Y%m%d')
            try:
                df = self._pro.fund_nav(nav_date=d, fields='ts_code')
                if df is not None and not df.empty:
                    return d
            except Exception:
                pass
        return None

    def _build_nav_series(self, nav_df):
        """从fund_nav结果构建可计算NAV序列，优先adj_nav，回退accum_nav/unit_nav。"""
        if nav_df is None or nav_df.empty or 'ts_code' not in nav_df.columns:
            return pd.Series(dtype=float)

        for col in ('adj_nav', 'accum_nav', 'unit_nav'):
            if col in nav_df.columns:
                s = pd.to_numeric(nav_df[col], errors='coerce')
                if s.notna().any():
                    return pd.Series(s.values, index=nav_df['ts_code'])

        return pd.Series(dtype=float)

    def _load_nav_performance(self, ts_codes):
        """
        批量加载基金近期NAV表现 (1个月/3个月回报率)
        使用日期批量查询: 3次API调用覆盖所有基金
        """
        if not self._pro or not ts_codes:
            return {}

        # 优先使用 _get_fund_pool 中预缓存的数据 (避免重复API调用)
        if self._nav_perf:
            return {k: v for k, v in self._nav_perf.items() if k in set(ts_codes)}


        result = {}
        today = datetime.now()

        # 动态查找最近有数据的交易日
        date_now = self._find_latest_nav_date(start_daysback=2)
        if not date_now:
            logger.warning("找不到最近NAV交易日, 跳过表现加载")
            return {}

        date_1m = self._find_nav_date_near(today - timedelta(days=33), max_search=10)
        date_3m = self._find_nav_date_near(today - timedelta(days=95), max_search=14)

        if not date_1m or not date_3m:
            logger.warning("找不到1M/3M基准NAV日期, 跳过表现加载")
            return {}

        try:
            nav_now = self._pro.fund_nav(nav_date=date_now, fields='ts_code,adj_nav,accum_nav,unit_nav')
            nav_1m  = self._pro.fund_nav(nav_date=date_1m, fields='ts_code,adj_nav,accum_nav,unit_nav')
            nav_3m  = self._pro.fund_nav(nav_date=date_3m, fields='ts_code,adj_nav,accum_nav,unit_nav')

            if nav_now is None or nav_now.empty:
                logger.warning("fund_nav today返回空")
                return {}

            # 对3个日期数据做索引
            nav_now = self._build_nav_series(nav_now)
            nav_1m  = self._build_nav_series(nav_1m)
            nav_3m  = self._build_nav_series(nav_3m)

            for code in ts_codes:
                try:
                    cur = float(nav_now.get(code, 0))
                    if cur <= 0:
                        continue
                    r1m = (cur - float(nav_1m.get(code, cur))) / float(nav_1m.get(code, cur)) * 100 if code in nav_1m.index else 0.0
                    r3m = (cur - float(nav_3m.get(code, cur))) / float(nav_3m.get(code, cur)) * 100 if code in nav_3m.index else 0.0
                    result[code] = {
                        'nav':    cur,
                        'nav_date': date_now,
                        'ret_1m': round(r1m, 2),
                        'ret_3m': round(r3m, 2),
                    }
                except Exception:
                    pass

            logger.info(
                f"✅ NAV表现加载完成: {len(result)}/{len(ts_codes)} 只基金有数据 "
                f"(now={date_now}, 1m={date_1m}, 3m={date_3m})"
            )
        except Exception as e:
            logger.warning(f"NAV批量加载失败: {e}")

        return result

    def _load_local_nav_performance(self, ts_codes):
        """从本地 RawFundData 补充 NAV 表现，避免主动基金全量缺失。"""
        if not ts_codes:
            return {}

        session = None
        try:
            code_variants = {}
            all_codes = set()
            for code in ts_codes:
                raw = str(code or '').strip().upper()
                base = raw[:-3] if raw.endswith('.OF') else raw.split('.')[0]
                variants = [c for c in [raw, base, f'{base}.OF'] if c]
                code_variants[code] = {'base': base, 'variants': variants}
                all_codes.update(variants)

            session = get_session()
            rows = (
                session.query(RawFundData)
                .filter(RawFundData.code.in_(list(all_codes)))
                .order_by(RawFundData.code.asc(), RawFundData.date.asc())
                .all()
            )

            grouped = {}
            for row in rows:
                key = str(row.code or '').strip().upper().split('.')[0]
                grouped.setdefault(key, []).append(row)

            result = {}
            for request_code, meta in code_variants.items():
                series = grouped.get(meta['base'], [])
                if not series:
                    continue

                latest = series[-1]
                latest_nav = float(latest.nav or 0)
                if latest_nav <= 0:
                    continue

                prev_1m = latest_nav
                prev_3m = latest_nav
                for item in reversed(series[:-1]):
                    nav = float(item.nav or 0)
                    if nav <= 0:
                        continue
                    days_gap = (latest.date - item.date).days if latest.date and item.date else 0
                    if prev_1m == latest_nav and days_gap >= 25:
                        prev_1m = nav
                    if prev_3m == latest_nav and days_gap >= 75:
                        prev_3m = nav
                    if prev_1m != latest_nav and prev_3m != latest_nav:
                        break

                ret_1m = ((latest_nav - prev_1m) / prev_1m * 100.0) if prev_1m > 0 else 0.0
                ret_3m = ((latest_nav - prev_3m) / prev_3m * 100.0) if prev_3m > 0 else ret_1m
                result[request_code] = {
                    'nav': round(latest_nav, 4),
                    'nav_date': latest.date.strftime('%Y%m%d') if latest.date else None,
                    'ret_1m': round(ret_1m, 2),
                    'ret_3m': round(ret_3m, 2),
                }

            if result:
                logger.info(f"✅ 本地基金NAV补充完成: {len(result)}/{len(ts_codes)}")
            return result
        except Exception as e:
            logger.warning(f"本地基金NAV补充失败: {e}")
            return {}
        finally:
            if session:
                session.close()

    def _load_risk_metrics(self, ts_codes, lookback_days=180):
        """
        加载基金风险特征:
        - vol_90d: 近90日收益波动率(%)
        - mdd_180d: 近180日最大回撤(%)
        - rar_3m: 风险调整回报 = ret_3m / max(vol_90d, 1)
        """
        if not self._pro or not ts_codes:
            return {}

        if self._risk_metrics:
            cached = {k: v for k, v in self._risk_metrics.items() if k in set(ts_codes)}
            if cached:
                return cached

        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y%m%d')
        perf = self._load_nav_performance(ts_codes)
        result = {}

        for code in ts_codes:
            try:
                nav_df = self._pro.fund_nav(
                    ts_code=code,
                    start_date=start_date,
                    end_date=end_date,
                    fields='ts_code,nav_date,adj_nav,accum_nav,unit_nav'
                )
                if nav_df is None or nav_df.empty:
                    continue

                nav_col = None
                for candidate in ('adj_nav', 'accum_nav', 'unit_nav'):
                    if candidate in nav_df.columns:
                        nav_col = candidate
                        break
                if nav_col is None:
                    continue

                nav_df = nav_df[['nav_date', nav_col]].copy()
                nav_df['nav_date'] = pd.to_datetime(nav_df['nav_date'], format='%Y%m%d', errors='coerce')
                nav_df[nav_col] = pd.to_numeric(nav_df[nav_col], errors='coerce')
                nav_df = nav_df.dropna(subset=['nav_date', nav_col]).sort_values('nav_date')
                if len(nav_df) < 30:
                    continue

                s = nav_df[nav_col].astype(float)
                ret = s.pct_change().dropna()

                # 近90日波动率(日频标准差, 转百分比)
                ret_90 = ret.tail(90)
                vol_90d = float(ret_90.std() * 100.0) if not ret_90.empty else None

                # 近180日最大回撤
                roll_max = s.cummax()
                drawdown = (s / roll_max - 1.0) * 100.0
                mdd_180d = float(drawdown.min()) if not drawdown.empty else None  # 负数

                ret_3m = (perf.get(code) or {}).get('ret_3m', 0.0)
                vol_denom = max(abs(vol_90d or 0.0), 1.0)
                rar_3m = float(ret_3m) / vol_denom

                result[code] = {
                    'vol_90d': round(vol_90d, 3) if vol_90d is not None else None,
                    'mdd_180d': round(mdd_180d, 3) if mdd_180d is not None else None,
                    'rar_3m': round(rar_3m, 3),
                }
            except Exception:
                continue

        if result:
            self._risk_metrics.update(result)
            logger.info(f"✅ 风险指标加载完成: {len(result)}/{len(ts_codes)}")

        return result

    def _get_fund_pool(self, limit=100):
        """
        从TuShare获取真实基金池 (增强版)

        策略: 以有日度NAV数据的基金为起点, 而非按AUM排序
        这样保证 _load_nav_performance 100% 覆盖
        """
        cached_pool = self._get_cached_or_local_fund_pool(limit=limit)
        if len(cached_pool) >= max(20, min(limit, 100)):
            logger.info(f"使用扩展基金池: {len(cached_pool)} 只")
            return cached_pool

        if not self._pro:
            return self._get_fallback_fund_pool(limit=limit)
        try:
            from datetime import datetime, timedelta
            today = datetime.now()

            # 1. 找最近有数据的交易日
            nav_date_now = self._find_latest_nav_date(start_daysback=2)
            if not nav_date_now:
                return self._get_fallback_fund_pool(limit=limit)

            date_1m = self._find_nav_date_near(today - timedelta(days=33), max_search=10)
            date_3m = self._find_nav_date_near(today - timedelta(days=95), max_search=14)

            if not date_1m or not date_3m:
                logger.warning("1M/3M基准NAV日期不可用")
                return self._get_fallback_fund_pool(limit=limit)

            # 2. 批量获取3个时点的NAV
            nav_now = self._pro.fund_nav(nav_date=nav_date_now, fields='ts_code,adj_nav,accum_nav,unit_nav')
            nav_1m  = self._pro.fund_nav(nav_date=date_1m, fields='ts_code,adj_nav,accum_nav,unit_nav')
            nav_3m  = self._pro.fund_nav(nav_date=date_3m, fields='ts_code,adj_nav,accum_nav,unit_nav')

            if any(df is None or df.empty for df in [nav_now, nav_1m, nav_3m]):
                logger.warning("部分NAV日期数据为空")
                return self._get_fallback_fund_pool(limit=limit)

            # 3. 只保留3个时点都有数据的基金 (.OF 开放式)
            s_now = self._build_nav_series(nav_now)
            s_1m  = self._build_nav_series(nav_1m)
            s_3m  = self._build_nav_series(nav_3m)

            common_codes = (set(s_now.index) & set(s_1m.index) & set(s_3m.index))
            common_codes = {c for c in common_codes if c.endswith('.OF')}

            if not common_codes:
                logger.info("基金历史时点不足，切换到扩展本地基金池")
                local_pool = self._get_cached_or_local_fund_pool(limit=limit)
                return local_pool if local_pool else self._get_fallback_fund_pool(limit=limit)

            logger.info(f"有完整1M+3M历史的基金: {len(common_codes)} 只")

            # 4. 计算每只基金的1M/3M回报率
            perf_list = []
            for code in common_codes:
                try:
                    cur = float(s_now.get(code))
                    p1m = float(s_1m.get(code))
                    p3m = float(s_3m.get(code))
                except (TypeError, ValueError):
                    continue

                if cur <= 0 or p1m <= 0 or p3m <= 0:
                    continue

                r1m = (cur - p1m) / p1m * 100
                r3m = (cur - p3m) / p3m * 100
                perf_list.append({
                    'code': code,
                    'nav': cur,
                    'nav_date': nav_date_now,
                    'ret_1m': round(r1m, 2),
                    'ret_3m': round(r3m, 2)
                })

            # 5. 按3M回报排序取TOP (高到低)
            perf_list.sort(key=lambda x: x['ret_3m'], reverse=True)
            top_perf = perf_list[:limit]

            # 预缓存NAV数据供 _load_nav_performance 直接使用
            self._nav_perf = {p['code']: p for p in perf_list}

            # 6. 合并fund_basic获取费率/名称等元数据
            basic_df = self._pro.fund_basic(status='L')
            basic_idx = {}
            fee_col = None
            if basic_df is not None and not basic_df.empty:
                basic_idx = basic_df.set_index('ts_code').to_dict('index')
                fee_col = 'mgmt_fee' if 'mgmt_fee' in basic_df.columns else (
                    'm_fee' if 'm_fee' in basic_df.columns else None)

            result = []
            for p in top_perf:
                code = p['code']
                row  = basic_idx.get(code, {})
                fee_val = 1.5
                if fee_col and row:
                    v = row.get(fee_col, 1.5)
                    if pd.notna(v):
                        try:
                            fee_val = float(v)
                        except (TypeError, ValueError):
                            fee_val = 1.5
                result.append({
                    'code':           code,
                    'name':           str(row.get('name', code)) if row else code,
                    'mgmt_fee':       fee_val,
                    'inception_date': str(row.get('inception_date', '')) if row else '',
                    'last_update':    datetime.now().isoformat(),
                    'data_source':    'TuShare',
                })

            logger.info(f"✅ 基金池加载完成: {len(result)} 只 (按3M回报排序)")
            return result

        except Exception as e:
            logger.error(f"❌ 获取基金池失败: {e}")
            return self._get_fallback_fund_pool(limit=limit)

    def _calculate_fund_score(self, fund, nav_perf=None, risk_metrics=None):
        """
        计算基金综合评分 (增强版)

                评分维度与权重:
                    近1月NAV回报      30% — 短中期表现
                    近3月NAV回报      20% — 趋势延续性
                    风险调整维度      25% — 波动率/回撤/风险调整回报
                    费率              15% — 成本控制
                    成立时长稳定性    10% — 存续时间越长稳定性通常更高
        """
        score = 3.0
        code  = fund.get('code', '')
        fee   = fund.get('mgmt_fee', 1.5)
        perf  = (nav_perf or {}).get(code, {})
        risk  = (risk_metrics or {}).get(code, {})
        ret_1m = perf.get('ret_1m', None)
        ret_3m = perf.get('ret_3m', None)
        vol_90d = risk.get('vol_90d', None)
        mdd_180d = risk.get('mdd_180d', None)
        rar_3m = risk.get('rar_3m', None)

        try:
            # ── 1. 近1月NAV回报 (30%) ────────────────────
            if ret_1m is not None:
                if   ret_1m >  8: score += 0.70
                elif ret_1m >  4: score += 0.45
                elif ret_1m >  1: score += 0.20
                elif ret_1m < -8: score -= 0.70
                elif ret_1m < -4: score -= 0.45
                elif ret_1m < -1: score -= 0.20
                # ret_1m in (-1, 1) → 中性, 不加减

            # ── 2. 近3月NAV回报 (20%) ────────────────────
            if ret_3m is not None:
                if   ret_3m > 15: score += 0.50
                elif ret_3m > 8:  score += 0.30
                elif ret_3m > 3:  score += 0.15
                elif ret_3m <-15: score -= 0.50
                elif ret_3m < -8: score -= 0.30
                elif ret_3m < -3: score -= 0.15

            # ── 3. 风险调整维度 (25%) ─────────────────────
            # 3.1 波动率 (日收益std, 百分比)
            if vol_90d is not None:
                if   vol_90d < 0.8: score += 0.20
                elif vol_90d < 1.2: score += 0.10
                elif vol_90d > 2.5: score -= 0.25
                elif vol_90d > 1.8: score -= 0.12

            # 3.2 最大回撤 (负数, 越小越差)
            if mdd_180d is not None:
                if   mdd_180d > -5:  score += 0.18
                elif mdd_180d > -10: score += 0.08
                elif mdd_180d < -25: score -= 0.25
                elif mdd_180d < -18: score -= 0.12

            # 3.3 风险调整回报
            if rar_3m is not None:
                if   rar_3m > 5:   score += 0.22
                elif rar_3m > 2.5: score += 0.12
                elif rar_3m < -5:  score -= 0.22
                elif rar_3m < -2.5:score -= 0.12

            # ── 4. 费率 (15%) ────────────────────────────
            if   fee < 0.80: score += 0.30
            elif fee < 1.20: score += 0.15
            elif fee < 1.50: score += 0.00
            elif fee < 2.00: score -= 0.10
            else:            score -= 0.20

            # ── 5. 成立时长稳定性 (10%) ──────────────────
            inception = str(fund.get('inception_date', ''))
            if inception and len(inception) == 8:
                try:
                    inc_year = int(inception[:4])
                    age_years = datetime.now().year - inc_year
                    if   age_years >= 10: score += 0.15
                    elif age_years >=  5: score += 0.08
                    elif age_years >=  3: score += 0.03
                    elif age_years <   1: score -= 0.10
                except Exception:
                    pass

            return max(1.0, min(5.0, round(score, 2)))

        except Exception as e:
            logger.warning(f"计算基金评分异常: {e}")
            return 3.0

    def _calculate_fund_score_with_ml(self, fund, nav_perf=None, risk_metrics=None):
        """
        规则评分 (70%) + ML概率评分 (30%) 融合。
        ML 不可用时自动退化为纯规则评分。
        """
        rule_score = self._calculate_fund_score(fund, nav_perf, risk_metrics)
        code = fund.get('code', '')
        ml_prob = self._ml_score_fund(code, nav_perf or {}, risk_metrics or {})
        if ml_prob is None:
            return rule_score
        # ML概率 [0,1] 映射到 [1,5] 分制
        ml_score = 1.0 + ml_prob * 4.0
        fused = round(0.7 * rule_score + 0.3 * ml_score, 2)
        return max(1.0, min(5.0, fused))

    def get_recommendations(self, limit=20):
        """获取基金推荐 (增强版: 使用真实NAV表现驱动评分)"""
        # 重新加载基金池
        self.fund_pool = self._get_fund_pool(limit * 2)
        if not self.fund_pool:
            logger.info("基金池暂为空，已跳过本次基金推荐")
            return []

        # 批量加载NAV表现 (2次API调用)
        codes = [f['code'] for f in self.fund_pool]
        nav_perf = self._load_nav_performance(codes)
        if len(nav_perf) < max(3, min(10, len(codes) // 3 or 1)):
            local_nav_perf = self._load_local_nav_performance(codes)
            if local_nav_perf:
                nav_perf.update({k: v for k, v in local_nav_perf.items() if k not in nav_perf})

        if nav_perf:
            logger.info(f"NAV表现已加载: {len(nav_perf)} 只")

        # 若 NAV 数据仍明显不足，则主动补采一轮真实基金净值
        if len(nav_perf) < max(3, min(8, len(codes))):
            try:
                from collectors.fund_collector import FundCollector
                collector = FundCollector()
                for code in codes[:min(8, len(codes))]:
                    collector.collect_fund_nav(str(code).split('.')[0], days=90)
                local_nav_perf = self._load_local_nav_performance(codes)
                if local_nav_perf:
                    nav_perf.update(local_nav_perf)
            except Exception as e:
                logger.warning(f"主动基金净值补采失败: {e}")

        risk_metrics = self._load_risk_metrics(codes)

        recommendations = []
        for fund in self.fund_pool:
            try:
                score = self._calculate_fund_score_with_ml(fund, nav_perf, risk_metrics)
                ml_prob = self._ml_score_fund(fund.get('code', ''), nav_perf, risk_metrics)
                perf  = nav_perf.get(fund['code'], {})
                risk  = risk_metrics.get(fund['code'], {})
                current_price = perf.get('nav')
                nav_date = perf.get('nav_date')
                ret_1m = perf.get('ret_1m')
                ret_3m = perf.get('ret_3m')
                vol_90d = risk.get('vol_90d')
                mdd_180d = risk.get('mdd_180d')
                rar_3m = risk.get('rar_3m')

                reasons = [f"管理费{fund['mgmt_fee']:.2f}%"]
                if current_price is not None and nav_date:
                    reasons.append(f"NAV({nav_date})={current_price:.4f}")
                if ml_prob is not None:
                    reasons.append(f"ML上涨概率{ml_prob:.1%}")
                if ret_1m is not None:
                    reasons.append(f"近1月{'↑' if ret_1m > 0 else '↓'}{abs(ret_1m):.1f}%")
                if ret_3m is not None:
                    reasons.append(f"近3月{'↑' if ret_3m > 0 else '↓'}{abs(ret_3m):.1f}%")
                if vol_90d is not None:
                    reasons.append(f"90日波动{vol_90d:.2f}%")
                if mdd_180d is not None:
                    reasons.append(f"180日回撤{mdd_180d:.1f}%")
                if rar_3m is not None:
                    reasons.append(f"风险调整回报{rar_3m:.2f}")

                recommendations.append({
                    'code':        fund['code'],
                    'name':        fund['name'],
                    'score':       score,
                    'mgmt_fee':    fund['mgmt_fee'],
                    'current_price': round(float(current_price), 4) if current_price is not None else None,
                    'nav_date':    nav_date,
                    'ret_1m':      ret_1m,
                    'ret_3m':      ret_3m,
                    'vol_90d':     vol_90d,
                    'mdd_180d':    mdd_180d,
                    'rar_3m':      rar_3m,
                    'reason':      '; '.join(reasons),
                    'data_source': 'TuShare',
                    'ml_score':    round(ml_prob, 4) if ml_prob is not None else None,
                    'update_time': fund.get('last_update', datetime.now().isoformat()),
                })
            except Exception as e:
                logger.warning(f"处理基金{fund.get('code')}异常: {e}")

        # 按 score 排序 (sort_by_score 用 total_score key, 基金用 score key)
        recommendations.sort(key=lambda x: x.get('score', 0), reverse=True)
        recommendations = recommendations[:limit]
        recommendations = self.add_rank(recommendations)
        return recommendations