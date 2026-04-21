"""
ETF推荐引擎 - recommenders/etf_recommender.py
推荐ETF投资标的 (增强版: 无随机数, 基于真实市场数据)
"""

import sys
import os
from datetime import datetime

import yfinance as yf

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from recommenders.base_recommender import BaseRecommender
from utils import get_logger

logger = get_logger(__name__)


class ETFRecommender(BaseRecommender):
    """ETF推荐引擎 (增强版)"""

    def __init__(self):
        super().__init__()
        self.etf_pool = self._get_etf_pool()
        self._market_signals = None  # 延迟加载

    def get_asset_type(self):
        return "etf"

    def _get_etf_pool(self):
        """ETF标的池（仅保留稳定静态属性: 代码/名称/类型/费率）"""
        return [
            {'code': '510300.SH', 'yf': '510300.SS', 'name': '沪深300ETF', 'type': '宽基', 'fee': 0.15},
            {'code': '510500.SH', 'yf': '510500.SS', 'name': '中证500ETF', 'type': '宽基', 'fee': 0.15},
            {'code': '510050.SH', 'yf': '510050.SS', 'name': '上证50ETF',  'type': '宽基', 'fee': 0.15},
            {'code': '159915.SZ', 'yf': '159915.SZ', 'name': '创业板ETF',  'type': '宽基', 'fee': 0.15},
            {'code': '588000.SH', 'yf': '588000.SS', 'name': '科创50ETF',  'type': '宽基', 'fee': 0.15},
            {'code': '512880.SH', 'yf': '512880.SS', 'name': '证券ETF',    'type': '行业', 'fee': 0.20},
            {'code': '512690.SH', 'yf': '512690.SS', 'name': '酒ETF',      'type': '行业', 'fee': 0.20},
            {'code': '515030.SH', 'yf': '515030.SS', 'name': '新能源车ETF','type': '行业', 'fee': 0.20},
            {'code': '512010.SH', 'yf': '512010.SS', 'name': '医药ETF',    'type': '行业', 'fee': 0.20},
            {'code': '518880.SH', 'yf': '518880.SS', 'name': '黄金ETF',    'type': '商品', 'fee': 0.20},
        ]

    # ──────────────────────────────────────────────────────────────────────
    # 市场信号 (单次批量加载)
    # ──────────────────────────────────────────────────────────────────────
    def _load_market_signals(self):
        """一次性批量加载全局信号 + 各ETF价格/回报"""
        if self._market_signals is not None:
            return self._market_signals

        signals = {
            'vix': 20.0,           # 默认中性
            'csi300_vs_ma20': 0.0, # 沪深300相对20日均线(%)
            'prices': {},          # code -> float
            'returns': {},         # code -> {'r1d','r5d','r20d','vs_ma20','vol20d','liq20d'}
        }

        # 1. VIX
        try:
            vix_data = yf.Ticker('^VIX').history(period='5d')
            if not vix_data.empty:
                signals['vix'] = float(vix_data['Close'].iloc[-1])
        except Exception as e:
            logger.warning(f"VIX获取失败: {e}")

        # 2. 沪深300大盘趋势
        try:
            csi_hist = yf.Ticker('000300.SS').history(period='2mo')
            if len(csi_hist) >= 20:
                cur = float(csi_hist['Close'].iloc[-1])
                ma20 = float(csi_hist['Close'].tail(20).mean())
                signals['csi300_vs_ma20'] = (cur - ma20) / ma20 * 100
        except Exception as e:
            logger.warning(f"沪深300趋势获取失败: {e}")

        # 3. 批量获取各ETF价格历史
        yf_codes = [e['yf'] for e in self.etf_pool]
        try:
            raw = yf.download(
                yf_codes, period='2mo',
                group_by='ticker', auto_adjust=True,
                threads=True, progress=False
            )
            for etf in self.etf_pool:
                code = etf['code']
                yfc  = etf['yf']
                try:
                    close = (raw[yfc]['Close'] if len(yf_codes) > 1 else raw['Close']).dropna()
                    volume = (raw[yfc]['Volume'] if len(yf_codes) > 1 else raw['Volume']).dropna()
                    hist = close
                    if len(hist) < 5:
                        continue
                    price = float(hist.iloc[-1])
                    p1  = float(hist.iloc[-2])
                    p5  = float(hist.iloc[-6])  if len(hist) >= 6  else price
                    p20 = float(hist.iloc[-21]) if len(hist) >= 21 else price
                    ma20 = float(hist.tail(20).mean())
                    ret_series = hist.pct_change().dropna()
                    vol20d = float(ret_series.tail(20).std() * 100.0) if len(ret_series) >= 5 else None
                    liq20d = float(volume.tail(20).mean()) if len(volume) >= 5 else None
                    signals['prices'][code] = price
                    signals['returns'][code] = {
                        'r1d':    (price - p1)  / p1  * 100,
                        'r5d':    (price - p5)  / p5  * 100,
                        'r20d':   (price - p20) / p20 * 100,
                        'vs_ma20':(price - ma20)/ ma20* 100,
                        'vol20d': vol20d,
                        'liq20d': liq20d,
                    }
                except Exception:
                    pass
        except Exception as e:
            logger.warning(f"ETF批量价格获取失败: {e}")

        self._market_signals = signals
        return signals

    # ──────────────────────────────────────────────────────────────────────
    # 评分
    # ──────────────────────────────────────────────────────────────────────
    def _calculate_etf_score(self, etf, signals):
        """
        ETF综合评分 (无随机数)

        维度与权重:
                    成本与交易性  : 费率(10%) + 流动性(15%) + 实现波动(10%) + 类型(5%)
                    价格动量     : 20日回报(25%) + 价格vs MA20(15%)
                    宏观环境     : VIX水平(10%) + 大盘趋势(10%)
        """
        score = 3.0
        code  = etf['code']

        # ── 成本与交易性 (40%) ──────────────────────────
        ret = signals['returns'].get(code)

        # 费率 (10%)
        f = etf['fee']
        if   f <= 0.10: score += 0.20
        elif f <= 0.15: score += 0.10
        elif f  > 0.25: score -= 0.10

        # 流动性 (15%) - 近20日平均成交量
        liq20d = ret.get('liq20d') if ret else None
        if liq20d is not None:
            if   liq20d > 8_000_000: score += 0.30
            elif liq20d > 4_000_000: score += 0.18
            elif liq20d > 1_000_000: score += 0.08
            elif liq20d <   200_000: score -= 0.12

        # 实现波动 (10%) - 适度波动更利于持有体验
        vol20d = ret.get('vol20d') if ret else None
        if vol20d is not None:
            if   vol20d < 1.0: score += 0.16
            elif vol20d < 1.8: score += 0.08
            elif vol20d > 3.5: score -= 0.18
            elif vol20d > 2.8: score -= 0.10

        # 类型 (5%)
        if etf['type'] == '宽基':
            score += 0.10

        # ── 价格动量 (40%) ───────────────────────────
        if ret:
            # 20日回报 (25%)
            r20 = ret['r20d']
            if   r20 >  8: score += 0.60
            elif r20 >  4: score += 0.35
            elif r20 >  1: score += 0.15
            elif r20 < -8: score -= 0.60
            elif r20 < -4: score -= 0.35
            elif r20 < -1: score -= 0.15

            # 价格 vs MA20 (15%)
            vm = ret['vs_ma20']
            if   vm >  3: score += 0.30
            elif vm >  0: score += 0.10
            elif vm < -3: score -= 0.30
            elif vm <  0: score -= 0.10

        # ── 宏观环境 (20%) ───────────────────────────
        vix = signals['vix']
        csi_trend = signals['csi300_vs_ma20']

        if etf['type'] == '商品':   # 黄金ETF受益于高VIX
            if   vix > 30: score += 0.40
            elif vix > 22: score += 0.20
            elif vix < 15: score -= 0.10
        else:                       # 股票类ETF
            if   vix > 35: score -= 0.50
            elif vix > 25: score -= 0.25
            elif vix < 15: score += 0.20

        if etf['type'] == '宽基':   # 大盘趋势只对宽基ETF有效
            if   csi_trend >  3: score += 0.30
            elif csi_trend >  0: score += 0.10
            elif csi_trend < -3: score -= 0.30
            elif csi_trend <  0: score -= 0.10

        return max(1.0, min(5.0, round(score, 2)))

    # ──────────────────────────────────────────────────────────────────────
    # 推荐入口
    # ──────────────────────────────────────────────────────────────────────
    def get_recommendations(self, limit=20):
        """获取ETF推荐 (无随机数, 真实市场数据)"""
        signals = self._load_market_signals()
        recommendations = []

        for etf in self.etf_pool:
            score  = self._calculate_etf_score(etf, signals)
            price  = signals['prices'].get(etf['code'])
            ret    = signals['returns'].get(etf['code'], {})

            # 推荐理由
            reasons = []
            r20 = ret.get('r20d')
            if r20 is not None:
                if   r20 >  2: reasons.append(f"20日涨{r20:.1f}%")
                elif r20 < -2: reasons.append(f"20日跌{abs(r20):.1f}%")
            vix = signals['vix']
            if etf['type'] == '商品' and vix > 22:
                reasons.append(f"VIX={vix:.1f}利好避险")
            if not reasons:
                reasons.append(f"费率{etf['fee']}%")

            # 波动等级 — 基于20日实现波动(确定性)
            vol20d = ret.get('vol20d')
            if vol20d is None:
                vol_level = 'medium'
            else:
                vol_level = 'low' if vol20d < 1.5 else ('medium' if vol20d < 2.8 else 'high')

            recommendations.append({
                'code':            etf['code'],
                'name':            etf['name'],
                'type':            etf['type'],
                'fee':             etf['fee'],
                'total_score':     score,
                'current_price':   round(price, 4) if price else None,
                'return_5d':       round(ret['r5d'],  2) if ret.get('r5d')  is not None else None,
                'return_20d':      round(ret['r20d'], 2) if ret.get('r20d') is not None else None,
                'realized_vol_20d': round(ret['vol20d'], 3) if ret.get('vol20d') is not None else None,
                'avg_volume_20d': int(ret['liq20d']) if ret.get('liq20d') is not None else None,
                'volatility_level': vol_level,
                'reason_summary':  '; '.join(reasons),
                'data_source':     'yfinance',
                'update_time':     datetime.now().isoformat(),
            })

        recommendations = self.sort_by_score(recommendations)
        recommendations = self.add_rank(recommendations)
        return recommendations[:limit]