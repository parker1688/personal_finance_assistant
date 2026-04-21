"""
黄金白银推荐引擎 - recommenders/gold_recommender.py
推荐黄金和白银投资标的
"""

import sys
import os
from datetime import datetime
import yfinance as yf
import pandas as pd

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
    
    def get_asset_type(self):
        return "gold_silver"

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
                recommendations.append({
                    'code':          item['code'],
                    'name':          item['name'],
                    'type':          item['type'],
                    'currency':      item['currency'],
                    'score':         score,
                    'current_price': item['price'],
                    'fee':           item['fee'],
                    'reason':        self._generate_metal_reason('gold', indicators),
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