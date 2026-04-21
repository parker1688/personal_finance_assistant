"""
持仓管理API - api/holdings.py
提供持仓增删改查接口，支持股票、基金、ETF、黄金、白银
"""

import sys
import os
import csv
import io
import re
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from flask import jsonify, request, send_file

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import get_session, Holding, Recommendation, Prediction, RawFundData, RawStockData, Warning as WarningModel
from utils import get_logger, get_today, SimpleCache

try:
    from config import MIN_PORTFOLIO_DIVERSIFICATION
except Exception:
    MIN_PORTFOLIO_DIVERSIFICATION = 5

logger = get_logger(__name__)
price_cache = SimpleCache(ttl=120)
exchange_rate_cache = SimpleCache(ttl=300)


def _normalize_fund_code_variants(code):
    """返回基金代码的常见查询形态，例如 009478 / 009478.OF。"""
    normalized = str(code or '').strip().upper()
    if not normalized:
        return []

    base = normalized[:-3] if normalized.endswith('.OF') else normalized.split('.')[0]
    variants = []
    for candidate in [normalized, base, f'{base}.OF']:
        if candidate and candidate not in variants:
            variants.append(candidate)
    return variants


def _get_latest_recommendation_price(target_code, target_type):
    session = None
    try:
        session = get_session()
        codes = _normalize_fund_code_variants(target_code) if target_type == 'active_fund' else [target_code]
        rec = (
            session.query(Recommendation)
            .filter(Recommendation.code.in_(codes))
            .filter(Recommendation.type == target_type)
            .filter(Recommendation.current_price.isnot(None))
            .order_by(Recommendation.date.desc(), Recommendation.rank.asc())
            .first()
        )
        if rec and rec.current_price and rec.current_price > 0:
            return float(rec.current_price)
    except Exception as e:
        logger.warning(f"读取推荐快照价格失败 {target_code}: {e}")
    finally:
        if session:
            session.close()
    return None


def _get_latest_raw_fund_nav_price(code):
    """优先读取本地已落库的最新基金净值。"""
    session = None
    try:
        session = get_session()
        variants = _normalize_fund_code_variants(code)
        if not variants:
            return None
        row = (
            session.query(RawFundData)
            .filter(RawFundData.code.in_(variants))
            .filter(RawFundData.nav.isnot(None))
            .order_by(RawFundData.date.desc(), RawFundData.created_at.desc())
            .first()
        )
        if row and row.nav and float(row.nav) > 0:
            return float(row.nav)
    except Exception as e:
        logger.warning(f"读取本地基金净值失败 {code}: {e}")
    finally:
        if session:
            session.close()
    return None


def _fetch_live_fund_nav_price(code):
    """从 TuShare / AkShare 拉取最新基金净值。"""
    variants = _normalize_fund_code_variants(code)
    if not variants:
        return None

    try:
        import tushare as ts
        if hasattr(ts, 'pro_connect'):
            pro = ts.pro_connect()
        else:
            from config import TUSHARE_TOKEN
            ts.set_token(TUSHARE_TOKEN)
            pro = ts.pro_api()

        for d in range(0, 10):
            nav_date = (datetime.now() - timedelta(days=d + 1)).strftime('%Y%m%d')
            for ts_code in variants:
                nav_df = pro.fund_nav(ts_code=ts_code, nav_date=nav_date, fields='ts_code,unit_nav')
                if nav_df is not None and not nav_df.empty:
                    unit_nav = pd.to_numeric(nav_df.iloc[0].get('unit_nav'), errors='coerce')
                    if not pd.isna(unit_nav) and unit_nav > 0:
                        return float(unit_nav)
    except Exception as e:
        logger.warning(f"TuShare基金净值失败 {code}: {e}")

    try:
        import akshare as ak
        df = ak.fund_open_fund_info_em(symbol=variants[0].split('.')[0], indicator='单位净值走势')
        if df is not None and len(df) > 0:
            latest_nav = pd.to_numeric(df.iloc[-1].get('单位净值'), errors='coerce')
            if not pd.isna(latest_nav) and latest_nav > 0:
                logger.info(f"基金 {code} 最新净值: {latest_nav}")
                return float(latest_nav)
    except Exception as e:
        logger.warning(f"获取基金净值失败 {code}: {e}")

    return None


def _infer_rec_type(asset_type, code):
    """将持仓资产类型映射为推荐类型。"""
    if asset_type == 'fund':
        return 'active_fund'
    if asset_type == 'etf':
        return 'etf'
    if asset_type == 'gold':
        return 'gold'
    if asset_type == 'silver':
        return 'silver'
    c = (code or '').upper()
    if c.endswith('.HK'):
        return 'hk_stock'
    if c.endswith('.SH') or c.endswith('.SZ'):
        return 'a_stock'
    return 'us_stock'


def _map_rec_type_to_asset_type(rec_type):
    rec_type = str(rec_type or '').strip().lower()
    if rec_type == 'active_fund':
        return 'fund'
    if rec_type in ('a_stock', 'hk_stock', 'us_stock', 'stock'):
        return 'stock'
    if rec_type in ('etf', 'gold', 'silver'):
        return rec_type
    return 'stock'


def _normalize_lookup_code(code):
    return str(code or '').strip().upper()


def _build_code_lookup_variants(code):
    normalized = _normalize_lookup_code(code)
    if not normalized:
        return []

    variants = []
    base = normalized[:-3] if normalized.endswith('.OF') else normalized
    if '.' in base:
        base = base.split('.')[0]

    for candidate in [normalized, base]:
        if candidate and candidate not in variants:
            variants.append(candidate)

    if base.isdigit():
        if len(base) == 6:
            for suffix in ('.OF', '.SH', '.SZ'):
                candidate = f'{base}{suffix}'
                if candidate not in variants:
                    variants.append(candidate)
        elif len(base) in (4, 5):
            candidate = f'{base}.HK'
            if candidate not in variants:
                variants.append(candidate)

    return variants


def _guess_asset_type_from_code(code):
    normalized = _normalize_lookup_code(code)
    base = normalized.split('.')[0]

    if normalized in {'GC=F', 'GLD', 'IAU', 'GLDM', 'SGOL'}:
        return 'gold'
    if normalized in {'SI=F', 'SLV', 'SIVR', 'PSLV'}:
        return 'silver'
    if normalized.endswith('.OF'):
        return 'fund'
    if normalized.endswith('.HK'):
        return 'stock'
    if normalized.endswith('.SH') or normalized.endswith('.SZ'):
        if re.match(r'^(15|16|50|51|56|58)', base):
            return 'etf'
        return 'stock'

    if re.fullmatch(r'\d{6}', base or ''):
        if re.match(r'^(15|50|51|56|58)', base):
            return 'etf'
        if re.match(r'^(009|010|011|012|013|014|015|016|161|162|163|164|165|166)', base):
            return 'fund'
        if re.match(r'^(000|001|002|003|300|301|600|601|603|605|688)', base):
            return 'stock'
        return 'fund'

    return 'stock'


def _guess_asset_name(code, asset_type):
    normalized = _normalize_lookup_code(code)
    known_map = {
        'GC=F': 'COMEX黄金',
        'GLD': 'SPDR Gold Shares',
        'IAU': 'iShares Gold Trust',
        'SI=F': 'COMEX白银',
        'SLV': 'iShares Silver Trust',
    }
    if normalized in known_map:
        return known_map[normalized]
    return ''


def _lookup_asset_identity(code, session=None):
    normalized = _normalize_lookup_code(code)
    if not normalized:
        return {
            'matched': False,
            'code': '',
            'name': '',
            'asset_type': 'stock',
            'source': 'empty',
        }

    own_session = False
    if session is None:
        session = get_session()
        own_session = True

    try:
        variants = _build_code_lookup_variants(normalized)

        holding = (
            session.query(Holding)
            .filter(Holding.code.in_(variants))
            .order_by(Holding.updated_at.desc(), Holding.id.desc())
            .first()
        )
        if holding:
            return {
                'matched': True,
                'code': holding.code,
                'name': holding.name or _guess_asset_name(holding.code, holding.asset_type),
                'asset_type': holding.asset_type or _guess_asset_type_from_code(holding.code),
                'source': 'holding',
            }

        recommendation = (
            session.query(Recommendation)
            .filter(Recommendation.code.in_(variants))
            .order_by(Recommendation.date.desc(), Recommendation.rank.asc())
            .first()
        )
        if recommendation:
            asset_type = _map_rec_type_to_asset_type(recommendation.type)
            return {
                'matched': True,
                'code': recommendation.code,
                'name': recommendation.name or _guess_asset_name(recommendation.code, asset_type),
                'asset_type': asset_type,
                'source': 'recommendation',
            }

        fund_row = (
            session.query(RawFundData)
            .filter(RawFundData.code.in_(variants))
            .order_by(RawFundData.date.desc())
            .first()
        )
        if fund_row:
            return {
                'matched': True,
                'code': fund_row.code,
                'name': fund_row.name or _guess_asset_name(fund_row.code, 'fund'),
                'asset_type': 'fund',
                'source': 'raw_fund',
            }

        stock_row = (
            session.query(RawStockData)
            .filter(RawStockData.code.in_(variants))
            .order_by(RawStockData.date.desc())
            .first()
        )
        if stock_row:
            return {
                'matched': True,
                'code': stock_row.code,
                'name': stock_row.name or _guess_asset_name(stock_row.code, 'stock'),
                'asset_type': 'stock',
                'source': 'raw_stock',
            }

        guessed_type = _guess_asset_type_from_code(normalized)
        return {
            'matched': False,
            'code': normalized,
            'name': _guess_asset_name(normalized, guessed_type),
            'asset_type': guessed_type,
            'source': 'heuristic',
        }
    finally:
        if own_session:
            session.close()


def _extract_probabilities(rec):
    """抽取5/20/60日上涨概率，缺失时回退中性值。"""
    p5 = float(rec.up_probability_5d or 50)
    p20 = float(rec.up_probability_20d or p5)
    p60 = float(rec.up_probability_60d or p20)
    return {
        5: max(5.0, min(95.0, p5)),
        20: max(5.0, min(95.0, p20)),
        60: max(5.0, min(95.0, p60)),
    }


def _target_prices(current_price, up_prob, horizon, rec=None):
    """计算目标价区间：优先使用推荐存量目标价，缺失时按概率映射。"""
    if rec is not None:
        if horizon == 5:
            low = rec.target_low_5d
            high = rec.target_high_5d
        elif horizon == 20:
            low = rec.target_low_20d
            high = rec.target_high_20d
        else:
            low = rec.target_low_60d
            high = rec.target_high_60d
        if low is not None and high is not None and low > 0 and high > 0:
            return float(low), float(high)

    down_prob = 100.0 - up_prob
    up_scale = {5: 0.08, 20: 0.16, 60: 0.30}.get(horizon, 0.16)
    dn_scale = {5: 0.06, 20: 0.12, 60: 0.24}.get(horizon, 0.12)
    up_move = max(0.01, (up_prob - 50.0) / 100.0 * up_scale)
    dn_move = max(0.01, (down_prob - 50.0) / 100.0 * dn_scale)
    target_high = current_price * (1 + up_move)
    target_low = current_price * (1 - dn_move)
    return float(target_low), float(target_high)


def _normalize_recommendation_bucket(rec_type):
    """规范推荐类别，便于持仓页做分散展示。"""
    normalized = str(rec_type or '').strip().lower()
    alias_map = {
        'stock': 'a_stock',
        'fund': 'active_fund',
    }
    return alias_map.get(normalized, normalized or 'other')


def _select_diversified_unheld_recommendations(unheld, limit=20, holding_asset_types=None):
    """对未持仓推荐做分散化筛选，避免 Top N 被单一海外/商品主题挤占。"""
    items = list(unheld or [])
    if not items or limit <= 0:
        return []

    holding_asset_types = {str(x or '').strip().lower() for x in (holding_asset_types or [])}
    domestic_types = {'a_stock', 'active_fund', 'etf'}
    preferred_order = ['a_stock', 'active_fund', 'etf', 'hk_stock', 'us_stock', 'gold', 'silver', 'other']

    type_caps = {
        'a_stock': 2,
        'active_fund': 2,
        'etf': 2,
        'hk_stock': 1,
        'us_stock': 1,
        'gold': 1,
        'silver': 1,
        'other': 1,
    }

    if 'stock' in holding_asset_types or 'etf' in holding_asset_types:
        type_caps['a_stock'] = 3
        type_caps['etf'] = 3

    scored = sorted(
        items,
        key=lambda x: (float(x.get('score', 0.0) or 0.0), -int(x.get('rank', 9999) or 9999)),
        reverse=True,
    )

    domestic_available = sum(1 for item in scored if _normalize_recommendation_bucket(item.get('type')) in domestic_types)
    min_domestic = min(2, domestic_available, limit)

    selected = []
    seen_codes = set()
    per_type_count = {}

    def _try_add(item, enforce_domestic_quota=False):
        code = str(item.get('code') or '').strip().upper()
        rec_type = _normalize_recommendation_bucket(item.get('type'))
        if not code or code in seen_codes:
            return False
        if per_type_count.get(rec_type, 0) >= type_caps.get(rec_type, 1):
            return False
        if enforce_domestic_quota and rec_type not in domestic_types:
            domestic_now = sum(1 for s in selected if _normalize_recommendation_bucket(s.get('type')) in domestic_types)
            if domestic_now < min_domestic:
                return False

        normalized_item = dict(item)
        normalized_item['type'] = rec_type
        selected.append(normalized_item)
        seen_codes.add(code)
        per_type_count[rec_type] = per_type_count.get(rec_type, 0) + 1
        return True

    for rec_type in preferred_order:
        if len(selected) >= limit:
            break
        for item in scored:
            if _normalize_recommendation_bucket(item.get('type')) == rec_type and _try_add(item):
                break

    for item in scored:
        if len(selected) >= limit:
            break
        _try_add(item, enforce_domestic_quota=True)

    for item in scored:
        if len(selected) >= limit:
            break
        _try_add(item, enforce_domestic_quota=False)

    return selected[:limit]


def _build_portfolio_health_summary(holding_signals, current_asset_actions, risk_alerts, action_suggestions, unheld_recommendations):
    """根据真实持仓生成组合健康度与调仓建议。"""
    holding_signals = list(holding_signals or [])
    current_asset_actions = list(current_asset_actions or [])
    risk_alerts = list(risk_alerts or [])
    action_suggestions = list(action_suggestions or [])
    unheld_recommendations = list(unheld_recommendations or [])

    if not holding_signals:
        return {
            'overall_risk': 'low',
            'health_score': 72,
            'diversification_status': '待建仓',
            'concentration_ratio_pct': 0.0,
            'recommended_cash_ratio_pct': 35,
            'key_issues': ['当前暂无持仓，可优先从高质量候选中分批建仓'],
            'next_actions': ['先建立观察池，再逐步分散配置'],
            'summary_text': '当前为空仓或近空仓状态，适合先观察并分批建立核心仓位。',
        }

    total_value = sum(float(x.get('market_value', 0.0) or 0.0) for x in holding_signals)
    market_values = [float(x.get('market_value', 0.0) or 0.0) for x in holding_signals if float(x.get('market_value', 0.0) or 0.0) > 0]
    top_ratio = (max(market_values) / total_value) if total_value > 0 and market_values else 0.0
    avg_profit = sum(float(x.get('profit_rate', 0.0) or 0.0) for x in holding_signals) / max(len(holding_signals), 1)

    high_risk_count = sum(1 for x in current_asset_actions if str(x.get('level')) == 'high') + sum(1 for x in risk_alerts if str(x.get('level')) == 'high')
    reduce_count = sum(1 for x in current_asset_actions if str(x.get('action')) in ['减仓', '清仓'])
    add_count = sum(1 for x in current_asset_actions if str(x.get('action')) == '增仓') + sum(1 for x in action_suggestions if str(x.get('action')) == 'add')

    health_score = 78
    if len(holding_signals) < int(MIN_PORTFOLIO_DIVERSIFICATION):
        health_score -= 10
    if top_ratio >= 0.45:
        health_score -= 18
    elif top_ratio >= 0.30:
        health_score -= 8
    if high_risk_count >= 2:
        health_score -= 15
    elif high_risk_count == 1:
        health_score -= 8
    if avg_profit <= -5:
        health_score -= 10
    elif avg_profit >= 5:
        health_score += 5
    if add_count >= 2 and high_risk_count == 0:
        health_score += 4

    health_score = int(max(25, min(95, round(health_score))))

    key_issues = []
    next_actions = []

    if top_ratio >= 0.45:
        key_issues.append(f'单一资产集中度偏高，最大仓位占比约{top_ratio * 100:.1f}%')
        next_actions.append('建议降低单一重仓占比，分散到更多高质量资产')
    elif len(holding_signals) < int(MIN_PORTFOLIO_DIVERSIFICATION):
        key_issues.append(f'持仓分散度不足，当前仅持有{len(holding_signals)}类资产')
        next_actions.append('适当增加不同风格与资产类型，降低组合波动')

    if high_risk_count >= 1:
        key_issues.append('组合中存在高风险或走弱仓位，需要优先风控')
        next_actions.append('先处理高风险仓位，再考虑新增配置')

    if avg_profit <= -5:
        key_issues.append('组合近期整体收益承压，需控制回撤')
        next_actions.append('收紧止损纪律，等待更明确信号后再进攻')

    if not key_issues:
        key_issues.append('组合结构整体平衡，当前可维持纪律化管理')
        next_actions.append('维持核心仓位，围绕强势资产做小幅优化')

    if high_risk_count >= 2 or top_ratio >= 0.45:
        overall_risk = 'high'
        diversification_status = '需优化'
        recommended_cash_ratio_pct = 35
        summary_text = '当前组合偏脆弱，建议先防守，降低集中度并提升现金比例。'
    elif health_score >= 75:
        overall_risk = 'low'
        diversification_status = '良好'
        recommended_cash_ratio_pct = 15
        summary_text = '当前组合质量较好，可继续以分批方式优化结构。'
    else:
        overall_risk = 'medium'
        diversification_status = '中性'
        recommended_cash_ratio_pct = 25
        summary_text = '当前组合处于均衡状态，建议边跟踪边微调仓位。'

    if unheld_recommendations and len(next_actions) < 3:
        next_actions.append('可从未持仓推荐中挑选1到2只高质量标的分批观察或建仓')

    return {
        'overall_risk': overall_risk,
        'health_score': health_score,
        'diversification_status': diversification_status,
        'concentration_ratio_pct': round(top_ratio * 100.0, 2),
        'recommended_cash_ratio_pct': int(recommended_cash_ratio_pct),
        'key_issues': key_issues[:3],
        'next_actions': next_actions[:3],
        'summary_text': summary_text,
    }


def get_exchange_rate(from_currency='USD', to_currency='CNY'):
    """获取实时汇率"""
    cache_key = f"{from_currency}:{to_currency}"
    cached = exchange_rate_cache.get(cache_key)
    if cached is not None:
        return float(cached)

    try:
        pair = f"{from_currency}{to_currency}=X"
        ticker = yf.Ticker(pair)
        history = ticker.history(period='1d')
        if len(history) > 0:
            rate = history['Close'].iloc[-1]
            if not pd.isna(rate) and rate > 0:
                logger.info(f"实时汇率: 1 {from_currency} = {rate:.4f} {to_currency}")
                exchange_rate_cache.set(cache_key, float(rate))
                return float(rate)
    except Exception as e:
        logger.warning(f"获取汇率失败: {e}")
    
    # 回退汇率
    fallback = {'USD': 7.25, 'HKD': 0.93}
    logger.warning(f"使用默认汇率: 1 {from_currency} = {fallback.get(from_currency, 1)} {to_currency}")
    fallback_rate = float(fallback.get(from_currency, 1.0))
    exchange_rate_cache.set(cache_key, fallback_rate)
    return fallback_rate


def get_current_price(code, asset_type='stock'):
    """获取当前价格，支持股票、ETF、场外基金、黄金、白银。

    统一策略: 优先本地可信快照，再尝试实时/准实时来源，失败后返回 None。
    """
    rec_type = _infer_rec_type(asset_type, code)
    cache_key = f"{asset_type}:{code}"
    cached_price = price_cache.get(cache_key)
    if cached_price is not None:
        return float(cached_price)

    try:
        if asset_type == 'fund':
            local_nav = _get_latest_raw_fund_nav_price(code)
            if local_nav is not None:
                price = round(float(local_nav), 4)
                price_cache.set(cache_key, price)
                return price

        fallback = _get_latest_recommendation_price(code, rec_type)
        live_market_enabled = os.environ.get('ENABLE_LIVE_MARKET_FETCH', 'false').lower() == 'true'
        if not live_market_enabled and asset_type != 'fund':
            if fallback is not None:
                price = round(float(fallback), 2)
                price_cache.set(cache_key, price)
                return price
            logger.info(f"实时行情安全模式已启用，{code} 使用本地快照/成本价")
            return None

        # 黄金
        if asset_type == 'gold':
            ticker = yf.Ticker('GC=F')
            history = ticker.history(period='1d')
            if len(history) > 0:
                price_usd = history['Close'].iloc[-1]
                if not pd.isna(price_usd) and price_usd > 0:
                    usd_to_cny = get_exchange_rate('USD', 'CNY')
                    price_cny_per_gram = price_usd * usd_to_cny / 31.1035
                    logger.info(f"黄金价格: ${price_usd:.2f}/盎司 = ¥{price_cny_per_gram:.2f}/克")
                    price = round(price_cny_per_gram, 2)
                    price_cache.set(cache_key, price)
                    return price
            if fallback is not None:
                price = round(float(fallback), 2)
                price_cache.set(cache_key, price)
                return price

        # 白银
        if asset_type == 'silver':
            ticker = yf.Ticker('SI=F')
            history = ticker.history(period='1d')
            if len(history) > 0:
                price_usd = history['Close'].iloc[-1]
                if not pd.isna(price_usd) and price_usd > 0:
                    usd_to_cny = get_exchange_rate('USD', 'CNY')
                    price_cny_per_gram = price_usd * usd_to_cny / 31.1035
                    logger.info(f"白银价格: ${price_usd:.2f}/盎司 = ¥{price_cny_per_gram:.2f}/克")
                    price = round(price_cny_per_gram, 2)
                    price_cache.set(cache_key, price)
                    return price
            if fallback is not None:
                price = round(float(fallback), 2)
                price_cache.set(cache_key, price)
                return price

        # 股票/ETF：使用 yfinance
        if asset_type in ['stock', 'etf']:
            ticker = yf.Ticker(code)
            history = ticker.history(period='1d')
            if len(history) > 0:
                close_price = history['Close'].iloc[-1]
                if not pd.isna(close_price) and close_price > 0:
                    price = float(close_price)
                    price_cache.set(cache_key, price)
                    return price

            if code.endswith('.SH'):
                alt_code = code.replace('.SH', '.SS')
            elif code.endswith('.SZ'):
                alt_code = code
            else:
                alt_code = None
            if alt_code:
                try:
                    alt_history = yf.Ticker(alt_code).history(period='1d')
                    if len(alt_history) > 0:
                        alt_close = alt_history['Close'].iloc[-1]
                        if not pd.isna(alt_close) and alt_close > 0:
                            price = float(alt_close)
                            price_cache.set(cache_key, price)
                            return price
                except Exception:
                    pass

            if fallback is not None:
                price = float(fallback)
                price_cache.set(cache_key, price)
                return price

        # 场外基金：优先本地NAV，其次尝试实时净值接口（即使安全模式也允许）
        if asset_type == 'fund':
            live_nav = _fetch_live_fund_nav_price(code)
            if live_nav is not None:
                price = round(float(live_nav), 4)
                price_cache.set(cache_key, price)
                return price

            if fallback is not None:
                price = round(float(fallback), 4)
                price_cache.set(cache_key, price)
                return price

        logger.warning(f"无法获取 {code} 的实时价格，使用成本价")
        return None

    except Exception as e:
        logger.error(f"获取价格失败 {code}: {e}")
        return None


def build_future_signals_data(session=None):
    """构建未来信号数据，供API与调度任务复用。"""
    own_session = False
    if session is None:
        session = get_session()
        own_session = True

    try:
        today = get_today()
        horizons = [5, 20, 60]

        raw_holdings = session.query(Holding).all()
        merged_positions = {}
        for h in raw_holdings:
            asset_type = h.asset_type if hasattr(h, 'asset_type') and h.asset_type else 'stock'
            key = (asset_type, h.code)
            if key not in merged_positions:
                merged_positions[key] = {
                    'id': h.id,
                    'asset_type': asset_type,
                    'code': h.code,
                    'name': h.name,
                    'quantity': float(h.quantity or 0.0),
                    'cost_amount': float(h.cost_price or 0.0) * float(h.quantity or 0.0),
                }
            else:
                merged_positions[key]['quantity'] += float(h.quantity or 0.0)
                merged_positions[key]['cost_amount'] += float(h.cost_price or 0.0) * float(h.quantity or 0.0)

        holdings = []
        for pos in merged_positions.values():
            quantity = float(pos['quantity'] or 0.0)
            avg_cost = (float(pos['cost_amount']) / quantity) if quantity > 0 else 0.0
            pos['cost_price'] = avg_cost
            holdings.append(type('MergedHolding', (), pos)())

        holding_codes = {h.code for h in holdings}

        holding_signals = []
        risk_alerts = []
        action_suggestions = []

        for h in holdings:
            asset_type = h.asset_type if hasattr(h, 'asset_type') and h.asset_type else 'stock'
            rec_type = _infer_rec_type(asset_type, h.code)
            current_price = get_current_price(h.code, asset_type) or float(h.cost_price)

            rec = (
                session.query(Recommendation)
                .filter(Recommendation.code == h.code)
                .filter(Recommendation.type == rec_type)
                .order_by(Recommendation.date.desc())
                .first()
            )

            probs = {5: 50.0, 20: 50.0, 60: 50.0}
            if rec:
                probs = _extract_probabilities(rec)
            else:
                preds = (
                    session.query(Prediction)
                    .filter(Prediction.code == h.code)
                    .filter(Prediction.period_days.in_(horizons))
                    .order_by(Prediction.date.desc())
                    .all()
                )
                pred_map = {p.period_days: p for p in preds}
                for hd in horizons:
                    if hd in pred_map and pred_map[hd].up_probability is not None:
                        probs[hd] = max(5.0, min(95.0, float(pred_map[hd].up_probability)))

            horizon_view = {}
            for hd in horizons:
                up_prob = probs[hd]
                down_prob = round(100.0 - up_prob, 2)
                target_low, target_high = _target_prices(current_price, up_prob, hd, rec=rec)
                horizon_view[str(hd)] = {
                    'up_probability': round(up_prob, 2),
                    'down_probability': down_prob,
                    'target_low': round(target_low, 2),
                    'target_high': round(target_high, 2),
                }

                if down_prob >= 60:
                    level = 'high' if down_prob >= 70 else 'medium'
                    risk_alerts.append({
                        'code': h.code,
                        'name': h.name,
                        'horizon': hd,
                        'level': level,
                        'down_probability': down_prob,
                        'message': f"{h.name} {hd}日下跌概率 {down_prob}%",
                        'suggestion': '建议减仓/收紧止损' if level == 'high' else '建议降低仓位并密切跟踪'
                    })

            cost_price = float(h.cost_price)
            profit_rate = ((current_price - cost_price) / cost_price * 100.0) if cost_price > 0 else 0.0

            p20 = horizon_view['20']['up_probability']
            p60 = horizon_view['60']['up_probability']
            tgt20 = horizon_view['20']['target_high']

            if p60 >= 68 and profit_rate <= 8 and current_price <= cost_price * 1.03:
                action_suggestions.append({
                    'code': h.code,
                    'name': h.name,
                    'action': 'add',
                    'reason': f"20/60日上涨概率较高（{p20:.1f}%/{p60:.1f}%）",
                    'reference_price': round(current_price, 2),
                    'profit_rate': round(profit_rate, 2),
                })

            if (p20 >= 65 and current_price >= tgt20 * 0.98) or profit_rate >= 15:
                action_suggestions.append({
                    'code': h.code,
                    'name': h.name,
                    'action': 'take_profit',
                    'reason': f"接近20日目标价或累计收益较高（{profit_rate:.1f}%）",
                    'reference_price': round(max(tgt20, current_price), 2),
                    'profit_rate': round(profit_rate, 2),
                })

            market_value = float(current_price) * float(h.quantity or 0)
            holding_signals.append({
                'id': h.id,
                'asset_type': asset_type,
                'code': h.code,
                'name': h.name,
                'quantity': h.quantity,
                'cost_price': round(cost_price, 2),
                'current_price': round(current_price, 2),
                'market_value': round(market_value, 2),
                'profit_rate': round(profit_rate, 2),
                'horizons': horizon_view,
            })

        # 当前资产操作建议：优先小时级事件影响预警（清仓/减仓/增仓）
        current_asset_actions = []
        try:
            recent_cutoff = datetime.now() - timedelta(hours=24)
            warnings = (
                session.query(WarningModel)
                .filter(WarningModel.warning_type == 'event_impact_hourly')
                .filter(WarningModel.warning_time >= recent_cutoff)
                .order_by(WarningModel.warning_time.desc())
                .all()
            )

            latest_by_code = {}
            for w in warnings:
                if w.code not in holding_codes:
                    continue
                if w.code in latest_by_code:
                    continue
                latest_by_code[w.code] = w

            def _extract_action(trigger_value, suggestion_text):
                tv = str(trigger_value or '')
                if 'action=清仓' in tv:
                    return '清仓'
                if 'action=减仓' in tv:
                    return '减仓'
                if 'action=增仓' in tv:
                    return '增仓'
                s = str(suggestion_text or '')
                if '清仓' in s:
                    return '清仓'
                if '减仓' in s:
                    return '减仓'
                if '增仓' in s:
                    return '增仓'
                return '持有'

            signal_map = {x['code']: x for x in holding_signals}
            for h in holdings:
                w = latest_by_code.get(h.code)
                signal = signal_map.get(h.code, {})
                p20 = float(signal.get('horizons', {}).get('20', {}).get('up_probability', 50.0) or 50.0)
                p60 = float(signal.get('horizons', {}).get('60', {}).get('up_probability', 50.0) or 50.0)
                profit_rate = float(signal.get('profit_rate', 0.0) or 0.0)

                if w:
                    action = _extract_action(w.trigger_value, w.suggestion)
                    level = w.level
                    message = w.message
                    suggestion = w.suggestion
                    review_in_days = 1 if level == 'high' else 2
                else:
                    if p20 <= 42 or p60 <= 40 or profit_rate <= -8:
                        action = '清仓' if profit_rate <= -12 else '减仓'
                        level = 'high' if (p20 <= 38 or p60 <= 38 or profit_rate <= -12) else 'medium'
                        message = f"中期信号偏弱，20/60日上涨概率为{p20:.1f}%/{p60:.1f}%"
                        suggestion = '优先降低仓位并收紧止损，避免回撤继续扩大'
                        review_in_days = 1
                    elif p20 >= 62 and p60 >= 65 and profit_rate <= 12:
                        action = '增仓'
                        level = 'low'
                        message = f"20/60日趋势共振偏强，上涨概率为{p20:.1f}%/{p60:.1f}%"
                        suggestion = '可考虑小幅分批加仓，但避免一次性重仓'
                        review_in_days = 3
                    else:
                        action = '持有'
                        level = 'low' if p20 >= 52 else 'medium'
                        message = '暂无明显反转或加速信号，建议继续持有观察'
                        suggestion = '保持纪律，按计划复查趋势与风险'
                        review_in_days = 5

                current_asset_actions.append({
                    'code': h.code,
                    'name': h.name,
                    'asset_type': getattr(h, 'asset_type', 'stock') or 'stock',
                    'action': action,
                    'level': level,
                    'message': message,
                    'suggestion': suggestion,
                    'review_in_days': review_in_days,
                    'updated_at': w.warning_time.isoformat() if w and w.warning_time else None,
                })

            action_order = {'清仓': 4, '减仓': 3, '增仓': 2, '持有': 1}
            current_asset_actions.sort(
                key=lambda x: (
                    action_order.get(x['action'], 0),
                    1 if x.get('level') == 'high' else (0 if x.get('level') == 'medium' else -1)
                ),
                reverse=True,
            )
        except Exception as e:
            logger.warning(f"构建当前资产操作建议失败: {e}")

        # 未持仓推荐：优先使用最近一批可用推荐，而不是仅限当天
        latest_recommendation = session.query(Recommendation).order_by(Recommendation.date.desc()).first()
        recommendation_date = latest_recommendation.date if latest_recommendation else today
        candidates = (
            session.query(Recommendation)
            .filter(Recommendation.date == recommendation_date)
            .filter(~Recommendation.code.in_(holding_codes) if holding_codes else True)
            .order_by(Recommendation.total_score.desc())
            .limit(80)
            .all()
        )

        unheld = []
        for rec in candidates:
            up5 = float(rec.up_probability_5d or 50)
            up20 = float(rec.up_probability_20d or up5)
            up60 = float(rec.up_probability_60d or up20)
            score = up5 * 0.5 + up20 * 0.3 + up60 * 0.2
            if score < 58:
                continue
            _, tgt_high = _target_prices(float(rec.current_price or 0), up20, 20, rec=rec)
            unheld.append({
                'type': rec.type,
                'code': rec.code,
                'name': rec.name,
                'rank': rec.rank,
                'current_price': round(float(rec.current_price or 0), 2),
                'up_probability_5d': round(up5, 2),
                'up_probability_20d': round(up20, 2),
                'up_probability_60d': round(up60, 2),
                'target_price_20d': round(tgt_high, 2),
                'suggestion': '可关注分批建仓' if up20 >= 62 else '观察等待更优点位',
                'score': round(score, 2),
            })

        unheld = _select_diversified_unheld_recommendations(
            unheld,
            limit=20,
            holding_asset_types=[getattr(h, 'asset_type', 'stock') for h in holdings],
        )

        risk_alerts.sort(key=lambda x: (1 if x['level'] == 'high' else 0, x['down_probability']), reverse=True)
        portfolio_advice = _build_portfolio_health_summary(
            holding_signals=holding_signals,
            current_asset_actions=current_asset_actions,
            risk_alerts=risk_alerts,
            action_suggestions=action_suggestions,
            unheld_recommendations=unheld,
        )

        return {
            'as_of': today.isoformat(),
            'holding_signals': holding_signals,
            'current_asset_actions': current_asset_actions,
            'risk_alerts': risk_alerts,
            'action_suggestions': action_suggestions,
            'unheld_recommendations': unheld,
            'portfolio_advice': portfolio_advice,
            'recommendation_as_of': recommendation_date.isoformat() if recommendation_date else None,
            'summary': {
                'holding_count': len(holding_signals),
                'current_action_count': len(current_asset_actions),
                'risk_alert_count': len(risk_alerts),
                'action_count': len(action_suggestions),
                'unheld_count': len(unheld),
            }
        }
    finally:
        if own_session:
            session.close()


def register_holdings_routes(app):
    """注册持仓相关路由"""

    @app.route('/api/holdings/lookup', methods=['GET'])
    def lookup_holding_asset():
        """根据输入代码自动识别资产类型与名称。"""
        try:
            code = request.args.get('code', '').strip()
            if not code:
                return jsonify({
                    'code': 400,
                    'status': 'error',
                    'message': '缺少代码参数',
                    'timestamp': datetime.now().isoformat()
                }), 400

            session = get_session()
            result = _lookup_asset_identity(code, session=session)
            session.close()

            return jsonify({
                'code': 200,
                'status': 'success',
                'data': result,
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"自动识别持仓代码失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/holdings', methods=['GET'])
    def get_holdings():
        """获取持仓列表"""
        try:
            session = get_session()
            holdings = session.query(Holding).all()
            
            total_value_cny = 0
            holdings_list = []
            
            for h in holdings:
                asset_type = h.asset_type if hasattr(h, 'asset_type') and h.asset_type else 'stock'
                
                # 获取当前价格（已经转换为人民币/单位）
                current_price = get_current_price(h.code, asset_type)
                if current_price is None:
                    current_price = h.cost_price
                
                # 成本价（原始录入值，已经是人民币/单位）
                cost_price = h.cost_price
                
                # 市值和盈亏计算
                market_value_cny = h.quantity * current_price
                cost_value_cny = h.quantity * cost_price
                profit_cny = market_value_cny - cost_value_cny
                profit_rate_cny = (profit_cny / cost_value_cny) * 100 if cost_value_cny > 0 else 0
                
                total_value_cny += market_value_cny
                
                holdings_list.append({
                    'id': h.id,
                    'asset_type': asset_type,
                    'code': h.code,
                    'name': h.name,
                    'quantity': h.quantity,
                    'cost_price_cny': round(cost_price, 2),
                    'current_price_cny': round(current_price, 2),
                    'market_value_cny': round(market_value_cny, 2),
                    'profit_cny': round(profit_cny, 2),
                    'profit_rate_cny': round(profit_rate_cny, 2),
                    'buy_date': h.buy_date.isoformat() if h.buy_date else None,
                    'notes': h.notes
                })
            
            session.close()
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': {
                    'total_value_cny': round(total_value_cny, 2),
                    'holdings': holdings_list
                },
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"获取持仓失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/holdings/<int:id>', methods=['GET'])
    def get_holding(id):
        """获取单个持仓"""
        try:
            session = get_session()
            holding = session.query(Holding).filter(Holding.id == id).first()
            
            if not holding:
                return jsonify({
                    'code': 404,
                    'status': 'error',
                    'message': '持仓不存在',
                    'timestamp': datetime.now().isoformat()
                }), 404
            
            result = {
                'id': holding.id,
                'asset_type': holding.asset_type if hasattr(holding, 'asset_type') else 'stock',
                'code': holding.code,
                'name': holding.name,
                'quantity': holding.quantity,
                'cost_price': round(holding.cost_price, 2),
                'buy_date': holding.buy_date.isoformat() if holding.buy_date else None,
                'notes': holding.notes
            }
            
            session.close()
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': result,
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"获取持仓失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/holdings', methods=['POST'])
    def add_holding():
        """添加持仓"""
        try:
            data = request.get_json()
            
            if not data:
                return jsonify({
                    'code': 400,
                    'status': 'error',
                    'message': '请求体为空',
                    'timestamp': datetime.now().isoformat()
                }), 400
            
            required_fields = ['code', 'quantity', 'cost_price']
            for field in required_fields:
                if field not in data:
                    return jsonify({
                        'code': 400,
                        'status': 'error',
                        'message': f'缺少必填字段: {field}',
                        'timestamp': datetime.now().isoformat()
                    }), 400
            
            session = get_session()
            lookup_meta = _lookup_asset_identity(data.get('code', ''), session=session)
            asset_type = data.get('asset_type') or lookup_meta.get('asset_type') or 'stock'
            name = str(data.get('name') or lookup_meta.get('name') or '').strip()
            if not name:
                session.close()
                return jsonify({
                    'code': 400,
                    'status': 'error',
                    'message': '无法自动识别资产名称，请手动填写名称',
                    'timestamp': datetime.now().isoformat()
                }), 400
            
            # 处理日期
            buy_date_str = data.get('buy_date', get_today().isoformat())
            try:
                from datetime import datetime as dt
                buy_date = dt.strptime(buy_date_str, '%Y-%m-%d').date()
            except:
                buy_date = get_today()
            
            holding = Holding(
                asset_type=asset_type,
                code=lookup_meta.get('code') or data['code'],
                name=name,
                quantity=float(data['quantity']),
                cost_price=float(data['cost_price']),
                buy_date=buy_date,
                notes=data.get('notes', '')
            )
            
            session.add(holding)
            session.commit()
            holding_id = holding.id
            session.close()
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': {'id': holding_id},
                'message': '添加成功',
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"添加持仓失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/holdings/<int:id>', methods=['PUT'])
    def update_holding(id):
        """更新持仓"""
        try:
            data = request.get_json()
            session = get_session()
            
            holding = session.query(Holding).filter(Holding.id == id).first()
            if not holding:
                session.close()
                return jsonify({
                    'code': 404,
                    'status': 'error',
                    'message': '持仓不存在',
                    'timestamp': datetime.now().isoformat()
                }), 404
            
            if 'code' in data:
                lookup_meta = _lookup_asset_identity(data['code'], session=session)
                holding.code = lookup_meta.get('code') or data['code']
                if ('asset_type' not in data or not data.get('asset_type')) and lookup_meta.get('asset_type'):
                    holding.asset_type = lookup_meta['asset_type']
                if ('name' not in data or not str(data.get('name') or '').strip()) and lookup_meta.get('name'):
                    holding.name = lookup_meta['name']
            if 'asset_type' in data and data.get('asset_type'):
                holding.asset_type = data['asset_type']
            if 'name' in data and str(data.get('name') or '').strip():
                holding.name = data['name']
            if 'quantity' in data:
                holding.quantity = float(data['quantity'])
            if 'cost_price' in data:
                holding.cost_price = float(data['cost_price'])
            if 'buy_date' in data:
                from datetime import datetime as dt
                try:
                    holding.buy_date = dt.strptime(data['buy_date'], '%Y-%m-%d').date()
                except:
                    pass
            if 'notes' in data:
                holding.notes = data['notes']
            
            session.commit()
            session.close()
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'message': '更新成功',
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"更新持仓失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/holdings/<int:id>', methods=['DELETE'])
    def delete_holding(id):
        """删除持仓"""
        try:
            session = get_session()
            holding = session.query(Holding).filter(Holding.id == id).first()
            
            if not holding:
                session.close()
                return jsonify({
                    'code': 404,
                    'status': 'error',
                    'message': '持仓不存在',
                    'timestamp': datetime.now().isoformat()
                }), 404
            
            session.delete(holding)
            session.commit()
            session.close()
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'message': '删除成功',
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"删除持仓失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/holdings/trend', methods=['GET'])
    def get_holdings_trend():
        """获取持仓市值趋势（基于历史快照）"""
        try:
            from models import HoldingSnapshot
            
            session = get_session()
            
            snapshots = session.query(HoldingSnapshot).order_by(
                HoldingSnapshot.snapshot_date.asc()
            ).all()
            
            if not snapshots:
                session.close()
                return jsonify({
                    'code': 200,
                    'status': 'success',
                    'data': {
                        'dates': [],
                        'values': []
                    },
                    'timestamp': datetime.now().isoformat()
                })
            
            daily_values = {}
            for s in snapshots:
                date_str = s.snapshot_date.strftime('%Y-%m-%d')
                daily_values[date_str] = daily_values.get(date_str, 0) + s.market_value
            
            dates = sorted(daily_values.keys())
            # 保留两位小数
            values = [round(daily_values[d], 2) for d in dates]
            
            session.close()
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': {
                    'dates': dates,
                    'values': values
                },
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"获取持仓趋势失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/holdings/asset_type_distribution', methods=['GET'])
    def get_asset_type_distribution():
        """获取资产类型分布（人民币）"""
        try:
            session = get_session()
            holdings = session.query(Holding).all()
            
            distribution = {}
            for h in holdings:
                asset_type = h.asset_type if hasattr(h, 'asset_type') and h.asset_type else 'stock'
                asset_type_name = {
                    'stock': '股票',
                    'fund': '基金',
                    'etf': 'ETF',
                    'gold': '黄金',
                    'silver': '白银'
                }.get(asset_type, asset_type)
                
                current_price = get_current_price(h.code, asset_type)
                if current_price is None:
                    current_price = h.cost_price

                # get_current_price 已统一返回人民币单价，避免在分布统计中重复换汇。
                value_cny = h.quantity * current_price
                
                distribution[asset_type_name] = distribution.get(asset_type_name, 0) + value_cny
            
            session.close()
            
            data = [{'name': k, 'value': round(v, 2)} for k, v in distribution.items()]
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': data,
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"获取资产类型分布失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/holdings/export', methods=['GET'])
    def export_holdings():
        """导出持仓为CSV"""
        try:
            session = get_session()
            holdings = session.query(Holding).all()
            
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(['资产类型', '代码', '名称', '数量', '成本价', '买入日期', '备注'])
            
            for h in holdings:
                asset_type = h.asset_type if hasattr(h, 'asset_type') else 'stock'
                writer.writerow([
                    asset_type, h.code, h.name, h.quantity, h.cost_price,
                    h.buy_date.isoformat() if h.buy_date else '', h.notes or ''
                ])
            
            session.close()
            
            output.seek(0)
            return send_file(
                io.BytesIO(output.getvalue().encode('utf-8-sig')),
                mimetype='text/csv',
                as_attachment=True,
                download_name=f'holdings_{datetime.now().strftime("%Y%m%d")}.csv'
            )
            
        except Exception as e:
            logger.error(f"导出持仓失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500
    
    @app.route('/api/holdings/import', methods=['POST'])
    def import_holdings():
        """导入CSV持仓"""
        try:
            if 'file' not in request.files:
                return jsonify({
                    'code': 400,
                    'status': 'error',
                    'message': '未上传文件',
                    'timestamp': datetime.now().isoformat()
                }), 400
            
            file = request.files['file']
            if file.filename == '':
                return jsonify({
                    'code': 400,
                    'status': 'error',
                    'message': '文件名为空',
                    'timestamp': datetime.now().isoformat()
                }), 400
            
            content = file.read().decode('utf-8-sig')
            reader = csv.reader(io.StringIO(content))
            next(reader)
            
            session = get_session()
            count = 0
            
            for row in reader:
                if len(row) >= 5:
                    buy_date_str = row[5] if len(row) > 5 and row[5] else get_today().isoformat()
                    try:
                        from datetime import datetime as dt
                        buy_date = dt.strptime(buy_date_str, '%Y-%m-%d').date()
                    except:
                        buy_date = get_today()
                    
                    holding = Holding(
                        asset_type=row[0] if len(row) > 0 else 'stock',
                        code=row[1] if len(row) > 1 else '',
                        name=row[2] if len(row) > 2 else '',
                        quantity=float(row[3]) if len(row) > 3 else 0,
                        cost_price=float(row[4]) if len(row) > 4 else 0,
                        buy_date=buy_date,
                        notes=row[6] if len(row) > 6 else ''
                    )
                    session.add(holding)
                    count += 1
            
            session.commit()
            session.close()
            
            return jsonify({
                'code': 200,
                'status': 'success',
                'data': {'imported_count': count},
                'message': f'成功导入 {count} 条记录',
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"导入持仓失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500

    @app.route('/api/holdings/future-signals', methods=['GET'])
    def get_future_signals():
        """未来5/20/60日信号：持仓预警、目标价建议、未持仓推荐。"""
        try:
            signals = build_future_signals_data()

            return jsonify({
                'code': 200,
                'status': 'success',
                'data': signals,
                'timestamp': datetime.now().isoformat()
            })

        except Exception as e:
            logger.error(f"获取未来信号失败: {e}")
            return jsonify({
                'code': 500,
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }), 500