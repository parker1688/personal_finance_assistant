"""
回填历史推荐快照（用于概率校准样本）

目标：为 active_fund / etf / gold / silver 生成过去 N 天的每日推荐记录，
包含 current_price、total_score、up_probability_5d/20d/60d。

用法：
  source .venv/bin/activate
  python scripts/backfill_recommendation_history.py --days 180
"""

import argparse
from datetime import date, datetime, timedelta
from math import tanh
import os
import sys

import pandas as pd
import tushare as ts
import yfinance as yf

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from config import TUSHARE_TOKEN
from models import get_session, Recommendation
from recommendation_probability import build_empirical_calibrators, derive_probabilities
from recommenders.etf_recommender import ETFRecommender
from recommenders.fund_recommender import FundRecommender
from recommenders.gold_recommender import GoldRecommender
from utils import get_logger

logger = get_logger(__name__)


def _clip_score(v):
    return max(1.0, min(5.0, float(v)))


def _calc_score_from_returns(r5, r20, r60):
    """把历史动量映射到 1-5 分，避免全3分。"""
    x5 = 0.0 if pd.isna(r5) else float(r5)
    x20 = 0.0 if pd.isna(r20) else float(r20)
    x60 = 0.0 if pd.isna(r60) else float(r60)

    score = 3.0
    score += 1.1 * tanh(x20 * 4.0)
    score += 0.7 * tanh(x5 * 6.0)
    score += 0.5 * tanh(x60 * 3.0)
    return round(_clip_score(score), 2)


def _volatility_level(ret20_std):
    if pd.isna(ret20_std):
        return "medium"
    if ret20_std < 0.012:
        return "low"
    if ret20_std > 0.03:
        return "high"
    return "medium"


def _load_fund_history(codes, start_date, end_date):
    ts.set_token(TUSHARE_TOKEN)
    pro = ts.pro_api()
    out = {}

    for code in codes:
        try:
            df = pro.fund_nav(
                ts_code=code,
                start_date=start_date.strftime("%Y%m%d"),
                end_date=end_date.strftime("%Y%m%d"),
                fields="ts_code,nav_date,accum_nav,unit_nav",
            )
            if df is None or df.empty:
                continue
            df["date"] = pd.to_datetime(df["nav_date"], format="%Y%m%d").dt.date
            # 累计净值优先，缺失时回退单位净值
            px = df["accum_nav"].fillna(df["unit_nav"]).astype(float)
            sdf = pd.DataFrame({"date": df["date"], "price": px}).dropna()
            sdf = sdf.sort_values("date").drop_duplicates(subset=["date"], keep="last")
            out[code] = sdf
        except Exception as e:
            logger.warning(f"基金历史读取失败 {code}: {e}")

    return out


def _load_yf_history(code_map, start_date, end_date):
    out = {}
    for code, ticker in code_map.items():
        try:
            df = yf.Ticker(ticker).history(start=start_date.isoformat(), end=(end_date + timedelta(days=1)).isoformat())
            if df is None or df.empty:
                continue
            sdf = pd.DataFrame({
                "date": pd.to_datetime(df.index).date,
                "price": df["Close"].astype(float),
            }).dropna()
            sdf = sdf.sort_values("date").drop_duplicates(subset=["date"], keep="last")
            out[code] = sdf
        except Exception as e:
            logger.warning(f"行情历史读取失败 {code}/{ticker}: {e}")
    return out


def _to_yf_ticker(code):
    c = str(code)
    if c.endswith('.SH'):
        return c.replace('.SH', '.SS')
    return c


def _build_daily_recs(rec_type, code_name_map, series_map, calibrators=None):
    """把单资产历史价格序列转成每日推荐记录（不含 rank）。"""
    rows = []
    for code, sdf in series_map.items():
        if sdf.empty:
            continue
        s = sdf.copy()
        s["ret_1d"] = s["price"].pct_change(1)
        s["ret_5d"] = s["price"].pct_change(5)
        s["ret_20d"] = s["price"].pct_change(20)
        s["ret_60d"] = s["price"].pct_change(60)
        s["vol20"] = s["ret_1d"].rolling(20).std()

        for _, r in s.iterrows():
            p = float(r["price"])
            if p <= 0:
                continue
            score = _calc_score_from_returns(r.get("ret_5d"), r.get("ret_20d"), r.get("ret_60d"))
            vol = _volatility_level(r.get("vol20"))

            rec = {
                "code": code,
                "name": code_name_map.get(code, code),
                "score": score,
                "current_price": round(p, 4),
                "volatility_level": vol,
                # 让概率引擎能用到动量修正
                "return_5d": float(r.get("ret_5d", 0) or 0) * 100,
                "return_20d": float(r.get("ret_20d", 0) or 0) * 100,
                "ret_1m": float(r.get("ret_20d", 0) or 0) * 100,
                "ret_3m": float(r.get("ret_60d", 0) or 0) * 100,
            }
            up5, up20, up60 = derive_probabilities(rec, rec_type, calibrators=calibrators)
            rec["up_probability_5d"] = up5
            rec["up_probability_20d"] = up20
            rec["up_probability_60d"] = up60
            rows.append((r["date"], rec_type, rec))
    return rows


def backfill(days):
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=days)

    logger.info(f"开始回填推荐历史: {start_date} ~ {end_date}")

    # 1) 构建资产池
    session = get_session()

    # active_fund 优先取当前推荐里的基金池
    fr = FundRecommender()
    fund_now = fr.get_recommendations(20)
    fund_codes = [x["code"] for x in fund_now]
    fund_names = {x["code"]: x.get("name", x["code"]) for x in fund_now}

    # etf 池
    er = ETFRecommender()
    etf_now = er.get_recommendations(20)
    etf_codes = [x["code"] for x in etf_now]
    etf_names = {x["code"]: x.get("name", x["code"]) for x in etf_now}
    etf_code_to_yf = {e["code"]: e.get("yf", e["code"]) for e in er.etf_pool if e["code"] in set(etf_codes)}

    # gold/silver 池
    gr = GoldRecommender()
    gold_now = gr.get_gold_recommendations()
    silver_now = gr.get_silver_recommendations()
    gold_codes = [x["code"] for x in gold_now]
    silver_codes = [x["code"] for x in silver_now]
    gold_names = {x["code"]: x.get("name", x["code"]) for x in gold_now}
    silver_names = {x["code"]: x.get("name", x["code"]) for x in silver_now}

    calibrator_types = ['a_stock', 'hk_stock', 'us_stock', 'active_fund', 'etf', 'gold', 'silver']
    calibrators = build_empirical_calibrators(
        session=session,
        recommendation_model=Recommendation,
        today=end_date + timedelta(days=1),
        rec_types=calibrator_types,
        lookback_days=max(240, days),
    )

    # 2) 读取历史价格
    fund_hist = _load_fund_history(fund_codes, start_date - timedelta(days=80), end_date)
    etf_hist = _load_yf_history(etf_code_to_yf, start_date - timedelta(days=80), end_date)
    gold_hist = _load_yf_history({c: c for c in gold_codes}, start_date - timedelta(days=80), end_date)
    silver_hist = _load_yf_history({c: c for c in silver_codes}, start_date - timedelta(days=80), end_date)

    # 股票资产池（从已有推荐中提取代码，避免全市场超大开销）
    stock_types = ['a_stock', 'hk_stock', 'us_stock']
    stock_code_name = {t: {} for t in stock_types}
    for t in stock_types:
        latest_rows = (
            session.query(Recommendation.code, Recommendation.name)
            .filter(Recommendation.type == t)
            .order_by(Recommendation.date.desc())
            .limit(40)
            .all()
        )
        for c, n in latest_rows:
            stock_code_name[t][str(c)] = n or str(c)

    a_hist = _load_yf_history({c: _to_yf_ticker(c) for c in stock_code_name['a_stock']}, start_date - timedelta(days=80), end_date)
    hk_hist = _load_yf_history({c: _to_yf_ticker(c) for c in stock_code_name['hk_stock']}, start_date - timedelta(days=80), end_date)
    us_hist = _load_yf_history({c: _to_yf_ticker(c) for c in stock_code_name['us_stock']}, start_date - timedelta(days=80), end_date)

    # 3) 生成回填记录
    rows = []
    rows += _build_daily_recs("active_fund", fund_names, fund_hist, calibrators=calibrators)
    rows += _build_daily_recs("etf", etf_names, etf_hist, calibrators=calibrators)
    rows += _build_daily_recs("gold", gold_names, gold_hist, calibrators=calibrators)
    rows += _build_daily_recs("silver", silver_names, silver_hist, calibrators=calibrators)
    rows += _build_daily_recs("a_stock", stock_code_name['a_stock'], a_hist, calibrators=calibrators)
    rows += _build_daily_recs("hk_stock", stock_code_name['hk_stock'], hk_hist, calibrators=calibrators)
    rows += _build_daily_recs("us_stock", stock_code_name['us_stock'], us_hist, calibrators=calibrators)

    # 只保留目标回填区间
    rows = [x for x in rows if start_date <= x[0] <= end_date]

    # 4) 按 date/type 分组打 rank
    grouped = {}
    for d, t, rec in rows:
        grouped.setdefault((d, t), []).append(rec)

    inserted = 0
    updated = 0

    for (d, t), recs in grouped.items():
        recs.sort(key=lambda x: x.get("score", 0), reverse=True)
        for i, rec in enumerate(recs, start=1):
            exist = (
                session.query(Recommendation)
                .filter(Recommendation.date == d)
                .filter(Recommendation.type == t)
                .filter(Recommendation.code == rec["code"])
                .first()
            )
            if exist:
                exist.rank = i
                exist.name = rec["name"]
                exist.total_score = rec["score"]
                exist.current_price = rec["current_price"]
                exist.up_probability_5d = rec["up_probability_5d"]
                exist.up_probability_20d = rec["up_probability_20d"]
                exist.up_probability_60d = rec["up_probability_60d"]
                exist.volatility_level = rec["volatility_level"]
                exist.reason_summary = "历史回填样本"
                updated += 1
            else:
                session.add(
                    Recommendation(
                        date=d,
                        code=rec["code"],
                        name=rec["name"],
                        type=t,
                        rank=i,
                        total_score=rec["score"],
                        current_price=rec["current_price"],
                        up_probability_5d=rec["up_probability_5d"],
                        up_probability_20d=rec["up_probability_20d"],
                        up_probability_60d=rec["up_probability_60d"],
                        volatility_level=rec["volatility_level"],
                        reason_summary="历史回填样本",
                    )
                )
                inserted += 1

    session.commit()
    session.close()
    logger.info(f"回填完成: inserted={inserted}, updated={updated}, groups={len(grouped)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=180, help="回填天数")
    args = parser.parse_args()
    backfill(args.days)
