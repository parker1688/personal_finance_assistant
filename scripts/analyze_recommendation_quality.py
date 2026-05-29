# -*- coding: utf-8 -*-
"""
推荐资产质量分析脚本

分析目标：
1. 统计 2025-11-05 到 2026-05-06 期间推荐标的的真实价格表现
2. 对比"推荐标的"与"同类型未推荐标的"的涨跌差异
3. 推荐准确率：推荐日后 5/20 个交易日的实际涨跌方向
4. 月度拆解：每月推荐质量趋势
5. 模拟交易员亏损交易的推荐来源分析
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date, timedelta
from collections import defaultdict
import statistics

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from models import (
    Recommendation, DailyPrice, SimulatedTrade, SimulatedDecisionLog,
    SimulatedDailyPnl
)

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       'data', 'database', 'finance.db')
engine = create_engine(f'sqlite:///{DB_PATH}', echo=False)
Session = sessionmaker(bind=engine)

START_DATE = date(2025, 11, 5)
END_DATE   = date(2026, 5, 6)
TRADER_ID  = 'default'

# ─────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────

def get_price(session, code: str, d: date):
    row = session.query(DailyPrice).filter_by(code=code, date=d).first()
    if row:
        return row.close
    # 往前找最近 5 天
    for delta in range(1, 6):
        row = session.query(DailyPrice).filter_by(
            code=code, date=d - timedelta(days=delta)).first()
        if row:
            return row.close
    return None

def get_price_after(session, code: str, from_date: date, n_trading_days: int):
    """从 from_date 起往后取第 n_trading_days 个有价格的交易日收盘价"""
    rows = (session.query(DailyPrice)
            .filter(DailyPrice.code == code,
                    DailyPrice.date > from_date,
                    DailyPrice.date <= from_date + timedelta(days=n_trading_days * 3))
            .order_by(DailyPrice.date)
            .limit(n_trading_days + 5)
            .all())
    if len(rows) >= n_trading_days:
        return rows[n_trading_days - 1].close
    if rows:
        return rows[-1].close
    return None

def pct(v_new, v_old):
    if v_old and v_old != 0:
        return (v_new - v_old) / v_old * 100
    return None

# ─────────────────────────────────────────────
# Section 1: 推荐数量概况
# ─────────────────────────────────────────────

def analyze_recommendation_overview(session):
    print("\n" + "="*60)
    print("【Section 1】 推荐概况")
    print("="*60)
    recs = (session.query(Recommendation)
            .filter(Recommendation.date >= START_DATE,
                    Recommendation.date <= END_DATE)
            .order_by(Recommendation.date)
            .all())
    if not recs:
        print("  [!] 推荐表中无数据（分析窗口内）")
        return []

    by_type = defaultdict(int)
    by_month = defaultdict(int)
    dates_set = set()
    for r in recs:
        by_type[r.type] += 1
        by_month[r.date.strftime('%Y-%m')] += 1
        dates_set.add(r.date)

    print(f"  总推荐条目: {len(recs)}")
    print(f"  推荐日期数: {len(dates_set)}")
    print(f"  按资产类型:")
    for t, n in sorted(by_type.items(), key=lambda x: -x[1]):
        print(f"    {t:<20} {n} 条")
    print(f"  按月分布:")
    for m in sorted(by_month.keys()):
        print(f"    {m}: {by_month[m]} 条")
    return recs

# ─────────────────────────────────────────────
# Section 2: 推荐准确率（推荐日后 5/20 交易日实际涨跌）
# ─────────────────────────────────────────────

def analyze_recommendation_accuracy(session, recs):
    print("\n" + "="*60)
    print("【Section 2】 推荐准确率（推荐后 5/20 交易日实际涨跌）")
    print("="*60)

    results_5d  = []   # (rec_date, code, type, score, actual_ret_5d)
    results_20d = []

    for r in recs:
        p0 = r.current_price or get_price(session, r.code, r.date)
        if not p0:
            continue

        p5  = get_price_after(session, r.code, r.date, 5)
        p20 = get_price_after(session, r.code, r.date, 20)

        if p5:
            results_5d.append({
                'date': r.date, 'code': r.code, 'name': r.name,
                'type': r.type, 'rank': r.rank, 'score': r.total_score,
                'up_prob': r.up_probability_5d,
                'p0': p0, 'p5': p5, 'ret': pct(p5, p0)
            })
        if p20:
            results_20d.append({
                'date': r.date, 'code': r.code, 'name': r.name,
                'type': r.type, 'rank': r.rank, 'score': r.total_score,
                'up_prob': r.up_probability_20d,
                'p0': p0, 'p20': p20, 'ret': pct(p20, p0)
            })

    def summarize(rows, label):
        if not rows:
            print(f"  [{label}] 无足够价格数据")
            return
        rets = [x['ret'] for x in rows if x['ret'] is not None]
        up = sum(1 for r in rets if r > 0)
        dn = sum(1 for r in rets if r <= 0)
        total = len(rets)
        avg_ret = statistics.mean(rets) if rets else 0
        median_ret = statistics.median(rets) if rets else 0
        hit_rate = up / total * 100 if total else 0
        print(f"  [{label}] 样本={total}, 上涨={up}, 下跌={dn}")
        print(f"         命中率(上涨)= {hit_rate:.1f}%")
        print(f"         平均收益= {avg_ret:.2f}%,  中位数= {median_ret:.2f}%")

        # 按资产类型分组
        by_type = defaultdict(list)
        for x in rows:
            if x['ret'] is not None:
                by_type[x['type']].append(x['ret'])
        print(f"         按类型细分:")
        for t, rv in sorted(by_type.items()):
            h = sum(1 for r in rv if r > 0) / len(rv) * 100
            print(f"           {t:<20} n={len(rv):<4} avg={statistics.mean(rv):+.2f}%  hit={h:.0f}%")

    summarize(results_5d,  "5日")
    summarize(results_20d, "20日")

    return results_5d, results_20d

# ─────────────────────────────────────────────
# Section 3: 推荐 vs 全量同类资产对比
# ─────────────────────────────────────────────

def analyze_recommended_vs_universe(session, recs):
    print("\n" + "="*60)
    print("【Section 3】 推荐标的 vs 同类全量资产表现对比 (20日)")
    print("="*60)

    # 只对数据比较充分的类型做分析
    FOCUS_TYPES = {'a_stock', 'etf', 'active_fund'}

    # 取推荐过的 code 集
    rec_codes_by_type = defaultdict(set)
    for r in recs:
        if r.type in FOCUS_TYPES:
            rec_codes_by_type[r.type].add(r.code)

    # 对每个推荐日期，计算同类全量资产 20 日涨跌
    # 全量 = daily_prices 中在该日期有价格记录的所有 code
    # 为节省时间，随机抽样最多 300 个非推荐 code
    import random
    random.seed(42)

    for asset_type, rec_codes in sorted(rec_codes_by_type.items()):
        # 收集推荐标的 20 日收益
        rec_rets = []
        # 收集同类未推荐标的 20 日收益（抽样）
        non_rec_rets = []

        # 取该类型推荐日期列表（去重）
        type_recs = [r for r in recs if r.type == asset_type]
        dates = list(set(r.date for r in type_recs))

        for rec_date in sorted(dates):
            # 该日期所有有价格的 code
            all_prices = (session.query(DailyPrice.code)
                          .filter(DailyPrice.date == rec_date)
                          .all())
            all_codes = [row[0] for row in all_prices]
            non_rec_codes = [c for c in all_codes if c not in rec_codes]
            sample_non_rec = random.sample(non_rec_codes, min(20, len(non_rec_codes)))

            # 计算推荐 code 该日收益
            date_rec_codes = [r.code for r in type_recs if r.date == rec_date]
            for code in date_rec_codes:
                p0 = get_price(session, code, rec_date)
                p20 = get_price_after(session, code, rec_date, 20)
                if p0 and p20:
                    rec_rets.append(pct(p20, p0))

            # 非推荐标的
            for code in sample_non_rec:
                p0 = get_price(session, code, rec_date)
                p20 = get_price_after(session, code, rec_date, 20)
                if p0 and p20:
                    non_rec_rets.append(pct(p20, p0))

        def fmt(rets, label):
            if not rets:
                return f"{label}: 无数据"
            avg = statistics.mean(rets)
            hit = sum(1 for r in rets if r > 0) / len(rets) * 100
            return f"{label}: n={len(rets):4d}  avg={avg:+.2f}%  hit={hit:.0f}%"

        print(f"\n  [{asset_type}]")
        print(f"    推荐标的:   {fmt(rec_rets,     '推荐')}")
        print(f"    未推荐标的: {fmt(non_rec_rets, '全量')}")
        if rec_rets and non_rec_rets:
            delta = statistics.mean(rec_rets) - statistics.mean(non_rec_rets)
            verdict = "[有效]" if delta > 0 else "[拖后腿]"
            print(f"    推荐超额:   {delta:+.2f}% {verdict}")

# ─────────────────────────────────────────────
# Section 4: 月度推荐质量趋势
# ─────────────────────────────────────────────

def analyze_monthly_rec_quality(session, results_20d):
    print("\n" + "="*60)
    print("【Section 4】 月度推荐准确率趋势（20日维度）")
    print("="*60)

    by_month = defaultdict(list)
    for x in results_20d:
        if x['ret'] is not None:
            m = x['date'].strftime('%Y-%m')
            by_month[m].append(x['ret'])

    for m in sorted(by_month.keys()):
        rets = by_month[m]
        avg = statistics.mean(rets)
        hit = sum(1 for r in rets if r > 0) / len(rets) * 100
        print(f"  {m}: n={len(rets):3d}  avg={avg:+.2f}%  hit={hit:.0f}%")

# ─────────────────────────────────────────────
# Section 5: 模拟交易员亏损归因
# ─────────────────────────────────────────────

def analyze_trader_loss_attribution(session):
    print("\n" + "="*60)
    print("【Section 5】 模拟交易员亏损归因分析")
    print("="*60)

    trades = (session.query(SimulatedTrade)
              .filter(SimulatedTrade.trader_id == TRADER_ID,
                      SimulatedTrade.trade_date >= START_DATE,
                      SimulatedTrade.trade_date <= END_DATE,
                      SimulatedTrade.action == 'sell')
              .order_by(SimulatedTrade.trade_date)
              .all())

    if not trades:
        print("  [!] 无卖出交易记录")
        return

    total_pnl = sum(t.pnl or 0 for t in trades)
    win = [t for t in trades if (t.pnl or 0) > 0]
    loss = [t for t in trades if (t.pnl or 0) <= 0]

    print(f"  卖出交易总数: {len(trades)}")
    print(f"  盈利交易: {len(win)},  亏损交易: {len(loss)}")
    print(f"  整体胜率: {len(win)/len(trades)*100:.1f}%")
    print(f"  累计PnL: {total_pnl:+,.0f} 元")

    # 按触发原因分组
    print("\n  按触发原因（亏损交易）:")
    by_trigger = defaultdict(list)
    for t in loss:
        by_trigger[t.trigger].append(t.pnl or 0)
    for trg, pnls in sorted(by_trigger.items(), key=lambda x: sum(x[1])):
        print(f"    {trg:<20} n={len(pnls):<4} pnl={sum(pnls):+,.0f}")

    # 按资产类型分组
    print("\n  按资产类型（所有卖出交易）:")
    by_type = defaultdict(list)
    for t in trades:
        by_type[t.asset_type].append(t.pnl or 0)
    for tp, pnls in sorted(by_type.items(), key=lambda x: sum(x[1])):
        wins = sum(1 for p in pnls if p > 0)
        print(f"    {tp:<20} n={len(pnls):<4} pnl={sum(pnls):+,.0f}  wr={wins/len(pnls)*100:.0f}%")

    # Top 10 亏损交易
    top_loss = sorted(trades, key=lambda t: t.pnl or 0)[:10]
    print("\n  Top10 单笔亏损:")
    for t in top_loss:
        print(f"    {t.trade_date} {t.code:<15} {t.asset_type:<15} "
              f"pnl={t.pnl:+,.0f}  pct={t.pnl_pct:+.1f}%  trigger={t.trigger}")

    # 月度亏损分布
    print("\n  月度 PnL 汇总:")
    by_month = defaultdict(list)
    for t in trades:
        by_month[t.trade_date.strftime('%Y-%m')].append(t.pnl or 0)
    for m in sorted(by_month.keys()):
        ps = by_month[m]
        wins = sum(1 for p in ps if p > 0)
        print(f"    {m}: n={len(ps):<3} pnl={sum(ps):+,.0f}  wr={wins/len(ps)*100:.0f}%")

# ─────────────────────────────────────────────
# Section 6: 推荐信号 up_probability vs 实际结果
# ─────────────────────────────────────────────

def analyze_prob_calibration(session, results_20d):
    print("\n" + "="*60)
    print("【Section 6】 推荐上涨概率预测校准度（20日）")
    print("="*60)

    # 按概率桶分组
    buckets = [(0, 50), (50, 55), (55, 60), (60, 65), (65, 70), (70, 101)]
    rows = [x for x in results_20d if x['ret'] is not None and x.get('up_prob') is not None]

    if not rows:
        print("  [!] 无 up_probability_20d 数据")
        return

    print(f"  样本总数: {len(rows)}")
    for lo, hi in buckets:
        bucket_rows = [x for x in rows if lo <= (x['up_prob'] or 0) < hi]
        if not bucket_rows:
            continue
        rets = [x['ret'] for x in bucket_rows]
        actual_hit = sum(1 for r in rets if r > 0) / len(rets) * 100
        avg_ret = statistics.mean(rets)
        print(f"  prob [{lo:3d}, {hi:3d}): n={len(bucket_rows):<4} "
              f"actual_hit={actual_hit:.0f}%  avg_ret={avg_ret:+.2f}%")

# ─────────────────────────────────────────────
# Section 7: 决策日志——被拒绝的推荐标的后续表现
# ─────────────────────────────────────────────

def analyze_rejected_vs_accepted(session):
    print("\n" + "="*60)
    print("【Section 7】 被拒绝推荐标的后续表现（交易员错过了什么？）")
    print("="*60)

    logs = (session.query(SimulatedDecisionLog)
            .filter(SimulatedDecisionLog.trader_id == TRADER_ID,
                    SimulatedDecisionLog.signal_date >= START_DATE,
                    SimulatedDecisionLog.signal_date <= END_DATE)
            .all())

    if not logs:
        print("  [!] 无决策日志")
        return

    accepted = [l for l in logs if l.final_action == 'buy']
    rejected = [l for l in logs if l.decision_type == 'reject']

    print(f"  决策日志总条数: {len(logs)}")
    print(f"  买入决策: {len(accepted)},  拒绝决策: {len(rejected)}")

    def calc_20d_perf(rows, label):
        rets = []
        for l in rows:
            p0 = get_price(session, l.code, l.signal_date)
            p20 = get_price_after(session, l.code, l.signal_date, 20)
            if p0 and p20:
                rets.append(pct(p20, p0))
        if not rets:
            print(f"  [{label}] 无价格数据")
            return
        avg = statistics.mean(rets)
        hit = sum(1 for r in rets if r > 0) / len(rets) * 100
        print(f"  [{label}] n={len(rets)}  avg_20d={avg:+.2f}%  hit={hit:.0f}%")

    calc_20d_perf(accepted, "买入的推荐")
    calc_20d_perf(rejected, "拒绝的推荐")

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print(f"\n推荐资产质量分析  |  窗口: {START_DATE} ~ {END_DATE}")
    print(f"交易员: {TRADER_ID}")

    session = Session()
    try:
        recs = analyze_recommendation_overview(session)
        if recs:
            r5, r20 = analyze_recommendation_accuracy(session, recs)
            analyze_recommended_vs_universe(session, recs)
            analyze_monthly_rec_quality(session, r20)
        analyze_trader_loss_attribution(session)
        if recs:
            analyze_prob_calibration(session, r20)
        analyze_rejected_vs_accepted(session)
        print("\n" + "="*60)
        print("分析完成")
        print("="*60)
    finally:
        session.close()

if __name__ == '__main__':
    main()
