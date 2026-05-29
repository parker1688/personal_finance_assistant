# -*- coding: utf-8 -*-
"""
深度诊断模拟交易员亏损原因
分析每个流程节点，找出哪里出了问题

运行方式：
    .venv\Scripts\python.exe -W ignore scripts/deep_diagnose_trader.py 2>&1
"""

import sys
import os
from datetime import date, timedelta
from collections import defaultdict

sys.path.insert(0, '.')

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

DB_PATH = 'data/database/finance.db'
engine = create_engine(f'sqlite:///{DB_PATH}', echo=False)
Session = sessionmaker(bind=engine)


def hr(title=''):
    print('\n' + '='*70)
    if title:
        print(f'  {title}')
        print('='*70)


def section(title):
    print(f'\n--- {title} ---')


def run_sql(sql, params=None):
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        return result.fetchall()


# ============================================================
# 模块1：总体盈亏概览
# ============================================================
def analyze_overall_pnl():
    hr('模块1：总体盈亏概览')

    rows = run_sql("""
        SELECT pnl_date, total_value, cash, positions_value, daily_return, total_return, max_drawdown, position_count
        FROM simulated_daily_pnl
        WHERE trader_id='default'
        ORDER BY pnl_date ASC
    """)
    if not rows:
        print('  [!] 无 daily_pnl 记录')
        return

    print(f'  记录总天数: {len(rows)}')
    print(f'  期间: {rows[0][0]} ~ {rows[-1][0]}')

    first_val = rows[0][1]
    last_val = rows[-1][1]
    total_return = (last_val - first_val) / first_val * 100 if first_val else 0
    max_dd = max(r[6] for r in rows)

    print(f'\n  初始净值:   {first_val:>12,.0f}')
    print(f'  最终净值:   {last_val:>12,.0f}')
    print(f'  绝对盈亏:   {last_val - first_val:>+12,.0f}')
    print(f'  总收益率:   {total_return:>+.2f}%')
    print(f'  最大回撤:   {max_dd:.2f}%')

    # 月度拆解
    section('按月收益')
    monthly = {}
    for r in rows:
        ym = str(r[0])[:7]
        monthly.setdefault(ym, [])
        monthly[ym].append(r[4])  # daily_return

    for ym, returns in sorted(monthly.items()):
        valid = [x for x in returns if x is not None]
        month_ret = sum(valid)
        print(f'    {ym}  月累计日收益: {month_ret:>+7.2f}%  交易日: {len(valid)}')

    # 上涨/下跌天数
    up_days = sum(1 for r in rows if (r[4] or 0) > 0)
    down_days = sum(1 for r in rows if (r[4] or 0) < 0)
    flat_days = len(rows) - up_days - down_days
    print(f'\n  上涨天数: {up_days}  下跌天数: {down_days}  持平天数: {flat_days}')

    # 持仓数分布
    section('持仓数分布')
    pos_counts = defaultdict(int)
    for r in rows:
        cnt = r[7] or 0
        pos_counts[cnt] += 1
    for cnt in sorted(pos_counts.keys()):
        print(f'    持仓={cnt:2d} 股  出现{pos_counts[cnt]:3d}天')


# ============================================================
# 模块2：已实现交易记录分析
# ============================================================
def analyze_closed_trades():
    hr('模块2：已实现交易分析（实际成交的买卖记录）')

    buys = run_sql("""
        SELECT trade_date, code, name, asset_type, shares, price, amount, trigger, signal_score
        FROM simulated_trades
        WHERE trader_id='default' AND action='buy' AND price > 0
        ORDER BY trade_date ASC
    """)
    sells = run_sql("""
        SELECT trade_date, code, name, asset_type, shares, price, amount, pnl, pnl_pct, trigger
        FROM simulated_trades
        WHERE trader_id='default' AND action='sell' AND price > 0
        ORDER BY trade_date ASC
    """)

    print(f'  买入记录: {len(buys)} 笔')
    print(f'  卖出记录: {len(sells)} 笔')

    if not sells:
        print('  [!] 无卖出记录，所有持仓均未平仓（纯浮亏）')
        return

    pnls = [float(r[7]) for r in sells if r[7] is not None]
    pnl_pcts = [float(r[8]) for r in sells if r[8] is not None]

    total_realized = sum(pnls)
    win_trades = [(p, pct) for p, pct in zip(pnls, pnl_pcts) if p > 0]
    loss_trades = [(p, pct) for p, pct in zip(pnls, pnl_pcts) if p <= 0]
    win_rate = len(win_trades) / len(pnls) * 100 if pnls else 0

    print(f'\n  已实现总盈亏: {total_realized:>+,.0f} 元')
    print(f'  胜率: {win_rate:.1f}%  ({len(win_trades)}胜 / {len(loss_trades)}负)')
    if win_trades:
        avg_win = sum(p for p, _ in win_trades) / len(win_trades)
        avg_win_pct = sum(pct for _, pct in win_trades) / len(win_trades)
        print(f'  平均盈利: {avg_win:>+,.0f} 元 ({avg_win_pct:>+.2f}%)')
    if loss_trades:
        avg_loss = sum(p for p, _ in loss_trades) / len(loss_trades)
        avg_loss_pct = sum(pct for _, pct in loss_trades) / len(loss_trades)
        print(f'  平均亏损: {avg_loss:>+,.0f} 元 ({avg_loss_pct:>+.2f}%)')
        if win_trades:
            ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0
            print(f'  盈亏比(win/loss): {ratio:.2f}')

    # 触发类型分布
    section('卖出触发类型分布')
    trigger_stats = defaultdict(lambda: {'count': 0, 'pnl': 0.0})
    for r in sells:
        trigger = r[9] or 'unknown'
        trigger_stats[trigger]['count'] += 1
        trigger_stats[trigger]['pnl'] += float(r[7] or 0)
    for trigger, stats in sorted(trigger_stats.items()):
        print(f'    {trigger:20s}: {stats["count"]:3d} 笔  盈亏合计: {stats["pnl"]:>+,.0f}')

    # 按资产类型分析
    section('按资产类型卖出盈亏')
    type_stats = defaultdict(lambda: {'count': 0, 'pnl': 0.0})
    for r in sells:
        atype = r[3] or 'unknown'
        type_stats[atype]['count'] += 1
        type_stats[atype]['pnl'] += float(r[7] or 0)
    for atype, stats in sorted(type_stats.items()):
        avg = stats['pnl'] / stats['count'] if stats['count'] else 0
        print(f'    {atype:15s}: {stats["count"]:3d} 笔  盈亏合计: {stats["pnl"]:>+,.0f}  均值: {avg:>+,.0f}')

    # 月度分布
    section('按月卖出盈亏')
    monthly_pnl = defaultdict(float)
    for r in sells:
        ym = str(r[0])[:7]
        monthly_pnl[ym] += float(r[7] or 0)
    for ym, pnl in sorted(monthly_pnl.items()):
        print(f'    {ym}  实现盈亏: {pnl:>+,.0f}')


# ============================================================
# 模块3：未平仓持仓分析（浮亏来源）
# ============================================================
def analyze_open_positions():
    hr('模块3：未平仓持仓分析（当前浮亏）')

    positions = run_sql("""
        SELECT code, name, asset_type, shares, cost_price, current_price, market_value,
               unrealized_pnl, unrealized_pnl_pct, buy_date, hold_days
        FROM simulated_portfolio
        WHERE trader_id='default'
        ORDER BY unrealized_pnl ASC
    """)

    if not positions:
        print('  当前无持仓')
        return

    print(f'  当前持仓数量: {len(positions)}')
    total_mv = sum(float(r[6] or 0) for r in positions)
    total_upnl = sum(float(r[7] or 0) for r in positions)
    print(f'  持仓总市值:   {total_mv:>12,.0f} 元')
    print(f'  总浮动盈亏:   {total_upnl:>+12,.0f} 元')

    print(f'\n  {"代码":<12} {"类型":<12} {"成本价":>8} {"现价":>8} {"市值":>10} {"浮亏%":>8} {"持仓天":<6} {"名称"}')
    print('  ' + '-' * 80)
    for r in positions:
        code, name, atype, shares, cost, cur, mv, upnl, upnl_pct, buy_dt, hold_d = r
        print(f'  {code:<12} {(atype or ""):<12} {float(cost or 0):>8.3f} {float(cur or 0):>8.3f} '
              f'{float(mv or 0):>10,.0f} {float(upnl_pct or 0):>+7.1f}% {int(hold_d or 0):<6} {name or ""}')

    # 持仓超时分析
    section('超时持仓（持仓天数 > 60）')
    long_hold = [r for r in positions if (r[10] or 0) > 60]
    if long_hold:
        for r in long_hold:
            print(f'    {r[0]}  {r[10]}天  浮盈: {float(r[8] or 0):>+.1f}%')
    else:
        print('  无超时持仓')

    # 止损未触发？
    section('应触发止损但未触发（浮亏超8%）')
    deep_loss = [r for r in positions if (r[8] or 0) <= -8.0]
    if deep_loss:
        for r in deep_loss:
            print(f'    {r[0]}  浮亏: {float(r[8] or 0):>+.1f}%  持仓{r[10]}天  ← 可能止损未触发！')
    else:
        print('  无深度浮亏持仓（止损运行正常）')


# ============================================================
# 模块4：决策日志分析（买入拒绝率与原因）
# ============================================================
def analyze_decision_logs():
    hr('模块4：交易员决策日志分析（信号过滤情况）')

    rows = run_sql("""
        SELECT decision_type, COUNT(*) as cnt
        FROM simulated_decision_logs
        WHERE trader_id='default'
        GROUP BY decision_type
        ORDER BY cnt DESC
    """)
    if not rows:
        print('  [!] 无决策日志记录（simulated_decision_logs 表为空）')
        return

    print('  决策类型分布:')
    for r in rows:
        print(f'    {(r[0] or "unknown"):15s}: {r[1]:5d} 次')

    # 被拒绝信号的原因
    section('被拒绝信号的原因分类（前20条）')
    reject_rows = run_sql("""
        SELECT reasons_text, COUNT(*) as cnt
        FROM simulated_decision_logs
        WHERE trader_id='default' AND decision_type='reject'
        GROUP BY reasons_text
        ORDER BY cnt DESC
        LIMIT 20
    """)
    if reject_rows:
        for r in reject_rows:
            reason_short = (r[0] or '')[:80]
            print(f'    [{r[1]:4d}次] {reason_short}')
    
    else:
        print('  无拒绝记录')

    # 月度买入数量
    section('月度买入数量趋势')
    monthly_buy = run_sql("""
        SELECT substr(signal_date,1,7) as ym, COUNT(*) as cnt
        FROM simulated_decision_logs
        WHERE trader_id='default' AND decision_type='buy'
        GROUP BY ym ORDER BY ym
    """)
    for r in monthly_buy:
        print(f'    {r[0]}: 买入 {r[1]} 笔')

    # 平均决策分 vs 阈值
    section('买入 vs 拒绝 平均决策分')
    score_rows = run_sql("""
        SELECT decision_type,
               AVG(decision_score) as avg_ds,
               AVG(recommendation_score) as avg_rs,
               COUNT(*) as cnt
        FROM simulated_decision_logs
        WHERE trader_id='default' AND decision_type IN ('buy','reject')
        GROUP BY decision_type
    """)
    for r in score_rows:
        print(f'    {(r[0] or ""):8s}: 决策分均值={float(r[1] or 0):.3f}  推荐分均值={float(r[2] or 0):.3f}  数量={r[3]}')


# ============================================================
# 模块5：推荐信号质量（前向验证）
# ============================================================
def analyze_signal_quality():
    hr('模块5：推荐信号质量（用买入后真实价格验证）')

    # 取所有实际买入交易，并查其后5/10/20天的价格
    trades = run_sql("""
        SELECT t.trade_date, t.code, t.asset_type, t.price as buy_price,
               t.signal_score, t.name
        FROM simulated_trades t
        WHERE t.trader_id='default' AND t.action='buy' AND t.price > 0
        ORDER BY t.trade_date ASC
    """)

    if not trades:
        print('  [!] 无买入记录')
        return

    print(f'  共 {len(trades)} 笔买入，验证买后5/10/20日涨跌...')

    results = {'win5': 0, 'lose5': 0, 'win10': 0, 'lose10': 0, 'win20': 0, 'lose20': 0, 'total': 0}
    rets_5, rets_10, rets_20 = [], [], []
    by_asset = defaultdict(lambda: {'ret5': [], 'ret20': []})

    with engine.connect() as conn:
        for trade in trades:
            trade_date = trade[0]
            code = trade[1]
            buy_price = float(trade[3] or 0)
            atype = trade[2]

            if buy_price <= 0:
                continue

            # 查buy后N个交易日的收盘价
            future_prices = conn.execute(text("""
                SELECT date, close FROM daily_prices
                WHERE code=:code AND date > :dt AND close > 0
                ORDER BY date ASC LIMIT 25
            """), {'code': code, 'dt': trade_date}).fetchall()

            if not future_prices:
                continue

            results['total'] += 1

            def get_nth_close(n):
                return float(future_prices[n - 1][1]) if len(future_prices) >= n else None

            c5 = get_nth_close(5)
            c10 = get_nth_close(10)
            c20 = get_nth_close(20)

            if c5:
                r5 = (c5 - buy_price) / buy_price * 100
                rets_5.append(r5)
                by_asset[atype]['ret5'].append(r5)
                if r5 > 0:
                    results['win5'] += 1
                else:
                    results['lose5'] += 1

            if c10:
                r10 = (c10 - buy_price) / buy_price * 100
                rets_10.append(r10)
                if r10 > 0:
                    results['win10'] += 1
                else:
                    results['lose10'] += 1

            if c20:
                r20 = (c20 - buy_price) / buy_price * 100
                rets_20.append(r20)
                by_asset[atype]['ret20'].append(r20)
                if r20 > 0:
                    results['win20'] += 1
                else:
                    results['lose20'] += 1

    total = results['total']
    print(f'  可验证买入: {total} 笔')
    if rets_5:
        wr5 = results['win5'] / (results['win5'] + results['lose5']) * 100
        print(f'  买后5日:  胜率={wr5:.1f}%  平均涨幅={sum(rets_5)/len(rets_5):>+.2f}%')
    if rets_10:
        wr10 = results['win10'] / (results['win10'] + results['lose10']) * 100
        print(f'  买后10日: 胜率={wr10:.1f}%  平均涨幅={sum(rets_10)/len(rets_10):>+.2f}%')
    if rets_20:
        wr20 = results['win20'] / (results['win20'] + results['lose20']) * 100
        print(f'  买后20日: 胜率={wr20:.1f}%  平均涨幅={sum(rets_20)/len(rets_20):>+.2f}%')

    section('按资产类型信号质量（5日/20日）')
    for atype, data in sorted(by_asset.items()):
        r5_list = data['ret5']
        r20_list = data['ret20']
        avg5 = sum(r5_list) / len(r5_list) if r5_list else None
        avg20 = sum(r20_list) / len(r20_list) if r20_list else None
        wr5 = sum(1 for x in r5_list if x > 0) / len(r5_list) * 100 if r5_list else 0
        print(f'    {atype:<15s}: 5日胜率={wr5:.0f}%  5日均涨={avg5:>+.2f}%  20日均涨={avg20:>+.2f}%' if avg5 and avg20 else f'    {atype}: 数据不足')


# ============================================================
# 模块6：市场行情 vs 策略表现（逐月对比）
# ============================================================
def analyze_market_vs_strategy():
    hr('模块6：市场行情 vs 策略表现（按月对比）')

    # 沪深300ETF基准
    benchmark_rows = run_sql("""
        SELECT date, close FROM daily_prices
        WHERE code='510300.SH'
        ORDER BY date ASC
    """)

    if not benchmark_rows:
        benchmark_rows = run_sql("""
            SELECT date, close FROM raw_stock_data
            WHERE code='510300.SH'
            ORDER BY date ASC
        """)

    if not benchmark_rows:
        print('  [!] 无沪深300ETF数据，跳过市场对比')
        return

    # 策略每日净值
    pnl_rows = run_sql("""
        SELECT pnl_date, total_value
        FROM simulated_daily_pnl
        WHERE trader_id='default'
        ORDER BY pnl_date ASC
    """)

    # 按月分组
    bench_monthly = {}
    for r in benchmark_rows:
        ym = str(r[0])[:7]
        bench_monthly.setdefault(ym, [])
        bench_monthly[ym].append(float(r[1] or 0))

    strat_monthly = {}
    for r in pnl_rows:
        ym = str(r[0])[:7]
        strat_monthly.setdefault(ym, [])
        strat_monthly[ym].append(float(r[1] or 0))

    all_months = sorted(set(list(bench_monthly.keys()) + list(strat_monthly.keys())))

    print(f'  {"月份":<8} {"基准首/末":>16} {"基准月涨":>9} {"策略首/末":>16} {"策略月涨":>9} {"超额":>8}')
    print('  ' + '-' * 70)
    for ym in all_months:
        b_list = bench_monthly.get(ym, [])
        s_list = strat_monthly.get(ym, [])
        if not b_list or not s_list:
            continue
        b_ret = (b_list[-1] - b_list[0]) / b_list[0] * 100 if b_list[0] else 0
        s_ret = (s_list[-1] - s_list[0]) / s_list[0] * 100 if s_list[0] else 0
        excess = s_ret - b_ret
        print(f'  {ym:<8} {b_list[0]:>8.3f}/{b_list[-1]:>7.3f} {b_ret:>+8.2f}%  '
              f'{s_list[0]:>9,.0f}/{s_list[-1]:>9,.0f} {s_ret:>+7.2f}%  {excess:>+7.2f}%')


# ============================================================
# 模块7：止损机制检查
# ============================================================
def analyze_stop_loss():
    hr('模块7：止损机制检查')

    # 检查实际成交的止损单
    stop_loss_trades = run_sql("""
        SELECT trade_date, code, name, pnl, pnl_pct, price
        FROM simulated_trades
        WHERE trader_id='default' AND action='sell' AND trigger='stop_loss' AND price > 0
        ORDER BY trade_date ASC
    """)

    print(f'  止损触发次数: {len(stop_loss_trades)}')

    if stop_loss_trades:
        pnl_pcts = [float(r[4] or 0) for r in stop_loss_trades]
        avg_loss_pct = sum(pnl_pcts) / len(pnl_pcts)
        max_loss_pct = min(pnl_pcts)
        print(f'  止损平均亏损: {avg_loss_pct:>+.2f}%')
        print(f'  止损最大亏损: {max_loss_pct:>+.2f}%')
        print(f'\n  止损明细:')
        for r in stop_loss_trades:
            print(f'    {r[0]}  {r[1]:<12} {float(r[4] or 0):>+7.1f}%  {r[2] or ""}')

    # 检查当前持仓中是否有应触发止损的（现在止损阈值是8%）
    cfg_row = run_sql("""
        SELECT stop_loss_pct FROM simulated_trader_config WHERE trader_id='default'
    """)
    sl_pct = float(cfg_row[0][0]) if cfg_row else 0.08
    print(f'\n  当前止损阈值: -{sl_pct*100:.1f}%')

    deep_loss_pos = run_sql("""
        SELECT code, name, unrealized_pnl_pct, hold_days, buy_date
        FROM simulated_portfolio
        WHERE trader_id='default' AND unrealized_pnl_pct < :threshold
    """, {'threshold': -(sl_pct * 100)})

    if deep_loss_pos:
        print(f'  [警告] {len(deep_loss_pos)} 个持仓浮亏超止损线但未被清仓:')
        for r in deep_loss_pos:
            print(f'    {r[0]:<12} {float(r[2] or 0):>+.1f}%  持仓{r[3]}天  买入{r[4]}')
    else:
        print('  当前持仓均在止损线以内 ✓')

    # 检查持仓更新是否正常（最新价格更新日期）
    section('持仓价格更新状态')
    price_check = run_sql("""
        SELECT code, current_price, cost_price, buy_date
        FROM simulated_portfolio
        WHERE trader_id='default'
        ORDER BY unrealized_pnl_pct ASC LIMIT 5
    """)
    if price_check:
        print('  （亏损最深的5只）')
        for r in price_check:
            pnl_pct = (float(r[1] or 0) - float(r[2] or 0)) / float(r[2] or 1) * 100
            print(f'    {r[0]:<12} 成本={float(r[2] or 0):.3f}  现价={float(r[1] or 0):.3f}  浮盈={pnl_pct:>+.1f}%  买入{r[3]}')


# ============================================================
# 模块8：推荐信号覆盖度（每日有多少信号）
# ============================================================
def analyze_recommendation_coverage():
    hr('模块8：推荐信号覆盖度（每日信号数量与质量）')

    monthly_recs = run_sql("""
        SELECT substr(date,1,7) as ym,
               COUNT(*) as total,
               AVG(total_score) as avg_score,
               SUM(CASE WHEN total_score >= 0.54 THEN 1 ELSE 0 END) as above_threshold,
               SUM(CASE WHEN type='a_stock' THEN 1 ELSE 0 END) as a_stock_cnt,
               SUM(CASE WHEN type='etf' THEN 1 ELSE 0 END) as etf_cnt,
               SUM(CASE WHEN type='active_fund' THEN 1 ELSE 0 END) as fund_cnt
        FROM recommendations
        GROUP BY ym
        ORDER BY ym
    """)

    if not monthly_recs:
        print('  [!] 无推荐记录')
        return

    print(f'  {"月份":<8} {"总数":>6} {"均分":>7} {"≥0.54":>7} {"A股":>6} {"ETF":>6} {"基金":>6}')
    print('  ' + '-' * 55)
    for r in monthly_recs:
        print(f'  {r[0]:<8} {r[1]:>6} {float(r[2] or 0):>7.3f} {r[3]:>7} {r[4]:>6} {r[5]:>6} {r[6]:>6}')

    # 特别检查2026年2月（之前发现缺失）
    section('2026年各月信号详情')
    months_2026 = run_sql("""
        SELECT substr(date,1,7) as ym, COUNT(*) as cnt, MIN(date) as min_dt, MAX(date) as max_dt
        FROM recommendations
        WHERE date >= '2026-01-01'
        GROUP BY ym ORDER BY ym
    """)
    for r in months_2026:
        print(f'    {r[0]}: {r[1]:4d} 条  日期范围: {r[2]} ~ {r[3]}')


# ============================================================
# 模块9：adaptive policy 分析（动态阈值是否合理）
# ============================================================
def analyze_adaptive_policy():
    hr('模块9：动态市场策略（adaptive policy）分析')

    # 从决策日志中提取 adaptive_buy_threshold 信息
    threshold_rows = run_sql("""
        SELECT substr(signal_date,1,7) as ym,
               reasons_text
        FROM simulated_decision_logs
        WHERE trader_id='default' AND decision_type IN ('buy', 'reject')
        ORDER BY signal_date ASC
        LIMIT 500
    """)

    monthly_thresholds = defaultdict(list)
    import re
    for r in threshold_rows:
        ym = r[0]
        reasons = r[1] or ''
        # 从 reasons_text 中提取 adaptive_buy_threshold=xxx
        m = re.search(r'adaptive_buy_threshold=([\d.]+)', reasons)
        if m:
            monthly_thresholds[ym].append(float(m.group(1)))

    if monthly_thresholds:
        print('  月度 adaptive_buy_threshold 均值:')
        for ym, vals in sorted(monthly_thresholds.items()):
            avg = sum(vals) / len(vals)
            print(f'    {ym}: 均值={avg:.3f}  样本数={len(vals)}')
    else:
        print('  [!] 无法从决策日志提取 adaptive_buy_threshold 信息')

    # 当前配置
    section('当前交易员配置')
    cfg_rows = run_sql("""
        SELECT buy_score_threshold, sell_score_threshold, stop_loss_pct, take_profit_pct,
               max_hold_days, max_position_count, max_single_position_pct, min_cash_reserve_pct
        FROM simulated_trader_config
        WHERE trader_id='default'
    """)
    if cfg_rows:
        r = cfg_rows[0]
        print(f'    买入分阈值:    {r[0]}')
        print(f'    卖出分阈值:    {r[1]}')
        print(f'    止损比例:      {float(r[2] or 0)*100:.1f}%')
        print(f'    止盈比例:      {float(r[3] or 0)*100:.1f}%')
        print(f'    最大持仓天数:  {r[4]}')
        print(f'    最大持仓数:    {r[5]}')
        print(f'    单笔最大仓位:  {float(r[6] or 0)*100:.1f}%')
        print(f'    最小现金保留:  {float(r[7] or 0)*100:.1f}%')


# ============================================================
# 模块10：资金利用率与现金分布
# ============================================================
def analyze_cash_utilization():
    hr('模块10：资金利用率（现金闲置问题）')

    rows = run_sql("""
        SELECT pnl_date, cash, positions_value, total_value, position_count
        FROM simulated_daily_pnl
        WHERE trader_id='default'
        ORDER BY pnl_date ASC
    """)

    if not rows:
        print('  无数据')
        return

    cash_ratios = []
    for r in rows:
        tv = float(r[3] or 0)
        cash = float(r[1] or 0)
        if tv > 0:
            cash_ratios.append(cash / tv)

    if cash_ratios:
        avg_cash = sum(cash_ratios) / len(cash_ratios) * 100
        max_cash = max(cash_ratios) * 100
        min_cash = min(cash_ratios) * 100
        full_cash_days = sum(1 for x in cash_ratios if x > 0.9)
        print(f'  平均现金占比: {avg_cash:.1f}%')
        print(f'  最高现金占比: {max_cash:.1f}%')
        print(f'  最低现金占比: {min_cash:.1f}%')
        print(f'  空仓天数(>90%): {full_cash_days} 天')

    # 月度现金占比
    section('月度平均现金占比')
    monthly_cash = defaultdict(list)
    for r in rows:
        ym = str(r[0])[:7]
        tv = float(r[3] or 0)
        cash = float(r[1] or 0)
        if tv > 0:
            monthly_cash[ym].append(cash / tv * 100)

    for ym, ratios in sorted(monthly_cash.items()):
        avg = sum(ratios) / len(ratios)
        print(f'    {ym}: 平均现金占比={avg:.1f}%  最大={max(ratios):.1f}%')


# ============================================================
# 模块11：得分分布与实际表现关系
# ============================================================
def analyze_score_vs_performance():
    hr('模块11：推荐分数分布 vs 买入后实际表现')

    # 按分数段分析
    score_buckets = run_sql("""
        SELECT
            CASE
                WHEN t.signal_score >= 0.7 THEN '>=0.70'
                WHEN t.signal_score >= 0.6 THEN '0.60-0.70'
                WHEN t.signal_score >= 0.5 THEN '0.50-0.60'
                WHEN t.signal_score >= 0.4 THEN '0.40-0.50'
                ELSE '<0.40'
            END as score_bucket,
            COUNT(*) as buy_cnt,
            AVG(t.signal_score) as avg_score
        FROM simulated_trades t
        WHERE t.trader_id='default' AND t.action='buy' AND t.price > 0
        GROUP BY score_bucket
        ORDER BY score_bucket DESC
    """)

    if score_buckets:
        print('  买入时信号分数分布:')
        for r in score_buckets:
            print(f'    {r[0]:12s}: {r[1]:3d} 笔  平均分={float(r[2] or 0):.3f}')
    else:
        print('  [!] 无信号分数数据（signal_score 为空）')

    # 查 signal_score 为 NULL 的比例
    null_score = run_sql("""
        SELECT COUNT(*) FROM simulated_trades
        WHERE trader_id='default' AND action='buy' AND (signal_score IS NULL OR signal_score=0)
    """)
    total_buys = run_sql("""
        SELECT COUNT(*) FROM simulated_trades
        WHERE trader_id='default' AND action='buy' AND price > 0
    """)
    if null_score and total_buys:
        null_cnt = null_score[0][0]
        total_cnt = total_buys[0][0]
        print(f'\n  信号分数缺失率: {null_cnt}/{total_cnt} ({null_cnt/total_cnt*100:.1f}%)')


# ============================================================
# 模块12：关键异常检测
# ============================================================
def detect_anomalies():
    hr('模块12：关键异常检测')

    anomalies = []

    # 1. 检查是否有 price=0 的挂起交易（未结算的 pending）
    pending = run_sql("""
        SELECT COUNT(*), MIN(signal_date), MAX(signal_date)
        FROM simulated_trades
        WHERE trader_id='default' AND price=0
    """)
    if pending and pending[0][0] > 0:
        anomalies.append(f'[危险] {pending[0][0]} 笔 price=0 的未结算交易！'
                         f' 日期范围: {pending[0][1]} ~ {pending[0][2]}')

    # 2. 检查持仓 current_price 是否异常（接近0）
    zero_price_pos = run_sql("""
        SELECT COUNT(*) FROM simulated_portfolio
        WHERE trader_id='default' AND (current_price IS NULL OR current_price <= 0)
    """)
    if zero_price_pos and zero_price_pos[0][0] > 0:
        anomalies.append(f'[危险] {zero_price_pos[0][0]} 个持仓 current_price <= 0（价格未更新！）')

    # 3. 检查日期连续性（是否有 daily_pnl 大跳跃）
    big_drop_days = run_sql("""
        SELECT pnl_date, daily_return
        FROM simulated_daily_pnl
        WHERE trader_id='default' AND daily_return < -5
        ORDER BY daily_return ASC
    """)
    if big_drop_days:
        anomalies.append(f'[注意] {len(big_drop_days)} 个交易日单日跌幅超5%:')
        for r in big_drop_days[:5]:
            anomalies.append(f'       {r[0]}: {float(r[1] or 0):>+.2f}%')

    # 4. 检查推荐分数是否被正确应用
    buy_below_threshold = run_sql("""
        SELECT COUNT(*) FROM simulated_trades
        WHERE trader_id='default' AND action='buy' AND price > 0
          AND signal_score < 0.54 AND signal_score > 0
    """)
    if buy_below_threshold and buy_below_threshold[0][0] > 0:
        anomalies.append(f'[警告] {buy_below_threshold[0][0]} 笔买入的信号分 < 0.54（低于设定阈值！）')

    # 5. 重复买入同一只股票
    repeat_buys = run_sql("""
        SELECT code, COUNT(*) as cnt FROM simulated_trades
        WHERE trader_id='default' AND action='buy' AND price > 0
        GROUP BY code HAVING cnt > 2
        ORDER BY cnt DESC
    """)
    if repeat_buys:
        anomalies.append(f'[注意] {len(repeat_buys)} 只股票被多次买入（可能加仓循环）:')
        for r in repeat_buys[:5]:
            anomalies.append(f'       {r[0]}: 买入{r[1]}次')

    # 6. 检查成交量最大的亏损股
    top_loss_stocks = run_sql("""
        SELECT code, name, SUM(pnl) as total_pnl, COUNT(*) as trade_cnt
        FROM simulated_trades
        WHERE trader_id='default' AND action='sell' AND price > 0
        GROUP BY code
        ORDER BY total_pnl ASC LIMIT 5
    """)
    if top_loss_stocks:
        anomalies.append('亏损最多的5只:')
        for r in top_loss_stocks:
            anomalies.append(f'    {r[0]:<12} 已实现盈亏={float(r[2] or 0):>+,.0f}  交易{r[3]}次  {r[1] or ""}')

    if anomalies:
        for a in anomalies:
            print('  ' + a)
    else:
        print('  无明显异常 ✓')


# ============================================================
# 主函数
# ============================================================
def main():
    print('=' * 70)
    print('  模拟交易员深度诊断报告')
    print(f'  生成时间: {date.today()}')
    print('=' * 70)

    analyze_overall_pnl()
    analyze_closed_trades()
    analyze_open_positions()
    analyze_decision_logs()
    analyze_signal_quality()
    analyze_market_vs_strategy()
    analyze_stop_loss()
    analyze_recommendation_coverage()
    analyze_adaptive_policy()
    analyze_cash_utilization()
    analyze_score_vs_performance()
    detect_anomalies()

    hr('诊断完成')
    print("""
  ===== 诊断摘要说明 =====
  重点检查：
  1. 模块2「已实现盈亏」— 止损后实际亏损多少？胜率多少？
  2. 模块3「未平仓持仓」— 浮亏主要来自哪些持仓？是否有未触发止损的？
  3. 模块5「信号质量」  — 买入后5/20日涨跌率，判断信号本身是否有效
  4. 模块6「市场对比」  — 策略跑输基准多少？是alpha问题还是beta问题？
  5. 模块8「信号覆盖」  — 某月是否无推荐导致空仓？
  6. 模块9「自适应阈值」— bearish期间阈值是否正确升高？
  7. 模块12「异常检测」 — 是否有 price=0 或低分买入等程序性bug？
""")


if __name__ == '__main__':
    main()
