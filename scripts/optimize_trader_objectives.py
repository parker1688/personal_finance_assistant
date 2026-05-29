"""多轮历史回放优化：目标为正收益 + 跑赢基准 + 控制回撤。"""

import sys
import os
from dataclasses import dataclass
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import (
    get_session,
    DailyPrice,
    SimulatedTrade,
    SimulatedDecisionLog,
    SimulatedPortfolio,
    SimulatedDailyPnl,
    SimulatedTraderConfig,
)
from scheduler import rebuild_recommendations_for_date
from trader.engine import SimulatedTrader, DEFAULT_TRADER_ID
from trader.profile_config import apply_threshold_adjustment, get_active_profile


@dataclass
class RoundResult:
    threshold: float
    stop_loss_pct: float
    max_position_count: int
    min_cash_reserve_pct: float
    total_return: float
    annualized_return: float
    benchmark_return: float
    excess_return: float
    max_drawdown: float
    total_pnl: float
    closed_trades: int
    win_rate: float


def _reset_trader_state() -> None:
    session = get_session()
    session.query(SimulatedDecisionLog).filter_by(trader_id=DEFAULT_TRADER_ID).delete()
    session.query(SimulatedTrade).filter_by(trader_id=DEFAULT_TRADER_ID).delete()
    session.query(SimulatedPortfolio).filter_by(trader_id=DEFAULT_TRADER_ID).delete()
    session.query(SimulatedDailyPnl).filter_by(trader_id=DEFAULT_TRADER_ID).delete()
    cfg = session.query(SimulatedTraderConfig).filter_by(trader_id=DEFAULT_TRADER_ID).first()
    if cfg:
        cfg.current_cash = cfg.initial_capital
    session.commit()
    session.close()


def _set_trader_config(stop_loss_pct: float = None, max_position_count: int = None,
                       min_cash_reserve_pct: float = None) -> None:
    """批量更新 SimulatedTraderConfig 中的仓位/止损参数"""
    session = get_session()
    try:
        cfg = session.query(SimulatedTraderConfig).filter_by(trader_id=DEFAULT_TRADER_ID).first()
        if cfg is None:
            cfg = SimulatedTraderConfig(
                trader_id=DEFAULT_TRADER_ID,
                initial_capital=1_000_000.0,
                current_cash=1_000_000.0,
            )
            session.add(cfg)
        if stop_loss_pct is not None:
            cfg.stop_loss_pct = stop_loss_pct
        if max_position_count is not None:
            cfg.max_position_count = max_position_count
        if min_cash_reserve_pct is not None:
            cfg.min_cash_reserve_pct = min_cash_reserve_pct
        session.commit()
    finally:
        session.close()


def _get_benchmark_return(start_date: date, end_date: date, benchmark_code: str) -> float:
    session = get_session()
    try:
        rows = (
            session.query(DailyPrice)
            .filter(DailyPrice.code == benchmark_code)
            .filter(DailyPrice.date >= start_date, DailyPrice.date <= end_date)
            .order_by(DailyPrice.date.asc())
            .all()
        )
        if len(rows) < 2:
            return 0.0
        start_close = float(rows[0].close or 0.0)
        end_close = float(rows[-1].close or 0.0)
        if start_close <= 0:
            return 0.0
        return (end_close - start_close) / start_close * 100.0
    finally:
        session.close()


def _collect_metrics(benchmark_return: float, start_date: date, end_date: date,
                     stop_loss_pct: float, max_position_count: int, min_cash_reserve_pct: float) -> RoundResult:
    session = get_session()
    try:
        pnl_rows = (
            session.query(SimulatedDailyPnl)
            .filter_by(trader_id=DEFAULT_TRADER_ID)
            .order_by(SimulatedDailyPnl.pnl_date.asc())
            .all()
        )
        if pnl_rows:
            last = pnl_rows[-1]
            total_return = float(last.total_return or 0.0)
            max_drawdown = float(last.max_drawdown or 0.0)
        else:
            total_return = 0.0
            max_drawdown = 0.0

        sells = session.query(SimulatedTrade).filter_by(trader_id=DEFAULT_TRADER_ID, action='sell').all()
        closed_trades = len(sells)
        total_pnl = sum(float(t.pnl or 0.0) for t in sells)
        win_count = sum(1 for t in sells if float(t.pnl or 0.0) > 0)
        win_rate = (win_count / closed_trades * 100.0) if closed_trades > 0 else 0.0

        active_profile = get_active_profile()
        threshold = float(active_profile.get('decision_threshold') or 0.60)

        days = max(1, (end_date - start_date).days + 1)
        total_ratio = total_return / 100.0
        if total_ratio <= -0.999:
            annualized_return = -100.0
        else:
            annualized_return = ((1.0 + total_ratio) ** (365.0 / days) - 1.0) * 100.0

        return RoundResult(
            threshold=threshold,
            stop_loss_pct=stop_loss_pct,
            max_position_count=max_position_count,
            min_cash_reserve_pct=min_cash_reserve_pct,
            total_return=total_return,
            annualized_return=annualized_return,
            benchmark_return=benchmark_return,
            excess_return=total_return - benchmark_return,
            max_drawdown=max_drawdown,
            total_pnl=total_pnl,
            closed_trades=closed_trades,
            win_rate=win_rate,
        )
    finally:
        session.close()


def _run_backfill(start_date: date, end_date: date, regenerate_recommendations: bool = False) -> None:
    trader = SimulatedTrader()
    cur = start_date
    while cur <= end_date:
        if regenerate_recommendations:
            rebuild_recommendations_for_date(cur, skip_if_exists=False)
        trader.settle_pending_trades(cur)
        trader.run_daily(cur)
        cur += timedelta(days=1)
    trader.settle_pending_trades(end_date + timedelta(days=1))


def _score(result: RoundResult) -> float:
    # 目标函数：优先超额收益，其次控制回撤，再考虑胜率
    return (
        result.excess_return * 2.0
        + result.total_return * 1.0
        + result.annualized_return * 0.8
        - result.max_drawdown * 1.5
        + result.win_rate * 0.05
    )


def optimize(
    start_date: date,
    end_date: date,
    benchmark_code: str = '510300.SH',
    candidate_thresholds: list[float] | None = None,
    candidate_stop_losses: list[float] | None = None,
    candidate_max_positions: list[int] | None = None,
    candidate_cash_reserves: list[float] | None = None,
    regenerate_recommendations: bool = False,
    target_return_pct: float = 10.0,
    max_drawdown_limit: float = 30.0,
) -> tuple['RoundResult | None', list['RoundResult']]:
    if candidate_thresholds is None:
        candidate_thresholds = [0.56]
    if candidate_stop_losses is None:
        candidate_stop_losses = [0.08]
    if candidate_max_positions is None:
        candidate_max_positions = [8, 10, 12, 15]
    if candidate_cash_reserves is None:
        candidate_cash_reserves = [0.25, 0.35, 0.45]

    total_rounds = (len(candidate_thresholds) * len(candidate_stop_losses)
                    * len(candidate_max_positions) * len(candidate_cash_reserves))
    benchmark_return = _get_benchmark_return(start_date, end_date, benchmark_code)
    print(f"Benchmark [{benchmark_code}] return: {benchmark_return:+.2f}%")
    print(f"Target: (interval OR annualized) > {target_return_pct:.1f}% AND max_dd < {max_drawdown_limit:.1f}%")
    print(f"Grid: {len(candidate_thresholds)}th x {len(candidate_stop_losses)}sl "
          f"x {len(candidate_max_positions)}pos x {len(candidate_cash_reserves)}cash = {total_rounds} rounds\n")

    os.environ['TRADER_DISABLE_INSTANT_REVIEW'] = 'true'

    all_results: list[RoundResult] = []
    round_idx = 0
    try:
        for th in candidate_thresholds:
            for sl in candidate_stop_losses:
                for mp in candidate_max_positions:
                    for cr in candidate_cash_reserves:
                        round_idx += 1
                        print(f"=== Round {round_idx}/{total_rounds} | th={th:.2f} | sl={sl*100:.0f}% "
                              f"| max_pos={mp} | cash_rsv={cr*100:.0f}% ===")
                        apply_threshold_adjustment(th, reason=f'optimizer_round_{round_idx}')
                        _set_trader_config(stop_loss_pct=sl, max_position_count=mp,
                                           min_cash_reserve_pct=cr)
                        _reset_trader_state()
                        _run_backfill(start_date, end_date,
                                      regenerate_recommendations=regenerate_recommendations)
                        result = _collect_metrics(benchmark_return, start_date, end_date,
                                                  stop_loss_pct=sl,
                                                  max_position_count=mp,
                                                  min_cash_reserve_pct=cr)
                        all_results.append(result)

                        valid = (
                            result.max_drawdown < max_drawdown_limit
                            and (result.total_return > target_return_pct
                                 or result.annualized_return > target_return_pct)
                            and result.excess_return > 0
                        )
                        flag = "[OK]" if valid else "[--]"
                        print(
                            f"{flag} interval={result.total_return:+.2f}% | ann={result.annualized_return:+.2f}% | "
                            f"excess={result.excess_return:+.2f}% | max_dd={result.max_drawdown:.2f}% | "
                            f"pnl={result.total_pnl:+.0f} | wr={result.win_rate:.1f}%"
                        )
    finally:
        os.environ.pop('TRADER_DISABLE_INSTANT_REVIEW', None)

    # 汇总表格
    print("\n" + "="*110)
    print(f"{'th':>5} {'sl%':>4} {'pos':>4} {'cash%':>6} {'intv%':>7} {'ann%':>7} "
          f"{'exc%':>7} {'dd%':>7} {'wr%':>6} {'valid':>6}")
    print("-"*110)
    valid_results = []
    for r in all_results:
        is_valid = (
            r.max_drawdown < max_drawdown_limit
            and (r.total_return > target_return_pct or r.annualized_return > target_return_pct)
            and r.excess_return > 0
        )
        tag = "YES" if is_valid else "no"
        if is_valid:
            valid_results.append(r)
        print(
            f"{r.threshold:>5.2f} {r.stop_loss_pct*100:>3.0f}% {r.max_position_count:>4} "
            f"{r.min_cash_reserve_pct*100:>5.0f}% "
            f"{r.total_return:>+7.2f} {r.annualized_return:>+7.2f} "
            f"{r.excess_return:>+7.2f} {r.max_drawdown:>7.2f} {r.win_rate:>6.1f} {tag:>6}"
        )
    print("="*110)

    if valid_results:
        best = max(valid_results, key=_score)
        print(f"\n[BEST VALID] th={best.threshold:.2f} | sl={best.stop_loss_pct*100:.0f}% "
              f"| max_pos={best.max_position_count} | cash_rsv={best.min_cash_reserve_pct*100:.0f}% | "
              f"interval={best.total_return:+.2f}% | ann={best.annualized_return:+.2f}% | "
              f"excess={best.excess_return:+.2f}% | max_dd={best.max_drawdown:.2f}%")
        apply_threshold_adjustment(best.threshold, reason='optimizer_best')
        _set_trader_config(stop_loss_pct=best.stop_loss_pct,
                           max_position_count=best.max_position_count,
                           min_cash_reserve_pct=best.min_cash_reserve_pct)
        print(f"[OK] 已应用最优参数")
        return best, all_results
    else:
        best = max(all_results, key=_score)
        print(f"\n[WARN] 无参数组合同时满足所有约束。得分最高组合:")
        print(f"  th={best.threshold:.2f} | sl={best.stop_loss_pct*100:.0f}% "
              f"| max_pos={best.max_position_count} | cash_rsv={best.min_cash_reserve_pct*100:.0f}% | "
              f"interval={best.total_return:+.2f}% | ann={best.annualized_return:+.2f}% | "
              f"max_dd={best.max_drawdown:.2f}%")
        apply_threshold_adjustment(best.threshold, reason='optimizer_best_fallback')
        _set_trader_config(stop_loss_pct=best.stop_loss_pct,
                           max_position_count=best.max_position_count,
                           min_cash_reserve_pct=best.min_cash_reserve_pct)
        return None, all_results


if __name__ == '__main__':
    s = date(2025, 11, 1)
    e = date(2026, 5, 7)
    # Round 3: 以确认最优 pos=15 为基准，探索不同止损幅度(5%-12%)和阈值(0.52-0.58)
    # 扩展回测窗口到今天，验证更长期稳定性
    optimize(
        s, e,
        candidate_thresholds=[0.52, 0.54, 0.56, 0.58],
        candidate_stop_losses=[0.05, 0.06, 0.08, 0.10, 0.12],
        candidate_max_positions=[15],
        candidate_cash_reserves=[0.15, 0.20],
        regenerate_recommendations=False,
        target_return_pct=10.0,
        max_drawdown_limit=30.0,
    )
