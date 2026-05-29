"""
模拟交易员 - 核心决策引擎 - trader/engine.py

每日任务流程（由 scheduler 在收盘后触发）：
  1. run_daily(trade_date) — 传入"今天"，代表信号日 T
     a. 用 T 日收盘价更新持仓市值 + 盈亏
     b. 检查卖出条件，生成 T+1 卖出意向
     c. 检查买入条件，生成 T+1 买入意向
     d. 记录每日净值快照
  2. settle_pending_trades(trade_date) — 传入"今天"，代表成交日 T+1
     a. 用 T+1 开盘价把"昨日意向"落实为实际成交流水
     b. 更新现金 + 持仓
"""

import sys
import os
from datetime import date, datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import (
    get_session, Recommendation, Prediction,
    SimulatedTraderConfig, SimulatedPortfolio,
    SimulatedTrade, SimulatedDailyPnl, SimulatedDecisionLog,
)
from trader.market_state import compute_adaptive_policy
from trader.profile_config import get_active_profile
from trader.portfolio import (
    get_latest_price,
    get_execution_price,
    get_recent_return_series,
    calculate_return_correlation,
)
from utils import get_logger

logger = get_logger(__name__)

DEFAULT_TRADER_ID = 'default'
DEFAULT_INITIAL_CAPITAL = 1_000_000.0

# 支持的资产类型（与 Recommendation.type 字段对应）
SUPPORTED_TYPES = {'a_stock', 'etf', 'active_fund', 'gold', 'silver'}


def _get_or_create_config(session) -> SimulatedTraderConfig:
    """获取或初始化默认交易员配置"""
    cfg = session.query(SimulatedTraderConfig).filter_by(trader_id=DEFAULT_TRADER_ID).first()
    if cfg is None:
        cfg = SimulatedTraderConfig(
            trader_id=DEFAULT_TRADER_ID,
            initial_capital=DEFAULT_INITIAL_CAPITAL,
            current_cash=DEFAULT_INITIAL_CAPITAL,
        )
        session.add(cfg)
        session.flush()
        logger.info(f"[Trader] 初始化交易员配置，本金={DEFAULT_INITIAL_CAPITAL:,.0f}元")
    return cfg


def _get_portfolio(session, trader_id: str) -> list[SimulatedPortfolio]:
    """获取当前所有持仓"""
    return session.query(SimulatedPortfolio).filter_by(trader_id=trader_id).all()


def _total_positions_value(positions: list[SimulatedPortfolio]) -> float:
    return sum((p.market_value or 0.0) for p in positions)


def _update_position_prices(session, positions: list[SimulatedPortfolio], as_of: date):
    """用 as_of 日期的收盘价更新持仓市值和浮盈"""
    for pos in positions:
        price = get_latest_price(session, pos.code, pos.asset_type, as_of=as_of)
        if price is not None:
            pos.current_price = price
            pos.market_value = price * pos.shares
            cost = pos.cost_price * pos.shares
            pos.unrealized_pnl = pos.market_value - cost
            pos.unrealized_pnl_pct = (pos.unrealized_pnl / cost * 100) if cost > 0 else 0.0
        pos.hold_days = (as_of - pos.buy_date).days if pos.buy_date else 0


def _snapshot_daily_pnl(session, cfg: SimulatedTraderConfig, positions: list[SimulatedPortfolio], snapshot_date: date):
    """记录每日净值快照（幂等：已存在则更新）"""
    positions_value = _total_positions_value(positions)
    total_value = cfg.current_cash + positions_value
    total_return = (total_value - cfg.initial_capital) / cfg.initial_capital * 100 if cfg.initial_capital > 0 else 0.0

    # 计算当日收益率
    yesterday = snapshot_date - timedelta(days=1)
    prev = (
        session.query(SimulatedDailyPnl)
        .filter_by(trader_id=cfg.trader_id)
        .filter(SimulatedDailyPnl.pnl_date < snapshot_date)
        .order_by(SimulatedDailyPnl.pnl_date.desc())
        .first()
    )
    daily_return = 0.0
    if prev and prev.total_value and prev.total_value > 0:
        daily_return = (total_value - prev.total_value) / prev.total_value * 100

    # 计算最大回撤
    all_values = [r.total_value for r in
                  session.query(SimulatedDailyPnl.total_value)
                  .filter_by(trader_id=cfg.trader_id)
                  .order_by(SimulatedDailyPnl.pnl_date.asc())
                  .all()]
    all_values.append(total_value)
    max_drawdown = 0.0
    peak = all_values[0]
    for v in all_values:
        if v > peak:
            peak = v
        dd = (peak - v) / peak * 100 if peak > 0 else 0.0
        if dd > max_drawdown:
            max_drawdown = dd

    existing = session.query(SimulatedDailyPnl).filter_by(
        trader_id=cfg.trader_id, pnl_date=snapshot_date
    ).first()
    if existing:
        existing.total_value = total_value
        existing.cash = cfg.current_cash
        existing.positions_value = positions_value
        existing.daily_return = daily_return
        existing.total_return = total_return
        existing.max_drawdown = max_drawdown
        existing.position_count = len(positions)
    else:
        session.add(SimulatedDailyPnl(
            trader_id=cfg.trader_id,
            pnl_date=snapshot_date,
            total_value=total_value,
            cash=cfg.current_cash,
            positions_value=positions_value,
            daily_return=daily_return,
            total_return=total_return,
            max_drawdown=max_drawdown,
            position_count=len(positions),
        ))


def _get_today_recommendations(session, signal_date: date) -> list[Recommendation]:
    """取 signal_date 当天的推荐列表"""
    return (
        session.query(Recommendation)
        .filter(
            Recommendation.date == signal_date,
            Recommendation.type.in_(SUPPORTED_TYPES),
        )
        .order_by(Recommendation.total_score.desc())
        .all()
    )


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _portfolio_asset_weights(active_positions: list[SimulatedPortfolio]) -> dict:
    if not active_positions:
        return {}
    values = {}
    total = 0.0
    for pos in active_positions:
        value = float(pos.market_value or (pos.current_price or 0.0) * (pos.shares or 0.0) or 0.0)
        values[pos.asset_type] = values.get(pos.asset_type, 0.0) + value
        total += value
    if total <= 0:
        count = len(active_positions)
        for pos in active_positions:
            values[pos.asset_type] = values.get(pos.asset_type, 0.0) + (1.0 / count)
        return values
    return {asset_type: val / total for asset_type, val in values.items()}


class SimulatedTrader:
    """模拟交易员决策引擎"""

    def run_daily(self, signal_date: date | None = None):
        """
        每日主流程（在 T 日收盘后调用）。
        signal_date = T（信号产生日，默认今天）
        """
        if signal_date is None:
            signal_date = date.today()

        session = get_session()
        try:
            cfg = _get_or_create_config(session)
            if not cfg.is_active:
                logger.info("[Trader] 交易员已禁用，跳过")
                return

            positions = _get_portfolio(session, cfg.trader_id)
            adaptive_policy = compute_adaptive_policy(session, cfg, as_of=signal_date)

            # 1. 用 T 日收盘价更新持仓
            _update_position_prices(session, positions, as_of=signal_date)

            # 2. 检查卖出条件，生成"待成交卖单"（pending 流水，price 为 None，待 T+1 填入）
            sell_codes = set()
            for pos in positions:
                trigger = self._check_sell_trigger(pos, cfg, signal_date, session)
                if trigger:
                    sell_codes.add(pos.code)
                    self._create_pending_trade(session, cfg, pos, 'sell', trigger, signal_date)
                    self._record_decision_log(
                        session=session,
                        cfg=cfg,
                        signal_date=signal_date,
                        code=pos.code,
                        name=pos.name,
                        asset_type=pos.asset_type,
                        decision_type='sell',
                        decision_score=1.0,
                        pred_score=0.0,
                        rec_score=0.0,
                        risk_score=1.0,
                        portfolio_score=0.5,
                        ai_confidence=None,
                        recommendation_score=None,
                        recommended_action='hold',
                        final_action='sell',
                        reasons=f'触发卖出规则: {trigger}',
                        source_recommendation_id=pos.source_recommendation_id,
                    )

            # 移除已触发卖出的持仓（等 settle 时再正式删除）
            active_positions = [p for p in positions if p.code not in sell_codes]

            # 3. 检查买入条件
            recs = _get_today_recommendations(session, signal_date)
            profile = get_active_profile()
            recs = self._rank_recommendations_for_profile(recs, active_positions, profile)
            total_value = cfg.current_cash + _total_positions_value(active_positions)
            if not recs:
                self._record_decision_log(
                    session=session,
                    cfg=cfg,
                    signal_date=signal_date,
                    code='SYSTEM',
                    name='无推荐信号',
                    asset_type='system',
                    decision_type='hold',
                    decision_score=0.5,
                    pred_score=0.5,
                    rec_score=0.0,
                    risk_score=0.8,
                    portfolio_score=0.8,
                    ai_confidence=None,
                    recommendation_score=None,
                    recommended_action='none',
                    final_action='hold',
                    reasons=(
                        f"当日无可用推荐信号，交易员保持观望。"
                        f" 市场状态={adaptive_policy['market']['state']}"
                        f" 阈值={adaptive_policy['buy_threshold']:.2f}"
                    ),
                    source_recommendation_id=None,
                )
            # 追踪当日已产生 pending 买单的代码，确保持仓上限正确计数
            pending_buy_codes: set[str] = set()
            # 根据市场状态动态调整可新建仓位数上限（熊市=0，震荡市减半）
            effective_max_positions = adaptive_policy.get('max_new_position_count', cfg.max_position_count)
            for rec in recs:
                if rec.code in {p.code for p in active_positions}:
                    continue  # 已持仓
                if rec.code in pending_buy_codes:
                    continue  # 当日已有 pending 买单
                # 当日已有持仓数 + 当日 pending 买单数 >= 上限时停止
                if len(active_positions) + len(pending_buy_codes) >= effective_max_positions:
                    break

                decision = self._build_buy_decision(
                    session=session,
                    cfg=cfg,
                    rec=rec,
                    signal_date=signal_date,
                    active_positions=active_positions,
                    adaptive_policy=adaptive_policy,
                    profile=profile,
                )

                if not decision['accepted']:
                    self._record_decision_log(
                        session=session,
                        cfg=cfg,
                        signal_date=signal_date,
                        code=rec.code,
                        name=rec.name,
                        asset_type=rec.type,
                        decision_type='reject',
                        decision_score=decision['decision_score'],
                        pred_score=decision['pred_score'],
                        rec_score=decision['rec_score'],
                        risk_score=decision['risk_score'],
                        portfolio_score=decision['portfolio_score'],
                        ai_confidence=decision['ai_confidence'],
                        recommendation_score=rec.total_score,
                        recommended_action='buy',
                        final_action='reject',
                        reasons=decision['reason'],
                        source_recommendation_id=rec.id,
                    )
                    continue

                # 仓位控制：单笔不超过总资产 5%，且现金保留 >= 20%
                buy_amount = min(
                    total_value * adaptive_policy['max_single_position_pct'],
                    cfg.current_cash - total_value * adaptive_policy['min_cash_reserve_pct'],
                )
                if buy_amount <= 0:
                    break
                pending_buy_codes.add(rec.code)  # 计入当日 pending，防止超仓
                self._create_pending_trade(
                    session, cfg, rec, 'buy', 'signal', signal_date, amount=buy_amount
                )
                self._record_decision_log(
                    session=session,
                    cfg=cfg,
                    signal_date=signal_date,
                    code=rec.code,
                    name=rec.name,
                    asset_type=rec.type,
                    decision_type='buy',
                    decision_score=decision['decision_score'],
                    pred_score=decision['pred_score'],
                    rec_score=decision['rec_score'],
                    risk_score=decision['risk_score'],
                    portfolio_score=decision['portfolio_score'],
                    ai_confidence=decision['ai_confidence'],
                    recommendation_score=rec.total_score,
                    recommended_action='buy',
                    final_action='buy',
                    reasons=decision['reason'],
                    source_recommendation_id=rec.id,
                )

            # 4. 每日净值快照 - 使用全部持仓（含当日待卖出），正确反映 T 日资产价值
            # 注意：active_positions 已剔除本日触发卖出的持仓（用于买入决策）
            # 但卖出实际 T+1 才结算，今日持仓市值仍属于总资产，必须纳入快照
            _snapshot_daily_pnl(session, cfg, positions, signal_date)

            session.commit()
            logger.info(f"[Trader] run_daily 完成，信号日={signal_date}")

        except Exception as e:
            session.rollback()
            logger.error(f"[Trader] run_daily 失败: {e}", exc_info=True)
        finally:
            session.close()

    def settle_pending_trades(self, trade_date: date | None = None):
        """
        用 T+1 开盘价把昨日意向落实为成交（在 T+1 日开盘后或盘前调用）。
        trade_date = T+1（成交日，默认今天）
        """
        if trade_date is None:
            trade_date = date.today()

        signal_date = trade_date - timedelta(days=1)

        session = get_session()
        try:
            cfg = _get_or_create_config(session)

            # 取所有"信号日=昨天、price=None"的待成交流水
            pending = (
                session.query(SimulatedTrade)
                .filter_by(trader_id=cfg.trader_id, signal_date=signal_date)
                .filter(SimulatedTrade.price == 0)  # price=0 作为 pending 标记
                .all()
            )

            for trade in pending:
                price, actual_date = get_execution_price(
                    session, trade.code, trade.asset_type, signal_date
                )
                if price is None or price <= 0:
                    logger.warning(f"[Trader] 无法获取 {trade.code} T+1 开盘价，跳过本次成交")
                    session.delete(trade)
                    continue

                trade.price = price
                trade.trade_date = actual_date or trade_date

                if trade.action == 'buy':
                    # 计算实际可买份数
                    shares = trade.amount / price if trade.amount and trade.amount > 0 else 0
                    if shares <= 0:
                        session.delete(trade)
                        continue
                    trade.shares = shares
                    trade.amount = shares * price

                    # 扣现金
                    if cfg.current_cash < trade.amount:
                        logger.warning(f"[Trader] 现金不足，跳过买入 {trade.code}")
                        session.delete(trade)
                        continue
                    cfg.current_cash -= trade.amount

                    # 新建或加仓持仓
                    pos = session.query(SimulatedPortfolio).filter_by(
                        trader_id=cfg.trader_id, code=trade.code
                    ).first()
                    if pos:
                        # 加仓：重新计算成本价
                        total_cost = pos.cost_price * pos.shares + price * shares
                        pos.shares += shares
                        pos.cost_price = total_cost / pos.shares
                        pos.last_signal_date = signal_date
                    else:
                        session.add(SimulatedPortfolio(
                            trader_id=cfg.trader_id,
                            code=trade.code,
                            name=trade.name,
                            asset_type=trade.asset_type,
                            shares=shares,
                            cost_price=price,
                            current_price=price,
                            market_value=shares * price,
                            unrealized_pnl=0.0,
                            unrealized_pnl_pct=0.0,
                            buy_date=actual_date or trade_date,
                            last_signal_date=signal_date,
                            hold_days=0,
                            source_recommendation_id=trade.source_recommendation_id,
                        ))

                elif trade.action == 'sell':
                    pos = session.query(SimulatedPortfolio).filter_by(
                        trader_id=cfg.trader_id, code=trade.code
                    ).first()
                    if pos is None:
                        session.delete(trade)
                        continue

                    trade.shares = pos.shares
                    trade.amount = pos.shares * price
                    cost_total = pos.cost_price * pos.shares
                    trade.pnl = trade.amount - cost_total
                    trade.pnl_pct = trade.pnl / cost_total * 100 if cost_total > 0 else 0.0

                    cfg.current_cash += trade.amount
                    session.delete(pos)

            session.commit()

            # 成交结算后立即补做一次到期待复盘同步，减少 /reviews 的时间滞后。
            # 在历史优化批量回放中可通过环境变量关闭，以避免复盘耗时拖慢参数搜索。
            instant_review_disabled = os.environ.get('TRADER_DISABLE_INSTANT_REVIEW', 'false').lower() == 'true'
            if not instant_review_disabled:
                try:
                    from reviews.reviewer import Reviewer
                    reviewer = Reviewer()
                    try:
                        reviewed_count = reviewer.check_expired_predictions()
                        if reviewed_count > 0:
                            logger.info(f"[Trader] 成交后即时复盘完成，新增 {reviewed_count} 条到期复盘")
                    finally:
                        reviewer.close()
                except Exception as review_err:
                    logger.warning(f"[Trader] 成交后即时复盘触发失败: {review_err}")

            logger.info(f"[Trader] settle_pending_trades 完成，成交日={trade_date}")

        except Exception as e:
            session.rollback()
            logger.error(f"[Trader] settle_pending_trades 失败: {e}", exc_info=True)
        finally:
            session.close()

    # ------------------------------------------------------------------
    # 内部辅助方法
    # ------------------------------------------------------------------

    def _check_sell_trigger(
        self,
        pos: SimulatedPortfolio,
        cfg: SimulatedTraderConfig,
        signal_date: date,
        session,
    ) -> str | None:
        """检查是否触发卖出条件，返回触发原因字符串或 None"""
        pnl_pct = pos.unrealized_pnl_pct or 0.0

        # 止损
        if pnl_pct <= -(cfg.stop_loss_pct * 100):
            return 'stop_loss'

        # 止盈
        if pnl_pct >= (cfg.take_profit_pct * 100):
            return 'take_profit'

        # 超时
        if pos.hold_days and pos.hold_days >= cfg.max_hold_days:
            return 'timeout'

        # 推荐评分跌落阈值
        latest_rec = (
            session.query(Recommendation.total_score)
            .filter(
                Recommendation.code == pos.code,
                Recommendation.date == signal_date,
            )
            .first()
        )
        if latest_rec is not None and latest_rec.total_score < cfg.sell_score_threshold:
            return 'score_drop'

        return None

    def _create_pending_trade(
        self,
        session,
        cfg: SimulatedTraderConfig,
        source,  # SimulatedPortfolio or Recommendation
        action: str,
        trigger: str,
        signal_date: date,
        amount: float = 0.0,
    ):
        """创建待成交流水（price=0 为 pending 标记）"""
        if isinstance(source, SimulatedPortfolio):
            code = source.code
            name = source.name
            asset_type = source.asset_type
            rec_id = source.source_recommendation_id
            score = None
        else:
            # Recommendation
            code = source.code
            name = source.name or ''
            asset_type = source.type
            rec_id = source.id
            score = source.total_score

        trade = SimulatedTrade(
            trader_id=cfg.trader_id,
            trade_date=signal_date + timedelta(days=1),  # 暂估，settle 时修正
            signal_date=signal_date,
            code=code,
            name=name,
            asset_type=asset_type,
            action=action,
            shares=0.0,     # settle 时填入
            price=0.0,      # 0 = pending 标记
            amount=amount,  # buy 时是预算金额，sell 时 settle 时计算
            trigger=trigger,
            signal_score=score,
            source_recommendation_id=rec_id,
        )
        session.add(trade)

    def _build_buy_decision(self, session, cfg, rec, signal_date: date, active_positions: list[SimulatedPortfolio], adaptive_policy: dict, profile: dict | None = None) -> dict:
        """构建买入决策分与理由（交易员独立思考核心）"""
        # 1) 预测可靠度分: 优先使用截止 signal_date 的预测；若无则回退到推荐快照中的 5 日概率
        pred = (
            session.query(Prediction)
            .filter(
                Prediction.code == rec.code,
                Prediction.date <= signal_date,
                Prediction.period_days == 5,
            )
            .order_by(Prediction.date.desc(), Prediction.id.desc())
            .first()
        )
        pred_up_prob = float(pred.up_probability) if pred and pred.up_probability is not None else None
        rec_up_prob = float(rec.up_probability_5d) if rec.up_probability_5d is not None else None
        if pred_up_prob is not None:
            up_prob = pred_up_prob
            pred_source = 'prediction'
        elif rec_up_prob is not None:
            up_prob = rec_up_prob
            pred_source = 'recommendation_fallback'
        else:
            up_prob = 50.0
            pred_source = 'default_50'
        pred_score = _clamp01((up_prob - 45.0) / 40.0)
        # 2) 推荐质量分: 推荐总分是 0-5 量纲，需按 5 归一化到 0-1。
        rec_score = _clamp01(float(rec.total_score or 0.0) / 5.0)

        # 3) 风险约束分: 基于波动等级与风险提示
        vol = (rec.volatility_level or '').lower()
        if vol in ('high', '高', 'h'):
            risk_score = 0.35
        elif vol in ('medium', '中', 'm'):
            risk_score = 0.60
        else:
            risk_score = 0.80
        if rec.risk_warning:
            risk_score = max(0.2, risk_score - 0.1)

        # 4) 组合匹配分: 同时考虑仓位占用和与现有持仓的同质化程度
        occupancy = len(active_positions) / max(1, cfg.max_position_count)
        occupancy_score = _clamp01(1.0 - occupancy)
        diversification = self._evaluate_diversification(session, rec, active_positions)
        portfolio_score = _clamp01(0.5 * occupancy_score + 0.5 * diversification['score'])
        coverage_score = self._compute_coverage_score(rec, active_positions, profile)

        # 综合决策分
        decision_score = _clamp01(
            0.32 * pred_score +
            0.28 * rec_score +
            0.18 * risk_score +
            0.12 * portfolio_score +
            0.10 * coverage_score
        )

        ai_confidence = float(pred.confidence) if pred and pred.confidence is not None else up_prob

        reasons = [
            f"market={adaptive_policy['market']['state']}",
            f"pred={pred_score:.2f}(up_prob={up_prob:.1f}%, source={pred_source})",
            f"rec={rec_score:.2f}(score={float(rec.total_score or 0):.1f})",
            f"risk={risk_score:.2f}(vol={rec.volatility_level or 'n/a'})",
            f"portfolio={portfolio_score:.2f}(positions={len(active_positions)}/{cfg.max_position_count}, same_type_ratio={diversification['same_type_ratio']:.2f}, max_corr={diversification['max_corr']:.2f})",
            f"coverage={coverage_score:.2f}(asset_type={rec.type})",
            f"decision={decision_score:.2f}",
            f"adaptive_buy_threshold={adaptive_policy['buy_threshold']:.2f}",
        ]

        profile_threshold = float((profile or {}).get('decision_threshold') or 0.60)
        # 个股（a_stock/hk_stock/us_stock）回退信号需要更严格阈值防止低质量信号驱动买入；
        # ETF/基金/贵金属在历史回放中通常只有代理信号，不额外惩罚，否则这些资产永远无法买入。
        _INDIVIDUAL_STOCK_TYPES = {'a_stock', 'hk_stock', 'us_stock'}
        if pred_source == 'recommendation_fallback' and (rec.type or '') in _INDIVIDUAL_STOCK_TYPES:
            fallback_penalty = 0.03
            fallback_buy_delta = 0.15
        else:
            fallback_penalty = 0.00
            fallback_buy_delta = 0.00
        effective_decision_threshold = profile_threshold + fallback_penalty
        effective_buy_threshold = float(adaptive_policy['buy_threshold']) + fallback_buy_delta

        # 采纳规则：综合分 + 推荐分（均为0-1归一化量纲）双阈値
        accepted = (
            (decision_score >= effective_decision_threshold) and
            (rec_score >= effective_buy_threshold) and  # 使用0-1归一化rec_score匹配阈値量纲
            (diversification['score'] >= 0.35)
        )
        if not accepted:
            if diversification['score'] < 0.35:
                reasons.append('rejected: 与现有持仓同质化过高')
            if pred_source == 'recommendation_fallback':
                reasons.append('rejected: 回退信号需要更高阈值')
            reasons.append('rejected: 未达交易员采纳阈值')
        else:
            reasons.append('accepted: 通过交易员采纳阈值')

        return {
            'accepted': accepted,
            'decision_score': decision_score,
            'pred_score': pred_score,
            'rec_score': rec_score,
            'risk_score': risk_score,
            'portfolio_score': portfolio_score,
            'ai_confidence': ai_confidence,
            'reason': '; '.join(reasons),
        }

    def _compute_coverage_score(self, rec, active_positions: list[SimulatedPortfolio], profile: dict | None) -> float:
        profile_data = profile or {}
        target_alloc = profile_data.get('target_allocations') or {}
        priority_map = profile_data.get('validation_priority') or {}
        current_weights = _portfolio_asset_weights(active_positions)

        current_weight = float(current_weights.get(rec.type, 0.0) or 0.0)
        target_weight = float(target_alloc.get(rec.type, 0.0) or 0.0)
        deficit = target_weight - current_weight
        deficit_score = _clamp01(0.5 + deficit * 2.0)
        priority_boost = float(priority_map.get(rec.type, 0.5) or 0.5)
        return _clamp01(0.75 * deficit_score + 0.25 * priority_boost)

    def _rank_recommendations_for_profile(self, recs: list[Recommendation], active_positions: list[SimulatedPortfolio], profile: dict | None) -> list[Recommendation]:
        items = list(recs or [])
        if not items:
            return items

        profile_data = profile or {}
        target_alloc = profile_data.get('target_allocations') or {}
        priority_map = profile_data.get('validation_priority') or {}
        current_weights = _portfolio_asset_weights(active_positions)

        def _coverage_bonus(rec):
            current_weight = float(current_weights.get(rec.type, 0.0) or 0.0)
            target_weight = float(target_alloc.get(rec.type, 0.0) or 0.0)
            deficit = target_weight - current_weight
            priority = float(priority_map.get(rec.type, 0.5) or 0.5)
            return (deficit * 60.0) + (priority * 20.0)

        return sorted(
            items,
            key=lambda rec: float(rec.total_score or 0.0) + _coverage_bonus(rec),
            reverse=True,
        )

    def _evaluate_diversification(self, session, rec, active_positions: list[SimulatedPortfolio]) -> dict:
        """评估候选资产与当前组合的同质化程度，返回 0-1 分数。"""
        if not active_positions:
            return {
                'score': 1.0,
                'same_type_ratio': 0.0,
                'max_corr': 0.0,
            }

        same_type_count = sum(1 for p in active_positions if p.asset_type == rec.type)
        same_type_ratio = same_type_count / len(active_positions)

        candidate_series = get_recent_return_series(session, rec.code, rec.type, lookback=30)
        max_corr = 0.0
        for pos in active_positions:
            pos_series = get_recent_return_series(session, pos.code, pos.asset_type, lookback=30)
            corr = calculate_return_correlation(candidate_series, pos_series)
            if corr is not None:
                max_corr = max(max_corr, corr)

        penalty = min(1.0, 0.40 * same_type_ratio + 0.50 * max(0.0, max_corr))
        score = _clamp01(1.0 - penalty)
        return {
            'score': score,
            'same_type_ratio': same_type_ratio,
            'max_corr': max_corr,
        }

    def _record_decision_log(
        self,
        session,
        cfg: SimulatedTraderConfig,
        signal_date: date,
        code: str,
        name: str,
        asset_type: str,
        decision_type: str,
        decision_score: float | None,
        pred_score: float | None,
        rec_score: float | None,
        risk_score: float | None,
        portfolio_score: float | None,
        ai_confidence: float | None,
        recommendation_score: float | None,
        recommended_action: str,
        final_action: str,
        reasons: str,
        source_recommendation_id: int | None,
    ):
        """写入交易员思考日志"""
        session.add(SimulatedDecisionLog(
            trader_id=cfg.trader_id,
            signal_date=signal_date,
            code=code,
            name=name or '',
            asset_type=asset_type,
            decision_type=decision_type,
            decision_score=decision_score,
            pred_score=pred_score,
            rec_score=rec_score,
            risk_score=risk_score,
            portfolio_score=portfolio_score,
            ai_confidence=ai_confidence,
            recommendation_score=recommendation_score,
            recommended_action=recommended_action,
            final_action=final_action,
            reasons_text=reasons,
            source_recommendation_id=source_recommendation_id,
        ))


# ------------------------------------------------------------------
# 模块级便捷函数（供 scheduler 直接调用）
# ------------------------------------------------------------------

def run_trader_daily(signal_date: date | None = None):
    """每日信号处理（在 T 日收盘后调用）"""
    SimulatedTrader().run_daily(signal_date)


def settle_trader_trades(trade_date: date | None = None):
    """T+1 成交结算（在 T+1 日开盘后调用）"""
    SimulatedTrader().settle_pending_trades(trade_date)
