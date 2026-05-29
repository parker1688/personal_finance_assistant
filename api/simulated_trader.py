"""
模拟交易员 API - api/simulated_trader.py

端点：
  GET  /api/simulated-trader/status       当前净值、持仓、今日操作摘要
  GET  /api/simulated-trader/trades       交易流水（分页）
  GET  /api/simulated-trader/pnl          每日净值曲线
  GET  /api/simulated-trader/validation   全链路验证报告
    GET  /api/simulated-trader/thinking     交易员思考日志
    GET  /api/simulated-trader/scorecard    AI评分卡
    POST /api/simulated-trader/run          手动触发当日信号处理
    POST /api/simulated-trader/backfill     历史区间批量回放（信号+结算）
  POST /api/simulated-trader/reset        重置（清空所有模拟数据，回到100万）
"""

import sys
import os
from datetime import date, datetime
from datetime import timedelta
from flask import jsonify, request

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import (
    get_session,
    SimulatedTraderConfig, SimulatedPortfolio,
    SimulatedTrade, SimulatedDailyPnl, SimulatedDecisionLog,
)
from trader.engine import SimulatedTrader, DEFAULT_TRADER_ID, DEFAULT_INITIAL_CAPITAL
from trader.maintenance import get_decision_log_archive_status, archive_old_decision_logs
from trader.market_state import compute_adaptive_policy
from trader.profile_config import get_trader_profiles, get_active_profile, set_active_profile
from trader.validator import compute_validation_report, compute_ai_scorecard
from utils import get_logger
from api.auth import require_admin_access, log_admin_audit

logger = get_logger(__name__)


def _cfg_to_dict(cfg: SimulatedTraderConfig) -> dict:
    return {
        'trader_id': cfg.trader_id,
        'initial_capital': cfg.initial_capital,
        'current_cash': cfg.current_cash,
        'buy_score_threshold': cfg.buy_score_threshold,
        'sell_score_threshold': cfg.sell_score_threshold,
        'stop_loss_pct': cfg.stop_loss_pct,
        'take_profit_pct': cfg.take_profit_pct,
        'max_position_count': cfg.max_position_count,
        'max_single_position_pct': cfg.max_single_position_pct,
        'min_cash_reserve_pct': cfg.min_cash_reserve_pct,
        'max_hold_days': cfg.max_hold_days,
        'is_active': cfg.is_active,
        'active_profile': get_active_profile().get('name', 'balanced_default'),
    }


def _pos_to_dict(pos: SimulatedPortfolio) -> dict:
    return {
        'code': pos.code,
        'name': pos.name,
        'asset_type': pos.asset_type,
        'shares': pos.shares,
        'cost_price': pos.cost_price,
        'current_price': pos.current_price,
        'market_value': pos.market_value,
        'unrealized_pnl': pos.unrealized_pnl,
        'unrealized_pnl_pct': pos.unrealized_pnl_pct,
        'buy_date': pos.buy_date.isoformat() if pos.buy_date else None,
        'hold_days': pos.hold_days,
        'last_signal_date': pos.last_signal_date.isoformat() if pos.last_signal_date else None,
    }


def _trade_to_dict(t: SimulatedTrade) -> dict:
    return {
        'id': t.id,
        'trade_date': t.trade_date.isoformat() if t.trade_date else None,
        'signal_date': t.signal_date.isoformat() if t.signal_date else None,
        'code': t.code,
        'name': t.name,
        'asset_type': t.asset_type,
        'action': t.action,
        'shares': t.shares,
        'price': t.price,
        'amount': t.amount,
        'trigger': t.trigger,
        'signal_score': t.signal_score,
        'pnl': t.pnl,
        'pnl_pct': t.pnl_pct,
        'pending': t.price == 0,
    }


def _pnl_to_dict(r: SimulatedDailyPnl) -> dict:
    return {
        'date': r.pnl_date.isoformat() if r.pnl_date else None,
        'total_value': r.total_value,
        'cash': r.cash,
        'positions_value': r.positions_value,
        'daily_return': r.daily_return,
        'total_return': r.total_return,
        'max_drawdown': r.max_drawdown,
        'position_count': r.position_count,
    }


def _decision_to_dict(d: SimulatedDecisionLog) -> dict:
    return {
        'id': d.id,
        'signal_date': d.signal_date.isoformat() if d.signal_date else None,
        'code': d.code,
        'name': d.name,
        'asset_type': d.asset_type,
        'decision_type': d.decision_type,
        'decision_score': d.decision_score,
        'pred_score': d.pred_score,
        'rec_score': d.rec_score,
        'risk_score': d.risk_score,
        'portfolio_score': d.portfolio_score,
        'ai_confidence': d.ai_confidence,
        'recommendation_score': d.recommendation_score,
        'recommended_action': d.recommended_action,
        'final_action': d.final_action,
        'reasons_text': d.reasons_text,
        'created_at': d.created_at.isoformat() if d.created_at else None,
    }


def _build_health_board(session, cfg: SimulatedTraderConfig | None) -> dict:
    alerts = []
    today = date.today()

    if cfg is None:
        alerts.append({'level': 'critical', 'code': 'trader_not_initialized', 'message': '模拟交易员尚未初始化'})
        return {
            'overall_status': 'critical',
            'alerts': alerts,
            'metrics': {
                'initialized': False,
            },
        }

    pending_count = (
        session.query(SimulatedTrade)
        .filter_by(trader_id=DEFAULT_TRADER_ID)
        .filter(SimulatedTrade.price == 0)
        .count()
    )
    latest_pnl = (
        session.query(SimulatedDailyPnl)
        .filter_by(trader_id=DEFAULT_TRADER_ID)
        .order_by(SimulatedDailyPnl.pnl_date.desc())
        .first()
    )
    archive_status = get_decision_log_archive_status(session, DEFAULT_TRADER_ID)
    validation = compute_validation_report(DEFAULT_TRADER_ID)

    stale_days = None
    if latest_pnl and latest_pnl.pnl_date:
        stale_days = (today - latest_pnl.pnl_date).days
        if stale_days >= 2:
            alerts.append({'level': 'warning', 'code': 'pnl_stale', 'message': f'净值快照已 {stale_days} 天未更新'})
    else:
        alerts.append({'level': 'warning', 'code': 'missing_pnl', 'message': '尚无净值快照数据'})

    if pending_count >= 10:
        alerts.append({'level': 'warning', 'code': 'too_many_pending_trades', 'message': f'待成交意向 {pending_count} 笔，需检查结算链路'})

    if archive_status['deletable_logs'] >= 200:
        alerts.append({'level': 'warning', 'code': 'decision_logs_need_archive', 'message': f"有 {archive_status['deletable_logs']} 条旧思考日志待归档"})

    loss_analysis = validation.get('loss_trade_analysis') or {}
    if (loss_analysis.get('total_loss_trades') or 0) >= 5:
        alerts.append({'level': 'warning', 'code': 'loss_trades_accumulating', 'message': f"累计亏损平仓 {loss_analysis.get('total_loss_trades')} 笔，建议优先复盘"})

    overall_status = 'healthy'
    if any(item['level'] == 'critical' for item in alerts):
        overall_status = 'critical'
    elif alerts:
        overall_status = 'warning'

    return {
        'overall_status': overall_status,
        'alerts': alerts,
        'metrics': {
            'initialized': True,
            'is_active': bool(cfg.is_active),
            'current_cash': round(cfg.current_cash or 0.0, 2),
            'latest_pnl_date': latest_pnl.pnl_date.isoformat() if latest_pnl and latest_pnl.pnl_date else None,
            'stale_days': stale_days,
            'pending_trade_count': pending_count,
            'decision_log_total': archive_status['total_logs'],
            'decision_log_deletable': archive_status['deletable_logs'],
            'decision_log_retention_days': archive_status['retention_days'],
        },
    }


def register_simulated_trader_routes(app):
    """注册模拟交易员相关路由"""

    # ------------------------------------------------------------------ GET /status
    @app.route('/api/simulated-trader/status', methods=['GET'])
    @require_admin_access(action='simulated_trader.read_status', audit_success=False, allow_local_with_key=True)
    def simulated_trader_status():
        """当前净值、持仓、配置摘要"""
        session = get_session()
        try:
            cfg = session.query(SimulatedTraderConfig).filter_by(trader_id=DEFAULT_TRADER_ID).first()
            if cfg is None:
                return jsonify({'code': 200, 'data': {'initialized': False}})

            positions = session.query(SimulatedPortfolio).filter_by(trader_id=DEFAULT_TRADER_ID).all()
            adaptive_policy = compute_adaptive_policy(session, cfg)
            positions_value = sum((p.market_value or 0.0) for p in positions)
            total_value = cfg.current_cash + positions_value
            total_return = (total_value - cfg.initial_capital) / cfg.initial_capital * 100 if cfg.initial_capital > 0 else 0.0

            # 今日待成交流水
            today = date.today()
            pending_trades = (
                session.query(SimulatedTrade)
                .filter_by(trader_id=DEFAULT_TRADER_ID)
                .filter(SimulatedTrade.signal_date == today, SimulatedTrade.price == 0)
                .all()
            )
            archive_status = get_decision_log_archive_status(session, DEFAULT_TRADER_ID)

            return jsonify({
                'code': 200,
                'data': {
                    'initialized': True,
                    'config': _cfg_to_dict(cfg),
                    'summary': {
                        'total_value': round(total_value, 2),
                        'cash': round(cfg.current_cash, 2),
                        'positions_value': round(positions_value, 2),
                        'total_return_pct': round(total_return, 2),
                        'position_count': len(positions),
                    },
                    'market_context': adaptive_policy,
                    'active_profile': get_active_profile(),
                    'archive_status': archive_status,
                    'positions': [_pos_to_dict(p) for p in positions],
                    'pending_trades_today': [_trade_to_dict(t) for t in pending_trades],
                },
                'timestamp': datetime.now().isoformat(),
            })
        except Exception as e:
            logger.error(f"[Trader API] status 失败: {e}", exc_info=True)
            return jsonify({'code': 500, 'message': str(e)}), 500
        finally:
            session.close()

    # ------------------------------------------------------------------ GET /trades
    @app.route('/api/simulated-trader/trades', methods=['GET'])
    @require_admin_access(action='simulated_trader.read_trades', audit_success=False, allow_local_with_key=True)
    def simulated_trader_trades():
        """交易流水（分页，支持 action/code 过滤）"""
        page = max(1, int(request.args.get('page', 1)))
        page_size = min(200, max(1, int(request.args.get('page_size', 50))))
        action_filter = request.args.get('action')
        code_filter = request.args.get('code')

        session = get_session()
        try:
            q = session.query(SimulatedTrade).filter_by(trader_id=DEFAULT_TRADER_ID)
            if action_filter:
                q = q.filter(SimulatedTrade.action == action_filter)
            if code_filter:
                q = q.filter(SimulatedTrade.code == code_filter)
            q = q.filter(SimulatedTrade.price > 0)  # 仅已成交
            total = q.count()
            trades = q.order_by(SimulatedTrade.trade_date.desc()).offset((page - 1) * page_size).limit(page_size).all()

            return jsonify({
                'code': 200,
                'data': {
                    'total': total,
                    'page': page,
                    'page_size': page_size,
                    'trades': [_trade_to_dict(t) for t in trades],
                },
            })
        except Exception as e:
            logger.error(f"[Trader API] trades 失败: {e}", exc_info=True)
            return jsonify({'code': 500, 'message': str(e)}), 500
        finally:
            session.close()

    # ------------------------------------------------------------------ GET /pnl
    @app.route('/api/simulated-trader/pnl', methods=['GET'])
    @require_admin_access(action='simulated_trader.read_pnl', audit_success=False, allow_local_with_key=True)
    def simulated_trader_pnl():
        """每日净值曲线（全量或最近 N 天，若指定 days 但无数据则 fallback 返回全部）"""
        days = request.args.get('days', type=int)
        session = get_session()
        try:
            q = session.query(SimulatedDailyPnl).filter_by(trader_id=DEFAULT_TRADER_ID)
            if days and days > 0:
                from datetime import date, timedelta
                cutoff = date.today() - timedelta(days=days)
                filtered = q.filter(SimulatedDailyPnl.pnl_date >= cutoff)
                rows = filtered.order_by(SimulatedDailyPnl.pnl_date.asc()).all()
                if not rows:
                    # 回测数据全在截止线之前，返回全部历史
                    rows = q.order_by(SimulatedDailyPnl.pnl_date.asc()).all()
            else:
                rows = q.order_by(SimulatedDailyPnl.pnl_date.asc()).all()
            return jsonify({
                'code': 200,
                'data': [_pnl_to_dict(r) for r in rows],
            })
        except Exception as e:
            logger.error(f"[Trader API] pnl 失败: {e}", exc_info=True)
            return jsonify({'code': 500, 'message': str(e)}), 500
        finally:
            session.close()

    # ------------------------------------------------------------------ GET /validation
    @app.route('/api/simulated-trader/validation', methods=['GET'])
    @require_admin_access(action='simulated_trader.read_validation', audit_success=False, allow_local_with_key=True)
    def simulated_trader_validation():
        """全链路验证报告"""
        try:
            report = compute_validation_report(DEFAULT_TRADER_ID)
            return jsonify({'code': 200, 'data': report})
        except Exception as e:
            logger.error(f"[Trader API] validation 失败: {e}", exc_info=True)
            return jsonify({'code': 500, 'message': str(e)}), 500

    # ------------------------------------------------------------------ GET /thinking
    @app.route('/api/simulated-trader/thinking', methods=['GET'])
    @require_admin_access(action='simulated_trader.read_thinking', audit_success=False, allow_local_with_key=True)
    def simulated_trader_thinking():
        """交易员思考日志（按时间倒序）"""
        page = max(1, int(request.args.get('page', 1)))
        page_size = min(200, max(1, int(request.args.get('page_size', 50))))

        session = get_session()
        try:
            q = session.query(SimulatedDecisionLog).filter_by(trader_id=DEFAULT_TRADER_ID)
            total = q.count()
            rows = q.order_by(SimulatedDecisionLog.created_at.desc()).offset((page - 1) * page_size).limit(page_size).all()
            return jsonify({
                'code': 200,
                'data': {
                    'total': total,
                    'page': page,
                    'page_size': page_size,
                    'logs': [_decision_to_dict(r) for r in rows],
                },
            })
        except Exception as e:
            logger.error(f"[Trader API] thinking 失败: {e}", exc_info=True)
            return jsonify({'code': 500, 'message': str(e)}), 500
        finally:
            session.close()

    # ------------------------------------------------------------------ GET /scorecard
    @app.route('/api/simulated-trader/scorecard', methods=['GET'])
    @require_admin_access(action='simulated_trader.read_scorecard', audit_success=False, allow_local_with_key=True)
    def simulated_trader_scorecard():
        """AI评分卡"""
        try:
            card = compute_ai_scorecard(DEFAULT_TRADER_ID)
            return jsonify({'code': 200, 'data': card})
        except Exception as e:
            logger.error(f"[Trader API] scorecard 失败: {e}", exc_info=True)
            return jsonify({'code': 500, 'message': str(e)}), 500

    # ------------------------------------------------------------------ GET /health
    @app.route('/api/simulated-trader/health', methods=['GET'])
    @require_admin_access(action='simulated_trader.read_health', audit_success=False, allow_local_with_key=True)
    def simulated_trader_health():
        """交易员健康看板数据"""
        session = get_session()
        try:
            cfg = session.query(SimulatedTraderConfig).filter_by(trader_id=DEFAULT_TRADER_ID).first()
            board = _build_health_board(session, cfg)
            return jsonify({'code': 200, 'data': board})
        except Exception as e:
            logger.error(f"[Trader API] health 失败: {e}", exc_info=True)
            return jsonify({'code': 500, 'message': str(e)}), 500
        finally:
            session.close()

    # ------------------------------------------------------------------ POST /run
    @app.route('/api/simulated-trader/run', methods=['POST'])
    @require_admin_access(action='simulated_trader.run')
    def simulated_trader_run():
        """手动触发当日信号处理（管理员权限）"""
        body = request.get_json(silent=True) or {}
        signal_date_str = body.get('signal_date')
        try:
            signal_date = date.fromisoformat(signal_date_str) if signal_date_str else date.today()
        except ValueError:
            return jsonify({'code': 400, 'message': 'signal_date 格式错误，需 YYYY-MM-DD'}), 400

        try:
            trader = SimulatedTrader()
            trader.run_daily(signal_date)
            log_admin_audit('simulated_trader.run', 'success', f'signal_date={signal_date}')
            return jsonify({'code': 200, 'message': f'run_daily 完成，信号日={signal_date}'})
        except Exception as e:
            logger.error(f"[Trader API] run 失败: {e}", exc_info=True)
            return jsonify({'code': 500, 'message': str(e)}), 500

    # ------------------------------------------------------------------ GET /profiles
    @app.route('/api/simulated-trader/profiles', methods=['GET'])
    @require_admin_access(action='simulated_trader.read_profiles', audit_success=False, allow_local_with_key=True)
    def simulated_trader_profiles():
        """获取可用交易员档位及当前激活档位。"""
        try:
            return jsonify({
                'code': 200,
                'data': {
                    'active_profile': get_active_profile(),
                    'profiles': get_trader_profiles(),
                },
            })
        except Exception as e:
            logger.error(f"[Trader API] profiles 失败: {e}", exc_info=True)
            return jsonify({'code': 500, 'message': str(e)}), 500

    # ------------------------------------------------------------------ POST /profiles/activate
    @app.route('/api/simulated-trader/profiles/activate', methods=['POST'])
    @require_admin_access(action='simulated_trader.activate_profile')
    def simulated_trader_activate_profile():
        """激活交易员档位，并把可映射参数同步写入交易员配置。"""
        body = request.get_json(silent=True) or {}
        profile_name = body.get('profile')
        if not profile_name:
            return jsonify({'code': 400, 'message': 'profile 不能为空'}), 400

        session = get_session()
        try:
            active_profile = set_active_profile(profile_name)
            cfg = session.query(SimulatedTraderConfig).filter_by(trader_id=DEFAULT_TRADER_ID).first()
            if cfg is None:
                trader = SimulatedTrader()
                trader.run_daily(date.today())
                cfg = session.query(SimulatedTraderConfig).filter_by(trader_id=DEFAULT_TRADER_ID).first()

            overrides = active_profile.get('config_overrides') or {}
            if cfg:
                for field, value in overrides.items():
                    if hasattr(cfg, field):
                        setattr(cfg, field, value)
                cfg.updated_at = datetime.now()
            session.commit()

            log_admin_audit('simulated_trader.activate_profile', 'success', f"profile={active_profile.get('name')}")
            return jsonify({
                'code': 200,
                'data': {
                    'active_profile': active_profile,
                    'config': _cfg_to_dict(cfg) if cfg else None,
                },
                'message': f"已激活档位: {active_profile.get('label')}",
            })
        except ValueError as e:
            session.rollback()
            return jsonify({'code': 400, 'message': str(e)}), 400
        except Exception as e:
            session.rollback()
            logger.error(f"[Trader API] activate profile 失败: {e}", exc_info=True)
            return jsonify({'code': 500, 'message': str(e)}), 500
        finally:
            session.close()

    # ------------------------------------------------------------------ POST /backfill
    @app.route('/api/simulated-trader/backfill', methods=['POST'])
    @require_admin_access(action='simulated_trader.backfill')
    def simulated_trader_backfill():
        """历史区间批量回放：按日执行 settle + run，快速积累验证数据。

        新增参数 regen_recommendations (bool, 默认 false)：
          若为 true，则在每个回放日之前先调用
          scheduler.rebuild_recommendations_for_date() 补充历史推荐，
          使交易员能在无推荐记录的历史区间内产生真实成交样本。
        """
        body = request.get_json(silent=True) or {}
        start_date_str = body.get('start_date')
        end_date_str = body.get('end_date')
        dry_run = bool(body.get('dry_run', False))
        reset_before_run = bool(body.get('reset_before_run', True))
        settle_after_last_day = bool(body.get('settle_after_last_day', True))
        regen_recommendations = bool(body.get('regen_recommendations', False))

        if not start_date_str:
            return jsonify({'code': 400, 'message': 'start_date 不能为空，格式 YYYY-MM-DD'}), 400

        try:
            start_date = date.fromisoformat(start_date_str)
            end_date = date.fromisoformat(end_date_str) if end_date_str else date.today()
        except ValueError:
            return jsonify({'code': 400, 'message': 'start_date/end_date 格式错误，需 YYYY-MM-DD'}), 400

        if end_date < start_date:
            return jsonify({'code': 400, 'message': 'end_date 不能早于 start_date'}), 400

        span_days = (end_date - start_date).days + 1
        if span_days > 730:
            return jsonify({'code': 400, 'message': '单次回放区间不能超过 730 天'}), 400

        if dry_run:
            return jsonify({
                'code': 200,
                'data': {
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat(),
                    'span_days': span_days,
                    'reset_before_run': reset_before_run,
                    'settle_after_last_day': settle_after_last_day,
                    'regen_recommendations': regen_recommendations,
                    'estimated_steps': span_days * 2 + (1 if settle_after_last_day else 0),
                },
                'message': 'dry_run 预检完成，参数可执行',
            })

        session = get_session()
        try:
            if reset_before_run:
                session.query(SimulatedDecisionLog).filter_by(trader_id=DEFAULT_TRADER_ID).delete()
                session.query(SimulatedTrade).filter_by(trader_id=DEFAULT_TRADER_ID).delete()
                session.query(SimulatedPortfolio).filter_by(trader_id=DEFAULT_TRADER_ID).delete()
                session.query(SimulatedDailyPnl).filter_by(trader_id=DEFAULT_TRADER_ID).delete()
                cfg = session.query(SimulatedTraderConfig).filter_by(trader_id=DEFAULT_TRADER_ID).first()
                if cfg:
                    cfg.current_cash = cfg.initial_capital
                session.commit()

            # 仅在启用 regen 时才导入，避免不必要的启动开销
            if regen_recommendations:
                from scheduler import rebuild_recommendations_for_date

            trader = SimulatedTrader()
            cur = start_date
            processed_days = 0
            regen_inserted_total = 0
            while cur <= end_date:
                if regen_recommendations:
                    try:
                        result = rebuild_recommendations_for_date(cur, skip_if_exists=True)
                        if not result.get('skipped'):
                            regen_inserted_total += result.get('inserted', 0)
                    except Exception as regen_err:
                        logger.warning(f"[Backfill] {cur} 历史推荐重建失败（继续回放）: {regen_err}")
                trader.settle_pending_trades(cur)
                trader.run_daily(cur)
                processed_days += 1
                cur += timedelta(days=1)

            if settle_after_last_day:
                trader.settle_pending_trades(end_date + timedelta(days=1))

            summary_session = get_session()
            try:
                trade_count = summary_session.query(SimulatedTrade).filter_by(trader_id=DEFAULT_TRADER_ID).count()
                pnl_count = summary_session.query(SimulatedDailyPnl).filter_by(trader_id=DEFAULT_TRADER_ID).count()
                decision_count = summary_session.query(SimulatedDecisionLog).filter_by(trader_id=DEFAULT_TRADER_ID).count()
            finally:
                summary_session.close()

            log_admin_audit(
                'simulated_trader.backfill',
                'success',
                f'start={start_date} end={end_date} span={processed_days} reset={reset_before_run} regen={regen_recommendations}'
            )
            return jsonify({
                'code': 200,
                'data': {
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat(),
                    'processed_days': processed_days,
                    'reset_before_run': reset_before_run,
                    'settle_after_last_day': settle_after_last_day,
                    'regen_recommendations': regen_recommendations,
                    'regen_inserted': regen_inserted_total if regen_recommendations else None,
                    'trade_count': trade_count,
                    'daily_pnl_count': pnl_count,
                    'decision_log_count': decision_count,
                },
                'message': '历史回放完成',
            })
        except Exception as e:
            session.rollback()
            logger.error(f"[Trader API] backfill 失败: {e}", exc_info=True)
            return jsonify({'code': 500, 'message': str(e)}), 500
        finally:
            session.close()

    # ------------------------------------------------------------------ POST /reset
    @app.route('/api/simulated-trader/reset', methods=['POST'])
    @require_admin_access(action='simulated_trader.reset')
    def simulated_trader_reset():
        """重置模拟交易员（清空所有模拟数据，重置本金）"""
        session = get_session()
        try:
            session.query(SimulatedDecisionLog).filter_by(trader_id=DEFAULT_TRADER_ID).delete()
            session.query(SimulatedTrade).filter_by(trader_id=DEFAULT_TRADER_ID).delete()
            session.query(SimulatedPortfolio).filter_by(trader_id=DEFAULT_TRADER_ID).delete()
            session.query(SimulatedDailyPnl).filter_by(trader_id=DEFAULT_TRADER_ID).delete()
            cfg = session.query(SimulatedTraderConfig).filter_by(trader_id=DEFAULT_TRADER_ID).first()
            if cfg:
                cfg.current_cash = cfg.initial_capital
            session.commit()
            log_admin_audit('simulated_trader.reset', 'success', f'trader_id={DEFAULT_TRADER_ID}')
            return jsonify({'code': 200, 'message': '模拟交易员已重置'})
        except Exception as e:
            session.rollback()
            logger.error(f"[Trader API] reset 失败: {e}", exc_info=True)
            return jsonify({'code': 500, 'message': str(e)}), 500
        finally:
            session.close()

    # ------------------------------------------------------------------ POST /archive-logs
    @app.route('/api/simulated-trader/archive-logs', methods=['POST'])
    @require_admin_access(action='simulated_trader.archive_logs')
    def simulated_trader_archive_logs():
        """按保留天数归档旧思考日志"""
        body = request.get_json(silent=True) or {}
        retention_days = body.get('retention_days')
        dry_run = bool(body.get('dry_run', False))

        try:
            result = archive_old_decision_logs(DEFAULT_TRADER_ID, retention_days=retention_days, dry_run=dry_run)
            log_admin_audit(
                'simulated_trader.archive_logs',
                'success',
                f"dry_run={dry_run} retention_days={result['retention_days']} archived={result['archived_logs']}",
            )
            return jsonify({'code': 200, 'data': result})
        except Exception as e:
            logger.error(f"[Trader API] archive-logs 失败: {e}", exc_info=True)
            return jsonify({'code': 500, 'message': str(e)}), 500
