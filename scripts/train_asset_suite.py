#!/usr/bin/env python3
"""
统一资产训练编排脚本

按优先级顺序执行训练：
A股 -> 基金 -> 黄金 -> 白银 -> ETF -> 港股 -> 美股

用途：
1. 先检查已有模型文件是否存在
2. 已存在则在原脚本基础上继续优化/重训
3. 缺失则补齐对应训练入口
4. 保持项目训练结构完整、可维护
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SCRIPTS_DIR = PROJECT_ROOT / 'scripts'
MODELS_DIR = PROJECT_ROOT / 'data' / 'models'
PROGRESS_FILE = MODELS_DIR / 'training_progress.json'
LEARNING_STATUS_FILE = PROJECT_ROOT / 'data' / 'cache' / 'learning_loop_status.json'


def _default_learning_status() -> Dict[str, Any]:
    return {
        'updated_at': datetime.now().isoformat(),
        'last_review': {'status': 'idle', 'message': '暂无复盘记录'},
        'last_reflection': {'status': 'idle', 'has_adjustments': False, 'adjustments_count': 0, 'retrain_targets': []},
        'last_retrain': {'status': 'idle', 'periods': [], 'assets': [], 'results': {}},
    }


def _load_learning_status() -> Dict[str, Any]:
    try:
        LEARNING_STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not LEARNING_STATUS_FILE.exists():
            return _default_learning_status()
        raw = json.loads(LEARNING_STATUS_FILE.read_text(encoding='utf-8'))
        if not isinstance(raw, dict):
            return _default_learning_status()
        merged = _default_learning_status()
        merged.update(raw)
        return merged
    except Exception:
        return _default_learning_status()


def _update_learning_status(section: str, payload: Dict[str, Any]) -> None:
    try:
        status = _load_learning_status()
        if section not in status or not isinstance(status.get(section), dict):
            status[section] = {}
        status[section].update(payload or {})
        status['updated_at'] = datetime.now().isoformat()
        LEARNING_STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
        LEARNING_STATUS_FILE.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding='utf-8')
    except Exception as exc:
        print(f'⚠️  写入学习闭环状态失败: {exc}')


def _run_post_training_review() -> Dict[str, Any]:
    """训练完成后立即检查一次复盘闭环。"""
    try:
        from reviews.reviewer import Reviewer
        reviewer = Reviewer()
        try:
            reviewed_count = int(reviewer.check_expired_predictions() or 0)
        finally:
            reviewer.close()
        return {
            'status': 'success',
            'reviewed_count': reviewed_count,
            'message': f'训练完成后已执行复盘检查，处理 {reviewed_count} 条到期预测'
        }
    except Exception as exc:
        return {
            'status': 'failed',
            'reviewed_count': 0,
            'message': f'训练后复盘检查失败: {exc}'
        }


def _pending_post_training_state() -> Dict[str, Any]:
    return {
        'post_training_review': {
            'status': 'pending',
            'reviewed_count': 0,
            'message': '训练中，复盘待执行',
        },
        'post_training_reflection': {
            'status': 'pending',
            'analysis': {},
            'adjustments': {},
            'retrain_targets': [],
            'auto_request': {'enabled': False, 'only_assets': [], 'periods': [], 'summary': '训练中'},
            'message': '训练中，反思待执行',
        },
        'post_training_auto_optimization': {
            'status': 'pending',
            'message': '训练中，等待反思结果',
            'request': {'enabled': False, 'only_assets': [], 'periods': [], 'summary': '训练中'},
            'results': [],
        },
        'finished_at': None,
    }


def _write_training_progress(payload: Dict[str, Any]) -> None:
    """将训练进度持久化，供前端实时展示。"""
    try:
        MODELS_DIR.mkdir(parents=True, exist_ok=True)
        current: Dict[str, Any] = {}
        if PROGRESS_FILE.exists():
            try:
                current = json.loads(PROGRESS_FILE.read_text(encoding='utf-8'))
                if not isinstance(current, dict):
                    current = {}
            except Exception:
                current = {}

        merged = {
            **current,
            **(payload or {}),
            'updated_at': datetime.now().isoformat(),
        }
        PROGRESS_FILE.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding='utf-8')
    except Exception as exc:
        print(f'⚠️  写入训练进度失败: {exc}')


ASSET_TRAINING_ORDER: List[Dict[str, Any]] = [
    {
        'asset_type': 'a_stock',
        'label': 'A股',
        'script': 'train_a_stock.py',
        'args': [],
        'expected_models': ['short_term_model.pkl', 'medium_term_model.pkl', 'long_term_model.pkl'],
    },
    {
        'asset_type': 'fund',
        'label': '基金',
        'script': 'train_fund.py',
        'args': [],
        'expected_models': ['fund_model.pkl'],
    },
    {
        'asset_type': 'gold',
        'label': '黄金',
        'script': 'train_gold.py',
        'args': ['--asset', 'gold', '--periods', '5,20,60'],
        'expected_models': ['gold_short_term_model.pkl', 'gold_medium_term_model.pkl', 'gold_long_term_model.pkl'],
    },
    {
        'asset_type': 'silver',
        'label': '白银',
        'script': 'train_gold.py',
        'args': ['--asset', 'silver', '--periods', '5,20,60'],
        'expected_models': ['silver_short_term_model.pkl', 'silver_medium_term_model.pkl', 'silver_long_term_model.pkl'],
    },
    {
        'asset_type': 'etf',
        'label': 'ETF',
        'script': 'train_etf.py',
        'args': ['--periods', '5,20,60'],
        'expected_models': ['etf_short_term_model.pkl', 'etf_medium_term_model.pkl', 'etf_long_term_model.pkl'],
    },
    {
        'asset_type': 'hk_stock',
        'label': '港股',
        'script': 'train_hk_stock.py',
        'args': [],
        'expected_models': ['hk_stock_short_term_model.pkl', 'hk_stock_medium_term_model.pkl', 'hk_stock_long_term_model.pkl'],
    },
    {
        'asset_type': 'us_stock',
        'label': '美股',
        'script': 'train_us_stock.py',
        'args': [],
        'expected_models': ['us_stock_short_term_model.pkl', 'us_stock_medium_term_model.pkl', 'us_stock_long_term_model.pkl'],
    },
]


def _normalize_asset_name(asset: str) -> str:
    raw = str(asset or '').strip().lower()
    mapping = {
        'a': 'a_stock',
        'a_stock': 'a_stock',
        'stock': 'a_stock',
        'fund': 'fund',
        'funds': 'fund',
        'gold': 'gold',
        'silver': 'silver',
        'etf': 'etf',
        'hk': 'hk_stock',
        'hk_stock': 'hk_stock',
        'us': 'us_stock',
        'us_stock': 'us_stock',
    }
    return mapping.get(raw, raw)


def _normalize_periods(periods: Optional[List[str] | str]) -> List[int]:
    if periods in (None, '', []):
        return []
    if isinstance(periods, (str, int)):
        raw_items = str(periods).split(',')
    else:
        raw_items = list(periods)

    normalized: List[int] = []
    for item in raw_items:
        try:
            period = int(str(item).strip())
        except Exception:
            continue
        if period in (5, 20, 60) and period not in normalized:
            normalized.append(period)
    return sorted(normalized)


def build_training_plan(only_assets: Optional[List[str]] = None, include_late_markets: bool = True, periods: Optional[List[str] | str] = None) -> List[Dict[str, Any]]:
    """构建统一训练计划。"""
    selected = {_normalize_asset_name(item) for item in (only_assets or []) if str(item).strip()}
    selected_periods = _normalize_periods(periods)
    late_markets = {'hk_stock', 'us_stock'}
    plan: List[Dict[str, Any]] = []

    for step in ASSET_TRAINING_ORDER:
        if not include_late_markets and step['asset_type'] in late_markets:
            continue
        if selected and step['asset_type'] not in selected:
            continue

        entry = dict(step)
        entry_args = list(step.get('args') or [])
        if selected_periods and step['asset_type'] != 'fund':
            period_text = ','.join(str(p) for p in selected_periods)
            if '--periods' in entry_args:
                idx = entry_args.index('--periods')
                if idx + 1 < len(entry_args):
                    entry_args[idx + 1] = period_text
                else:
                    entry_args.extend(['--periods', period_text])
            elif len(selected_periods) == 1:
                entry_args.extend(['--period', str(selected_periods[0])])
            else:
                entry_args.extend(['--periods', period_text])
            entry['label'] = f"{step['label']}（{'/'.join(f'{p}日' for p in selected_periods)}）"
        else:
            entry['label'] = step['label']
        entry['args'] = entry_args
        script_path = SCRIPTS_DIR / step['script']
        existing_models = [name for name in step.get('expected_models', []) if (MODELS_DIR / name).exists()]
        missing_models = [name for name in step.get('expected_models', []) if name not in existing_models]

        entry['script_path'] = str(script_path)
        entry['script_exists'] = script_path.exists()
        entry['existing_models'] = existing_models
        entry['missing_models'] = missing_models
        entry['mode'] = 'optimize' if existing_models else 'bootstrap'
        plan.append(entry)

    return plan


def _build_auto_optimization_request(retrain_targets, analysis=None, max_assets=3):
    """把反思建议转换为一次有限、可控的自动优化请求。"""
    analysis = analysis or {}
    asset_counts = analysis.get('by_asset') or {}
    period_counts = analysis.get('by_period') or {}

    selected_assets: List[str] = []
    selected_periods = set()
    global_requested = False

    for period, asset in retrain_targets or []:
        normalized_asset = _normalize_asset_name(asset) if asset not in (None, '', 'all') else 'all'
        if normalized_asset != 'all' and normalized_asset not in selected_assets:
            selected_assets.append(normalized_asset)
        elif asset == 'all':
            global_requested = True

        if period not in (None, '', 'all'):
            try:
                period_val = int(period)
            except Exception:
                period_val = None
            if period_val in (5, 20, 60):
                selected_periods.add(period_val)
        elif period == 'all':
            global_requested = True

    if global_requested:
        ranked_assets = [
            _normalize_asset_name(asset)
            for asset, _count in sorted(asset_counts.items(), key=lambda item: item[1], reverse=True)
            if str(asset).strip()
        ]
        for asset in ranked_assets:
            if asset != 'all' and asset not in selected_assets:
                selected_assets.append(asset)
            if len(selected_assets) >= max_assets:
                break

        if not selected_periods:
            ranked_periods = []
            for period, _count in sorted(period_counts.items(), key=lambda item: item[1], reverse=True):
                try:
                    period_val = int(period)
                except Exception:
                    continue
                if period_val in (5, 20, 60) and period_val not in ranked_periods:
                    ranked_periods.append(period_val)
            selected_periods.update(ranked_periods[:2] or [5])

    selected_assets = selected_assets[:max_assets]
    period_list = sorted(selected_periods)
    summary_parts = []
    if selected_assets:
        summary_parts.append('资产=' + '、'.join(selected_assets))
    if period_list:
        summary_parts.append('周期=' + '、'.join(f'{p}日' for p in period_list))

    return {
        'enabled': bool(selected_assets or period_list),
        'only_assets': selected_assets,
        'periods': period_list,
        'summary': '；'.join(summary_parts) if summary_parts else '暂无自动优化目标',
    }


def _run_post_training_reflection() -> Dict[str, Any]:
    """训练完成后执行一次反思学习，并输出自动优化请求。"""
    try:
        from reviews.reflection import ReflectionLearner

        learner = ReflectionLearner()
        try:
            analysis = learner.analyze_errors(days=30)
            adjustments = learner.update_model_weights() or {}
            retrain_targets = learner.check_retrain_needed() or []
            if analysis.get('total_predictions', 0) > 0 or sum((analysis.get('pending_by_period') or {}).values()) > 0:
                learner.save_insight(analysis, days_analyzed=30)
        finally:
            learner.close()

        auto_request = _build_auto_optimization_request(retrain_targets, analysis=analysis)
        _update_learning_status('last_reflection', {
            'time': datetime.now().isoformat(),
            'status': 'success',
            'has_adjustments': bool(adjustments),
            'adjustments_count': len(adjustments),
            'retrain_targets': [{'period': p, 'asset': a} for p, a in retrain_targets],
            'auto_request': auto_request,
            'message': f"已完成训练后反思，识别 {len(retrain_targets)} 个重训目标",
        })
        return {
            'status': 'success',
            'analysis': analysis,
            'adjustments': adjustments,
            'retrain_targets': retrain_targets,
            'auto_request': auto_request,
        }
    except Exception as exc:
        _update_learning_status('last_reflection', {
            'time': datetime.now().isoformat(),
            'status': 'failed',
            'message': f'训练后反思失败: {exc}',
        })
        return {
            'status': 'failed',
            'analysis': {},
            'adjustments': {},
            'retrain_targets': [],
            'auto_request': {'enabled': False, 'only_assets': [], 'periods': [], 'summary': '反思失败'},
            'message': str(exc),
        }


def print_training_plan(plan: List[Dict[str, Any]]) -> None:
    print('\n' + '=' * 72)
    print('统一资产训练计划')
    print('=' * 72)
    print(f'生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

    for idx, step in enumerate(plan, start=1):
        model_state = '已有模型，执行优化' if step['existing_models'] else '缺少模型，执行补建'
        print(
            f"[{idx}/{len(plan)}] {step['label']} ({step['asset_type']}) | "
            f"script={step['script']} | {model_state}"
        )
        if step['existing_models']:
            print(f"    已有: {', '.join(step['existing_models'])}")
        if step['missing_models']:
            print(f"    待补: {', '.join(step['missing_models'])}")
        if not step['script_exists']:
            print('    ⚠️  训练脚本缺失')

    print('=' * 72)


def run_training_plan(
    plan: Optional[List[Dict[str, Any]]] = None,
    *,
    dry_run: bool = False,
    stop_on_error: bool = False,
    skip_existing: bool = False,
    enable_self_optimization: bool = True,
) -> List[Dict[str, Any]]:
    """按顺序运行训练计划。"""
    plan = plan or build_training_plan()
    results: List[Dict[str, Any]] = []
    total_steps = len(plan)

    _write_training_progress({
        'status': 'running',
        'total_steps': total_steps,
        'completed_steps': 0,
        'current_step': 0,
        'progress_percent': 0.0,
        'current_asset': '准备中',
        'current_stage': 'prepare',
        'message': '正在准备全量模型训练任务',
        'started_at': datetime.now().isoformat(),
        'pid': os.getpid(),
        'results': [],
        **_pending_post_training_state(),
    })

    for idx, step in enumerate(plan, start=1):
        result = {
            'asset_type': step['asset_type'],
            'label': step['label'],
            'script': step['script'],
            'status': 'pending',
            'returncode': None,
        }

        _write_training_progress({
            'status': 'running',
            'total_steps': total_steps,
            'completed_steps': idx - 1,
            'current_step': idx,
            'progress_percent': round(((idx - 1) / total_steps) * 100, 1) if total_steps else 0.0,
            'current_asset': step['label'],
            'current_asset_type': step['asset_type'],
            'current_stage': step.get('mode', 'optimize'),
            'message': f"正在训练 {step['label']} 模型",
            'pid': os.getpid(),
            'results': results,
            **_pending_post_training_state(),
        })

        if skip_existing and not step['missing_models']:
            result['status'] = 'skipped'
            results.append(result)
            _write_training_progress({
                'status': 'running',
                'total_steps': total_steps,
                'completed_steps': idx,
                'current_step': idx,
                'progress_percent': round((idx / total_steps) * 100, 1) if total_steps else 100.0,
                'current_asset': step['label'],
                'current_asset_type': step['asset_type'],
                'current_stage': 'skipped',
                'message': f"已跳过 {step['label']}（模型已存在）",
                'results': results,
                **_pending_post_training_state(),
            })
            print(f"⏭️  [{idx}/{len(plan)}] 跳过 {step['label']}：模型已存在")
            continue

        if not step['script_exists']:
            result['status'] = 'failed'
            result['error'] = 'training script not found'
            results.append(result)
            _write_training_progress({
                'status': 'failed' if stop_on_error else 'running',
                'total_steps': total_steps,
                'completed_steps': idx,
                'current_step': idx,
                'progress_percent': round((idx / total_steps) * 100, 1) if total_steps else 100.0,
                'current_asset': step['label'],
                'current_asset_type': step['asset_type'],
                'current_stage': 'failed',
                'message': f"{step['label']} 训练脚本缺失",
                'results': results,
                **_pending_post_training_state(),
            })
            print(f"❌ [{idx}/{len(plan)}] {step['label']} 失败：脚本不存在 {step['script']}")
            if stop_on_error:
                break
            continue

        cmd = [sys.executable, str(step['script_path']), *(step.get('args') or [])]
        print(f"🚀 [{idx}/{len(plan)}] 开始{step['mode']} {step['label']} 模型")
        print(f"    命令: {' '.join(cmd)}")

        if dry_run:
            result['status'] = 'planned'
            results.append(result)
            continue

        completed = subprocess.run(cmd, cwd=str(PROJECT_ROOT), check=False)
        result['returncode'] = int(completed.returncode)
        result['status'] = 'success' if completed.returncode == 0 else 'failed'
        results.append(result)

        if result['status'] == 'success':
            print(f"✅ [{idx}/{len(plan)}] {step['label']} 完成")
        else:
            print(f"❌ [{idx}/{len(plan)}] {step['label']} 失败，退出码={completed.returncode}")

        _write_training_progress({
            'status': 'failed' if (result['status'] != 'success' and stop_on_error) else 'running',
            'total_steps': total_steps,
            'completed_steps': idx,
            'current_step': idx,
            'progress_percent': round((idx / total_steps) * 100, 1) if total_steps else 100.0,
            'current_asset': step['label'],
            'current_asset_type': step['asset_type'],
            'current_stage': result['status'],
            'message': f"{step['label']} {'完成' if result['status'] == 'success' else '失败'}",
            'results': results,
            **_pending_post_training_state(),
        })

        if stop_on_error and result['status'] != 'success':
            break

    final_failed_count = sum(1 for item in results if item.get('status') == 'failed')
    _write_training_progress({
        'status': 'running' if final_failed_count == 0 else 'failed',
        'total_steps': total_steps,
        'completed_steps': len(results),
        'current_step': len(results),
        'progress_percent': round((len(results) / total_steps) * 100, 1) if total_steps else 100.0,
        'current_asset': results[-1]['label'] if results else '无',
        'current_stage': 'reviewing',
        'message': '模型训练完成，正在执行复盘检查' if final_failed_count == 0 else f'训练完成，但有 {final_failed_count} 个资产失败',
        'results': results,
        **_pending_post_training_state(),
    })

    review_result = _run_post_training_review() if final_failed_count == 0 else {
        'status': 'skipped',
        'reviewed_count': 0,
        'message': '因训练失败，跳过自动复盘检查'
    }
    print(review_result.get('message'))

    reflection_result = {
        'status': 'skipped',
        'analysis': {},
        'adjustments': {},
        'retrain_targets': [],
        'auto_request': {'enabled': False, 'only_assets': [], 'periods': [], 'summary': '未启用'},
    }
    auto_optimization_result = {
        'status': 'skipped',
        'message': '未触发自动优化',
        'request': {'enabled': False, 'only_assets': [], 'periods': [], 'summary': '未触发'},
        'results': [],
    }

    if final_failed_count == 0:
        reflection_result = _run_post_training_reflection()
        auto_request = reflection_result.get('auto_request') or {}
        if enable_self_optimization and auto_request.get('enabled'):
            optimization_plan = build_training_plan(
                only_assets=auto_request.get('only_assets') or None,
                periods=auto_request.get('periods') or None,
                include_late_markets=True,
            )
            if optimization_plan:
                print(f"🧠 训练后自动优化已启动: {auto_request.get('summary')}")
                auto_results = run_training_plan(
                    optimization_plan,
                    dry_run=False,
                    stop_on_error=stop_on_error,
                    skip_existing=False,
                    enable_self_optimization=False,
                )
                auto_optimization_result = {
                    'status': 'success',
                    'message': '已根据反思结果自动执行一轮定向优化',
                    'request': auto_request,
                    'results': auto_results,
                }
                _update_learning_status('last_retrain', {
                    'time': datetime.now().isoformat(),
                    'status': 'success',
                    'periods': auto_request.get('periods', []),
                    'assets': auto_request.get('only_assets', []),
                    'results': auto_results,
                    'message': auto_optimization_result['message'],
                })
            else:
                auto_optimization_result = {
                    'status': 'skipped',
                    'message': '反思已完成，但未生成有效优化计划',
                    'request': auto_request,
                    'results': [],
                }
                _update_learning_status('last_retrain', {
                    'time': datetime.now().isoformat(),
                    'status': 'skipped',
                    'periods': auto_request.get('periods', []),
                    'assets': auto_request.get('only_assets', []),
                    'results': {},
                    'message': auto_optimization_result['message'],
                })
        else:
            auto_optimization_result = {
                'status': 'skipped',
                'message': '训练后反思未发现需要自动优化的明确目标',
                'request': auto_request,
                'results': [],
            }
            _update_learning_status('last_retrain', {
                'time': datetime.now().isoformat(),
                'status': 'skipped',
                'periods': auto_request.get('periods', []),
                'assets': auto_request.get('only_assets', []),
                'results': {},
                'message': auto_optimization_result['message'],
            })

    _write_training_progress({
        'status': 'completed' if final_failed_count == 0 else 'failed',
        'total_steps': total_steps,
        'completed_steps': len(results),
        'current_step': len(results),
        'progress_percent': round((len(results) / total_steps) * 100, 1) if total_steps else 100.0,
        'current_asset': results[-1]['label'] if results else '无',
        'current_stage': 'finished',
        'message': '训练、复盘与自动优化已完成' if final_failed_count == 0 else f'训练完成，但有 {final_failed_count} 个资产失败',
        'results': results,
        'post_training_review': review_result,
        'post_training_reflection': reflection_result,
        'post_training_auto_optimization': auto_optimization_result,
        'finished_at': datetime.now().isoformat(),
    })

    return results


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='统一资产训练编排脚本')
    parser.add_argument('--only', help='仅训练指定资产，多个用逗号分隔，例如 a_stock,fund,etf')
    parser.add_argument('--periods', help='仅训练指定周期，支持 5、20、60，多个用逗号分隔')
    parser.add_argument('--without-late-markets', action='store_true', help='先不训练港股和美股')
    parser.add_argument('--skip-existing', action='store_true', help='对已有模型的资产直接跳过')
    parser.add_argument('--plan-only', action='store_true', help='只展示计划，不实际执行')
    parser.add_argument('--stop-on-error', action='store_true', help='遇到失败立即停止')
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    only_assets = [item.strip() for item in (args.only or '').split(',') if item.strip()]
    plan = build_training_plan(
        only_assets=only_assets,
        include_late_markets=not bool(args.without_late_markets),
        periods=args.periods,
    )

    print_training_plan(plan)
    results = run_training_plan(
        plan,
        dry_run=bool(args.plan_only),
        stop_on_error=bool(args.stop_on_error),
        skip_existing=bool(args.skip_existing),
    )

    failed_count = sum(1 for item in results if item.get('status') == 'failed')
    skipped_count = sum(1 for item in results if item.get('status') == 'skipped')
    success_count = sum(1 for item in results if item.get('status') == 'success')
    planned_count = sum(1 for item in results if item.get('status') == 'planned')

    print('\n' + '=' * 72)
    print(
        f'训练摘要: success={success_count}, failed={failed_count}, '
        f'skipped={skipped_count}, planned={planned_count}'
    )
    print('=' * 72)

    return 0 if failed_count == 0 else 1


if __name__ == '__main__':
    raise SystemExit(main())
