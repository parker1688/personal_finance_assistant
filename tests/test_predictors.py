"""
预测模块测试 - tests/test_predictors.py
测试预测模型功能
"""

import sys
import os
import json
import unittest
import tempfile
import pickle
import importlib.util
import types
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from predictors.short_term import ShortTermPredictor
from predictors.medium_term import MediumTermPredictor
from predictors.long_term import LongTermPredictor
from predictors.model_manager import ModelManager
from predictors.model_trainer import ModelTrainer
from recommendation_probability import derive_unified_trend
from recommenders.stock_recommender import StockRecommender
from recommenders.fund_recommender import FundRecommender
from reviews.backtest_validator import BacktestValidator
from models import get_session, ModelVersion
from api.holdings import (
    _build_portfolio_health_summary,
    _normalize_fund_code_variants,
    _select_diversified_unheld_recommendations,
)
from api.model import _select_current_model_version
from api import model as model_api_module
from api import logs as logs_api_module
from api import reviews as reviews_api_module
from api.reviews import _extract_version_metrics
from api.dashboard import _build_advisor_workflow
from api.recommendations import (
    _build_horizon_top_picks,
    _build_holding_recommendation,
    _build_strategy_framework,
    _build_detail_recommendation_rationale,
    _build_recommendation_quality_gate,
    _build_data_quality_summary,
    _build_recommendation_advisor_payload,
    _build_asset_model_status,
    _classify_recommendation_strength,
)


def create_test_data(days=200):
    """创建测试数据"""
    dates = pd.date_range(start='2024-01-01', periods=days, freq='D')
    np.random.seed(42)
    
    price = 100
    prices = []
    for i in range(days):
        price = price + np.random.normal(0, 1)
        prices.append(max(price, 50))
    
    df = pd.DataFrame({
        'open': prices,
        'high': [p * (1 + abs(np.random.normal(0, 0.02))) for p in prices],
        'low': [p * (1 - abs(np.random.normal(0, 0.02))) for p in prices],
        'close': prices,
        'volume': np.random.randint(1000000, 10000000, days)
    }, index=dates)
    
    return df


class TestFundAndModelHelpers(unittest.TestCase):
    """基金与模型监控辅助逻辑测试"""

    def test_normalize_fund_code_variants_includes_of_suffix(self):
        variants = _normalize_fund_code_variants('009478')
        self.assertIn('009478', variants)
        self.assertIn('009478.OF', variants)

    def test_select_current_model_version_falls_back_to_latest(self):
        from types import SimpleNamespace
        from datetime import date, datetime

        versions = [
            SimpleNamespace(version='v_old', is_active=False, train_date=date(2026, 4, 15), created_at=datetime(2026, 4, 15, 8, 0), validation_accuracy=0.55),
            SimpleNamespace(version='v_new', is_active=False, train_date=date(2026, 4, 16), created_at=datetime(2026, 4, 16, 12, 0), validation_accuracy=0.60),
        ]
        current, source = _select_current_model_version(versions)
        self.assertEqual(current.version, 'v_new')
        self.assertEqual(source, 'latest_fallback')

    def test_fund_recommender_fallback_pool_is_non_empty(self):
        recommender = FundRecommender.__new__(FundRecommender)
        pool = recommender._get_fallback_fund_pool(limit=5)
        self.assertGreaterEqual(len(pool), 1)
        self.assertIn('code', pool[0])

    def test_extract_version_metrics_reads_model_file_metadata(self):
        with tempfile.NamedTemporaryFile(suffix='.pkl', delete=False) as tmp:
            pickle.dump({'metadata': {
                'validation_accuracy': 0.612,
                'validation_f1': 0.561,
                'validation_auc': 0.689,
                'validation_brier': 0.208,
            }}, tmp)
            tmp_path = tmp.name

        try:
            from types import SimpleNamespace
            version = SimpleNamespace(model_path=tmp_path, params=None, validation_accuracy=0.55)
            metrics = _extract_version_metrics(version)
            self.assertAlmostEqual(metrics['accuracy'], 0.612, places=3)
            self.assertAlmostEqual(metrics['f1'], 0.561, places=3)
            self.assertAlmostEqual(metrics['auc'], 0.689, places=3)
            self.assertAlmostEqual(metrics['brier'], 0.208, places=3)
        finally:
            os.remove(tmp_path)

    def test_build_current_runtime_summary_lists_each_horizon(self):
        recent_versions = [
            {'version': 'v5_latest', 'period_days': 5},
            {'version': 'v20_latest', 'period_days': 20},
            {'version': 'v60_latest', 'period_days': 60},
        ]
        summary, current_map = model_api_module._build_current_runtime_summary(recent_versions, runtime_metadata_map={})
        self.assertIn('5日:v5_latest', summary)
        self.assertIn('20日:v20_latest', summary)
        self.assertIn('60日:v60_latest', summary)
        self.assertEqual(current_map[5], 'v5_latest')

    def test_load_asset_training_results_covers_multi_asset_models(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fixtures = {
                'short_term_model.pkl': {'asset_type': 'a_stock', 'period_days': 5, 'train_date': '2026-04-17T15:58:22', 'val_accuracy': 0.7624},
                'fund_model.pkl': {'asset_type': 'fund', 'train_date': '2026-04-17T16:09:15', 'val_score': 0.9966},
                'etf_model.pkl': {'asset_type': 'etf', 'period_days': 10, 'train_date': '2026-04-17T16:26:27', 'val_accuracy': 0.5782},
                'hk_stock_short_term_model.pkl': {'asset_type': 'hk_stock', 'period_days': 5, 'train_date': '2026-04-17T16:26:29', 'val_accuracy': 0.6286},
            }
            for name, payload in fixtures.items():
                with open(os.path.join(tmpdir, name), 'wb') as f:
                    pickle.dump(payload, f)

            results = model_api_module._load_asset_training_results(tmpdir)
            asset_labels = {item['asset_label'] for item in results}
            self.assertIn('A股', asset_labels)
            self.assertIn('基金', asset_labels)
            self.assertIn('ETF', asset_labels)
            self.assertIn('港股', asset_labels)
            fund_row = next(item for item in results if item['asset_type'] == 'fund')
            self.assertEqual(fund_row['metric_label'], '历史回测R²')
            self.assertAlmostEqual(fund_row['metric_value'], 99.66, places=2)

    def test_load_asset_training_results_includes_multi_horizon_precious_metals_and_etf(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fixtures = {
                'gold_short_term_model.pkl': {'asset_type': 'gold', 'period_days': 5, 'train_date': '2026-04-17T20:44:44', 'val_accuracy': 0.6562},
                'gold_medium_term_model.pkl': {'asset_type': 'gold', 'period_days': 20, 'train_date': '2026-04-17T20:44:45', 'val_accuracy': 0.6120},
                'silver_long_term_model.pkl': {'asset_type': 'silver', 'period_days': 60, 'train_date': '2026-04-17T20:44:46', 'val_accuracy': 0.7350},
                'etf_short_term_model.pkl': {'asset_type': 'etf', 'period_days': 5, 'train_date': '2026-04-17T20:55:09', 'val_accuracy': 0.5582},
                'etf_medium_term_model.pkl': {'asset_type': 'etf', 'period_days': 20, 'train_date': '2026-04-17T20:55:10', 'val_accuracy': 0.5982},
                'etf_long_term_model.pkl': {'asset_type': 'etf', 'period_days': 60, 'train_date': '2026-04-17T20:55:11', 'val_accuracy': 0.6382},
            }
            for name, payload in fixtures.items():
                with open(os.path.join(tmpdir, name), 'wb') as f:
                    pickle.dump(payload, f)

            results = model_api_module._load_asset_training_results(tmpdir)
            gold_periods = sorted(item['period_days'] for item in results if item['asset_type'] == 'gold')
            silver_periods = sorted(item['period_days'] for item in results if item['asset_type'] == 'silver')
            etf_periods = sorted(item['period_days'] for item in results if item['asset_type'] == 'etf')

            self.assertEqual(gold_periods, [5, 20])
            self.assertEqual(silver_periods, [60])
            self.assertEqual(etf_periods, [5, 20, 60])

    def test_load_asset_training_results_marks_overfit_risk_when_gap_is_large(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, 'us_stock_medium_term_model.pkl'), 'wb') as f:
                pickle.dump({
                    'asset_type': 'us_stock',
                    'period_days': 20,
                    'train_date': '2026-04-17T16:26:31',
                    'train_accuracy': 1.0,
                    'val_accuracy': 0.5375,
                }, f)

            results = model_api_module._load_asset_training_results(tmpdir)
            row = next(item for item in results if item['asset_type'] == 'us_stock')
            self.assertEqual(row['overfit_risk'], 'high')
            self.assertAlmostEqual(row['generalization_gap'], 46.25, places=2)
            self.assertEqual(row['status'], 'warning')

    def test_train_asset_suite_build_plan_supports_single_period(self):
        spec = importlib.util.spec_from_file_location(
            'train_asset_suite_module',
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scripts', 'train_asset_suite.py')
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        plan = module.build_training_plan(only_assets=['us_stock'], periods=['5'])
        self.assertEqual(len(plan), 1)
        self.assertEqual(plan[0]['asset_type'], 'us_stock')
        self.assertIn('--period', plan[0]['args'])
        self.assertIn('5', plan[0]['args'])

    def test_train_asset_suite_builds_auto_optimization_request_from_reflection(self):
        spec = importlib.util.spec_from_file_location(
            'train_asset_suite_module',
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scripts', 'train_asset_suite.py')
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        request = module._build_auto_optimization_request(
            retrain_targets=[('all', 'all'), (5, 'all'), ('all', 'etf')],
            analysis={
                'by_asset': {'etf': 12, 'us_stock': 8, 'hk_stock': 4},
                'by_period': {5: 10, 20: 3, 60: 1},
            },
            max_assets=2,
        )

        self.assertIn('etf', request['only_assets'])
        self.assertIn(5, request['periods'])
        self.assertLessEqual(len(request['only_assets']), 2)

    def test_train_asset_suite_adds_project_root_to_sys_path_for_direct_execution(self):
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        script_path = os.path.join(project_root, 'scripts', 'train_asset_suite.py')
        original_sys_path = list(sys.path)
        try:
            sys.path[:] = [
                item for item in sys.path
                if os.path.abspath(item or os.getcwd()) != project_root
            ]
            spec = importlib.util.spec_from_file_location('train_asset_suite_runtime_module', script_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            normalized_paths = {os.path.abspath(item or os.getcwd()) for item in sys.path}
            self.assertIn(project_root, normalized_paths)
        finally:
            sys.path[:] = original_sys_path

    def test_train_asset_suite_running_progress_clears_stale_post_training_state(self):
        spec = importlib.util.spec_from_file_location(
            'train_asset_suite_runtime_module',
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scripts', 'train_asset_suite.py')
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        with tempfile.TemporaryDirectory() as tmpdir:
            module.MODELS_DIR = Path(tmpdir)
            module.PROGRESS_FILE = Path(tmpdir) / 'training_progress.json'
            module.PROGRESS_FILE.write_text(json.dumps({
                'post_training_review': {'status': 'failed', 'message': 'stale'},
                'post_training_reflection': {'status': 'failed', 'message': 'stale'},
                'post_training_auto_optimization': {'status': 'failed', 'message': 'stale'},
                'finished_at': 'stale-finished-at',
            }, ensure_ascii=False), encoding='utf-8')

            observed = {}
            original_run = module.subprocess.run

            def fake_run(cmd, cwd=None, check=False):
                observed.update(json.loads(module.PROGRESS_FILE.read_text(encoding='utf-8')))
                return types.SimpleNamespace(returncode=0)

            module.subprocess.run = fake_run
            try:
                module.run_training_plan([
                    {
                        'asset_type': 'a_stock',
                        'label': 'A股',
                        'script': 'train_a_stock.py',
                        'script_exists': True,
                        'script_path': Path(tmpdir) / 'train_a_stock.py',
                        'args': [],
                        'mode': 'optimize',
                        'existing_models': [],
                        'missing_models': [],
                    }
                ], enable_self_optimization=False)
            finally:
                module.subprocess.run = original_run

            self.assertEqual(observed['post_training_review']['status'], 'pending')
            self.assertEqual(observed['post_training_reflection']['status'], 'pending')
            self.assertEqual(observed['post_training_auto_optimization']['status'], 'pending')
            self.assertIsNone(observed['finished_at'])

    def test_scheduler_continuous_learning_cycle_uses_reflection_targets(self):
        import scheduler as scheduler_module
        core_module = getattr(scheduler_module, '_scheduler_module', scheduler_module)

        with tempfile.TemporaryDirectory() as tmpdir:
            status_path = Path(tmpdir) / 'learning_loop_status.json'
            progress_path = Path(tmpdir) / 'training_progress.json'
            status_path.write_text(json.dumps({
                'last_reflection': {
                    'status': 'success',
                    'auto_request': {
                        'enabled': True,
                        'only_assets': ['etf', 'us_stock'],
                        'periods': [5],
                    },
                },
                'last_retrain': {
                    'status': 'idle',
                },
            }, ensure_ascii=False), encoding='utf-8')
            progress_path.write_text(json.dumps({'status': 'completed'}, ensure_ascii=False), encoding='utf-8')

            old_status_path = getattr(core_module, 'LEARNING_STATUS_FILE', None)
            old_progress_path = getattr(core_module, 'TRAINING_PROGRESS_FILE', None)
            old_suite_module = sys.modules.get('scripts.train_asset_suite')
            captured = {}

            core_module.LEARNING_STATUS_FILE = status_path
            core_module.TRAINING_PROGRESS_FILE = progress_path
            sys.modules['scripts.train_asset_suite'] = types.SimpleNamespace(
                build_training_plan=lambda only_assets=None, periods=None, include_late_markets=True: captured.update({
                    'only_assets': only_assets,
                    'periods': periods,
                    'include_late_markets': include_late_markets,
                }) or [{
                    'asset_type': 'etf',
                    'label': 'ETF（5日）',
                    'script': 'train_etf.py',
                    'script_exists': True,
                    'script_path': Path(tmpdir) / 'train_etf.py',
                    'args': ['--period', '5'],
                    'mode': 'optimize',
                    'existing_models': [],
                    'missing_models': [],
                }],
                run_training_plan=lambda plan, dry_run=False, stop_on_error=False, skip_existing=False, enable_self_optimization=True: captured.update({
                    'plan': plan,
                    'enable_self_optimization': enable_self_optimization,
                }) or [{'asset_type': 'etf', 'status': 'success'}],
            )

            try:
                result = scheduler_module.run_continuous_learning_cycle(force=True)
            finally:
                if old_status_path is not None:
                    core_module.LEARNING_STATUS_FILE = old_status_path
                if old_progress_path is not None:
                    core_module.TRAINING_PROGRESS_FILE = old_progress_path
                if old_suite_module is not None:
                    sys.modules['scripts.train_asset_suite'] = old_suite_module
                else:
                    sys.modules.pop('scripts.train_asset_suite', None)

            self.assertTrue(result['executed'])
            self.assertEqual(captured['only_assets'], ['etf', 'us_stock'])
            self.assertEqual(captured['periods'], [5])
            self.assertFalse(captured['enable_self_optimization'])

    def test_load_training_progress_reads_runtime_progress_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            progress_path = os.path.join(tmpdir, 'training_progress.json')
            with open(progress_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'status': 'running',
                    'current_step': 2,
                    'total_steps': 7,
                    'current_asset': '基金',
                    'progress_percent': 28.6,
                    'pid': os.getpid(),
                }, f)

            data = model_api_module._load_training_progress(progress_path)
            self.assertEqual(data['status'], 'running')
            self.assertEqual(data['current_step'], 2)
            self.assertEqual(data['total_steps'], 7)
            self.assertEqual(data['current_asset'], '基金')
            self.assertAlmostEqual(data['progress_percent'], 28.6, places=1)

    def test_load_training_progress_marks_dead_pid_as_stopped(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            progress_path = os.path.join(tmpdir, 'training_progress.json')
            with open(progress_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'status': 'running',
                    'pid': 999999,
                    'current_asset': 'A股',
                    'current_step': 1,
                    'total_steps': 7,
                    'message': '正在训练 A股 模型',
                }, f)

            data = model_api_module._load_training_progress(progress_path)
            self.assertEqual(data['status'], 'failed')
            self.assertIn('已停止', data['message'])

    def test_load_runtime_training_logs_filters_xgboost_noise(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            progress_path = os.path.join(tmpdir, 'training_progress.json')
            log_path = os.path.join(tmpdir, 'model_training.log')
            with open(progress_path, 'w', encoding='utf-8') as f:
                json.dump({'status': 'running', 'message': '正在训练 A股 模型', 'current_asset': 'A股', 'current_step': 1, 'total_steps': 7}, f)
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write('/Users/test/.venv/lib/python3.11/site-packages/xgboost/training.py:200: UserWarning\n')
                f.write('Parameters: { "use_label_encoder" } are not used.\n')
                f.write('  bst.update(dtrain, iteration=i, fobj=obj)\n')
                f.write('  ✅ 模型已保存: data/models/short_term_model.pkl\n')

            logs = logs_api_module._load_runtime_training_logs(progress_file=progress_path, log_file=log_path)
            joined = '\n'.join(item['message'] for item in logs)
            self.assertIn('正在训练 A股 模型', joined)
            self.assertIn('模型已保存', joined)
            self.assertNotIn('use_label_encoder', joined)
            self.assertNotIn('bst.update', joined)

    def test_reflection_analysis_includes_reviewed_and_pending_periods(self):
        from reviews.reflection import ReflectionLearner

        learner = ReflectionLearner()
        try:
            analysis = learner.analyze_errors(days=30)
        finally:
            learner.close()

        self.assertIn('reviewed_by_period', analysis)
        self.assertIn('pending_by_period', analysis)
        self.assertEqual(sorted(int(k) for k in analysis['reviewed_by_period'].keys()), [5, 20, 60])
        self.assertEqual(sorted(int(k) for k in analysis['pending_by_period'].keys()), [5, 20, 60])

    def test_optimization_loops_payload_includes_long_term(self):
        payload = reviews_api_module._build_optimization_loops_payload()
        self.assertIn('short_term', payload)
        self.assertIn('medium_term', payload)
        self.assertIn('long_term', payload)

    def test_model_manager_load_runtime_bundle_accepts_legacy_top_level_metrics(self):
        manager = ModelManager()
        with tempfile.NamedTemporaryFile(suffix='.pkl', delete=False) as tmp:
            pickle.dump({
                'model': {'kind': 'legacy-runtime'},
                'feature_columns': ['f1', 'f2'],
                'period_days': 20,
                'val_accuracy': 0.69,
                'train_accuracy': 0.71,
            }, tmp)
            tmp_path = tmp.name

        try:
            bundle = manager.load_runtime_model_bundle(tmp_path, period_days=20)
            self.assertTrue(bundle['loaded'])
            self.assertIsNotNone(bundle['model'])
            self.assertIn(bundle['gate'], {'legacy_top_level_metrics', 'legacy_compat'})
            self.assertAlmostEqual(bundle['metadata']['validation_accuracy'], 0.69, places=3)
            self.assertEqual(bundle['period_days'], 20)
        finally:
            os.remove(tmp_path)


class TestDashboardWorkflow(unittest.TestCase):
    """首页理财师工作流测试"""

    def test_build_advisor_workflow_highlights_monitoring_and_review(self):
        workflow = _build_advisor_workflow(
            today_recommendations={'total': 95, 'a_stock': 20, 'active_fund': 20, 'etf': 10, 'gold': 3},
            portfolio_overview={'holding_count': 7, 'overall_risk': 'medium'},
            warning_stats={'high': 1, 'medium': 2, 'total': 3},
            pending_validation_count=303,
            validated_prediction_count=18,
            overall_accuracy=64.2,
            action_backtest={'has_action_samples': True, 'overall_grade': 'B'}
        )
        self.assertEqual(len(workflow['stages']), 3)
        self.assertIn('预测前', workflow['stages'][0]['title'])
        self.assertIn('持有中', workflow['stages'][1]['title'])
        self.assertIn('到期复盘', workflow['stages'][2]['title'])
        self.assertIn('303', workflow['stages'][2]['metrics'][1]['value'])
        self.assertEqual(workflow['stages'][1]['status'], 'warning')


class TestRecommendationHorizonHelpers(unittest.TestCase):
    """推荐周期与持有建议测试"""

    def test_build_holding_recommendation_prefers_short_term_when_5d_is_strongest(self):
        advice = _build_holding_recommendation({
            'up_probability_5d': 71.2,
            'up_probability_20d': 59.4,
            'up_probability_60d': 56.8,
            'risk_level': 'medium',
            'total_score': 4.1,
        }, asset_type='a_stock')
        self.assertEqual(advice['horizon_key'], 'short')
        self.assertIn('4-8个交易日', advice['holding_period_text'])

    def test_build_holding_recommendation_shortens_window_for_reduce_signal(self):
        advice = _build_holding_recommendation({
            'up_probability_5d': 53.9,
            'up_probability_20d': 47.3,
            'up_probability_60d': 30.7,
            'risk_level': 'high',
            'total_score': 3.46,
            'advisor_action': 'reduce',
        }, asset_type='a_stock')
        self.assertIn('1-3个交易日', advice['holding_period_text'])
        self.assertIn('减仓', advice['reason'])

    def test_build_horizon_top_picks_returns_one_pick_per_horizon(self):
        picks = _build_horizon_top_picks([
            {'code': 'AAA', 'name': '短期强', 'up_probability_5d': 72, 'up_probability_20d': 58, 'up_probability_60d': 52, 'total_score': 4.1, 'risk_level': 'medium'},
            {'code': 'BBB', 'name': '中期强', 'up_probability_5d': 60, 'up_probability_20d': 69, 'up_probability_60d': 61, 'total_score': 4.2, 'risk_level': 'medium'},
            {'code': 'CCC', 'name': '长期强', 'up_probability_5d': 54, 'up_probability_20d': 61, 'up_probability_60d': 74, 'total_score': 4.0, 'risk_level': 'low'},
        ])
        self.assertEqual(picks['short']['code'], 'AAA')
        self.assertEqual(picks['medium']['code'], 'BBB')
        self.assertEqual(picks['long']['code'], 'CCC')
        self.assertIn('4-8个交易日', picks['short']['holding_period_text'])
        self.assertIn('12-25个交易日', picks['medium']['holding_period_text'])
        self.assertIn('3-6个月', picks['long']['holding_period_text'])

    def test_build_horizon_top_picks_uses_horizon_window_not_item_bias(self):
        picks = _build_horizon_top_picks([
            {'code': 'ONLY', 'name': '同一资产', 'up_probability_5d': 68, 'up_probability_20d': 60, 'up_probability_60d': 62, 'total_score': 4.0, 'risk_level': 'medium'}
        ])
        self.assertIn('2-5个月', picks['long']['holding_period_text'])

    def test_build_strategy_framework_summarizes_macro_and_industry(self):
        framework = _build_strategy_framework([
            {'code': 'A', 'name': '甲', 'industry': '半导体', 'up_probability_5d': 66, 'up_probability_20d': 63, 'up_probability_60d': 58, 'total_score': 4.3, 'risk_level': 'medium'},
            {'code': 'B', 'name': '乙', 'industry': '半导体', 'up_probability_5d': 62, 'up_probability_20d': 68, 'up_probability_60d': 64, 'total_score': 4.1, 'risk_level': 'medium'},
            {'code': 'C', 'name': '丙', 'industry': 'AI算力', 'up_probability_5d': 61, 'up_probability_20d': 65, 'up_probability_60d': 67, 'total_score': 4.2, 'risk_level': 'low'},
        ], query_type='a_stock', market_sentiment=0.62)
        self.assertEqual(framework['macro_regime'], 'constructive')
        self.assertIn('半导体', framework['focus_industries'][0])
        self.assertEqual(len(framework['sections']), 4)

    def test_build_strategy_framework_adds_policy_rotation_and_allocation(self):
        framework = _build_strategy_framework([
            {'code': 'A', 'name': '甲', 'industry': '半导体', 'up_probability_5d': 68, 'up_probability_20d': 66, 'up_probability_60d': 61, 'total_score': 4.4, 'risk_level': 'low'},
            {'code': 'B', 'name': '乙', 'industry': 'AI算力', 'up_probability_5d': 64, 'up_probability_20d': 69, 'up_probability_60d': 63, 'total_score': 4.2, 'risk_level': 'medium'},
            {'code': 'C', 'name': '丙', 'industry': '机器人', 'up_probability_5d': 59, 'up_probability_20d': 62, 'up_probability_60d': 66, 'total_score': 4.0, 'risk_level': 'medium'},
        ], query_type='etf', market_sentiment=0.61)
        self.assertIn('policy_score', framework)
        self.assertGreaterEqual(framework['policy_score'], 60)
        self.assertTrue(framework['global_context'])
        self.assertEqual(framework['policy_bias'], 'supportive')
        self.assertGreaterEqual(len(framework['sector_rotation']), 2)
        self.assertIn('cash_pct', framework['allocation_plan'])
        self.assertEqual(framework['allocation_plan']['cash_pct'] + framework['allocation_plan']['equity_pct'] + framework['allocation_plan']['defense_pct'], 100)
        self.assertEqual(len(framework['screening_flow']), 3)
        self.assertGreaterEqual(len(framework['rebalance_plan']['triggers']), 3)
        self.assertIn('review_cycle', framework['rebalance_plan'])

    def test_build_recommendation_quality_gate_flags_weak_signal(self):
        gate = _build_recommendation_quality_gate({
            'up_probability_5d': 53.9,
            'up_probability_20d': 47.3,
            'up_probability_60d': 30.7,
            'advisor': {'evidence_score': 16.3, 'risk_level': 'high', 'action': 'reduce', 'model_reliability': {'label': 'stable'}},
        })
        self.assertEqual(gate['grade'], 'D')
        self.assertEqual(gate['confidence_label'], '低置信')
        self.assertFalse(gate['actionable'])
        self.assertGreaterEqual(len(gate['warnings']), 2)

    def test_build_recommendation_quality_gate_downgrades_large_horizon_conflict(self):
        gate = _build_recommendation_quality_gate({
            'up_probability_5d': 72.0,
            'up_probability_20d': 49.0,
            'up_probability_60d': 32.0,
            'advisor': {'evidence_score': 66.0, 'risk_level': 'medium', 'action': 'add', 'model_reliability': {'label': 'stable'}},
        })
        self.assertEqual(gate['agreement_label'], '分歧较大')
        self.assertFalse(gate['actionable'])

    def test_build_data_quality_summary_flags_stale_data(self):
        summary = _build_data_quality_summary({
            'update_time': '2026-04-10 09:30:00',
            'predictions': [{'period': 5, 'up_probability': 60}],
            'analysis': {'technical': {'score': 4.1}},
            'data_source': 'fallback'
        }, today=datetime(2026, 4, 17).date())
        self.assertEqual(summary['freshness_status'], 'stale')
        self.assertEqual(summary['source_quality'], 'low')
        self.assertGreaterEqual(summary['completeness_pct'], 20)
        self.assertGreaterEqual(len(summary['warnings']), 1)

    def test_build_recommendation_quality_gate_uses_historical_hit_rate(self):
        gate = _build_recommendation_quality_gate({
            'up_probability_5d': 66.0,
            'up_probability_20d': 68.0,
            'up_probability_60d': 61.0,
            'advisor': {'evidence_score': 72.0, 'risk_level': 'low', 'action': 'buy', 'model_reliability': {'label': 'stable'}},
        }, historical_context={
            'preferred_horizon': 20,
            'status': 'ok',
            'grade': 'D',
            'samples': 86,
            'hit_rate': 41.2,
            'brier': 0.312,
            'calibration_gap': 14.8,
        })
        self.assertFalse(gate['actionable'])
        self.assertEqual(gate['historical_grade'], 'D')
        self.assertIn('历史', ''.join(gate['warnings']))

    def test_build_recommendation_quality_gate_blocks_action_when_review_samples_too_thin(self):
        gate = _build_recommendation_quality_gate({
            'up_probability_5d': 72.0,
            'up_probability_20d': 70.0,
            'up_probability_60d': 66.0,
            'advisor': {'evidence_score': 82.0, 'risk_level': 'low', 'action': 'buy', 'model_reliability': {'label': 'stable'}},
        }, historical_context={
            'preferred_horizon': 20,
            'status': 'ok',
            'grade': 'A',
            'samples': 8,
            'hit_rate': 100.0,
            'brier': 0.02,
            'calibration_gap': 1.5,
        })
        self.assertFalse(gate['actionable'])
        self.assertIn('样本', ''.join(gate['warnings']))
        self.assertIn(gate['grade'], ('B', 'C', 'D'))

    def test_build_data_quality_summary_aggregates_provenance_sources(self):
        summary = _build_data_quality_summary({
            'update_time': '2026-04-17 09:30:00',
            'current_price': 10.5,
            'predictions': [
                {'period': 5, 'up_probability': 61, 'source': 'trained_model + yfinance'},
                {'period': 20, 'up_probability': 58, 'source': 'trained_model + system_db'},
            ],
            'analysis': {
                'technical': {'score': 4.1, 'source': 'system_db'},
                'news': {'score': 3.8, 'source': 'TuShare'},
            },
            'advisor': {'action': 'hold'},
            'data_source': 'system_db'
        }, today=datetime(2026, 4, 17).date())
        self.assertEqual(summary['source_quality'], 'high')
        self.assertGreaterEqual(len(summary['provenance_items']), 2)
        self.assertTrue('yfinance' in summary['source_text'] or 'TuShare' in summary['source_text'])

    def test_build_detail_recommendation_rationale_uses_prediction_values_when_root_fields_missing(self):
        detail = {
            'asset_type': 'a_stock',
            'industry': '半导体',
            'total_score': 3.46,
            'advisor': {'action': 'reduce', 'risk_level': 'high', 'position_size_pct': -8, 'review_in_days': 1, 'evidence_score': 16.3},
            'predictions': [
                {'period': 5, 'up_probability': 53.91},
                {'period': 20, 'up_probability': 47.32},
                {'period': 60, 'up_probability': 30.71},
            ]
        }
        rationale = _build_detail_recommendation_rationale(detail, snapshot={'asset_type': 'a_stock', 'predictions': detail['predictions']})
        joined = ''.join(rationale['evidence_points'])
        self.assertIn('53.9%', joined)
        self.assertIn('47.3%', joined)
        self.assertIn('30.7%', joined)

    def test_build_detail_recommendation_rationale_explains_full_flow(self):
        detail = {
            'asset_type': 'a_stock',
            'industry': '半导体',
            'total_score': 4.3,
            'suggested_period': '建议持有 12-25个交易日',
            'advisor': {'action': 'buy', 'risk_level': 'medium', 'position_size_pct': 8, 'review_in_days': 3},
            'holding_advice': {'holding_period_text': '建议持有 12-25个交易日', 'reason': '中期趋势较顺，适合波段跟踪。'},
            'predictions': [
                {'period': 5, 'up_probability': 62},
                {'period': 20, 'up_probability': 68},
                {'period': 60, 'up_probability': 64},
            ]
        }
        snapshot = {
            'code': 'AAA',
            'name': '测试资产',
            'industry': '半导体',
            'asset_type': 'a_stock',
            'up_probability_5d': 62,
            'up_probability_20d': 68,
            'up_probability_60d': 64,
            'total_score': 4.3,
            'risk_level': 'medium',
        }
        rationale = _build_detail_recommendation_rationale(
            detail,
            snapshot=snapshot,
            market_sentiment=0.61,
            historical_context={
                'preferred_horizon': 20,
                'status': 'ok',
                'grade': 'B',
                'samples': 120,
                'hit_rate': 62.4,
                'brier': 0.218,
                'calibration_gap': 3.6,
            }
        )
        self.assertEqual(len(rationale['steps']), 5)
        self.assertIn('宏观环境', rationale['steps'][0]['title'])
        self.assertIn('半导体', rationale['steps'][2]['summary'])
        self.assertIn('12-25个交易日', rationale['steps'][4]['summary'])
        self.assertIn('训练模型', rationale['model_summary'])
        self.assertGreaterEqual(len(rationale['evidence_points']), 3)
        self.assertIn('20日', ''.join(rationale['evidence_points']))
        self.assertIn('命中率', ''.join(rationale['evidence_points']))

    def test_build_detail_recommendation_rationale_explains_bullish_but_no_chase(self):
        detail = {
            'asset_type': 'a_stock',
            'industry': '机器人',
            'total_score': 4.05,
            'suggested_period': '建议先持有 3-6个交易日',
            'advisor': {
                'action': 'hold',
                'risk_level': 'low',
                'position_size_pct': 0,
                'review_in_days': 5,
                'evidence_score': 61.0,
                'summary': '趋势偏强，但当前更适合耐心等确认。',
            },
            'holding_advice': {'holding_period_text': '建议先持有 3-6个交易日', 'reason': '短线仍偏强，但不宜追高。'},
            'predictions': [
                {'period': 5, 'up_probability': 67},
                {'period': 20, 'up_probability': 64},
                {'period': 60, 'up_probability': 60},
            ]
        }
        rationale = _build_detail_recommendation_rationale(
            detail,
            snapshot={
                'code': 'ROB',
                'name': '机器人ETF',
                'industry': '机器人',
                'asset_type': 'a_stock',
                'up_probability_5d': 67,
                'up_probability_20d': 64,
                'up_probability_60d': 60,
                'total_score': 4.05,
                'risk_level': 'low',
            },
        )
        execution_text = rationale['steps'][4]['summary']
        self.assertIn('若未持有', execution_text)
        self.assertTrue('不追高' in execution_text or '等待更合适' in execution_text)


class TestPortfolioAdviceEngine(unittest.TestCase):
    """组合再平衡建议测试"""

    def test_build_portfolio_advice_includes_rebalance_actions(self):
        recommender = StockRecommender()
        advice = recommender._build_portfolio_advice([
            {'advisor_view': {'action': 'reduce', 'risk_level': 'high'}},
            {'advisor_view': {'action': 'sell', 'risk_level': 'high'}},
            {'advisor_view': {'action': 'watch', 'risk_level': 'medium'}},
        ])
        self.assertIn('rebalance_actions', advice)
        self.assertGreaterEqual(len(advice['rebalance_actions']), 2)
        self.assertIn('priority', advice)


class TestBacktestValidatorCompatibility(unittest.TestCase):
    """回测验证兼容当前数据模型的测试"""

    class _FakeQuery:
        def __init__(self, result):
            self.result = result

        def filter_by(self, **kwargs):
            return self

        def filter(self, *args, **kwargs):
            return self

        def order_by(self, *args, **kwargs):
            return self

        def first(self):
            return self.result

    class _FakeSession:
        def __init__(self, holding, rec):
            self.holding = holding
            self.rec = rec

        def query(self, model):
            name = getattr(model, '__name__', '')
            if name == 'Holding':
                return TestBacktestValidatorCompatibility._FakeQuery(self.holding)
            return TestBacktestValidatorCompatibility._FakeQuery(self.rec)

    def test_validate_take_profit_signals_uses_current_schema_fields(self):
        from types import SimpleNamespace

        validator = BacktestValidator.__new__(BacktestValidator)
        holding = SimpleNamespace(id=1, code='000001.SZ', name='平安银行', asset_type='stock', cost_price=100.0)
        rec = SimpleNamespace(
            code='000001.SZ',
            type='a_stock',
            current_price=102.0,
            target_high_20d=107.0,
            created_at=datetime(2026, 4, 1, 9, 30),
        )
        validator.session = self._FakeSession(holding, rec)

        df = pd.DataFrame({'close': [102.0, 105.0, 108.0]}, index=pd.to_datetime(['2026-04-01', '2026-04-02', '2026-04-03']))
        df.index.name = 'date'
        validator.collector = SimpleNamespace(get_stock_data_from_db=lambda code: df)

        result = validator.validate_take_profit_signals(1)
        self.assertNotIn('error', result)
        self.assertTrue(result['target_hit'])
        self.assertEqual(result['status'], 'hit')

    def test_validate_add_signals_falls_back_to_recommendation_or_cost_price(self):
        from types import SimpleNamespace

        validator = BacktestValidator.__new__(BacktestValidator)
        holding = SimpleNamespace(id=2, code='0700.HK', name='腾讯控股', asset_type='stock', cost_price=300.0)
        rec = SimpleNamespace(
            code='0700.HK',
            type='hk_stock',
            current_price=310.0,
            target_low_5d=305.0,
            created_at=datetime(2026, 4, 1, 9, 30),
        )
        validator.session = self._FakeSession(holding, rec)

        df = pd.DataFrame({'close': [304.0, 312.0, 318.0]}, index=pd.to_datetime(['2026-04-01', '2026-04-02', '2026-04-03']))
        df.index.name = 'date'
        validator.collector = SimpleNamespace(get_stock_data_from_db=lambda code: df)

        result = validator.validate_add_signals(2)
        self.assertNotIn('error', result)
        self.assertIn(result['status'], ['profitable', 'breakeven'])
        self.assertGreaterEqual(result['actual_best_price'], 300.0)


class TestShortTermPredictor(unittest.TestCase):
    """短期预测器测试"""
    
    def setUp(self):
        self.predictor = ShortTermPredictor()
        self.df = create_test_data(200)
    
    def test_prepare_features(self):
        """测试特征准备"""
        X = self.predictor.prepare_features(self.df)
        
        if X is not None:
            self.assertIsInstance(X, pd.DataFrame)
            print(f"✅ 特征准备测试通过: {len(X.columns)} 个特征")
        else:
            print("⚠️ 特征准备测试: 数据不足")
    
    def test_predict(self):
        """测试预测"""
        X = self.predictor.prepare_features(self.df)
        
        if X is not None:
            prob = self.predictor.predict(X)
            self.assertTrue(0 <= prob <= 100)
            print(f"✅ 预测测试通过: 上涨概率 {prob:.1f}%")
        else:
            print("⚠️ 预测测试: 数据不足")
    
    def test_get_prediction_result(self):
        """测试预测结果"""
        result = self.predictor.get_prediction_result(self.df)
        
        self.assertIn('period_days', result)
        self.assertIn('up_probability', result)
        self.assertIn('target_low', result)
        self.assertIn('target_high', result)
        print(f"✅ 预测结果测试通过: 5日上涨概率 {result['up_probability']}%")

    def test_align_features_for_numpy_feature_names(self):
        """测试对 numpy 数组形式的 feature_names_in_ 兼容"""
        X = pd.DataFrame([[1.0, 2.0]], columns=['foo', 'bar'])

        class DummyModel:
            feature_names_in_ = np.array(['foo', 'baz'])

        aligned = self.predictor._align_features_for_model(X, model=DummyModel())
        self.assertEqual(list(aligned.columns), ['foo', 'baz'])
        self.assertEqual(float(aligned.iloc[0]['foo']), 1.0)
        self.assertEqual(float(aligned.iloc[0]['baz']), 0.0)

    def test_align_features_uses_saved_feature_columns_for_legacy_models(self):
        """验证短期预测对仅保存 feature_columns 的旧模型保持兼容。"""
        X = pd.DataFrame([[1.0, 2.0]], columns=['foo', 'bar'])
        self.predictor.feature_columns = ['foo', 'baz']

        class DummyLegacyModel:
            n_features_in_ = 2

        aligned = self.predictor._align_features_for_model(X, model=DummyLegacyModel())
        self.assertEqual(list(aligned.columns), ['foo', 'baz'])
        self.assertEqual(float(aligned.iloc[0]['foo']), 1.0)
        self.assertEqual(float(aligned.iloc[0]['baz']), 0.0)


class TestMediumTermPredictor(unittest.TestCase):
    """中期预测器测试"""
    
    def setUp(self):
        self.predictor = MediumTermPredictor()
        self.df = create_test_data(300)
    
    def test_prepare_features(self):
        """测试特征准备"""
        X = self.predictor.prepare_features(self.df)
        
        if X is not None:
            self.assertIsInstance(X, pd.DataFrame)
            print(f"✅ 中期特征准备测试通过: {len(X.columns)} 个特征")
        else:
            print("⚠️ 中期特征准备测试: 数据不足")
    
    def test_predict(self):
        """测试预测"""
        X = self.predictor.prepare_features(self.df)
        
        if X is not None:
            prob = self.predictor.predict(X)
            self.assertTrue(0 <= prob <= 100)
            print(f"✅ 中期预测测试通过: 上涨概率 {prob:.1f}%")
        else:
            print("⚠️ 中期预测测试: 数据不足")
    
    def test_get_prediction_result(self):
        """测试预测结果"""
        result = self.predictor.get_prediction_result(self.df)
        
        self.assertIn('period_days', result)
        self.assertEqual(result['period_days'], 20)
        print(f"✅ 中期预测结果测试通过: 20日上涨概率 {result['up_probability']}%")

    def test_prepare_features_include_macro_fields(self):
        """验证中期模型已接入宏观与跨资产特征"""
        X = self.predictor.prepare_features(
            self.df,
            valuation_data={'pe': 18, 'pb': 2.1, 'eps': 1.2, 'roe': 12},
            market_data={'cpi_yoy': 1.2, 'pmi': 50.8, 'shibor_1m': 1.7, 'macro_regime_score': 0.3, 'risk_off_proxy': -0.1}
        )
        self.assertIn('macro_regime_score', X.columns)
        self.assertIn('risk_off_proxy', X.columns)

    def test_prepare_features_include_asset_flags(self):
        """验证中期模型已接入资产类别标记，便于多资产联合训练"""
        X = self.predictor.prepare_features(
            self.df,
            market_data={'is_hk_asset': 1, 'is_foreign_asset': 1, 'is_fund_asset': 0, 'is_metal_asset': 0}
        )
        self.assertIn('is_hk_asset', X.columns)
        self.assertIn('is_foreign_asset', X.columns)

    def test_prepare_features_include_medium_cycle_discriminators(self):
        """验证20日模型加入更有区分度的中周期趋势/风险特征。"""
        X = self.predictor.prepare_features(self.df)
        self.assertIn('atr_ratio', X.columns)
        self.assertIn('drawdown_60d', X.columns)
        self.assertIn('trend_consistency_20d', X.columns)
        self.assertIn('volume_trend_20d', X.columns)

    def test_train_accepts_sample_weight(self):
        """验证中期模型训练支持样本权重"""
        X = pd.DataFrame(np.random.randn(40, 4), columns=list('abcd'))
        y = pd.Series([0, 1] * 20)
        score = self.predictor.train(X, y, sample_weight=np.ones(len(y)))
        self.assertGreaterEqual(score, 0)

    def test_medium_term_strategy_includes_penalty_map_for_repeat_error_codes(self):
        """验证20日优化会对重复高误判标的生成降权映射"""
        trainer = ModelTrainer()
        reflection = {
            'metrics': {'accuracy': 0.56, 'f1': 0.35, 'auc': 0.54},
            'data_profile': {'train_pos_rate': 0.47, 'val_pos_rate': 0.29},
            'prediction_bias': {'false_positive_rate': 0.36, 'rate_gap': 0.08},
            'failure_analysis': {
                'segment_stats': {
                    'top_error_codes': [
                        {'code': '000890.SZ', 'count': 30, 'error_rate': 1.0},
                        {'code': '001872.SZ', 'count': 30, 'error_rate': 0.96},
                    ]
                }
            }
        }

        overrides = trainer._derive_medium_term_strategy_overrides(reflection, round_idx=2)
        self.assertIn('penalty_map', overrides)
        self.assertLess(overrides['penalty_map'].get('000890.SZ', 1.0), 1.0)

    def test_align_features_for_legacy_medium_model(self):
        """验证中期预测对旧版特征列模型保持兼容"""
        X = pd.DataFrame([[1.0, 2.0]], columns=['foo', 'bar'])

        class DummyModel:
            feature_names_in_ = np.array(['foo', 'baz'])

        aligned = self.predictor._align_features_for_model(X, model=DummyModel())
        self.assertEqual(list(aligned.columns), ['foo', 'baz'])
        self.assertEqual(float(aligned.iloc[0]['baz']), 0.0)


class TestLongTermPredictor(unittest.TestCase):
    """长期预测器测试"""
    
    def setUp(self):
        self.predictor = LongTermPredictor()
        self.df = create_test_data(400)
    
    def test_calculate_valuation_regression(self):
        """测试估值回归"""
        prob = self.predictor.calculate_valuation_regression(20, 2, 30, 35)
        self.assertTrue(0 <= prob <= 100)
        print(f"✅ 估值回归测试通过: {prob:.1f}%")
    
    def test_calculate_trend_regression(self):
        """测试趋势回归"""
        prob = self.predictor.calculate_trend_regression(self.df)
        self.assertTrue(0 <= prob <= 100)
        print(f"✅ 趋势回归测试通过: {prob:.1f}%")
    
    def test_predict(self):
        """测试预测"""
        valuation_data = {'pe': 20, 'pb': 2, 'pe_percentile': 35, 'pb_percentile': 40}
        prob = self.predictor.predict(self.df, valuation_data)
        self.assertTrue(0 <= prob <= 100)
        print(f"✅ 长期预测测试通过: 上涨概率 {prob:.1f}%")
    
    def test_get_prediction_result(self):
        """测试预测结果"""
        valuation_data = {'pe': 20, 'pb': 2, 'pe_percentile': 35, 'pb_percentile': 40}
        result = self.predictor.get_prediction_result(self.df, valuation_data)
        
        self.assertIn('period_days', result)
        self.assertEqual(result['period_days'], 60)
        print(f"✅ 长期预测结果测试通过: 60日上涨概率 {result['up_probability']}%")

    def test_prepare_features_include_long_cycle_fields(self):
        """验证长期模型已接入长期周期与宏观特征"""
        X = self.predictor.prepare_features(
            self.df,
            valuation_data={'pe': 20, 'pb': 2, 'eps': 1.5, 'roe': 13},
            market_data={'cpi_yoy': 1.0, 'pmi': 51.2, 'shibor_3m': 1.9, 'gold_oil_ratio': 28.0, 'dollar_proxy': 0.2}
        )
        self.assertIn('gold_oil_ratio', X.columns)
        self.assertIn('dollar_proxy', X.columns)


class TestModelTrainerPerformance(unittest.TestCase):
    """训练性能相关测试"""

    def test_slice_feature_window_limits_history_length(self):
        """验证训练特征窗口不会无限扩大"""
        trainer = ModelTrainer()
        df = create_test_data(500).reset_index().rename(columns={'index': 'date'})
        window_df = trainer._slice_feature_window(df, end_idx=420, period_days=5)
        self.assertLessEqual(len(window_df), 180)
        self.assertGreaterEqual(len(window_df), 120)

    def test_default_training_codes_include_multi_asset_pools(self):
        """验证默认训练代码池会纳入港股、美股等多资产标的"""
        trainer = ModelTrainer()
        trainer.collector.a_stock_pool = ['000001.SZ']
        trainer.collector.hk_stock_pool = ['0700.HK']
        trainer.collector.us_stock_pool = ['AAPL', 'GLD']

        codes = trainer._get_default_training_codes(limit=20)
        self.assertIn('000001.SZ', codes)
        self.assertIn('0700.HK', codes)
        self.assertIn('AAPL', codes)
        self.assertIn('GLD', codes)

    def test_asset_balance_weights_upweight_underrepresented_markets(self):
        """验证多资产训练会给稀缺市场更高权重，避免被单一市场淹没"""
        trainer = ModelTrainer()
        weights = trainer._build_asset_balance_weights([
            '000001.SZ', '000002.SZ', '000003.SZ', '0700.HK', 'AAPL'
        ])
        self.assertEqual(len(weights), 5)
        self.assertGreater(float(weights[-1]), float(weights[0]))

    def test_infer_asset_flags_treat_plain_six_digit_stock_as_a_share(self):
        """验证无后缀的6位股票代码不会被误判成基金。"""
        trainer = ModelTrainer()
        flags = trainer._infer_asset_flags('002325')
        self.assertEqual(flags['is_a_asset'], 1)
        self.assertEqual(flags['is_fund_asset'], 0)

    def test_prepare_training_data_returns_technical_meta_for_5d(self):
        """验证5日训练元数据包含技术分层所需字段，便于真实复盘优化"""
        trainer = ModelTrainer()
        synthetic = create_test_data(220).reset_index().rename(columns={'index': 'date'})
        trainer.collector.get_stock_data_from_db = lambda code: synthetic.copy()
        trainer._build_external_feature_payload = lambda code, asof_date: ({'pe': 18, 'pb': 2.0}, {'sentiment': 0.1, 'macro_regime_score': 0.2})

        X, y, meta_df = trainer.prepare_training_data(
            ['000001.SZ'],
            period_days=5,
            lookback_years=0.6,
            neutral_zone=0.0,
            return_meta=True,
        )
        self.assertIsNotNone(X)
        self.assertIsNotNone(meta_df)
        self.assertIn('volatility', meta_df.columns)
        self.assertIn('rsi', meta_df.columns)
        self.assertIn('volume_ratio', meta_df.columns)
        self.assertIn('price_ma20_ratio', meta_df.columns)

    def test_full_data_mode_uses_single_short_experiment(self):
        """验证全量模式下不会重复跑多套5日实验"""
        trainer = ModelTrainer()
        params, experiments = trainer._get_short_term_search_plan(full_data_mode=True)
        self.assertGreaterEqual(len(params), 1)
        self.assertEqual(len(experiments), 1)
        self.assertEqual(experiments[0]['name'], 'event_adaptive_label_q18')

    def test_runtime_promotion_blocks_weaker_candidate(self):
        """验证运行时模型不会被更弱的新候选覆盖"""
        trainer = ModelTrainer()
        existing = {'validation_accuracy': 0.6400, 'validation_f1': 0.6680, 'validation_auc': 0.7030, 'validation_brier': 0.2226}
        candidate = {'validation_accuracy': 0.6301, 'validation_f1': 0.5836, 'validation_auc': 0.6755, 'validation_brier': 0.2382}
        self.assertFalse(trainer._should_promote_runtime_model(existing, candidate))

    def test_medium_term_strategy_reacts_to_false_positive_bias(self):
        """验证20日优化策略会针对高误报自动收紧"""
        trainer = ModelTrainer()
        reflection = {
            'metrics': {'accuracy': 0.58, 'f1': 0.36, 'auc': 0.55},
            'data_profile': {'train_pos_rate': 0.44, 'val_pos_rate': 0.28},
            'prediction_bias': {'false_positive_rate': 0.48, 'predicted_positive_rate': 0.62, 'actual_positive_rate': 0.28},
        }
        plan = trainer._derive_medium_term_strategy_overrides(reflection, round_idx=2)
        self.assertIn('reduce_false_positives', plan['notes'])

    def test_horizon_threshold_search_allows_higher_cutoff_for_20d(self):
        """验证20日阈值搜索可超过0.70，以适应低胜率市场阶段"""
        trainer = ModelTrainer()
        y_true = np.array([1, 1, 0, 0, 0, 0])
        y_proba = np.array([0.82, 0.79, 0.76, 0.74, 0.71, 0.31])
        threshold, _ = trainer._find_best_threshold_for_horizon(20, y_true, y_proba)
        self.assertGreaterEqual(threshold, 0.71)

    def test_direction_balance_weights_reduce_overbullish_skew(self):
        """验证方向权重会在正样本过多时提升负样本权重"""
        trainer = ModelTrainer()
        weights = trainer._build_direction_balance_weights(np.array([1, 1, 1, 1, 0]), target_positive_rate=0.35)
        self.assertGreater(float(weights[-1]), float(np.mean(weights[:-1])))

    def test_medium_term_neutral_zone_filters_small_moves(self):
        """验证20日训练会过滤小波动噪声样本，而不是全部纳入标签学习"""
        trainer = ModelTrainer()
        synthetic = create_test_data(220).reset_index().rename(columns={'index': 'date'})
        synthetic['close'] = np.linspace(100.0, 100.8, len(synthetic))
        synthetic['open'] = synthetic['close'] * 0.999
        synthetic['high'] = synthetic['close'] * 1.001
        synthetic['low'] = synthetic['close'] * 0.998
        trainer.collector.get_stock_data_from_db = lambda code: synthetic.copy()
        trainer._build_external_feature_payload = lambda code, asof_date: ({'pe': 18, 'pb': 2.0}, {'macro_regime_score': 0.1})

        X_all, y_all, _ = trainer.prepare_training_data(['000001.SZ'], period_days=20, lookback_years=0.6, neutral_zone=0.0, return_meta=True)
        X_filtered, y_filtered, _ = trainer.prepare_training_data(['000001.SZ'], period_days=20, lookback_years=0.6, neutral_zone=0.02, return_meta=True)
        self.assertIsNotNone(X_all)
        filtered_count = 0 if X_filtered is None else len(X_filtered)
        self.assertLess(filtered_count, len(X_all))

    def test_prepare_training_data_returns_real_meta_for_20d(self):
        """验证20日训练元数据会保留真实宏观和技术快照，便于后续自动优化。"""
        trainer = ModelTrainer()
        synthetic = create_test_data(240).reset_index().rename(columns={'index': 'date'})
        trainer.collector.get_stock_data_from_db = lambda code: synthetic.copy()
        trainer._build_external_feature_payload = lambda code, asof_date: (
            {'pe': 18, 'pb': 2.0, 'eps': 1.1, 'roe': 12.5},
            {'macro_regime_score': 0.35, 'risk_off_proxy': -0.1, 'dollar_proxy': 0.2, 'sentiment': 0.15, 'has_report': 1}
        )

        X, y, meta_df = trainer.prepare_training_data(
            ['000001.SZ'],
            period_days=20,
            lookback_years=0.6,
            neutral_zone=0.0,
            return_meta=True,
        )
        self.assertIsNotNone(X)
        self.assertIsNotNone(meta_df)
        self.assertIn('macro_regime_score', meta_df.columns)
        self.assertIn('volatility', meta_df.columns)
        self.assertGreater(float(meta_df['macro_regime_score'].abs().sum()), 0.0)

    def test_regime_alignment_weights_favor_nearby_regime(self):
        """验证制度对齐权重会优先保留更接近当前市场状态的训练样本"""
        trainer = ModelTrainer()
        X_train = pd.DataFrame([
            {'macro_regime_score': -0.8, 'risk_off_proxy': 0.9, 'dollar_proxy': 0.7, 'volatility': 0.40},
            {'macro_regime_score': 0.1, 'risk_off_proxy': 0.1, 'dollar_proxy': 0.1, 'volatility': 0.16},
            {'macro_regime_score': 0.2, 'risk_off_proxy': 0.0, 'dollar_proxy': 0.1, 'volatility': 0.15},
        ])
        X_val = pd.DataFrame([
            {'macro_regime_score': 0.15, 'risk_off_proxy': 0.05, 'dollar_proxy': 0.08, 'volatility': 0.14},
        ])
        weights = trainer._build_regime_alignment_weights(X_train, X_val)
        self.assertIsNotNone(weights)
        self.assertGreater(float(weights[1]), float(weights[0]))
        self.assertGreater(float(weights[2]), float(weights[0]))

    def test_runtime_promotion_accepts_stronger_candidate(self):
        """验证更优的新候选可以升级为运行时模型"""
        trainer = ModelTrainer()
        existing = {'validation_accuracy': 0.5771, 'validation_f1': 0.5330, 'validation_auc': 0.6631, 'validation_brier': 0.2149}
        candidate = {'validation_accuracy': 0.6120, 'validation_f1': 0.5610, 'validation_auc': 0.6890, 'validation_brier': 0.2080}
        self.assertTrue(trainer._should_promote_runtime_model(existing, candidate))

    def test_build_horizon_reflection_contains_failure_diagnosis(self):
        """验证失败训练会生成可执行的复盘结论"""
        trainer = ModelTrainer()
        reflection = trainer._build_horizon_reflection(
            period_days=20,
            passed=False,
            gate_name='failed',
            eval_metrics={'accuracy': 0.31, 'f1': 0.42, 'auc': 0.56, 'brier': 0.27},
            data_profile={'pos_rate': 0.28, 'train_pos_rate': 0.41, 'val_pos_rate': 0.28},
        )
        self.assertGreaterEqual(len(reflection['reasons']), 2)
        self.assertGreaterEqual(len(reflection['actions']), 2)
        self.assertTrue(any('市场状态' in x or '标签分布' in x for x in reflection['reasons']))

    def test_build_code_quality_weights_prefers_higher_score(self):
        """验证短期代码质量分高的样本权重更高"""
        trainer = ModelTrainer()
        weights = trainer._build_code_quality_weights(['A', 'B', 'C'], {'A': 0.9, 'B': 0.2, 'C': 0.5})
        self.assertGreater(weights['A'], weights['C'])
        self.assertGreater(weights['C'], weights['B'])

    def test_build_code_quality_weights_downweights_repeat_error_codes(self):
        """验证历史高误判代码会在下一轮被降权"""
        trainer = ModelTrainer()
        weights = trainer._build_code_quality_weights(
            ['A', 'B'],
            {'A': 0.8, 'B': 0.8},
            penalty_map={'A': 0.4}
        )
        self.assertLess(weights['A'], weights['B'])

    def test_filter_codes_by_penalty_removes_worst_repeat_offenders(self):
        """验证极高误判代码会在后续轮次被优先过滤"""
        trainer = ModelTrainer()
        kept = trainer._filter_codes_by_penalty(
            ['A', 'B', 'C', 'D', 'E'],
            penalty_map={'A': 0.35, 'B': 0.42, 'C': 0.8},
            min_penalty=0.5,
            max_drop_ratio=0.4,
        )
        self.assertNotIn('A', kept)
        self.assertNotIn('B', kept)
        self.assertIn('C', kept)

    def test_analyze_failure_factors_returns_dominant_segments(self):
        """验证失败分析能识别高风险因子分组"""
        trainer = ModelTrainer()
        meta = pd.DataFrame({
            'code': ['A', 'B', 'C', 'D'],
            'sentiment': [0.8, -0.9, -0.8, 0.1],
            'macro_regime_score': [0.4, -0.7, -0.6, 0.2],
            'has_report': [1, 0, 0, 1],
        })
        result = trainer._analyze_failure_factors(
            meta,
            y_true=[1, 0, 0, 1],
            y_pred=[0, 1, 1, 1],
            y_proba=[0.3, 0.8, 0.75, 0.6],
            period_days=5,
        )
        self.assertGreaterEqual(result['error_count'], 1)
        self.assertIn('dominant_factors', result)

    def test_analyze_failure_factors_includes_technical_segments(self):
        """验证复盘会输出技术状态分桶，便于真正优化"""
        trainer = ModelTrainer()
        meta = pd.DataFrame({
            'code': ['A', 'B', 'C', 'D', 'E', 'F'],
            'volatility': [0.06, 0.05, 0.01, 0.015, 0.055, 0.012],
            'rsi': [78, 74, 42, 38, 80, 45],
            'volume_ratio': [2.2, 1.9, 0.7, 0.8, 2.4, 0.9],
            'price_ma20_ratio': [0.09, 0.08, -0.01, 0.0, 0.1, -0.02],
        })
        result = trainer._analyze_failure_factors(
            meta,
            y_true=[0, 0, 1, 1, 0, 1],
            y_pred=[1, 1, 1, 0, 1, 1],
            y_proba=[0.8, 0.77, 0.58, 0.41, 0.83, 0.56],
            period_days=5,
        )
        self.assertIn('volatility_bucket', result['segment_stats'])
        self.assertIn('rsi_bucket', result['segment_stats'])

    def test_find_best_threshold_reduces_false_positive_bias(self):
        """验证阈值搜索不会在弱信号时一味偏向看涨"""
        trainer = ModelTrainer()
        y_true = np.array([0, 0, 0, 0, 0, 0, 0, 1, 0, 0])
        y_proba = np.array([0.59, 0.58, 0.57, 0.56, 0.55, 0.54, 0.53, 0.52, 0.51, 0.50])
        threshold, _ = trainer._find_best_threshold(y_true, y_proba)
        self.assertGreaterEqual(threshold, 0.55)

    def test_derive_short_term_strategy_overrides_targets_false_positives(self):
        """验证5日复盘后会自动生成抑制误报的下一轮策略"""
        trainer = ModelTrainer()
        overrides = trainer._derive_short_term_strategy_overrides(
            {
                'metrics': {'accuracy': 0.44, 'auc': 0.58},
                'prediction_bias': {'false_positive_rate': 0.46, 'false_negative_rate': 0.22},
                'failure_analysis': {'false_positive_count': 180, 'false_negative_count': 40},
            },
            round_idx=2,
        )
        self.assertIn('reduce_false_positives', overrides['notes'])
        self.assertTrue(any(float(exp.get('neutral_zone', 0.0)) >= 0.012 for exp in overrides['experiment_grid']))
        self.assertTrue(any(int(p.get('max_depth', 99)) <= 3 for p in overrides['candidate_params']))

    def test_derive_short_term_strategy_overrides_targets_low_recall(self):
        """验证低召回场景会触发补强正样本识别的策略"""
        trainer = ModelTrainer()
        overrides = trainer._derive_short_term_strategy_overrides(
            {
                'metrics': {'accuracy': 0.55, 'auc': 0.55},
                'prediction_bias': {'false_positive_rate': 0.35, 'false_negative_rate': 0.60},
                'failure_analysis': {'false_positive_count': 90, 'false_negative_count': 180},
            },
            round_idx=3,
        )
        self.assertIn('recover_missed_positives', overrides['notes'])

    def test_full_data_mode_keeps_override_experiments(self):
        """验证全量模式下自动优化的新增实验不会被静默丢弃"""
        trainer = ModelTrainer()
        overrides = {
            'preferred_names': ['signal_focus_q20_r2'],
            'experiment_grid': [
                {
                    'name': 'signal_focus_q20_r2',
                    'lookback_years': 1.0,
                    'neutral_zone': 0.01,
                    'decay_days': 50.0,
                    'adaptive_band': True,
                    'adaptive_band_quantile': 0.2,
                    'adaptive_label_zone': True,
                    'use_event_features': True,
                }
            ],
            'candidate_params': [
                {
                    'n_estimators': 180,
                    'max_depth': 2,
                    'learning_rate': 0.03,
                }
            ],
        }
        params, experiments = trainer._get_short_term_search_plan(full_data_mode=True, overrides=overrides)
        self.assertTrue(any(exp.get('name') == 'signal_focus_q20_r2' for exp in experiments))
        self.assertTrue(any(int(p.get('max_depth', 99)) == 2 for p in params))

    def test_quality_selector_keeps_full_trainable_pool_when_no_target_limit(self):
        """验证20日/60日在未显式限流时会保留全部可训练标的。"""
        trainer = ModelTrainer()
        history_df = create_test_data(800)
        trainer.collector.get_stock_data_from_db = lambda code: history_df

        codes = [f"{i:06d}.SZ" for i in range(2600)]
        selected_20 = trainer._select_quality_training_codes(codes, period_days=20, target_count=None)
        selected_60 = trainer._select_quality_training_codes(codes, period_days=60, target_count=None)

        self.assertEqual(len(selected_20), len(codes))
        self.assertEqual(len(selected_60), len(codes))

    def test_derive_medium_term_strategy_overrides_targets_regime_drift(self):
        """验证20日复盘会针对市场漂移自动收缩窗口并加强近期权重"""
        trainer = ModelTrainer()
        overrides = trainer._derive_medium_term_strategy_overrides(
            {
                'metrics': {'accuracy': 0.46, 'f1': 0.45, 'auc': 0.59},
                'data_profile': {'train_pos_rate': 0.42, 'val_pos_rate': 0.28},
            },
            round_idx=2,
        )
        self.assertIn('rebalance_regime_drift', overrides['notes'])
        self.assertTrue(any(float(exp.get('lookback_years', 9)) <= 1.5 for exp in overrides['experiment_grid']))

    def test_train_medium_term_until_target_stops_when_target_met(self):
        """验证20日自动优化达到目标后会及时停止"""
        trainer = ModelTrainer()
        trainer._save_horizon_optimization_loop = lambda *args, **kwargs: None

        def fake_train(stock_codes=None, plan_override=None, round_idx=None, max_rounds=None):
            trainer._last_training_diagnostics['medium_term'] = {
                'gate': 'acc_f1',
                'passed': True,
                'metrics': {'accuracy': 0.57, 'f1': 0.52, 'auc': 0.61, 'brier': 0.24},
                'reasons': [],
                'actions': [],
            }
            return 0.57

        trainer.train_medium_term_model = fake_train
        result = trainer.train_medium_term_until_target(stock_codes=['000001.SZ'], target_accuracy=0.56, target_f1=0.5, max_rounds=3)
        self.assertGreaterEqual(result, 0.56)
        self.assertEqual(len(trainer._medium_term_optimization_history), 1)

    def test_train_long_term_until_target_stops_when_target_met(self):
        """验证60日自动优化达到目标后也会及时停止"""
        trainer = ModelTrainer()
        trainer._save_horizon_optimization_loop = lambda *args, **kwargs: None

        def fake_train(stock_codes=None, plan_override=None, round_idx=None, max_rounds=None):
            trainer._last_training_diagnostics['long_term'] = {
                'gate': 'acc_f1',
                'passed': True,
                'metrics': {'accuracy': 0.61, 'f1': 0.54, 'auc': 0.66, 'brier': 0.21},
                'reasons': [],
                'actions': [],
            }
            return 0.61

        trainer.train_long_term_model = fake_train
        result = trainer.train_long_term_until_target(stock_codes=['000001.SZ'], target_accuracy=0.60, target_f1=0.5, max_rounds=3)
        self.assertGreaterEqual(result, 0.60)
        self.assertEqual(len(trainer._long_term_optimization_history), 1)

    def test_train_all_models_records_continuous_optimization_summary(self):
        """验证全模型训练会产出统一的连续优化摘要"""
        trainer = ModelTrainer()
        trainer._save_training_backtest_summary = lambda *args, **kwargs: None
        trainer._save_continuous_improvement_summary = lambda *args, **kwargs: None
        cycle_calls = []

        def fake_short(*args, **kwargs):
            cycle_calls.append('short')
            trainer._last_training_diagnostics['short_term'] = {
                'gate': 'acc_f1',
                'passed': True,
                'metrics': {'accuracy': 0.66, 'f1': 0.58},
            }
            return 0.66

        def fake_medium(*args, **kwargs):
            cycle_calls.append('medium')
            trainer._last_training_diagnostics['medium_term'] = {
                'gate': 'acc_f1',
                'passed': True,
                'metrics': {'accuracy': 0.60, 'f1': 0.52},
            }
            return 0.60

        def fake_long(*args, **kwargs):
            cycle_calls.append('long')
            trainer._last_training_diagnostics['long_term'] = {
                'gate': 'acc_f1',
                'passed': True,
                'metrics': {'accuracy': 0.62, 'f1': 0.51},
            }
            return 0.62

        trainer.train_short_term_until_target = fake_short
        trainer.train_medium_term_until_target = fake_medium
        trainer.train_long_term_until_target = fake_long

        results = trainer.train_all_models(
            stock_codes=['000001.SZ'],
            target_periods=[5, 20, 60],
            continuous_improvement_rounds=2,
            auto_optimize_long_term=True,
        )
        self.assertIn('continuous_improvement', results)
        self.assertGreaterEqual(len(results['continuous_improvement']['cycles']), 1)
        self.assertEqual(cycle_calls[:3], ['short', 'medium', 'long'])


class TestAdvisorDecisionLayer(unittest.TestCase):
    """投顾决策层测试"""

    def test_build_advisor_view_flags_buy_candidate(self):
        recommender = StockRecommender.__new__(StockRecommender)
        advisor = recommender._build_advisor_view(
            total_score=4.35,
            trend={'trend': 'bullish', 'trend_text': '上升趋势'},
            unified_trend={'trend_direction': 'bullish', 'trend_score': 71, 'trend_confidence': 78},
            tech_indicators={'rsi': 58, 'volatility': 0.18, 'price_ma20_ratio': 0.03},
        )
        self.assertEqual(advisor['action'], 'buy')
        self.assertGreaterEqual(advisor['position_size_pct'], 10)

    def test_build_advisor_view_flags_sell_risk(self):
        recommender = StockRecommender.__new__(StockRecommender)
        advisor = recommender._build_advisor_view(
            total_score=2.1,
            trend={'trend': 'strong_bearish', 'trend_text': '下降趋势'},
            unified_trend={'trend_direction': 'bearish', 'trend_score': 31, 'trend_confidence': 82},
            tech_indicators={'rsi': 78, 'volatility': 0.42, 'price_ma20_ratio': -0.09},
        )
        self.assertEqual(advisor['action'], 'sell')
        self.assertEqual(advisor['risk_level'], 'high')

    def test_build_portfolio_advice_marks_defensive_posture(self):
        recommender = StockRecommender.__new__(StockRecommender)
        advice = recommender._build_portfolio_advice([
            {'advisor_view': {'action': 'sell', 'risk_level': 'high'}},
            {'advisor_view': {'action': 'reduce', 'risk_level': 'high'}},
            {'advisor_view': {'action': 'hold', 'risk_level': 'medium'}},
        ])
        self.assertEqual(advice['overall_risk'], 'high')
        self.assertGreaterEqual(advice['recommended_cash_ratio_pct'], 35)

    def test_build_portfolio_advice_marks_constructive_posture(self):
        recommender = StockRecommender.__new__(StockRecommender)
        advice = recommender._build_portfolio_advice([
            {'advisor_view': {'action': 'buy', 'risk_level': 'low'}},
            {'advisor_view': {'action': 'add', 'risk_level': 'low'}},
            {'advisor_view': {'action': 'hold', 'risk_level': 'medium'}},
        ])
        self.assertEqual(advice['overall_risk'], 'low')
        self.assertLessEqual(advice['recommended_cash_ratio_pct'], 20)

    def test_build_portfolio_advice_avoids_constructive_bias_when_only_holding(self):
        recommender = StockRecommender.__new__(StockRecommender)
        advice = recommender._build_portfolio_advice([
            {'advisor_view': {'action': 'hold', 'risk_level': 'low'}},
            {'advisor_view': {'action': 'hold', 'risk_level': 'low'}},
            {'advisor_view': {'action': 'watch', 'risk_level': 'low'}},
        ])
        self.assertEqual(advice['stance'], 'balanced')
        self.assertGreaterEqual(advice['recommended_cash_ratio_pct'], 20)

    def test_build_advisor_view_sets_review_cycle_for_high_risk(self):
        recommender = StockRecommender.__new__(StockRecommender)
        advisor = recommender._build_advisor_view(
            total_score=2.2,
            trend={'trend': 'bearish', 'trend_text': '走弱'},
            unified_trend={'trend_direction': 'bearish', 'trend_score': 35, 'trend_confidence': 75},
            tech_indicators={'rsi': 75, 'volatility': 0.38, 'price_ma20_ratio': 0.09},
        )
        self.assertLessEqual(advisor['review_in_days'], 1)
        self.assertIn('risk_control', advisor['review_focus'])

    def test_build_advisor_view_avoids_aggressive_action_on_weak_confidence(self):
        recommender = StockRecommender.__new__(StockRecommender)
        advisor = recommender._build_advisor_view(
            total_score=4.0,
            trend={'trend': 'bullish', 'trend_text': '上升趋势'},
            unified_trend={'trend_direction': 'bullish', 'trend_score': 61, 'trend_confidence': 32},
            tech_indicators={'rsi': 59, 'volatility': 0.16, 'price_ma20_ratio': 0.02},
        )
        self.assertIn(advisor['action'], ['hold', 'watch'])

    def test_build_advisor_view_respects_low_model_reliability(self):
        recommender = StockRecommender.__new__(StockRecommender)
        advisor = recommender._build_advisor_view(
            total_score=4.45,
            trend={'trend': 'bullish', 'trend_text': '上升趋势'},
            unified_trend={'trend_direction': 'bullish', 'trend_score': 73, 'trend_confidence': 82},
            tech_indicators={'rsi': 55, 'volatility': 0.15, 'price_ma20_ratio': 0.03},
            model_reliability={'score': 34, 'level': 'low', 'label': 'guarded'},
        )
        self.assertIn(advisor['action'], ['hold', 'watch'])
        self.assertEqual(advisor['model_reliability']['level'], 'low')

    def test_build_recommendation_advisor_payload_uses_item_reliability_override(self):
        advisor = _build_recommendation_advisor_payload({
            'total_score': 4.5,
            'up_probability_5d': 68,
            'up_probability_20d': 66,
            'up_probability_60d': 64,
            'volatility_level': 'low',
            'unified_trend': {'trend_direction': 'bullish', 'trend_score': 69, 'trend_confidence': 80},
            'market_model_reliability': {'score': 32, 'level': 'low', 'label': 'guarded'},
        })
        self.assertIn(advisor['action'], ['hold', 'watch'])
        self.assertEqual((advisor.get('model_reliability') or {}).get('level'), 'low')

    def test_classify_recommendation_strength_does_not_overstate_hold_signal(self):
        strength = _classify_recommendation_strength({
            'advisor_action': 'hold',
            'risk_level': 'low',
            'up_probability_5d': 61,
            'up_probability_20d': 67,
            'up_probability_60d': 69,
            'total_score': 4.1,
            'evidence_score': 58,
            'position_size_pct': 0,
        })
        self.assertNotEqual(strength['label'], '强看涨')
        self.assertIn(strength['level'], ['bullish_hold', 'bullish_watch', 'neutral_watch'])

    def test_build_asset_model_status_downgrades_stock_model_when_probability_health_is_weak(self):
        from types import SimpleNamespace
        from unittest import mock

        fake_context = {
            'short_term': SimpleNamespace(is_trained=True),
            'medium_term': SimpleNamespace(is_trained=True),
            'long_term': SimpleNamespace(is_trained=True),
            'quality_snapshot': {'short_term': {'label': 'stable'}},
        }

        with mock.patch('api.recommendations.recommender._resolve_market_predictor_context', return_value=fake_context):
            status = _build_asset_model_status('us_stock', {
                '5': {
                    'status': 'ok',
                    'grade': 'D',
                    'samples': 2494,
                    'hit_rate': 51.72,
                    'brier': 0.2494,
                    'calibration_gap': -4.53,
                },
                '20': {
                    'status': 'ok',
                    'grade': 'D',
                    'samples': 2136,
                    'hit_rate': 62.31,
                    'brier': 0.2348,
                    'calibration_gap': -20.5,
                },
                '60': {
                    'status': 'ok',
                    'grade': 'D',
                    'samples': 1080,
                    'hit_rate': 64.81,
                    'brier': 0.2268,
                    'calibration_gap': -26.94,
                },
            })

        self.assertFalse(status['short_term_validated'])
        self.assertEqual(status['short_term_source'], 'rule_fallback')
        self.assertEqual((status['market_model_reliability'] or {}).get('label'), 'guarded')

    def test_resolve_market_predictor_context_prefers_market_specific_bundle(self):
        recommender = StockRecommender.__new__(StockRecommender)
        recommender._predictor_contexts = {
            'A': {'market': 'A'},
            'H': {'market': 'H'},
            'US': {'market': 'US'},
        }
        self.assertEqual(recommender._resolve_market_predictor_context('A')['market'], 'A')
        self.assertEqual(recommender._resolve_market_predictor_context('H')['market'], 'H')
        self.assertEqual(recommender._resolve_market_predictor_context('US')['market'], 'US')
        self.assertEqual(recommender._resolve_market_predictor_context('unknown')['market'], 'A')


class TestHoldingAdvisorLayer(unittest.TestCase):
    """持仓投顾汇总测试"""

    def test_build_portfolio_health_summary_flags_concentration_risk(self):
        summary = _build_portfolio_health_summary(
            holding_signals=[
                {'market_value': 85000, 'profit_rate': -6.0},
                {'market_value': 10000, 'profit_rate': 2.0},
            ],
            current_asset_actions=[
                {'action': '减仓', 'level': 'high'},
                {'action': '清仓', 'level': 'high'},
            ],
            risk_alerts=[{'level': 'high'}],
            action_suggestions=[],
            unheld_recommendations=[],
        )
        self.assertEqual(summary['overall_risk'], 'high')
        self.assertTrue(any('集中' in x for x in summary['key_issues']))

    def test_build_portfolio_health_summary_rewards_balanced_setup(self):
        summary = _build_portfolio_health_summary(
            holding_signals=[
                {'market_value': 25000, 'profit_rate': 8.0},
                {'market_value': 23000, 'profit_rate': 5.0},
                {'market_value': 21000, 'profit_rate': 3.0},
                {'market_value': 19000, 'profit_rate': 4.0},
                {'market_value': 17000, 'profit_rate': 6.0},
            ],
            current_asset_actions=[
                {'action': '持有', 'level': 'low'},
                {'action': '增仓', 'level': 'low'},
            ],
            risk_alerts=[],
            action_suggestions=[{'action': 'add'}],
            unheld_recommendations=[{'code': 'AAA'}],
        )
        self.assertIn(summary['overall_risk'], ['low', 'medium'])
        self.assertGreaterEqual(summary['health_score'], 60)

    def test_select_diversified_unheld_recommendations_limits_overseas_clusters(self):
        picks = _select_diversified_unheld_recommendations(
            [
                {'code': 'IAU', 'type': 'gold', 'score': 67.3, 'rank': 1},
                {'code': 'GLDM', 'type': 'gold', 'score': 67.2, 'rank': 2},
                {'code': 'SGOL', 'type': 'gold', 'score': 67.1, 'rank': 3},
                {'code': 'AAPL', 'type': 'us_stock', 'score': 66.8, 'rank': 4},
                {'code': '000001.SZ', 'type': 'a_stock', 'score': 64.9, 'rank': 5},
                {'code': '000006', 'type': 'active_fund', 'score': 64.8, 'rank': 6},
                {'code': '510300.SH', 'type': 'etf', 'score': 64.7, 'rank': 7},
            ],
            limit=5,
            holding_asset_types=['fund', 'gold'],
        )
        pick_types = [item['type'] for item in picks]
        self.assertIn('a_stock', pick_types)
        self.assertIn('active_fund', pick_types)
        self.assertLessEqual(pick_types.count('gold'), 1)
        self.assertEqual(len(picks), 5)


class TestUnifiedTrendLayer(unittest.TestCase):
    """统一趋势融合层测试"""

    def test_derive_unified_trend_prefers_validated_medium_long(self):
        """验证已通过校验的中长期模型应主导统一趋势"""
        result = derive_unified_trend({
            'up_probability_5d': 41,
            'up_probability_20d': 66,
            'up_probability_60d': 72,
            'model_status': {
                'short_term_validated': False,
                'medium_term_validated': True,
                'long_term_validated': True,
            }
        })

        self.assertIn('trend_direction', result)
        self.assertIn('trend_score', result)
        self.assertGreater(result['trend_score'], 60)
        self.assertEqual(result['trend_direction'], 'bullish')

    def test_derive_unified_trend_fallback_to_neutral(self):
        """验证缺失概率时能稳定回退到中性"""
        result = derive_unified_trend({})
        self.assertEqual(result['trend_direction'], 'neutral')
        self.assertEqual(result['trend_score'], 50.0)


class TestTrainingCodeQualitySelection(unittest.TestCase):
    """训练样本质量筛选测试"""

    def test_select_quality_training_codes_prefers_stable_liquid_history(self):
        from types import SimpleNamespace

        stable = create_test_data(days=260)
        stable['close'] = np.linspace(100, 130, 260) + np.sin(np.linspace(0, 8, 260)) * 1.5
        stable['open'] = stable['close'] * 0.998
        stable['high'] = stable['close'] * 1.01
        stable['low'] = stable['close'] * 0.99
        stable['volume'] = 5_000_000

        noisy = create_test_data(days=260)
        rng = np.random.default_rng(0)
        noisy['close'] = 100 + rng.normal(0, 10, 260).cumsum()
        noisy['open'] = noisy['close'] + rng.normal(0, 2, 260)
        noisy['high'] = noisy[['open', 'close']].max(axis=1) + np.abs(rng.normal(0, 3, 260))
        noisy['low'] = noisy[['open', 'close']].min(axis=1) - np.abs(rng.normal(0, 3, 260))
        noisy['volume'] = 20_000

        trainer = ModelTrainer.__new__(ModelTrainer)
        trainer.collector = SimpleNamespace(get_stock_data_from_db=lambda code: {'STABLE': stable, 'NOISY': noisy}.get(code))

        selected = trainer._select_quality_training_codes(['NOISY', 'STABLE'], period_days=20, target_count=1)
        self.assertEqual(selected, ['STABLE'])


class TestModelManager(unittest.TestCase):
    """模型管理器测试"""
    
    def setUp(self):
        self.manager = ModelManager()
    
    def test_list_models(self):
        """测试列出模型"""
        models = self.manager.list_models()
        self.assertIsInstance(models, list)
        print(f"✅ 列出模型测试通过: {len(models)} 个模型")
    
    def test_get_model_info(self):
        """测试获取模型信息"""
        info = self.manager.get_model_info()
        if info:
            self.assertIn('version', info)
            print(f"✅ 模型信息测试通过: {info['version']}")
        else:
            print("⚠️ 模型信息测试: 无模型")

    def test_evaluate_validation_gate_acc_f1(self):
        """测试主门槛 acc+f1 通过"""
        passed, gate, _ = self.manager.evaluate_validation_gate(
            20,
            {'accuracy': 0.56, 'f1': 0.51}
        )
        self.assertTrue(passed)
        self.assertEqual(gate, 'acc_f1')

    def test_evaluate_validation_gate_auc_brier_for_5d(self):
        """测试5日辅助门槛 auc+brier 通过"""
        passed, gate, _ = self.manager.evaluate_validation_gate(
            5,
            {'accuracy': 0.54, 'f1': 0.49, 'auc': 0.63, 'brier': 0.24}
        )
        self.assertTrue(passed)
        self.assertEqual(gate, 'auc_brier')

    def test_evaluate_validation_gate_failed(self):
        """测试门槛不通过"""
        passed, gate, _ = self.manager.evaluate_validation_gate(
            5,
            {'accuracy': 0.52, 'f1': 0.46, 'auc': 0.58, 'brier': 0.31}
        )
        self.assertFalse(passed)
        self.assertEqual(gate, 'failed')

    def test_activate_model_scopes_to_same_horizon(self):
        """测试激活版本按周期隔离，不会互相覆盖"""
        version_5 = f"test5_{datetime.now().strftime('%H%M%S%f')}"
        version_20 = f"test20_{datetime.now().strftime('%H%M%S%f')}"

        saved_5 = self.manager.save_model(
            model={'kind': 'short-test'},
            model_type='xgboost',
            period_days=5,
            version=version_5,
            metadata={'validation_accuracy': 0.61, 'validation_f1': 0.55}
        )
        saved_20 = self.manager.save_model(
            model={'kind': 'medium-test'},
            model_type='xgboost',
            period_days=20,
            version=version_20,
            metadata={'validation_accuracy': 0.63, 'validation_f1': 0.57}
        )

        self.assertEqual(saved_5, version_5)
        self.assertEqual(saved_20, version_20)
        self.assertTrue(self.manager.activate_model(version_5, model_type='xgboost', period_days=5))
        self.assertTrue(self.manager.activate_model(version_20, model_type='xgboost', period_days=20))

        session = get_session()
        try:
            record_5 = session.query(ModelVersion).filter(ModelVersion.version == version_5).first()
            record_20 = session.query(ModelVersion).filter(ModelVersion.version == version_20).first()
            self.assertIsNotNone(record_5)
            self.assertIsNotNone(record_20)
            self.assertTrue(record_5.is_active)
            self.assertTrue(record_20.is_active)
        finally:
            session.close()
            self.manager.delete_model(version_5)
            self.manager.delete_model(version_20)


def run_tests():
    """运行所有测试"""
    print("=" * 50)
    print("预测模块测试")
    print("=" * 50)
    
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestShortTermPredictor))
    suite.addTests(loader.loadTestsFromTestCase(TestMediumTermPredictor))
    suite.addTests(loader.loadTestsFromTestCase(TestLongTermPredictor))
    suite.addTests(loader.loadTestsFromTestCase(TestModelTrainerPerformance))
    suite.addTests(loader.loadTestsFromTestCase(TestUnifiedTrendLayer))
    suite.addTests(loader.loadTestsFromTestCase(TestModelManager))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print("\n" + "=" * 50)
    print(f"测试完成: 运行 {result.testsRun} 个测试")
    print(f"成功: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"失败: {len(result.failures)}")
    print(f"错误: {len(result.errors)}")
    print("=" * 50)
    
    return result


if __name__ == '__main__':
    run_tests()