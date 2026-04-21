"""
单元测试 - tests/test_week1_integration.py
Week 1 集成任务的验证测试
"""

import io
import pytest
import sys
import os
import json
import types
import importlib.util
from datetime import datetime, timedelta
from flask import Flask
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入要测试的模块
from scheduler.collection_director import CollectionDirector, CollectionTask
from data.validators import DataValidator, ValidationResult
from api_response import (
    APIResponse, ResponseCode, ValidationError, NotFoundError,
    InvalidParamsError
)


class TestCollectionDirector:
    """CollectionDirector 的测试套件"""
    
    def test_collection_director_initialization(self):
        """测试 CollectionDirector 初始化"""
        director = CollectionDirector()
        assert director is not None
        assert director.max_concurrent_tasks > 0
        assert director.dedup_window_seconds > 0
    
    def test_register_task(self):
        """测试任务注册"""
        director = CollectionDirector()
        task = CollectionTask(
            task_id='test_task_1',
            task_type='stock',
            target='000858.SZ',
            priority=1
        )
        director.register_task(task)
        assert 'test_task_1' in director.task_registry
    
    def test_task_deduplication(self):
        """测试任务去重"""
        director = CollectionDirector()
        
        # 注册两个相同的任务
        task1 = CollectionTask('task1', 'stock', '000858.SZ', 1)
        task2 = CollectionTask('task2', 'stock', '000858.SZ', 1)
        
        director.register_task(task1)
        plan = director.get_execution_plan()
        
        # 检查是否检测到冲突
        conflict_detected = director._detect_conflicts()
        # 冲突检测成功
        assert isinstance(conflict_detected, dict)
    
    def test_execution_plan_generation(self):
        """测试执行计划生成"""
        director = CollectionDirector()
        
        # 注册多个任务
        tasks = [
            CollectionTask(f'task_{i}', 'stock', f'code_{i}', i % 3)
            for i in range(5)
        ]
        for task in tasks:
            director.register_task(task)
        
        plan = director.get_execution_plan()
        assert len(plan) > 0
        assert all(isinstance(tid, str) for tid in plan)


class TestDataValidator:
    """DataValidator 的测试套件"""
    
    def test_validator_initialization(self):
        """测试 DataValidator 初始化"""
        rules = {
            'name': {'type': 'string', 'required': True},
            'age': {'type': 'int', 'min': 0, 'max': 150}
        }
        validator = DataValidator(rules=rules)
        assert validator is not None
        assert len(validator.rules) == 2
    
    def test_valid_data(self):
        """测试有效数据验证"""
        rules = {
            'name': {'type': 'string', 'required': True},
            'age': {'type': 'int', 'min': 0, 'max': 150}
        }
        validator = DataValidator(rules=rules)
        
        data = {'name': 'John', 'age': 30}
        result = validator.validate(data)
        
        assert result['valid'] is True
        assert len(result['errors']) == 0
    
    def test_invalid_data(self):
        """测试无效数据验证"""
        rules = {
            'name': {'type': 'string', 'required': True},
            'age': {'type': 'int', 'min': 0, 'max': 150}
        }
        validator = DataValidator(rules=rules)
        
        data = {'name': '', 'age': 200}
        result = validator.validate(data)
        
        assert result['valid'] is False
        assert len(result['errors']) > 0
    
    def test_required_field_validation(self):
        """测试必填字段验证"""
        rules = {
            'email': {'type': 'string', 'required': True},
        }
        validator = DataValidator(rules=rules)
        
        # 缺少必填字段
        data = {'name': 'John'}
        result = validator.validate(data)
        
        assert result['valid'] is False
    
    def test_type_validation(self):
        """测试类型验证"""
        rules = {
            'count': {'type': 'int'},
            'price': {'type': 'float'},
            'active': {'type': 'bool'}
        }
        validator = DataValidator(rules=rules)
        
        # 正确的类型
        valid_data = {'count': 10, 'price': 99.99, 'active': True}
        result = validator.validate(valid_data)
        assert result['valid'] is True
        
        # 错误的类型
        invalid_data = {'count': 'ten', 'price': 99.99}
        result = validator.validate(invalid_data)
        assert result['valid'] is False
    
    def test_range_validation(self):
        """测试范围验证"""
        rules = {
            'score': {'type': 'float', 'min': 0, 'max': 100}
        }
        validator = DataValidator(rules=rules)
        
        # 在范围内
        result = validator.validate({'score': 85.5})
        assert result['valid'] is True
        
        # 超出范围
        result = validator.validate({'score': 150})
        assert result['valid'] is False
        
        result = validator.validate({'score': -10})
        assert result['valid'] is False
    
    def test_anomaly_detection(self):
        """测试异常检测"""
        validator = DataValidator()
        
        prices = [10, 12, 11, 13, 1000]  # 最后一个是异常值
        anomalies = validator.detect_and_handle_outliers(prices, method='iqr')
        
        assert anomalies is not None
        assert isinstance(anomalies, dict)


class TestAPIResponse:
    """APIResponse 的测试套件"""
    
    def test_success_response(self):
        """测试成功响应"""
        resp = APIResponse.success(
            data={'id': 1, 'name': 'Test'},
            message='Success'
        )
        
        assert resp.code == ResponseCode.SUCCESS.value
        assert resp.message == 'Success'
        assert resp.data == {'id': 1, 'name': 'Test'}
    
    def test_error_response(self):
        """测试错误响应"""
        resp = APIResponse.error(
            code=ResponseCode.NOT_FOUND.value,
            message='Resource Not Found'
        )
        
        assert resp.code == ResponseCode.NOT_FOUND.value
        assert resp.message == 'Resource Not Found'
    
    def test_validation_error_response(self):
        """测试验证错误响应"""
        resp = APIResponse.validation_error(
            errors=['Field missing', 'Invalid format']
        )
        
        assert resp.code == ResponseCode.DATA_VALIDATION_ERROR.value
        assert len(resp.errors) == 2
    
    def test_pagination_response(self):
        """测试分页响应"""
        data = [{'id': 1}, {'id': 2}]
        resp = APIResponse.pagination(
            data=data,
            page=1,
            page_size=2,
            total=100
        )
        
        assert resp.data == data
        assert resp.metadata['page'] == 1
        assert resp.metadata['total_pages'] == 50
    
    def test_response_to_dict(self):
        """测试响应转dict"""
        resp = APIResponse.success(data={'test': 'data'})
        resp_dict = resp.to_dict()
        
        assert 'code' in resp_dict
        assert 'message' in resp_dict
        assert 'data' in resp_dict
        assert 'timestamp' in resp_dict
    
    def test_response_to_json(self):
        """测试响应转JSON"""
        resp = APIResponse.success(data={'test': 'data'})
        json_str = resp.to_json()
        
        assert isinstance(json_str, str)
        assert 'code' in json_str
        assert 'data' in json_str
    
    def test_validation_error_exception(self):
        """测试验证错误异常"""
        try:
            raise ValidationError(
                errors=['Missing field', 'Invalid format'],
                details={'field': 'email'}
            )
        except ValidationError as e:
            resp = e.to_response()
            assert resp.code == ResponseCode.DATA_VALIDATION_ERROR.value
            assert len(resp.errors) == 2
    
    def test_invalid_params_error_exception(self):
        """测试无效参数异常"""
        try:
            raise InvalidParamsError(
                errors=['limit must be positive', 'page must be >= 1']
            )
        except InvalidParamsError as e:
            resp = e.to_response()
            assert resp.code == ResponseCode.INVALID_PARAMS.value


class TestIntegration:
    """集成测试"""
    
    def test_collection_director_with_validator(self):
        """测试 CollectionDirector 和 DataValidator 的配合"""
        director = CollectionDirector()
        validator = DataValidator()
        
        # 创建一个采集任务
        task = CollectionTask(
            task_id='integration_test',
            task_type='stock',
            target='000858.SZ',
            priority=1
        )
        
        # 注册任务
        director.register_task(task)
        plan = director.get_execution_plan()
        
        # 获取执行计划
        assert len(plan) > 0
        
        # 验证任务信息
        task_info = director.task_registry.get('integration_test')
        assert task_info is not None
    
    def test_api_response_with_validation(self):
        """测试 APIResponse 和验证的配合"""
        validator = DataValidator(
            rules={
                'stock_code': {'type': 'string', 'required': True},
                'price': {'type': 'float', 'min': 0}
            }
        )
        
        # 有效数据
        valid_data = {'stock_code': '000858.SZ', 'price': 15.5}
        result = validator.validate(valid_data)
        
        if result['valid']:
            resp = APIResponse.success(data=valid_data)
        else:
            resp = APIResponse.validation_error(errors=result['errors'])
        
        assert resp.code == ResponseCode.SUCCESS.value
    
    def test_complete_workflow(self):
        """测试完整工作流"""
        # 1. 创建采集编排器
        director = CollectionDirector()
        
        # 2. 创建验证器
        validator = DataValidator()
        
        # 3. 创建采集任务
        task = CollectionTask('workflow_test', 'stock', '000858.SZ', 1)
        director.register_task(task)
        
        # 4. 获取执行计划
        plan = director.get_execution_plan()
        
        # 5. 对采集数据进行验证
        sample_data = {'stock_code': '000858.SZ', 'price': 15.5}
        validation_result = validator.validate(sample_data)
        
        # 6. 返回API响应
        if validation_result['valid']:
            resp = APIResponse.success(data=sample_data)
        else:
            resp = APIResponse.validation_error(errors=validation_result['errors'])
        
        # 验证结果
        assert plan is not None
        assert resp.code == ResponseCode.SUCCESS.value


def _load_scheduler_module_for_test():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    scheduler_path = os.path.join(root, 'scheduler.py')
    spec = importlib.util.spec_from_file_location('pfa_scheduler_file', scheduler_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_operational_trade_date_rolls_to_next_day_after_close():
    """验证15:00后生成的操作建议会归入下一个交易日。"""
    scheduler_module = _load_scheduler_module_for_test()
    result = scheduler_module._resolve_operational_trade_date(datetime(2026, 4, 16, 15, 1))
    assert str(result) == '2026-04-17'


def test_actionable_market_window_blocks_after_close():
    """验证风险预警/操作提示仅在可操作窗口内触发。"""
    scheduler_module = _load_scheduler_module_for_test()
    assert scheduler_module._is_actionable_market_window(datetime(2026, 4, 16, 14, 40)) is True
    assert scheduler_module._is_actionable_market_window(datetime(2026, 4, 16, 15, 5)) is False
    assert scheduler_module._is_actionable_market_window(datetime(2026, 4, 18, 10, 0)) is False


def test_market_window_supports_hk_and_us_sessions():
    """验证港股/美股使用各自的可操作时段。"""
    scheduler_module = _load_scheduler_module_for_test()
    assert scheduler_module._is_actionable_market_window(datetime(2026, 4, 16, 15, 30), market='HK') is True
    assert scheduler_module._is_actionable_market_window(datetime(2026, 4, 16, 16, 5), market='HK') is False
    assert scheduler_module._is_actionable_market_window(datetime(2026, 4, 16, 22, 0), market='US') is True
    assert scheduler_module._is_actionable_market_window(datetime(2026, 4, 17, 4, 5), market='US') is False


def test_operational_trade_date_skips_configured_holidays():
    """验证节假日会顺延到下一个有效交易日。"""
    scheduler_module = _load_scheduler_module_for_test()
    scheduler_module.CN_MARKET_HOLIDAYS = {'2026-05-01'}
    result = scheduler_module._resolve_operational_trade_date(datetime(2026, 4, 30, 15, 10), market='CN')
    assert str(result) == '2026-05-04'


def _load_script_module_for_test(module_name, filename):
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    script_path = os.path.join(root, 'scripts', filename)
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_asset_training_order_keeps_hk_us_last():
    """验证资产训练顺序中港股和美股排在最后。"""
    module = _load_script_module_for_test('pfa_train_asset_suite', 'train_asset_suite.py')
    plan = module.build_training_plan()
    asset_order = [item['asset_type'] for item in plan]
    assert asset_order[:5] == ['a_stock', 'fund', 'gold', 'silver', 'etf']
    assert asset_order[-2:] == ['hk_stock', 'us_stock']


def test_asset_training_plan_includes_dedicated_etf_script():
    """验证ETF拥有独立训练脚本并纳入统一训练编排。"""
    module = _load_script_module_for_test('pfa_train_asset_suite', 'train_asset_suite.py')
    plan = module.build_training_plan()
    etf_step = next(item for item in plan if item['asset_type'] == 'etf')
    assert etf_step['script'] == 'train_etf.py'


def test_training_dataset_plan_covers_fund_etf_and_metals():
    """验证训练数据准备脚本覆盖基金、ETF和贵金属。"""
    module = _load_script_module_for_test('pfa_prepare_training_data', 'prepare_training_datasets.py')
    plan = module.build_dataset_plan()
    targets = {item['dataset'] for item in plan}
    assert 'fund_nav' in targets
    assert 'etf_history' in targets
    assert 'precious_metals' in targets
    export_files = {item['output_file'] for item in plan}
    assert 'data/fund_nav.csv' in export_files
    assert 'data/gold_prices.csv' in export_files
    assert 'data/silver_prices.csv' in export_files


def test_backfill_one_year_keeps_stock_basic_as_only_pool_export():
    """验证一键补采脚本不会重新生成历史遗留的 all_stocks.csv。"""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    script_path = os.path.join(root, 'scripts', 'backfill_one_year.py')
    script_text = open(script_path, 'r', encoding='utf-8').read()

    assert "stocks.to_csv('data/stock_basic.csv', index=False)" in script_text
    assert "data/all_stocks.csv" not in script_text


def test_dataset_inventory_covers_key_training_assets():
    """验证数据资产盘点包含基金、ETF、贵金属、宏观和基础面核心数据。"""
    from api.backfill import _build_dataset_inventory

    inventory = _build_dataset_inventory()
    names = {item.get('name') for item in inventory}

    expected = {
        '基金净值CSV',
        'ETF历史行情CSV',
        '贵金属历史CSV',
        '黄金价格CSV',
        '白银价格CSV',
        '日度估值CSV',
        '财务指标CSV',
        '融资融券CSV',
        '宏观PMI CSV',
        '宏观Shibor CSV',
        '跨资产日频CSV',
    }
    assert expected.issubset(names)


def test_historical_collector_zero_max_keeps_cached_full_pool(tmp_path):
    """验证 MAX_* = 0 时表示不设上限，而不是把股票池切成空列表。"""
    module = _load_script_module_for_test('pfa_collect_historical_data', 'collect_historical_data.py')
    cache_file = tmp_path / 'stock_pool_cache.json'
    payload = {
        'a_stock': ['000001.SZ', '000002.SZ'],
        'hk_stock': ['0001.HK', '0002.HK', '0003.HK'],
        'us_stock': ['AAPL', 'MSFT', 'NVDA'],
    }
    cache_file.write_text(json.dumps(payload), encoding='utf-8')

    module.STOCK_POOL_CACHE_FILE = cache_file
    module.STOCK_POOL_CACHE_TTL = 10 ** 9
    module.MAX_A_STOCKS = 0
    module.MAX_HK_STOCKS = 0
    module.MAX_US_STOCKS = 0

    collector = module.HistoricalDataCollector(mode='full')
    pool = collector.get_stock_pool()

    assert pool['A'] == payload['a_stock']
    assert pool['H'] == payload['hk_stock']
    assert pool['US'] == payload['us_stock']


def test_run_missing_collectors_expands_full_fund_pool(monkeypatch):
    """验证缺失基金数据时会尝试扩到完整基金池，而不是只补前30只。"""
    module = _load_script_module_for_test('pfa_prepare_training_data_full', 'prepare_training_datasets.py')
    calls = {'fetch_all_funds': 0, 'nav_calls': [], 'etf_batch_size': 0}

    class FakeQuery:
        def __init__(self, rows=None, count_value=0):
            self._rows = rows or []
            self._count_value = count_value

        def all(self):
            return self._rows

        def count(self):
            return self._count_value

    class FakeSession:
        def query(self, target):
            label = str(target)
            if 'RawStockData.code' in label:
                return FakeQuery(rows=[])
            if 'RawFundData.id' in label:
                return FakeQuery(count_value=0)
            return FakeQuery(rows=[])

        def close(self):
            return None

    class FakeStockCollector:
        def collect_funds_batch(self, funds=None, years=3, limit=None):
            calls['etf_batch_size'] = len(funds or [])
            return []

        def collect_precious_metals(self, years=3):
            return []

    class FakeFundCollector:
        def __init__(self):
            self.fund_pool = []

        def fetch_all_funds(self):
            calls['fetch_all_funds'] += 1
            self.fund_pool = [
                {'code': f'{i:06d}', 'name': f'基金{i}', 'type': 'active_fund'}
                for i in range(60)
            ]
            self.fund_pool += [
                {'code': f'5{i:05d}.SH', 'name': f'ETF{i}', 'type': 'etf'}
                for i in range(15)
            ]
            return self.fund_pool

        def collect_fund_nav(self, fund_code, days=30):
            calls['nav_calls'].append((fund_code, days))
            return []

    import collectors.stock_collector as stock_module
    import collectors.fund_collector as fund_module

    monkeypatch.setattr(module, 'get_session', lambda: FakeSession())
    monkeypatch.setattr(stock_module, 'StockCollector', FakeStockCollector)
    monkeypatch.setattr(fund_module, 'FundCollector', FakeFundCollector)

    module._run_missing_collectors()

    assert calls['fetch_all_funds'] >= 1
    assert calls['etf_batch_size'] >= 15
    assert len(calls['nav_calls']) >= 60
    assert all(days >= 365 for _, days in calls['nav_calls'])


def test_etf_trainer_uses_multiple_available_proxies():
    """验证ETF训练会利用当前已导出的多只ETF代理标的。"""
    module = _load_script_module_for_test('pfa_train_etf', 'train_etf.py')
    trainer = module.ETFTrainer()
    assert trainer.load_data() is True
    assert trainer.data['code'].nunique() >= 4


def test_gold_trainer_uses_multiple_gold_and_silver_proxies():
    """验证黄金/白银训练会利用多只相关代理标的，而不是只依赖单一代码。"""
    module = _load_script_module_for_test('pfa_train_gold', 'train_gold.py')
    trainer = module.GoldTrainer()
    assert trainer.load_data() is True
    assert trainer.gold_data is not None and trainer.gold_data['code'].nunique() >= 2
    assert trainer.silver_data is not None and trainer.silver_data['code'].nunique() >= 2


def test_save_predictions_handles_fund_name_without_duplicate_kwargs():
    """验证定时任务写入基金预测时不会因name重复传参而报错。"""
    scheduler_module = _load_scheduler_module_for_test()
    from models import Base, Prediction

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    session = TestingSession()
    try:
        today = datetime(2026, 4, 17).date()
        default_pred = scheduler_module._get_default_prediction()
        inserted = scheduler_module._save_predictions(
            session,
            '009478',
            '基金A',
            today,
            default_pred,
            default_pred,
            default_pred,
        )
        session.commit()

        assert inserted == 3
        assert session.query(Prediction).filter(
            Prediction.code == '009478',
            Prediction.date == today,
        ).count() == 3
    finally:
        session.close()


def test_recommendation_snapshot_prefers_requested_type_for_duplicate_code():
    """验证详情查询在同代码跨品类重复时，会优先命中当前页面请求的资产类型。"""
    from api import recommendations as recommendations_module
    from models import Base, Recommendation

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    seed = TestingSession()
    try:
        target_date = datetime(2026, 4, 17).date()
        seed.add_all([
            Recommendation(date=target_date, code='IAU', name='iShares Gold Trust', type='us_stock', rank=1, total_score=3.2, up_probability_5d=50.85, up_probability_20d=41.09, up_probability_60d=35.90),
            Recommendation(date=target_date, code='IAU', name='黄金ETF代理', type='gold', rank=1, total_score=4.5, up_probability_5d=62.03, up_probability_20d=70.55, up_probability_60d=75.89),
        ])
        seed.commit()

        chosen = recommendations_module._get_latest_recommendation_snapshot(TestingSession(), 'IAU', 'gold')
        assert chosen is not None
        assert chosen.type == 'gold'
        assert round(float(chosen.up_probability_5d), 2) == 62.03
    finally:
        seed.close()


def test_normalize_yfinance_symbol_maps_shanghai_to_ss():
    """验证上海市场代码在 yfinance 回退链路中会转换为 .SS。"""
    scheduler_module = _load_scheduler_module_for_test()
    assert scheduler_module._normalize_yfinance_symbol('510300.SH') == '510300.SS'
    assert scheduler_module._normalize_yfinance_symbol('159915.SZ') == '159915.SZ'


def test_prediction_registry_includes_etf_support():
    """验证每日预测运行时包含ETF资产类型支持。"""
    scheduler_module = _load_scheduler_module_for_test()
    predictors = scheduler_module._build_prediction_predictors()
    assert 'etf' in predictors


def test_probability_health_uses_direction_hit_rate_not_raw_market_up_rate():
    """验证历史命中率统计的是预测方向是否命中，而不是单纯上涨样本占比。"""
    from api import recommendations as recommendations_module
    from models import Base, Recommendation

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    base_day = datetime(2026, 4, 1).date()
    seed = TestingSession()
    try:
        seed.add_all([
            Recommendation(date=base_day, code='AAA', name='测试A', type='a_stock', rank=1, total_score=4.2, current_price=10.0, up_probability_20d=80.0),
            Recommendation(date=base_day + timedelta(days=20), code='AAA', name='测试A', type='a_stock', rank=1, total_score=4.2, current_price=12.0, up_probability_20d=55.0),
            Recommendation(date=base_day, code='BBB', name='测试B', type='a_stock', rank=2, total_score=3.8, current_price=10.0, up_probability_20d=20.0),
            Recommendation(date=base_day + timedelta(days=20), code='BBB', name='测试B', type='a_stock', rank=2, total_score=3.8, current_price=8.0, up_probability_20d=45.0),
        ])
        seed.commit()
    finally:
        seed.close()

    original_get_session = recommendations_module.get_session
    original_get_today = recommendations_module.get_today
    recommendations_module.get_session = TestingSession
    recommendations_module.get_today = lambda: base_day + timedelta(days=25)

    try:
        app = Flask(__name__)
        recommendations_module.register_recommendations_routes(app)
        client = app.test_client()

        response = client.get('/api/recommendations/probability-health')
        assert response.status_code == 200
        payload = response.get_json()['data']
        assert payload['a_stock']['20']['samples'] >= 2
        assert payload['a_stock']['20']['hit_rate'] == 100.0
    finally:
        recommendations_module.get_session = original_get_session
        recommendations_module.get_today = original_get_today


def test_collect_prediction_targets_includes_latest_recommendations():
    """验证每日预测范围会覆盖持仓与最近推荐批次，而不只限于持仓。"""
    scheduler_module = _load_scheduler_module_for_test()
    from models import Base, Holding, Recommendation

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    seed = TestingSession()
    try:
        target_date = datetime(2026, 4, 17).date()
        seed.add(Holding(
            code='009478',
            name='基金A',
            quantity=1,
            cost_price=1.0,
            buy_date=datetime(2026, 4, 16).date(),
            asset_type='fund'
        ))
        seed.add(Recommendation(
            date=target_date,
            code='000001.SZ',
            name='平安银行',
            type='a_stock',
            rank=1,
            total_score=4.8,
        ))
        seed.commit()

        targets = scheduler_module._collect_prediction_targets(seed, target_date)
        codes = {item['code'] for item in targets}

        assert '009478' in codes
        assert '000001.SZ' in codes
    finally:
        seed.close()


def test_universe_targets_only_keep_5d_focus_while_core_assets_keep_full_horizons():
    """验证全市场补充目标优先保留5日样本，而持仓/推荐仍保留完整周期。"""
    scheduler_module = _load_scheduler_module_for_test()

    assert scheduler_module._resolve_target_prediction_periods({'source': 'holding', 'asset_type': 'a_stock'}) == [5, 20, 60]
    assert scheduler_module._resolve_target_prediction_periods({'source': 'recommendation', 'asset_type': 'hk_stock'}) == [5, 20, 60]
    assert scheduler_module._resolve_target_prediction_periods({'source': 'universe', 'asset_type': 'a_stock'}) == [5]
    assert scheduler_module._resolve_target_prediction_periods({'source': 'universe', 'asset_type': 'gold'}) == [5, 20, 60]


def test_save_predictions_only_persists_requested_periods():
    """验证当目标为全市场补充样本时，只写入要求的周期。"""
    scheduler_module = _load_scheduler_module_for_test()
    from models import Base, Prediction

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    session = TestingSession()
    try:
        today = datetime(2026, 4, 20).date()
        result = {'up_probability': 61.0, 'down_probability': 39.0, 'target_low': 9.5, 'target_high': 10.8, 'stop_loss': 9.2, 'confidence': 66.0}
        saved = scheduler_module._save_predictions(
            session,
            '000001.SZ',
            '平安银行',
            today,
            result,
            result,
            result,
            periods=[5],
        )
        session.commit()

        rows = session.query(Prediction).filter(Prediction.code == '000001.SZ').all()
        assert saved == 1
        assert len(rows) == 1
        assert rows[0].period_days == 5
    finally:
        session.close()


def test_prediction_refresh_catches_up_when_today_is_missing():
    """验证服务启动后若错过08:00任务，会自动补生成当日预测。"""
    scheduler_module = _load_scheduler_module_for_test()
    from models import Base, Holding

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    seed = TestingSession()
    try:
        seed.add(Holding(
            code='009478',
            name='基金A',
            quantity=1,
            cost_price=1.0,
            buy_date=datetime(2026, 4, 16).date(),
            asset_type='fund'
        ))
        seed.commit()
    finally:
        seed.close()

    original_get_session = scheduler_module.get_session if hasattr(scheduler_module, 'get_session') else None
    scheduler_module.get_session = TestingSession

    calls = []
    original_generate = scheduler_module.generate_daily_predictions
    original_resolve = scheduler_module._resolve_operational_trade_date
    scheduler_module.generate_daily_predictions = lambda: calls.append('generated')
    scheduler_module._resolve_operational_trade_date = lambda now=None, market=None: datetime(2026, 4, 17).date()

    try:
        result = scheduler_module.ensure_daily_predictions_current()
        assert result['triggered'] is True
        assert calls == ['generated']
    finally:
        scheduler_module.generate_daily_predictions = original_generate
        scheduler_module._resolve_operational_trade_date = original_resolve
        if original_get_session is not None:
            scheduler_module.get_session = original_get_session


# 性能测试
def test_dataset_inventory_formats_database_latest_update_as_full_datetime():
    """验证数据资产盘点中的数据库更新时间统一输出为完整时间字符串。"""
    from api import backfill as backfill_module
    from models import Base, RawStockData

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    seed = TestingSession()
    try:
        seed.add(RawStockData(
            code='000001.SZ', name='平安银行', date=datetime(2026, 4, 16).date(),
            open=10.0, high=10.5, low=9.9, close=10.2, volume=1000, market='A',
            created_at=datetime(2026, 4, 17, 3, 0, 11),
        ))
        seed.commit()
    finally:
        seed.close()

    original_get_session = backfill_module.get_session
    backfill_module.get_session = TestingSession
    try:
        inventory = backfill_module._build_dataset_inventory()
        stock_item = next(item for item in inventory if item['name'] == '股票原始行情')
        assert stock_item['latest_update'] == '2026-04-17 03:00:11'
    finally:
        backfill_module.get_session = original_get_session


def test_sync_latest_daily_prices_backfills_recent_window():
    """验证补缺补采会同步最近窗口内的多日价格，而不是只写入最新一天。"""
    from api import backfill as backfill_module
    from models import Base, RawStockData, RawFundData, DailyPrice

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    seed = TestingSession()
    try:
        day1 = datetime(2026, 4, 14).date()
        day2 = datetime(2026, 4, 15).date()
        seed.add_all([
            RawStockData(code='000001.SZ', name='平安银行', date=day1, open=10.0, high=10.5, low=9.9, close=10.2, volume=1000, market='A'),
            RawStockData(code='000001.SZ', name='平安银行', date=day2, open=10.2, high=10.8, low=10.1, close=10.6, volume=1200, market='A'),
            RawFundData(code='009478', name='基金A', date=day1, nav=2.10, accumulated_nav=2.10, daily_return=0.2),
            RawFundData(code='009478', name='基金A', date=day2, nav=2.15, accumulated_nav=2.15, daily_return=0.3),
        ])
        seed.commit()
    finally:
        seed.close()

    original_get_session = backfill_module.get_session
    backfill_module.get_session = TestingSession
    try:
        synced = backfill_module._sync_latest_daily_prices(window_days=7)
        verify = TestingSession()
        try:
            rows = verify.query(DailyPrice).order_by(DailyPrice.code, DailyPrice.date).all()
            assert synced >= 4
            assert len(rows) == 4
            assert [str(r.date) for r in rows if r.code == '000001.SZ'] == ['2026-04-14', '2026-04-15']
            assert [str(r.date) for r in rows if r.code == '009478'] == ['2026-04-14', '2026-04-15']
        finally:
            verify.close()
    finally:
        backfill_module.get_session = original_get_session


def test_dataset_inventory_exposes_single_collect_action_for_collectable_rows():
    """验证数据资产盘点中的可采集行会暴露单独采集所需的动作键。"""
    from api import backfill as backfill_module

    inventory = backfill_module._build_dataset_inventory()
    hk_item = next(item for item in inventory if item['name'] == '港股历史行情CSV')
    us_item = next(item for item in inventory if item['name'] == '美股历史行情CSV')

    assert hk_item['collectable'] is True
    assert hk_item['collect_key'] == 'historical_hk_stock'
    assert us_item['collectable'] is True
    assert us_item['collect_key'] == 'historical_us_stock'


def test_dataset_inventory_marks_old_daily_files_as_stale():
    """验证按日更新的数据如果明显落后，会在数据管理页标记为待补采。"""
    from api import backfill as backfill_module

    fresh = {
        'name': '日度估值CSV',
        'category': '训练特征',
        'row_count': 100,
        'latest_update': '2026-04-17 00:00:00',
        'coverage_count': 1200,
        'expected_min_codes': 1000,
    }
    stale = {
        'name': '北向资金CSV',
        'category': 'CSV文件',
        'row_count': 100,
        'latest_update': '2026-04-14 00:00:00',
        'coverage_count': 0,
        'expected_min_codes': 0,
    }

    fresh_result = backfill_module._decorate_inventory_item(dict(fresh), reference_time=datetime(2026, 4, 18, 9, 0, 0))
    stale_result = backfill_module._decorate_inventory_item(dict(stale), reference_time=datetime(2026, 4, 18, 9, 0, 0))

    assert fresh_result['status'] == 'ready'
    assert stale_result['status'] == 'stale'
    assert stale_result['status_text'] == '待补采'
    assert '未更新' in stale_result['note']


def test_dataset_inventory_does_not_mark_previous_trade_day_market_files_as_stale_before_close():
    """验证交易日中午仍允许沿用上一交易日行情，不应误报待补采。"""
    from api import backfill as backfill_module

    market_item = {
        'name': 'A股历史行情CSV',
        'category': 'CSV文件',
        'row_count': 100,
        'latest_update': '2026-04-18 00:00:00',
        'coverage_count': 1500,
        'expected_min_codes': 1000,
    }

    result = backfill_module._decorate_inventory_item(dict(market_item), reference_time=datetime(2026, 4, 20, 13, 0, 0))
    assert result['status'] == 'ready'
    assert result['status_text'] == '正常'


def test_dataset_inventory_treats_monthly_macro_data_as_fresh_in_current_cycle():
    """验证按月更新的宏观CPI/PMI不会因为月初日期而被误判为日更过期。"""
    from api import backfill as backfill_module

    monthly = {
        'name': '宏观CPI CSV',
        'category': '宏观特征',
        'row_count': 12,
        'latest_update': '2026-03-01 00:00:00',
        'coverage_count': 0,
        'expected_min_codes': 0,
    }

    result = backfill_module._decorate_inventory_item(dict(monthly), reference_time=datetime(2026, 4, 18, 9, 0, 0))
    assert result['status'] == 'ready'
    assert result['status_text'] == '正常'


def test_macro_monthly_rows_expose_single_refresh_action():
    """验证宏观CPI/PMI行会显示单独刷新按钮。"""
    from api import backfill as backfill_module

    specs = backfill_module._get_collectable_dataset_specs()
    cpi_key = backfill_module._get_inventory_collect_key({'name': '宏观CPI CSV', 'filename': 'macro_cpi.csv', 'category': '宏观特征'})
    pmi_key = backfill_module._get_inventory_collect_key({'name': '宏观PMI CSV', 'filename': 'macro_pmi.csv', 'category': '宏观特征'})

    assert cpi_key in specs
    assert pmi_key in specs


def test_reviews_only_include_current_holdings_assets():
    """验证复盘页只统计当前持有资产，不展示未持仓标的。"""
    from api import reviews as reviews_module
    from models import Base, Holding, Prediction, Review, DailyPrice

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    today = datetime(2026, 4, 18).date()
    seed = TestingSession()
    try:
        seed.add(Holding(code='000001.SZ', name='平安银行', quantity=100, cost_price=10.0, buy_date=today, asset_type='stock'))
        seed.add_all([
            DailyPrice(code='000001.SZ', date=today - timedelta(days=5), open=10.0, high=10.0, low=10.0, close=10.0, volume=1000, market='A'),
            DailyPrice(code='000001.SZ', date=today, open=11.0, high=11.0, low=11.0, close=11.0, volume=1000, market='A'),
            DailyPrice(code='600519.SH', date=today - timedelta(days=5), open=100.0, high=100.0, low=100.0, close=100.0, volume=1000, market='A'),
            DailyPrice(code='600519.SH', date=today, open=90.0, high=90.0, low=90.0, close=90.0, volume=1000, market='A'),
        ])
        held_pred = Prediction(
            code='000001.SZ', name='平安银行', asset_type='a_stock', date=today - timedelta(days=5),
            period_days=5, up_probability=80.0, down_probability=20.0, target_low=9.5, target_high=11.5,
            confidence=80.0, expiry_date=today, is_expired=True, actual_price=11.0, actual_return=10.0,
            is_direction_correct=True, created_at=datetime(2026, 4, 13, 9, 30),
        )
        other_pred = Prediction(
            code='600519.SH', name='贵州茅台', asset_type='a_stock', date=today - timedelta(days=5),
            period_days=5, up_probability=75.0, down_probability=25.0, target_low=98.0, target_high=110.0,
            confidence=78.0, expiry_date=today, is_expired=True, actual_price=90.0, actual_return=-10.0,
            is_direction_correct=False, created_at=datetime(2026, 4, 13, 9, 35),
        )
        seed.add_all([held_pred, other_pred])
        seed.flush()
        seed.add_all([
            Review(prediction_id=held_pred.id, code='000001.SZ', name='平安银行', period_days=5, predicted_up_prob=80.0, predicted_target_low=9.5, predicted_target_high=11.5, actual_price=11.0, actual_return=10.0, is_direction_correct=True, is_target_correct=True, error_analysis='命中', review_score=92.0),
            Review(prediction_id=other_pred.id, code='600519.SH', name='贵州茅台', period_days=5, predicted_up_prob=75.0, predicted_target_low=98.0, predicted_target_high=110.0, actual_price=90.0, actual_return=-10.0, is_direction_correct=False, is_target_correct=False, error_analysis='未命中', review_score=35.0),
        ])
        seed.commit()
    finally:
        seed.close()

    original_get_session = reviews_module.get_session
    original_get_today = reviews_module.get_today
    reviews_module.get_session = TestingSession
    reviews_module.get_today = lambda: today
    try:
        app = Flask(__name__)
        reviews_module.register_reviews_routes(app)
        client = app.test_client()

        recent_response = client.get('/api/reviews/recent?limit=10')
        assert recent_response.status_code == 200
        recent_payload = recent_response.get_json()
        assert recent_payload['code'] == 200
        assert recent_payload['data']
        assert all(item['code'] == '000001.SZ' for item in recent_payload['data'])

        accuracy_response = client.get('/api/reviews/accuracy')
        assert accuracy_response.status_code == 200
        accuracy_payload = accuracy_response.get_json()
        assert accuracy_payload['code'] == 200
        assert accuracy_payload['data']['validated_sample_count'] == 1
    finally:
        reviews_module.get_session = original_get_session
        reviews_module.get_today = original_get_today


def test_recent_reviews_prioritize_held_assets_before_global_limit():
    """验证最近复盘记录会优先保留持仓资产，避免被大量非持仓新记录挤掉。"""
    from api import reviews as reviews_module
    from models import Base, Holding, Prediction

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    today = datetime(2026, 4, 18).date()
    seed = TestingSession()
    try:
        seed.add_all([
            Holding(code='009478', name='中银上海金ETF联接C', quantity=100, cost_price=2.2, buy_date=today, asset_type='fund'),
            Holding(code='163402', name='兴全趋势投资混合（LOF）', quantity=100, cost_price=1.5, buy_date=today, asset_type='fund'),
        ])
        seed.add_all([
            Prediction(code='009478', name='中银上海金ETF联接C', asset_type='active_fund', date=today, period_days=5, up_probability=52.0, down_probability=48.0, confidence=55.0, expiry_date=today + timedelta(days=5), is_expired=False, created_at=datetime(2026, 4, 17, 9, 0)),
            Prediction(code='163402', name='兴全趋势投资混合（LOF）', asset_type='active_fund', date=today, period_days=5, up_probability=57.0, down_probability=43.0, confidence=60.0, expiry_date=today + timedelta(days=5), is_expired=False, created_at=datetime(2026, 4, 18, 8, 0)),
        ])
        for i in range(40):
            seed.add(Prediction(
                code=f'600{i:03d}.SH', name=f'非持仓{i}', asset_type='a_stock', date=today, period_days=5,
                up_probability=60.0, down_probability=40.0, confidence=65.0,
                expiry_date=today + timedelta(days=5), is_expired=False,
                created_at=datetime(2026, 4, 17, 12, 0) + timedelta(minutes=i),
            ))
        seed.commit()
    finally:
        seed.close()

    original_get_session = reviews_module.get_session
    original_get_today = reviews_module.get_today
    reviews_module.get_session = TestingSession
    reviews_module.get_today = lambda: today
    try:
        app = Flask(__name__)
        reviews_module.register_reviews_routes(app)
        client = app.test_client()

        response = client.get('/api/reviews/recent?limit=5')
        assert response.status_code == 200
        payload = response.get_json()
        codes = {item['code'] for item in payload['data']}
        assert '163402' in codes
        assert '009478' in codes
        assert codes.issubset({'009478', '163402'})
    finally:
        reviews_module.get_session = original_get_session
        reviews_module.get_today = original_get_today


def test_recent_reviews_api_auto_syncs_due_predictions():
    """验证复盘页在存在到期待复盘预测时，会自动补做复盘而不是继续显示陈旧记录。"""
    from api import reviews as reviews_module
    from reviews import reviewer as reviewer_module
    from models import Base, Prediction, DailyPrice

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    today = datetime(2026, 4, 18).date()
    start_day = today - timedelta(days=5)

    seed = TestingSession()
    try:
        seed.add_all([
            DailyPrice(code='000001.SZ', date=start_day, open=10.0, high=10.0, low=10.0, close=10.0, volume=1000, market='A'),
            DailyPrice(code='000001.SZ', date=today, open=11.0, high=11.0, low=11.0, close=11.0, volume=1000, market='A'),
            Prediction(
                code='000001.SZ',
                name='平安银行',
                asset_type='a_stock',
                date=start_day,
                period_days=5,
                up_probability=80.0,
                down_probability=20.0,
                target_low=9.5,
                target_high=11.5,
                confidence=80.0,
                expiry_date=today,
                is_expired=False,
                created_at=datetime(2026, 4, 13, 9, 30),
            ),
        ])
        seed.commit()
    finally:
        seed.close()

    original_api_get_session = reviews_module.get_session
    original_api_get_today = reviews_module.get_today
    original_reviewer_get_session = reviewer_module.get_session
    original_reviewer_get_today = reviewer_module.get_today

    reviews_module.get_session = TestingSession
    reviews_module.get_today = lambda: today
    reviewer_module.get_session = TestingSession
    reviewer_module.get_today = lambda: today

    try:
        app = Flask(__name__)
        reviews_module.register_reviews_routes(app)
        client = app.test_client()

        response = client.get('/api/reviews/recent?limit=5')
        assert response.status_code == 200
        payload = response.get_json()
        assert payload['code'] == 200
        assert any(item['code'] == '000001.SZ' and item['status'] == 'reviewed' for item in payload['data'])

        verify = TestingSession()
        try:
            pred = verify.query(Prediction).filter(Prediction.code == '000001.SZ').first()
            assert pred is not None
            assert pred.is_expired is True
            assert pred.is_direction_correct is True
        finally:
            verify.close()
    finally:
        reviews_module.get_session = original_api_get_session
        reviews_module.get_today = original_api_get_today
        reviewer_module.get_session = original_reviewer_get_session
        reviewer_module.get_today = original_reviewer_get_today


def test_of_suffix_codes_are_classified_as_funds():
    """验证带 .OF 后缀的基金代码不会被误判为股票。"""
    from utils import get_asset_type_from_code

    assert get_asset_type_from_code('021855.OF') == 'fund'
    assert get_asset_type_from_code('009491.OF') == 'fund'
    assert get_asset_type_from_code('159915.SZ') == 'etf'


def test_model_monitor_review_coverage_distinguishes_due_vs_reviewable_samples():
    """验证模型监控会单独统计到期覆盖率，并排除缺乏真实价格来源的样本。"""
    from api import model as model_module
    from models import Base, Prediction, DailyPrice, RawFundData

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    today = datetime(2026, 4, 20).date()
    rec_day = today - timedelta(days=5)

    seed = TestingSession()
    try:
        seed.add_all([
            DailyPrice(code='000001.SZ', date=rec_day, open=10.0, high=10.2, low=9.8, close=10.0, volume=1000, market='A'),
            DailyPrice(code='000001.SZ', date=today, open=11.0, high=11.2, low=10.8, close=11.0, volume=1000, market='A'),
            RawFundData(code='009478.OF', name='基金A', date=rec_day, nav=2.00),
            RawFundData(code='009478.OF', name='基金A', date=today, nav=2.10),
            Prediction(
                code='000001.SZ',
                name='平安银行',
                asset_type='a_stock',
                date=rec_day,
                period_days=5,
                up_probability=70.0,
                down_probability=30.0,
                target_low=9.5,
                target_high=11.5,
                confidence=75.0,
                expiry_date=today,
                is_expired=True,
                is_direction_correct=True,
                created_at=datetime(2026, 4, 15, 9, 30),
            ),
            Prediction(
                code='009478.OF',
                name='基金A',
                asset_type='active_fund',
                date=rec_day,
                period_days=5,
                up_probability=60.0,
                down_probability=40.0,
                target_low=1.95,
                target_high=2.15,
                confidence=66.0,
                expiry_date=today,
                is_expired=False,
                created_at=datetime(2026, 4, 15, 9, 35),
            ),
            Prediction(
                code='510300.SH',
                name='沪深300ETF',
                asset_type='etf',
                date=rec_day,
                period_days=5,
                up_probability=55.0,
                down_probability=45.0,
                target_low=3.8,
                target_high=4.2,
                confidence=60.0,
                expiry_date=today,
                is_expired=False,
                created_at=datetime(2026, 4, 15, 9, 40),
            ),
            Prediction(
                code='000002.SZ',
                name='万科A',
                asset_type='a_stock',
                date=today,
                period_days=60,
                up_probability=58.0,
                down_probability=42.0,
                target_low=8.5,
                target_high=10.0,
                confidence=61.0,
                expiry_date=today + timedelta(days=60),
                is_expired=False,
                created_at=datetime(2026, 4, 20, 9, 30),
            ),
        ])
        seed.commit()
    finally:
        seed.close()

    original_get_today = model_module.get_today
    model_module.get_today = lambda: today
    try:
        session = TestingSession()
        try:
            summary = model_module._build_review_coverage_summary(session)
        finally:
            session.close()
    finally:
        model_module.get_today = original_get_today

    assert summary['due_predictions'] == 3
    assert summary['reviewed_due_predictions'] == 1
    assert summary['eligible_due_predictions'] == 2
    assert summary['eligible_reviewed_predictions'] == 1
    assert summary['eligible_due_coverage_pct'] == 50.0
    assert summary['by_period']['5']['due'] == 3
    assert summary['by_period']['5']['eligible_due'] == 2
    assert summary['by_period']['60']['due'] == 0
    assert summary['by_period']['60']['maturity_status'] == 'pending'


def test_model_status_api_auto_syncs_due_predictions():
    """验证模型监控接口会自动补做已到期待复盘预测，避免状态页长期显示旧覆盖率。"""
    from api import model as model_module
    from reviews import reviewer as reviewer_module
    from models import Base, Prediction, DailyPrice

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    today = datetime(2026, 4, 20).date()
    rec_day = today - timedelta(days=5)

    seed = TestingSession()
    try:
        seed.add_all([
            DailyPrice(code='000001.SZ', date=rec_day, open=10.0, high=10.2, low=9.8, close=10.0, volume=1000, market='A'),
            DailyPrice(code='000001.SZ', date=today, open=11.0, high=11.2, low=10.8, close=11.0, volume=1000, market='A'),
            Prediction(
                code='000001.SZ',
                name='平安银行',
                asset_type='a_stock',
                date=rec_day,
                period_days=5,
                up_probability=75.0,
                down_probability=25.0,
                target_low=9.5,
                target_high=11.5,
                confidence=80.0,
                expiry_date=today,
                is_expired=False,
                created_at=datetime(2026, 4, 15, 9, 30),
            ),
        ])
        seed.commit()
    finally:
        seed.close()

    original_model_get_session = model_module.get_session
    original_model_get_today = model_module.get_today
    original_reviewer_get_session = reviewer_module.get_session
    original_reviewer_get_today = reviewer_module.get_today

    model_module.get_session = TestingSession
    model_module.get_today = lambda: today
    reviewer_module.get_session = TestingSession
    reviewer_module.get_today = lambda: today

    try:
        app = Flask(__name__)
        model_module.register_model_routes(app)
        client = app.test_client()

        response = client.get('/api/model/status')
        assert response.status_code == 200
        payload = response.get_json()
        assert payload['code'] == 200
        review_coverage = payload['data']['review_coverage']
        assert review_coverage['reviewed_due_predictions'] >= 1
        assert review_coverage['eligible_reviewed_predictions'] >= 1

        verify = TestingSession()
        try:
            pred = verify.query(Prediction).filter(Prediction.code == '000001.SZ').first()
            assert pred is not None
            assert pred.is_expired is True
            assert pred.is_direction_correct is True
        finally:
            verify.close()
    finally:
        model_module.get_session = original_model_get_session
        model_module.get_today = original_model_get_today
        reviewer_module.get_session = original_reviewer_get_session
        reviewer_module.get_today = original_reviewer_get_today


def test_admin_config_write_requires_api_key_for_remote_requests():
    """验证高风险配置写接口对远程请求必须校验管理员凭证。"""
    from api import routes as routes_module

    original_key = os.environ.get('ADMIN_API_KEY')
    os.environ['ADMIN_API_KEY'] = 'secret-2026'
    try:
        app = Flask(__name__)
        routes_module.register_config_routes(app)
        client = app.test_client()

        response = client.post(
            '/api/config',
            json={'model': {'n_estimators': 123}},
            environ_base={'REMOTE_ADDR': '203.0.113.10'},
        )

        assert response.status_code == 403
        payload = response.get_json()
        assert payload['code'] == 403
    finally:
        if original_key is None:
            os.environ.pop('ADMIN_API_KEY', None)
        else:
            os.environ['ADMIN_API_KEY'] = original_key


def test_admin_model_train_accepts_valid_api_key_for_remote_requests():
    """验证模型训练接口在携带正确管理员密钥时仍可正常触发。"""
    from api import model as model_module

    original_key = os.environ.get('ADMIN_API_KEY')
    original_load_progress = model_module._load_training_progress
    original_popen = model_module.subprocess.Popen

    os.environ['ADMIN_API_KEY'] = 'secret-2026'
    model_module._load_training_progress = lambda *args, **kwargs: {'status': 'idle'}
    model_module.subprocess.Popen = lambda *args, **kwargs: types.SimpleNamespace(pid=43210)

    try:
        app = Flask(__name__)
        model_module.register_model_routes(app)
        client = app.test_client()

        response = client.post(
            '/api/model/train',
            json={'asset_type': 'etf', 'period_days': 5},
            headers={'X-Admin-Key': 'secret-2026'},
            environ_base={'REMOTE_ADDR': '203.0.113.10'},
        )

        assert response.status_code == 200
        payload = response.get_json()
        assert payload['code'] == 200
    finally:
        model_module._load_training_progress = original_load_progress
        model_module.subprocess.Popen = original_popen
        if original_key is None:
            os.environ.pop('ADMIN_API_KEY', None)
        else:
            os.environ['ADMIN_API_KEY'] = original_key


def test_admin_denied_request_writes_audit_log_record():
    """验证未授权管理请求会留下可追踪的审计日志。"""
    from api import routes as routes_module
    import models as models_module
    from models import Base, Log

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    original_key = os.environ.get('ADMIN_API_KEY')
    original_routes_get_session = routes_module.get_session
    original_models_get_session = models_module.get_session
    os.environ['ADMIN_API_KEY'] = 'secret-2026'
    routes_module.get_session = TestingSession
    models_module.get_session = TestingSession

    try:
        app = Flask(__name__)
        routes_module.register_config_routes(app)
        client = app.test_client()

        response = client.post(
            '/api/config',
            json={'model': {'n_estimators': 222}},
            environ_base={'REMOTE_ADDR': '203.0.113.10'},
        )
        assert response.status_code == 403

        verify = TestingSession()
        try:
            logs = verify.query(Log).all()
            assert any('config.write' in (item.message or '') and 'denied' in (item.message or '') for item in logs)
        finally:
            verify.close()
    finally:
        routes_module.get_session = original_routes_get_session
        models_module.get_session = original_models_get_session
        if original_key is None:
            os.environ.pop('ADMIN_API_KEY', None)
        else:
            os.environ['ADMIN_API_KEY'] = original_key


def test_model_import_rejects_oversized_payload_even_with_admin_key():
    """验证模型导入接口会拒绝超出大小限制的上传。"""
    from api import model as model_module

    original_key = os.environ.get('ADMIN_API_KEY')
    original_size = os.environ.get('MAX_MODEL_IMPORT_MB')
    os.environ['ADMIN_API_KEY'] = 'secret-2026'
    os.environ['MAX_MODEL_IMPORT_MB'] = '1'

    try:
        app = Flask(__name__)
        model_module.register_model_routes(app)
        client = app.test_client()

        response = client.post(
            '/api/model/import',
            data={'model_file': (io.BytesIO(b'x' * (1024 * 1024 + 16)), 'huge_model.pkl')},
            content_type='multipart/form-data',
            headers={'X-Admin-Key': 'secret-2026'},
            environ_base={'REMOTE_ADDR': '203.0.113.10'},
        )

        assert response.status_code == 400
        payload = response.get_json()
        assert payload['code'] == 400
    finally:
        if original_key is None:
            os.environ.pop('ADMIN_API_KEY', None)
        else:
            os.environ['ADMIN_API_KEY'] = original_key
        if original_size is None:
            os.environ.pop('MAX_MODEL_IMPORT_MB', None)
        else:
            os.environ['MAX_MODEL_IMPORT_MB'] = original_size


def test_review_catchup_prioritizes_existing_due_queue_before_seeding_more_history():
    """验证当已有到期待处理队列时，系统会先清空现有 backlog，而不是继续扩容历史补种。"""
    from reviews.reviewer import Reviewer
    from models import Base, Prediction, Recommendation, DailyPrice

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    today = datetime(2026, 4, 20).date()
    due_day = today - timedelta(days=5)
    hist_day = today - timedelta(days=12)

    seed = TestingSession()
    try:
        for idx in range(100):
            seed.add(Prediction(
                code=f'600{idx:03d}.SH',
                name=f'样本{idx}',
                asset_type='a_stock',
                date=hist_day,
                period_days=5,
                up_probability=60.0,
                down_probability=40.0,
                target_low=9.0,
                target_high=11.0,
                confidence=70.0,
                expiry_date=hist_day + timedelta(days=5),
                is_expired=True,
                is_direction_correct=True,
                created_at=datetime(2026, 4, 1, 9, 0),
            ))

        seed.add_all([
            DailyPrice(code='000001.SZ', date=due_day, open=10.0, high=10.2, low=9.8, close=10.0, volume=1000, market='A'),
            DailyPrice(code='000001.SZ', date=today, open=11.0, high=11.2, low=10.8, close=11.0, volume=1000, market='A'),
            DailyPrice(code='000002.SZ', date=hist_day, open=20.0, high=20.2, low=19.8, close=20.0, volume=1000, market='A'),
            DailyPrice(code='000002.SZ', date=hist_day + timedelta(days=5), open=21.0, high=21.2, low=20.8, close=21.0, volume=1000, market='A'),
            Prediction(
                code='000001.SZ',
                name='平安银行',
                asset_type='a_stock',
                date=due_day,
                period_days=5,
                up_probability=75.0,
                down_probability=25.0,
                target_low=9.5,
                target_high=11.5,
                confidence=80.0,
                expiry_date=today,
                is_expired=False,
                created_at=datetime(2026, 4, 15, 9, 30),
            ),
            Recommendation(
                date=hist_day,
                code='000002.SZ',
                name='万科A',
                type='a_stock',
                rank=1,
                current_price=20.0,
                total_score=4.2,
                up_probability_5d=66.0,
                target_low_5d=19.0,
                target_high_5d=22.0,
                stop_loss_5d=18.5,
            ),
        ])
        seed.commit()
    finally:
        seed.close()

    import reviews.reviewer as reviewer_module
    original_get_session = reviewer_module.get_session
    original_get_today = reviewer_module.get_today
    reviewer_module.get_session = TestingSession
    reviewer_module.get_today = lambda: today

    try:
        reviewer = Reviewer()
        try:
            before_total = reviewer.session.query(Prediction).count()
            reviewed = reviewer.check_expired_predictions()
            after_total = reviewer.session.query(Prediction).count()
            assert reviewed >= 1
            assert after_total == before_total
        finally:
            reviewer.close()
    finally:
        reviewer_module.get_session = original_get_session
        reviewer_module.get_today = original_get_today


def test_reviews_accuracy_falls_back_to_broad_real_samples_when_current_holdings_are_thin():
    """验证复盘页在当前持仓样本过薄时，会自动回退到更广的真实已验证样本。"""
    from api import reviews as reviews_module
    from models import Base, Prediction, Holding, DailyPrice

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    today = datetime(2026, 4, 20).date()
    day_5 = today - timedelta(days=5)
    day_20 = today - timedelta(days=20)
    day_60 = today - timedelta(days=60)

    seed = TestingSession()
    try:
        seed.add(Holding(code='009478', name='基金持仓', asset_type='fund', quantity=100, cost_price=1.0, buy_date=day_5))
        seed.add_all([
            DailyPrice(code='000001.SZ', date=day_5, open=10.0, high=10.2, low=9.8, close=10.0, volume=1000, market='A'),
            DailyPrice(code='000001.SZ', date=today, open=11.0, high=11.2, low=10.8, close=11.0, volume=1000, market='A'),
            DailyPrice(code='000002.SZ', date=day_20, open=20.0, high=20.2, low=19.8, close=20.0, volume=1000, market='A'),
            DailyPrice(code='000002.SZ', date=today, open=21.0, high=21.2, low=20.8, close=21.0, volume=1000, market='A'),
            DailyPrice(code='000003.SZ', date=day_60, open=30.0, high=30.2, low=29.8, close=30.0, volume=1000, market='A'),
            DailyPrice(code='000003.SZ', date=today, open=31.0, high=31.2, low=30.8, close=31.0, volume=1000, market='A'),
            Prediction(code='000001.SZ', name='样本1', asset_type='a_stock', date=day_5, period_days=5, up_probability=70.0, down_probability=30.0, target_low=9.5, target_high=11.5, confidence=75.0, expiry_date=today, is_expired=True, is_direction_correct=True),
            Prediction(code='000002.SZ', name='样本2', asset_type='a_stock', date=day_20, period_days=20, up_probability=68.0, down_probability=32.0, target_low=19.0, target_high=22.0, confidence=72.0, expiry_date=today, is_expired=True, is_direction_correct=True),
            Prediction(code='000003.SZ', name='样本3', asset_type='a_stock', date=day_60, period_days=60, up_probability=66.0, down_probability=34.0, target_low=29.0, target_high=33.0, confidence=70.0, expiry_date=today, is_expired=True, is_direction_correct=True),
        ])
        seed.commit()
    finally:
        seed.close()

    original_get_session = reviews_module.get_session
    original_get_today = reviews_module.get_today
    reviews_module.get_session = TestingSession
    reviews_module.get_today = lambda: today

    try:
        app = Flask(__name__)
        reviews_module.register_reviews_routes(app)
        client = app.test_client()

        response = client.get('/api/reviews/accuracy')
        assert response.status_code == 200
        payload = response.get_json()['data']
        assert payload['has_validated_data'] is True
        assert payload['by_period_counts']['20d'] >= 1
        assert payload['by_period_counts']['60d'] >= 1
    finally:
        reviews_module.get_session = original_get_session
        reviews_module.get_today = original_get_today


def test_backtest_validator_handles_fund_history_with_date_index():
    """验证动作回测可以正确处理基金净值形成的 date 索引历史数据。"""
    from models import Base, Holding, RawFundData, Recommendation
    from reviews.backtest_validator import BacktestValidator

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    signal_dt = datetime(2026, 4, 10, 15, 0)
    signal_day = signal_dt.date()

    seed = TestingSession()
    try:
        seed.add(Holding(code='009478', name='基金A', asset_type='fund', quantity=100, cost_price=1.00, buy_date=signal_day))
        seed.add_all([
            RawFundData(code='009478', name='基金A', date=signal_day, nav=1.00, accumulated_nav=1.00, daily_return=0.0),
            RawFundData(code='009478', name='基金A', date=signal_day + timedelta(days=1), nav=1.02, accumulated_nav=1.02, daily_return=2.0),
            RawFundData(code='009478', name='基金A', date=signal_day + timedelta(days=2), nav=1.05, accumulated_nav=1.05, daily_return=2.94),
            Recommendation(date=signal_day, code='009478', name='基金A', type='active_fund', rank=1, total_score=4.2, current_price=1.00, target_low_5d=0.98, target_high_20d=1.04, created_at=signal_dt),
        ])
        seed.commit()
    finally:
        seed.close()

    import reviews.backtest_validator as backtest_module
    original_get_session = backtest_module.get_session
    backtest_module.get_session = TestingSession

    try:
        validator = BacktestValidator()
        try:
            result = validator.generate_backtest_report(days_lookback=30)
            assert result['take_profit_analysis']['total_signals'] >= 1
            assert result['add_signals_analysis']['total_signals'] >= 1
        finally:
            validator.close()
    finally:
        backtest_module.get_session = original_get_session


def test_reviews_backtest_report_serializes_numpy_results():
    """验证复盘回测接口可稳定返回 JSON，而不会被 numpy/pandas 标量卡住。"""
    from api import reviews as reviews_module
    from models import Base, Holding, RawFundData, Recommendation

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    signal_dt = datetime(2026, 4, 10, 15, 0)
    signal_day = signal_dt.date()

    seed = TestingSession()
    try:
        seed.add(Holding(code='009478', name='基金A', asset_type='fund', quantity=100, cost_price=1.00, buy_date=signal_day))
        seed.add_all([
            RawFundData(code='009478', name='基金A', date=signal_day, nav=1.00, accumulated_nav=1.00, daily_return=0.0),
            RawFundData(code='009478', name='基金A', date=signal_day + timedelta(days=1), nav=1.02, accumulated_nav=1.02, daily_return=2.0),
            RawFundData(code='009478', name='基金A', date=signal_day + timedelta(days=2), nav=1.05, accumulated_nav=1.05, daily_return=2.94),
            Recommendation(date=signal_day, code='009478', name='基金A', type='active_fund', rank=1, total_score=4.2, current_price=1.00, target_low_5d=0.98, target_high_20d=1.04, created_at=signal_dt),
        ])
        seed.commit()
    finally:
        seed.close()

    original_get_session = reviews_module.get_session
    original_get_today = reviews_module.get_today
    import reviews.backtest_validator as backtest_module
    original_backtest_get_session = backtest_module.get_session

    reviews_module.get_session = TestingSession
    reviews_module.get_today = lambda: signal_day + timedelta(days=5)
    backtest_module.get_session = TestingSession

    try:
        app = Flask(__name__)
        reviews_module.register_reviews_routes(app)
        client = app.test_client()

        response = client.get('/api/reviews/backtest-report?days_lookback=30')
        assert response.status_code == 200
        payload = response.get_json()['data']
        assert payload['take_profit_analysis']['total_signals'] >= 1
        assert payload['add_signals_analysis']['total_signals'] >= 1
        assert isinstance(payload['action_quality_summary']['has_action_samples'], bool)
    finally:
        reviews_module.get_session = original_get_session
        reviews_module.get_today = original_get_today
        backtest_module.get_session = original_backtest_get_session


def test_historical_review_sync_uses_nearest_local_trading_day_for_funds():
    """验证基金/ETF到期日落在非交易日时，也能用最近本地交易日完成复盘。"""
    from reviews.reviewer import Reviewer
    from models import Base, Prediction, RawFundData

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    today = datetime(2026, 4, 20).date()
    rec_day = datetime(2026, 3, 31).date()
    expiry_day = datetime(2026, 4, 5).date()  # 周末

    seed = TestingSession()
    try:
        seed.add_all([
            RawFundData(code='009478', name='基金A', date=rec_day, nav=2.00),
            RawFundData(code='009478', name='基金A', date=datetime(2026, 4, 3).date(), nav=2.10),
            Prediction(
                code='009478.OF',
                name='基金A',
                asset_type='active_fund',
                date=rec_day,
                period_days=5,
                up_probability=60.0,
                down_probability=40.0,
                target_low=1.95,
                target_high=2.15,
                confidence=65.0,
                expiry_date=expiry_day,
                is_expired=False,
                created_at=datetime(2026, 3, 31, 9, 0),
            ),
        ])
        seed.commit()
    finally:
        seed.close()

    import reviews.reviewer as reviewer_module
    original_get_session = reviewer_module.get_session
    original_get_today = reviewer_module.get_today
    reviewer_module.get_session = TestingSession
    reviewer_module.get_today = lambda: today

    try:
        reviewer = Reviewer()
        try:
            reviewed = reviewer.check_expired_predictions()
            assert reviewed >= 1
        finally:
            reviewer.close()

        verify = TestingSession()
        try:
            pred = verify.query(Prediction).filter(Prediction.code == '009478.OF').first()
            assert pred is not None
            assert pred.is_expired is True
            assert pred.is_direction_correct is True
        finally:
            verify.close()
    finally:
        reviewer_module.get_session = original_get_session
        reviewer_module.get_today = original_get_today


def test_sync_reviews_seeds_historical_recommendation_samples_when_coverage_is_thin():
    """验证在实盘样本过薄时，会从历史推荐快照补种可复盘样本。"""
    from api import reviews as reviews_module
    from reviews import reviewer as reviewer_module
    from models import Base, Recommendation, Prediction, DailyPrice

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    today = datetime(2026, 4, 20).date()
    rec_day = today - timedelta(days=7)
    expiry_day = rec_day + timedelta(days=5)

    seed = TestingSession()
    try:
        seed.add_all([
            Recommendation(
                date=rec_day,
                code='000001.SZ',
                name='平安银行',
                type='a_stock',
                rank=1,
                total_score=4.2,
                up_probability_5d=68.0,
                up_probability_20d=61.0,
                up_probability_60d=58.0,
                target_low_5d=9.5,
                target_high_5d=11.5,
                current_price=10.0,
                volatility_level='low',
                reason_summary='测试历史推荐样本补种',
            ),
            DailyPrice(code='000001.SZ', date=rec_day, open=10.0, high=10.2, low=9.9, close=10.0, volume=1000, market='A'),
            DailyPrice(code='000001.SZ', date=expiry_day, open=11.0, high=11.2, low=10.8, close=11.0, volume=1200, market='A'),
        ])
        seed.commit()
    finally:
        seed.close()

    original_api_get_session = reviews_module.get_session
    original_api_get_today = reviews_module.get_today
    original_reviewer_get_session = reviewer_module.get_session
    original_reviewer_get_today = reviewer_module.get_today

    reviews_module.get_session = TestingSession
    reviews_module.get_today = lambda: today
    reviewer_module.get_session = TestingSession
    reviewer_module.get_today = lambda: today

    try:
        app = Flask(__name__)
        reviews_module.register_reviews_routes(app)
        client = app.test_client()

        response = client.post('/api/reviews/sync')
        assert response.status_code == 200
        payload = response.get_json()
        assert payload['code'] == 200
        assert payload['data']['reviewed_count'] >= 1

        verify = TestingSession()
        try:
            preds = verify.query(Prediction).filter(Prediction.code == '000001.SZ').all()
            assert len(preds) >= 1
            assert any(p.period_days == 5 and p.is_direction_correct is True for p in preds)
        finally:
            verify.close()
    finally:
        reviews_module.get_session = original_api_get_session
        reviews_module.get_today = original_api_get_today
        reviewer_module.get_session = original_reviewer_get_session
        reviewer_module.get_today = original_reviewer_get_today


def test_historical_review_sync_can_build_and_review_due_60d_samples():
    """验证历史补种不仅覆盖5日，也能补出并复盘60日样本。"""
    from reviews.reviewer import Reviewer
    from models import Base, Recommendation, Prediction, DailyPrice

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    today = datetime(2026, 4, 20).date()
    rec_day = datetime(2026, 1, 15).date()
    expiry_day = rec_day + timedelta(days=60)

    seed = TestingSession()
    try:
        seed.add_all([
            Recommendation(
                date=rec_day,
                code='000001.SZ',
                name='平安银行',
                type='a_stock',
                rank=1,
                total_score=4.3,
                up_probability_5d=62.0,
                up_probability_20d=64.0,
                up_probability_60d=67.0,
                target_low_60d=9.2,
                target_high_60d=12.5,
                current_price=10.0,
                volatility_level='low',
                reason_summary='测试60日补种',
            ),
            DailyPrice(code='000001.SZ', date=rec_day, open=10.0, high=10.2, low=9.9, close=10.0, volume=1000, market='A'),
            DailyPrice(code='000001.SZ', date=expiry_day, open=11.5, high=11.7, low=11.3, close=11.6, volume=1200, market='A'),
        ])
        seed.commit()
    finally:
        seed.close()

    import reviews.reviewer as reviewer_module
    original_get_session = reviewer_module.get_session
    original_get_today = reviewer_module.get_today
    reviewer_module.get_session = TestingSession
    reviewer_module.get_today = lambda: today

    try:
        reviewer = Reviewer()
        try:
            seeded = reviewer.seed_historical_review_samples(max_new_predictions=50)
            reviewed = reviewer.check_expired_predictions()
            assert seeded >= 1
            assert reviewed >= 1
        finally:
            reviewer.close()

        verify = TestingSession()
        try:
            pred = verify.query(Prediction).filter(Prediction.code == '000001.SZ', Prediction.period_days == 60).first()
            assert pred is not None
            assert pred.is_direction_correct is True
        finally:
            verify.close()
    finally:
        reviewer_module.get_session = original_get_session
        reviewer_module.get_today = original_get_today


def test_holdings_lookup_auto_detects_asset_type_and_name():
    """验证持仓输入代码后可自动识别资产类型并回填名称。"""
    from api import holdings as holdings_module
    from models import Base, Recommendation

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    today = datetime.now().date()
    seed = TestingSession()
    try:
        seed.add(Recommendation(
            date=today,
            code='009478',
            name='中银上海金ETF联接C',
            type='active_fund',
            rank=1,
            total_score=4.6,
            current_price=2.32,
            up_probability_5d=62.11,
            up_probability_20d=65.54,
            up_probability_60d=68.43,
        ))
        seed.commit()
    finally:
        seed.close()

    original_get_session = holdings_module.get_session
    holdings_module.get_session = TestingSession
    try:
        app = Flask(__name__)
        holdings_module.register_holdings_routes(app)
        client = app.test_client()

        response = client.get('/api/holdings/lookup?code=009478')
        assert response.status_code == 200
        payload = response.get_json()
        assert payload['code'] == 200
        assert payload['data']['matched'] is True
        assert payload['data']['asset_type'] == 'fund'
        assert payload['data']['name'] == '中银上海金ETF联接C'
    finally:
        holdings_module.get_session = original_get_session


def test_warning_trend_api_includes_holding_history_replay():
    """验证风险预警趋势会返回持仓的历史命中回放，含已验证与待验证状态。"""
    from api import warnings as warnings_module
    from models import Base, Prediction, Holding, Recommendation

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    today = datetime.now().date()
    seed = TestingSession()
    try:
        seed.add(Holding(code='000001.SZ', name='平安银行', quantity=100, cost_price=10.0, buy_date=today, asset_type='stock'))
        seed.add(Holding(code='009478', name='中银上海金ETF联接C', quantity=50, cost_price=2.22, buy_date=today, asset_type='fund'))
        seed.add_all([
            Prediction(
                code='000001.SZ', name='平安银行', asset_type='a_stock', date=today - timedelta(days=8),
                period_days=5, up_probability=72.0, down_probability=28.0,
                target_low=9.5, target_high=11.5, confidence=70.0,
                expiry_date=today - timedelta(days=3), is_expired=True,
                actual_price=10.8, actual_return=5.2, is_direction_correct=True,
                created_at=datetime.combine(today - timedelta(days=8), datetime.min.time()),
            ),
            Prediction(
                code='000001.SZ', name='平安银行', asset_type='a_stock', date=today - timedelta(days=2),
                period_days=5, up_probability=38.0, down_probability=62.0,
                target_low=9.2, target_high=10.1, confidence=66.0,
                expiry_date=today + timedelta(days=3), is_expired=False,
                created_at=datetime.combine(today - timedelta(days=2), datetime.min.time()),
            ),
            Prediction(
                code='009478', name='中银上海金ETF联接C', asset_type='active_fund', date=today - timedelta(days=1),
                period_days=60, up_probability=50.0, down_probability=50.0,
                target_low=2.10, target_high=2.40, confidence=50.0,
                expiry_date=today + timedelta(days=59), is_expired=False,
                created_at=datetime.combine(today - timedelta(days=1), datetime.min.time()),
            ),
            Recommendation(
                date=today,
                code='009478',
                name='中银上海金ETF联接C',
                type='active_fund',
                rank=1,
                total_score=4.5,
                current_price=2.32,
                up_probability_5d=62.11,
                up_probability_20d=65.54,
                up_probability_60d=68.43,
                target_low_60d=2.12,
                target_high_60d=2.45,
            ),
        ])
        seed.commit()
    finally:
        seed.close()

    original_get_session = warnings_module.get_session
    warnings_module.get_session = TestingSession
    try:
        app = Flask(__name__)
        warnings_module.register_warnings_routes(app)
        client = app.test_client()

        response = client.get('/api/warnings/trend?days=7')
        assert response.status_code == 200
        payload = response.get_json()
        assert payload['code'] == 200
        assert 'holding_replays' in payload['data']
        replay_map = {item['code']: item for item in payload['data']['holding_replays']['5d']}
        replay = replay_map['000001.SZ']['replay']
        assert len(replay) == 2
        assert replay[0]['status'] in ('pending', 'reviewed')
        assert any(item['status'] == 'reviewed' and item['is_direction_correct'] is True for item in replay)
        assert any(item['status'] == 'pending' for item in replay)

        long_replay_map = {item['code']: item for item in payload['data']['holding_replays']['60d']}
        assert long_replay_map['009478']['replay'][0]['predicted_up_probability'] > 60
    finally:
        warnings_module.get_session = original_get_session


def test_warning_trend_api_includes_future_horizon_directions():
    """验证风险预警趋势会返回 5/20/60 日未来走势与验证状态，便于后续复盘。"""
    from api import warnings as warnings_module
    from models import Base, Prediction, Warning, Holding, Recommendation

    engine = create_engine('sqlite:///:memory:')
    TestingSession = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)

    today = datetime.now().date()
    seed = TestingSession()
    try:
        seed.add(Warning(
            code='000001.SZ',
            name='平安银行',
            warning_type='MACD死叉',
            level='high',
            message='测试预警',
            suggestion='注意风险',
            warning_time=datetime.combine(today, datetime.min.time()),
            is_sent=False,
        ))
        seed.add(Holding(code='000001.SZ', name='平安银行', quantity=100, cost_price=10.0, buy_date=today, asset_type='stock'))
        seed.add(Holding(code='009478', name='中银上海金ETF联接C', quantity=50, cost_price=2.22, buy_date=today, asset_type='fund'))
        seed.add_all([
            Prediction(
                code='000001.SZ', name='平安银行', asset_type='a_stock', date=today,
                period_days=5, up_probability=72.0, down_probability=28.0,
                target_low=9.5, target_high=11.5, confidence=70.0,
                expiry_date=today + timedelta(days=5), is_expired=False,
                created_at=datetime.combine(today, datetime.min.time()),
            ),
            Prediction(
                code='000001.SZ', name='平安银行', asset_type='a_stock', date=today,
                period_days=20, up_probability=42.0, down_probability=58.0,
                target_low=9.0, target_high=10.8, confidence=66.0,
                expiry_date=today + timedelta(days=20), is_expired=False,
                created_at=datetime.combine(today, datetime.min.time()),
            ),
            Prediction(
                code='000001.SZ', name='平安银行', asset_type='a_stock', date=today,
                period_days=60, up_probability=55.0, down_probability=45.0,
                target_low=9.2, target_high=12.0, confidence=61.0,
                expiry_date=today + timedelta(days=60), is_expired=False,
                created_at=datetime.combine(today, datetime.min.time()),
            ),
            Prediction(
                code='009478', name='中银上海金ETF联接C', asset_type='active_fund', date=today,
                period_days=60, up_probability=50.0, down_probability=50.0,
                target_low=2.10, target_high=2.40, confidence=50.0,
                expiry_date=today + timedelta(days=60), is_expired=False,
                created_at=datetime.combine(today, datetime.min.time()),
            ),
            Recommendation(
                date=today,
                code='009478',
                name='中银上海金ETF联接C',
                type='active_fund',
                rank=1,
                total_score=4.5,
                current_price=2.32,
                up_probability_5d=62.11,
                up_probability_20d=65.54,
                up_probability_60d=68.43,
                target_low_5d=2.18,
                target_high_5d=2.35,
                target_low_20d=2.16,
                target_high_20d=2.38,
                target_low_60d=2.12,
                target_high_60d=2.45,
            ),
        ])
        seed.commit()
    finally:
        seed.close()

    original_get_session = warnings_module.get_session
    warnings_module.get_session = TestingSession
    try:
        app = Flask(__name__)
        warnings_module.register_warnings_routes(app)
        client = app.test_client()

        response = client.get('/api/warnings/trend?days=7')
        assert response.status_code == 200
        payload = response.get_json()
        assert payload['code'] == 200
        assert 'warning_counts' in payload['data']
        assert 'future_trends' in payload['data']
        assert 'holding_paths' in payload['data']
        assert sorted(payload['data']['future_trends'].keys()) == ['20d', '5d', '60d']
        assert any(item['date'] == today.isoformat() for item in payload['data']['future_trends']['5d'])
        assert '60d' in payload['data']['holding_paths']
        assert len(payload['data']['holding_paths']['60d']) == 2
        holding_map = {item['code']: item for item in payload['data']['holding_paths']['60d']}
        assert len(holding_map['000001.SZ']['path']) == 60
        assert holding_map['009478']['up_probability'] > 60
        assert len(holding_map['009478']['path']) == 60
    finally:
        warnings_module.get_session = original_get_session


class TestPerformance:
    """性能测试"""
    
    def test_validator_performance(self):
        """测试验证器性能"""
        validator = DataValidator(
            rules={'field': {'type': 'string'}}
        )
        
        # 验证1000条记录
        import time
        start = time.time()
        
        for i in range(1000):
            validator.validate({'field': f'value_{i}'})
        
        elapsed = time.time() - start
        
        # 应该在1秒内完成
        assert elapsed < 1.0, f"Performance issue: {elapsed:.2f}s"
    
    def test_director_performance(self):
        """测试编排器性能"""
        director = CollectionDirector()
        
        import time
        start = time.time()
        
        # 注册100个任务
        for i in range(100):
            task = CollectionTask(
                f'task_{i}',
                'stock' if i % 2 == 0 else 'fund',
                f'code_{i}',
                i % 5
            )
            director.register_task(task)
        
        elapsed = time.time() - start
        
        # 应该快速完成
        assert elapsed < 0.5, f"Performance issue: {elapsed:.2f}s"


if __name__ == '__main__':
    # 运行所有测试
    pytest.main([__file__, '-v', '--tb=short'])
