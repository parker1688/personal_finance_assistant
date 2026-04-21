"""
模型管理模块 - predictors/model_manager.py
管理模型版本、保存、加载等
"""

import os
import json
import pickle
from datetime import datetime
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import get_session, ModelVersion
from config import (
    MIN_MODEL_ACCURACY,
    MIN_MODEL_F1_SCORE,
    MIN_SHORT_HORIZON_AUC,
    MAX_SHORT_HORIZON_BRIER,
    ALLOW_LEGACY_UNVALIDATED_MODEL,
    AUTO_ACTIVATE_BEST_MODEL,
)
from utils import get_logger

logger = get_logger(__name__)


class ModelManager:
    """模型管理器"""
    
    def __init__(self):
        self.models_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'data', 'models'
        )
        os.makedirs(self.models_dir, exist_ok=True)

    @staticmethod
    def _safe_float(value, default=None):
        try:
            if value is None:
                return default
            return float(value)
        except Exception:
            return default

    @staticmethod
    def _safe_int(value, default=None):
        try:
            if value is None:
                return default
            return int(value)
        except Exception:
            return default

    def evaluate_validation_gate(self, period_days, metrics):
        """统一模型上线门槛判断，返回 (是否通过, gate名称, 原因)。"""
        period = self._safe_int(period_days, default=0) or 0

        acc = self._safe_float(
            metrics.get('validation_accuracy', metrics.get('accuracy')),
            default=None
        )
        f1 = self._safe_float(
            metrics.get('validation_f1', metrics.get('f1')),
            default=None
        )
        auc = self._safe_float(
            metrics.get('validation_auc', metrics.get('auc')),
            default=None
        )
        brier = self._safe_float(
            metrics.get('validation_brier', metrics.get('brier')),
            default=None
        )

        if acc is not None and f1 is not None:
            if acc >= float(MIN_MODEL_ACCURACY) and f1 >= float(MIN_MODEL_F1_SCORE):
                return True, 'acc_f1', 'passed'

        if period <= 5 and auc is not None and brier is not None:
            if auc >= float(MIN_SHORT_HORIZON_AUC) and brier <= float(MAX_SHORT_HORIZON_BRIER):
                return True, 'auc_brier', 'passed'

        reason = (
            f"acc={acc}, f1={f1}, auc={auc}, brier={brier}, "
            f"thresholds(acc>={MIN_MODEL_ACCURACY}, f1>={MIN_MODEL_F1_SCORE}, "
            f"auc>={MIN_SHORT_HORIZON_AUC if period <= 5 else 'n/a'}, "
            f"brier<={MAX_SHORT_HORIZON_BRIER if period <= 5 else 'n/a'})"
        )
        return False, 'failed', reason

    @staticmethod
    def _extract_runtime_metadata(model_data, period_days=None):
        """兼容新旧运行时模型文件格式，尽量恢复验证元数据。"""
        if not isinstance(model_data, dict):
            return {}

        metadata = dict(model_data.get('metadata') or {})
        if metadata:
            return metadata

        legacy_acc = ModelManager._safe_float(
            model_data.get('validation_accuracy', model_data.get('val_accuracy')),
            default=None
        )
        legacy_f1 = ModelManager._safe_float(model_data.get('validation_f1', model_data.get('f1')), default=None)
        legacy_auc = ModelManager._safe_float(model_data.get('validation_auc', model_data.get('auc')), default=None)
        legacy_brier = ModelManager._safe_float(model_data.get('validation_brier', model_data.get('brier')), default=None)
        legacy_period = ModelManager._safe_int(model_data.get('period_days'), default=ModelManager._safe_int(period_days))

        if all(v is None for v in [legacy_acc, legacy_f1, legacy_auc, legacy_brier]) and legacy_period is None:
            return {}

        extracted = {
            'validation_accuracy': legacy_acc,
            'validation_f1': legacy_f1,
            'validation_auc': legacy_auc,
            'validation_brier': legacy_brier,
            'period_days': legacy_period,
            'legacy_runtime_format': True,
        }
        if model_data.get('feature_columns') is not None:
            extracted['feature_columns'] = model_data.get('feature_columns')
        return extracted

    def load_runtime_model_bundle(self, model_path, period_days=None, allow_legacy=None):
        """
        加载运行时模型并执行统一可部署性校验。
        Returns:
            dict: {
              loaded: bool,
              model: object|None,
              metadata: dict,
              gate: str,
              reason: str,
              period_days: int|None,
            }
        """
        result = {
            'loaded': False,
            'model': None,
            'calibrator': None,
            'calibration_method': 'none',
            'regime_models': {},
            'volatility_split': None,
            'feature_columns': None,
            'blend_model': None,
            'blend_weight': None,
            'blend_enabled': False,
            'metadata': {},
            'gate': 'failed',
            'reason': '',
            'period_days': self._safe_int(period_days),
        }

        if not os.path.exists(model_path):
            result['reason'] = f"模型文件不存在: {model_path}"
            return result

        try:
            with open(model_path, 'rb') as f:
                model_data = pickle.load(f)

            if not isinstance(model_data, dict):
                if allow_legacy is None:
                    allow_legacy = bool(ALLOW_LEGACY_UNVALIDATED_MODEL)
                if allow_legacy:
                    result['loaded'] = model_data is not None
                    result['model'] = model_data
                    result['gate'] = 'legacy_compat'
                    result['reason'] = '旧格式模型对象，按兼容模式加载'
                else:
                    result['reason'] = '模型文件格式过旧，且未开启兼容模式'
                return result

            metadata = self._extract_runtime_metadata(model_data, period_days=period_days)
            result['metadata'] = metadata
            result['calibrator'] = model_data.get('calibrator')
            result['calibration_method'] = model_data.get('calibration_method', 'none')
            result['regime_models'] = model_data.get('regime_models', {}) or {}
            result['volatility_split'] = model_data.get('volatility_split')
            result['feature_columns'] = model_data.get('feature_columns') or metadata.get('feature_columns')
            result['blend_model'] = model_data.get('blend_model')
            result['blend_weight'] = model_data.get('blend_weight')
            result['blend_enabled'] = bool(model_data.get('blend_enabled', False))

            model_period = self._safe_int(metadata.get('period_days'))
            if model_period is None:
                model_period = self._safe_int(period_days)
            result['period_days'] = model_period

            acc = self._safe_float(metadata.get('validation_accuracy'))
            f1 = self._safe_float(metadata.get('validation_f1'))

            if allow_legacy is None:
                allow_legacy = bool(ALLOW_LEGACY_UNVALIDATED_MODEL)

            if acc is None or f1 is None:
                legacy_acc = self._safe_float(model_data.get('val_accuracy', model_data.get('validation_accuracy')))
                if model_data.get('model') is not None and legacy_acc is not None and legacy_acc >= float(MIN_MODEL_ACCURACY):
                    result['loaded'] = True
                    result['model'] = model_data.get('model')
                    result['gate'] = 'legacy_top_level_metrics'
                    result['reason'] = '旧格式运行时模型，已基于顶层验证精度兼容加载'
                    if acc is None:
                        result['metadata']['validation_accuracy'] = legacy_acc
                    if result['period_days'] is None:
                        result['period_days'] = self._safe_int(model_data.get('period_days'), default=self._safe_int(period_days))
                    return result

                if allow_legacy:
                    result['loaded'] = model_data.get('model') is not None
                    result['model'] = model_data.get('model')
                    result['gate'] = 'legacy_compat'
                    result['reason'] = '模型缺少完整验证元数据，按兼容模式加载'
                    return result

                result['reason'] = (
                    '模型缺少验证元数据，已跳过加载 '
                    '(可通过 ALLOW_LEGACY_UNVALIDATED_MODEL=true 开启兼容)'
                )
                return result

            passed, gate, reason = self.evaluate_validation_gate(model_period, metadata)
            result['gate'] = gate
            result['reason'] = reason

            if not passed:
                return result

            result['model'] = model_data.get('model')
            result['loaded'] = result['model'] is not None
            if not result['loaded']:
                result['reason'] = '模型对象为空，无法加载'
            return result

        except Exception as e:
            result['reason'] = f"读取模型失败: {e}"
            return result
    
    def save_model(self, model, model_type, period_days, version=None, metadata=None):
        """
        保存模型
        Args:
            model: 模型对象
            model_type: 模型类型 (xgboost/lstm)
            period_days: 预测周期
            version: 版本号
            metadata: 元数据
        Returns:
            str: 版本号
        """
        try:
            # 生成版本号
            if version is None:
                version = f"v{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # 保存模型文件
            model_filename = f"{model_type}_{period_days}d_{version}.pkl"
            model_path = os.path.join(self.models_dir, model_filename)
            
            with open(model_path, 'wb') as f:
                pickle.dump({
                    'model': model,
                    'model_type': model_type,
                    'period_days': period_days,
                    'version': version,
                    'created_at': datetime.now().isoformat(),
                    'metadata': metadata or {}
                }, f)
            
            # 保存到数据库
            session = get_session()

            safe_metadata = dict(metadata or {})
            safe_metadata.pop('_runtime_extras', None)

            model_record = ModelVersion(
                version=version,
                model_type=model_type,
                period_days=period_days,
                train_date=datetime.now().date(),
                validation_accuracy=safe_metadata.get('validation_accuracy', 0) if safe_metadata else 0,
                train_data_count=safe_metadata.get('train_data_count', 0) if safe_metadata else 0,
                model_path=model_path,
                params=json.dumps(safe_metadata, ensure_ascii=False, default=str),
                is_active=False
            )
            session.add(model_record)
            session.commit()
            session.close()
            
            logger.info(f"模型已保存: {version} -> {model_path}")
            return version
            
        except Exception as e:
            logger.error(f"保存模型失败: {e}")
            return None
    
    def load_model(self, version=None, model_type=None, period_days=None):
        """
        加载模型
        Args:
            version: 版本号（指定版本）
            model_type: 模型类型（获取最新）
            period_days: 预测周期（获取最新）
        Returns:
            object: 模型对象
        """
        try:
            session = get_session()
            
            if version:
                # 按版本加载
                model_record = session.query(ModelVersion).filter(
                    ModelVersion.version == version
                ).first()
            elif model_type and period_days:
                # 按类型和周期加载最新
                model_record = session.query(ModelVersion).filter(
                    ModelVersion.model_type == model_type,
                    ModelVersion.period_days == period_days
                ).order_by(ModelVersion.train_date.desc()).first()
            else:
                # 加载激活的模型
                model_record = session.query(ModelVersion).filter(
                    ModelVersion.is_active == True
                ).first()
            
            if not model_record:
                logger.warning("未找到模型")
                return None
            
            # 加载模型文件
            with open(model_record.model_path, 'rb') as f:
                data = pickle.load(f)
            
            session.close()
            logger.info(f"模型已加载: {model_record.version}")
            return data['model']
            
        except Exception as e:
            logger.error(f"加载模型失败: {e}")
            return None
    
    def activate_model(self, version, model_type=None, period_days=None):
        """
        激活指定版本模型
        Args:
            version: 版本号
            model_type: 可选，限制在同模型类型范围内切换
            period_days: 可选，限制在同周期范围内切换
        Returns:
            bool: 是否成功
        """
        try:
            session = get_session()

            model_record = session.query(ModelVersion).filter(
                ModelVersion.version == version
            ).first()

            if not model_record:
                logger.warning(f"模型不存在: {version}")
                return False

            scope_model_type = model_type if model_type is not None else model_record.model_type
            scope_period_days = period_days if period_days is not None else model_record.period_days

            scoped_query = session.query(ModelVersion)
            if scope_model_type is not None:
                scoped_query = scoped_query.filter(ModelVersion.model_type == scope_model_type)
            if scope_period_days is not None:
                scoped_query = scoped_query.filter(ModelVersion.period_days == scope_period_days)

            scoped_query.update({'is_active': False}, synchronize_session=False)

            model_record.is_active = True
            session.commit()
            logger.info(
                f"模型已激活: {version} (model_type={scope_model_type}, period_days={scope_period_days})"
            )
            return True

        except Exception as e:
            logger.error(f"激活模型失败: {e}")
            return False
        finally:
            session.close()
    
    def list_models(self, limit=20):
        """
        列出所有模型
        Returns:
            list: 模型列表
        """
        try:
            session = get_session()
            models = session.query(ModelVersion).order_by(
                ModelVersion.train_date.desc()
            ).limit(limit).all()
            
            result = []
            for m in models:
                result.append({
                    'id': m.id,
                    'version': m.version,
                    'model_type': m.model_type,
                    'period_days': m.period_days,
                    'train_date': m.train_date.isoformat() if m.train_date else None,
                    'validation_accuracy': m.validation_accuracy,
                    'train_data_count': m.train_data_count,
                    'is_active': m.is_active
                })
            
            session.close()
            return result
            
        except Exception as e:
            logger.error(f"列出模型失败: {e}")
            return []
    
    def delete_model(self, version):
        """
        删除模型
        Args:
            version: 版本号
        Returns:
            bool: 是否成功
        """
        try:
            session = get_session()
            model_record = session.query(ModelVersion).filter(
                ModelVersion.version == version
            ).first()
            
            if model_record:
                # 删除文件
                if os.path.exists(model_record.model_path):
                    os.remove(model_record.model_path)
                
                # 删除记录
                session.delete(model_record)
                session.commit()
                logger.info(f"模型已删除: {version}")
                return True
            else:
                logger.warning(f"模型不存在: {version}")
                return False
                
        except Exception as e:
            logger.error(f"删除模型失败: {e}")
            return False
        finally:
            session.close()
    
    def get_model_info(self, version=None):
        """
        获取模型信息
        Returns:
            dict: 模型信息
        """
        try:
            session = get_session()
            
            if version:
                model_record = session.query(ModelVersion).filter(
                    ModelVersion.version == version
                ).first()
            else:
                model_record = session.query(ModelVersion).filter(
                    ModelVersion.is_active == True
                ).first()
            
            if not model_record:
                return None
            
            result = {
                'version': model_record.version,
                'model_type': model_record.model_type,
                'period_days': model_record.period_days,
                'train_date': model_record.train_date.isoformat() if model_record.train_date else None,
                'validation_accuracy': model_record.validation_accuracy,
                'train_data_count': model_record.train_data_count,
                'is_active': model_record.is_active,
                'params': json.loads(model_record.params) if model_record.params else {}
            }
            
            session.close()
            return result
            
        except Exception as e:
            logger.error(f"获取模型信息失败: {e}")
            return None


# 测试代码
if __name__ == '__main__':
    manager = ModelManager()
    
    # 列出模型
    models = manager.list_models()
    print(f"模型列表: {len(models)} 个")
    
    # 获取当前激活模型
    info = manager.get_model_info()
    if info:
        print(f"当前模型: {info['version']}")