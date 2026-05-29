"""模拟交易员资产配置档位配置。"""

import json
import os


_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PROFILE_FILE = os.path.join(_PROJECT_ROOT, 'data', 'cache', 'simulated_trader_profile.json')


TRADER_PROFILES = {
    'balanced_default': {
        'label': '均衡基线',
        'description': '以收益与风险平衡为主，适合日常稳定跟踪。',
        'decision_threshold': 0.60,
        'target_allocations': {
            'a_stock': 0.35,
            'etf': 0.25,
            'active_fund': 0.20,
            'gold': 0.10,
            'silver': 0.10,
        },
        'validation_priority': {
            'a_stock': 0.5,
            'etf': 0.5,
            'active_fund': 0.5,
            'gold': 0.4,
            'silver': 0.4,
        },
        'config_overrides': {
            'buy_score_threshold': 0.60,
            'sell_score_threshold': 0.40,
            'max_position_count': 15,
            'max_single_position_pct': 0.05,
            'min_cash_reserve_pct': 0.25,
        },
    },
    'validation_boost': {
        'label': '样本加速',
        'description': '优先补齐ETF与主动基金样本，提升验证数据覆盖。',
        'decision_threshold': 0.56,
        'target_allocations': {
            'a_stock': 0.20,
            'etf': 0.35,
            'active_fund': 0.30,
            'gold': 0.10,
            'silver': 0.05,
        },
        'validation_priority': {
            'a_stock': 0.3,
            'etf': 0.8,
            'active_fund': 1.0,
            'gold': 0.4,
            'silver': 0.4,
        },
        'config_overrides': {
            'buy_score_threshold': 0.55,
            'sell_score_threshold': 0.42,
            'max_position_count': 15,
            'max_single_position_pct': 0.045,
            'min_cash_reserve_pct': 0.25,
        },
    },
}


def _ensure_dir():
    os.makedirs(os.path.dirname(_PROFILE_FILE), exist_ok=True)


def get_trader_profiles() -> dict:
    return TRADER_PROFILES


def get_active_profile_name() -> str:
    _ensure_dir()
    if not os.path.exists(_PROFILE_FILE):
        return 'balanced_default'
    try:
        with open(_PROFILE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        profile = str(data.get('active_profile') or 'balanced_default')
        if profile not in TRADER_PROFILES:
            return 'balanced_default'
        return profile
    except Exception:
        return 'balanced_default'


def get_active_profile() -> dict:
    name = get_active_profile_name()
    profile = dict(TRADER_PROFILES[name])
    # 读取运行时自适应调参覆盖（由复盘分析自动写入，不影响静态基线）
    _ensure_dir()
    if os.path.exists(_PROFILE_FILE):
        try:
            with open(_PROFILE_FILE, 'r', encoding='utf-8') as f:
                saved = json.load(f)
            overrides = saved.get('threshold_overrides') or {}
            if overrides.get('decision_threshold') is not None:
                profile['decision_threshold'] = float(overrides['decision_threshold'])
        except Exception:
            pass
    return {'name': name, **profile}


def apply_threshold_adjustment(new_decision_threshold: float, reason: str = '') -> dict:
    """将自适应调参结果写入 JSON 覆盖层，不修改静态 TRADER_PROFILES。

    Args:
        new_decision_threshold: 经过调整后的决策阈值（已 clamp 到合理范围）
        reason: 调整原因描述，记录到 JSON 便于审计

    Returns:
        更新后的 profile dict
    """
    _ensure_dir()
    saved: dict = {}
    if os.path.exists(_PROFILE_FILE):
        try:
            with open(_PROFILE_FILE, 'r', encoding='utf-8') as f:
                saved = json.load(f)
        except Exception:
            saved = {}

    active_name = str(saved.get('active_profile') or 'balanced_default')
    if active_name not in TRADER_PROFILES:
        active_name = 'balanced_default'

    base_threshold = float(TRADER_PROFILES[active_name].get('decision_threshold', 0.60))
    # 限制调整幅度：不超过基线 ±0.06
    clamped = max(base_threshold - 0.06, min(base_threshold + 0.06, float(new_decision_threshold)))

    saved['threshold_overrides'] = {
        'decision_threshold': round(clamped, 4),
        'base_threshold': base_threshold,
        'reason': reason,
    }
    with open(_PROFILE_FILE, 'w', encoding='utf-8') as f:
        json.dump(saved, f, ensure_ascii=False, indent=2)

    return get_active_profile()


def set_active_profile(profile_name: str) -> dict:
    name = str(profile_name or '').strip()
    if name not in TRADER_PROFILES:
        raise ValueError(f'未知档位: {name}')
    _ensure_dir()
    # 切换 profile 时保留已有覆盖层数据（JSON merge）
    saved: dict = {}
    if os.path.exists(_PROFILE_FILE):
        try:
            with open(_PROFILE_FILE, 'r', encoding='utf-8') as f:
                saved = json.load(f)
        except Exception:
            saved = {}
    saved['active_profile'] = name
    # 切换 profile 时清除旧覆盖，避免旧档位的调参影响新档位
    saved.pop('threshold_overrides', None)
    with open(_PROFILE_FILE, 'w', encoding='utf-8') as f:
        json.dump(saved, f, ensure_ascii=False, indent=2)
    return {'name': name, **TRADER_PROFILES[name]}
