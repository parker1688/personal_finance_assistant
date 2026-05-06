#!/usr/bin/env python3
"""
全资产端到端冒烟测试
验证所有推荐器的 ML 推理链路可正常运行
"""
import sys
import os
import warnings

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
warnings.filterwarnings('ignore')

PASS = []
FAIL = []


def check(label, fn):
    try:
        result = fn()
        print(f"  OK   {label}: {result}")
        PASS.append(label)
    except Exception as e:
        print(f"  FAIL {label}: {e}")
        FAIL.append(label)


print("=" * 60)
print("全资产 ML 推理链路冒烟测试")
print("=" * 60)

# ── 基金 ──────────────────────────────────────────────────────
print("\n[基金]")
try:
    from recommenders.fund_recommender import FundRecommender
    r_fund = FundRecommender()
    check("fund model load",      lambda: r_fund._load_ml_model())
    check("fund macro snapshot",  lambda: r_fund._get_macro_snapshot())
    check("fund ML prob (110011.OF)", lambda: r_fund._ml_score_fund('110011.OF', {}, {}))
except Exception as e:
    print(f"  FAIL fund import: {e}")
    FAIL.append("fund import")

# ── 黄金/白银 ─────────────────────────────────────────────────
print("\n[黄金/白银]")
try:
    from recommenders.gold_recommender import GoldRecommender
    r_gold = GoldRecommender()
    dummy_item = {
        'name': 'gold', 'code': 'AU9999', 'current_price': 700.0,
        'change_pct': 0.5, 'trend': 'up', 'strength': 65,
        'ma5': 695.0, 'ma20': 685.0, 'volume_ratio': 1.2,
        'rsi': 58.0, 'bollinger_pos': 0.6, 'support': 680.0, 'resistance': 720.0
    }
    check("gold model load",  lambda: r_gold._load_gold_ml_model())
    check("gold ML prob",     lambda: r_gold._ml_score_metal(dummy_item))
    check("silver ML prob",   lambda: r_gold._ml_score_silver(dummy_item))
except Exception as e:
    print(f"  FAIL gold/silver import: {e}")
    FAIL.append("gold/silver import")

# ── ETF ───────────────────────────────────────────────────────
print("\n[ETF]")
try:
    from recommenders.etf_recommender import ETFRecommender
    r_etf = ETFRecommender()
    dummy_etf = {
        'code': '510300.SH', 'current_price': 4.0, 'change_pct': 0.3,
        'volume_ratio': 1.1, 'total_score': 3.0, 'fund_type': 'broad_index',
        'fee_rate': 0.0015
    }
    check("etf model load",  lambda: r_etf._load_etf_ml_model())
    check("etf ML prob",     lambda: r_etf._ml_score_etf(dummy_etf))
    check("etf fused score", lambda: r_etf._fuse_etf_score(3.0, r_etf._ml_score_etf(dummy_etf)))
except Exception as e:
    print(f"  FAIL etf import: {e}")
    FAIL.append("etf import")

# ── A股模型文件 ───────────────────────────────────────────────
print("\n[A股]")
import pickle
import numpy as np

models_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'models')

a_stock_files = {
    'short_term': 'short_term_model.pkl',
    'medium_term': 'medium_term_model.pkl',
    'long_term': 'long_term_model.pkl',
}

for label, fname in a_stock_files.items():
    def _check_a(f=fname, lbl=label):
        path = os.path.join(models_dir, f)
        if not os.path.exists(path):
            raise FileNotFoundError(f"not found: {f}")
        with open(path, 'rb') as fp:
            obj = pickle.load(fp)
        m = obj['model']
        # feature_columns 可能在顶层或 metadata 里
        fc = obj.get('feature_columns') or (obj.get('metadata') or {}).get('feature_columns') or []
        if fc:
            prob = float(m.predict_proba(np.zeros((1, len(fc))))[0][1])
            assert 0.0 <= prob <= 1.0, f"prob={prob} out of range"
            return f"feats={len(fc)} prob={round(prob,3)}"
        else:
            # 模型不依赖外部 feature_columns（A股中期/长期用 pipeline 内部处理）
            return f"loaded OK (no external feature_columns)"
    check(f"a_stock {label}", _check_a)

# ── 港股/美股模型文件 ─────────────────────────────────────────
print("\n[港股/美股模型文件]")
import pickle
import numpy as np

models_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'models')
for fname in [
    'hk_stock_short_term_model.pkl', 'hk_stock_medium_term_model.pkl',
    'us_stock_short_term_model.pkl', 'us_stock_medium_term_model.pkl',
    'us_stock_long_term_model.pkl',
]:
    def _check_pkl(f=fname):
        path = os.path.join(models_dir, f)
        with open(path, 'rb') as fp:
            obj = pickle.load(fp)
        m = obj['model']
        fc = obj.get('feature_columns', [])
        prob = float(m.predict_proba(np.zeros((1, len(fc))))[0][1])
        assert 0.0 <= prob <= 1.0, f"invalid prob {prob}"
        return round(prob, 3)
    check(fname, _check_pkl)

# ── 汇总 ──────────────────────────────────────────────────────
print()
print("=" * 60)
print(f"通过: {len(PASS)}  失败: {len(FAIL)}")
if FAIL:
    print("失败项:")
    for f in FAIL:
        print(f"  - {f}")
else:
    print("全部通过!")
print("=" * 60)

sys.exit(1 if FAIL else 0)
