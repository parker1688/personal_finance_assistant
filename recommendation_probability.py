"""
推荐概率引擎
- 统一非股票概率生成逻辑
- 提供历史命中率分桶校准器
- 供 scheduler 与 API refresh 复用
"""

from bisect import bisect_left
from collections import defaultdict
from datetime import timedelta
from math import exp

import numpy as np


DEFAULT_NON_STOCK_TYPES = ["active_fund", "etf", "gold", "silver"]

_UNIFIED_TREND_WEIGHTS = {
    5: 0.20,
    20: 0.35,
    60: 0.45,
}

_HORIZON_TUNING = {
    5: {"max_weight": 0.65, "sample_scale": 90.0, "prior_strength": 8.0, "neutral_shrink": 1.00},
    20: {"max_weight": 0.72, "sample_scale": 75.0, "prior_strength": 12.0, "neutral_shrink": 0.92},
    60: {"max_weight": 0.82, "sample_scale": 55.0, "prior_strength": 18.0, "neutral_shrink": 0.82},
}


def _clip(v, lo=5.0, hi=95.0):
    try:
        v = float(v)
    except Exception:
        v = 50.0
    return max(lo, min(hi, v))


def _detect_market_regime(vol_level, m_short, m_mid):
    """根据波动水平与短中期动量识别市场状态。"""
    vol = str(vol_level or "medium").lower()
    try:
        ms = float(m_short)
    except Exception:
        ms = 0.0
    try:
        mm = float(m_mid)
    except Exception:
        mm = 0.0

    if vol == "high" and (mm < -1.0 or ms < -1.5):
        return "risk_off"
    if vol == "low" and (mm > 1.0 or ms > 1.5):
        return "risk_on"
    if vol == "low" and abs(mm) < 0.8 and abs(ms) < 1.2:
        return "calm"
    return "balanced"


def _regime_shift(rec_type, regime):
    """返回不同资产在不同市场状态下的20/60日偏移量。"""
    shifts = {
        "active_fund": {
            "risk_on": (1.0, 2.5),
            "calm": (0.6, 1.5),
            "balanced": (0.0, 0.0),
            "risk_off": (-1.5, -3.5),
        },
        "etf": {
            "risk_on": (0.6, 1.6),
            "calm": (0.3, 0.8),
            "balanced": (0.0, 0.0),
            "risk_off": (-1.0, -2.2),
        },
        "gold": {
            "risk_on": (-0.4, -0.8),
            "calm": (0.0, 0.0),
            "balanced": (0.0, 0.0),
            "risk_off": (0.8, 1.8),
        },
        "silver": {
            "risk_on": (0.4, 0.9),
            "calm": (0.2, 0.4),
            "balanced": (0.0, 0.0),
            "risk_off": (-0.8, -1.8),
        },
        "a_stock": {
            "risk_on": (0.7, 1.8),
            "calm": (0.3, 0.8),
            "balanced": (0.0, 0.0),
            "risk_off": (-1.1, -2.5),
        },
        "hk_stock": {
            "risk_on": (0.7, 1.8),
            "calm": (0.3, 0.8),
            "balanced": (0.0, 0.0),
            "risk_off": (-1.1, -2.5),
        },
        "us_stock": {
            "risk_on": (0.7, 1.8),
            "calm": (0.3, 0.8),
            "balanced": (0.0, 0.0),
            "risk_off": (-1.1, -2.5),
        },
        "other": {
            "risk_on": (0.4, 1.0),
            "calm": (0.2, 0.5),
            "balanced": (0.0, 0.0),
            "risk_off": (-0.7, -1.5),
        },
    }
    conf = shifts.get(rec_type, shifts["other"])
    return conf.get(regime, (0.0, 0.0))


def build_empirical_calibrators(session, recommendation_model, today, rec_types=None, lookback_days=240):
    """基于历史推荐命中率构建分桶校准器（样本不足自动回退）。"""
    if rec_types is None:
        rec_types = DEFAULT_NON_STOCK_TYPES

    def _build_one(rec_type, horizon_days):
        start_date = today - timedelta(days=lookback_days + horizon_days + 5)
        rows = (
            session.query(
                recommendation_model.code,
                recommendation_model.date,
                recommendation_model.total_score,
                recommendation_model.current_price,
            )
            .filter(recommendation_model.type == rec_type)
            .filter(recommendation_model.date >= start_date)
            .filter(recommendation_model.current_price.isnot(None))
            .filter(recommendation_model.current_price > 0)
            .order_by(recommendation_model.code.asc(), recommendation_model.date.asc())
            .all()
        )

        by_code = defaultdict(list)
        for code, d, score, price in rows:
            try:
                by_code[code].append((d, float(score), float(price)))
            except Exception:
                continue

        bin_stats = defaultdict(lambda: [0, 0])  # score_bin -> [total, hit]
        overall_total = 0
        overall_hit = 0
        
        # active_fund 60d 使用更细分桶以提升校准精度
        bucket_step = 0.25 if (rec_type == "active_fund" and horizon_days == 60) else 0.5

        for items in by_code.values():
            if len(items) < 2:
                continue
            dates = [x[0] for x in items]
            for i, (d0, s0, p0) in enumerate(items):
                target_date = d0 + timedelta(days=horizon_days)
                j = bisect_left(dates, target_date, lo=i + 1)
                if j >= len(items):
                    continue
                p1 = items[j][2]
                hit = 1 if p1 > p0 else 0
                b = round(s0 / bucket_step) * bucket_step  # 动态分桶
                stat = bin_stats[b]
                stat[0] += 1
                stat[1] += hit
                overall_total += 1
                overall_hit += hit

        def _calibrate(score, base_prob):
            try:
                score = float(score)
            except Exception:
                return _clip(base_prob)

            cfg = _HORIZON_TUNING.get(horizon_days, _HORIZON_TUNING[20])
            bucket_step = 0.25 if (rec_type == "active_fund" and horizon_days == 60) else 0.5
            b = round(score / bucket_step) * bucket_step
            total, hit = bin_stats.get(b, [0, 0])
            
            # active_fund 60d: 优先信任更细分桶的样本，补充整体数据
            if rec_type == "active_fund" and horizon_days == 60 and total < 8:
                # 查找邻近分桶补充样本
                for offset in [bucket_step, -bucket_step]:
                    nb = round((score + offset) / bucket_step) * bucket_step
                    nt, nh = bin_stats.get(nb, [0, 0])
                    if nt > 0:
                        total += nt // 2
                        hit += nh // 2
                        break
            
            if total < 12:
                total, hit = overall_total, overall_hit
            if total < 12:
                return _clip(base_prob)

            # 使用总体命中率作为先验，缓解小样本分桶抖动。
            prior_rate = (overall_hit * 100.0 / overall_total) if overall_total > 0 else 50.0
            prior_k = cfg["prior_strength"]
            empirical = ((hit + prior_rate / 100.0 * prior_k) / (total + prior_k)) * 100.0
            weight = min(cfg["max_weight"], total / cfg["sample_scale"])

            # 主动基金60日历史样本充足且趋势性更强，允许更充分贴合实证命中率。
            if rec_type == "active_fund" and horizon_days == 60:
                weight = min(0.98, total / 25.0)

            mixed = float(base_prob) * (1 - weight) + empirical * weight
            shrink = cfg["neutral_shrink"]
            if rec_type == "active_fund" and horizon_days == 60:
                shrink = 1.02  # 轻微反向收敛，保留更多长期性
            mixed = 50.0 + (mixed - 50.0) * shrink
            return _clip(mixed)

        return _calibrate

    return {
        t: {
            5: _build_one(t, 5),
            20: _build_one(t, 20),
            60: _build_one(t, 60),
        }
        for t in rec_types
    }


def _extract_horizon_probability(payload, horizon):
    """兼容多种输入结构提取指定周期概率。"""
    if not isinstance(payload, dict):
        return None

    direct_key = f"up_probability_{int(horizon)}d"
    if payload.get(direct_key) is not None:
        return _clip(payload.get(direct_key))

    predictions = payload.get("predictions") or {}
    pred_key = {5: "short_term", 20: "medium_term", 60: "long_term"}.get(int(horizon))
    pred_item = predictions.get(pred_key) if isinstance(predictions, dict) else None
    if isinstance(pred_item, dict) and pred_item.get("up_probability") is not None:
        return _clip(pred_item.get("up_probability"))

    return None


def derive_unified_trend(payload, weights=None):
    """将5/20/60日概率融合为单一趋势视图。"""
    payload = payload or {}
    weights = weights or _UNIFIED_TREND_WEIGHTS
    model_status = payload.get("model_status") or {}

    horizons = [
        (5, "short_term_validated"),
        (20, "medium_term_validated"),
        (60, "long_term_validated"),
    ]

    validated_points = []
    fallback_points = []
    validated_weight_sum = 0.0
    base_weight_sum = 0.0

    for horizon, status_key in horizons:
        prob = _extract_horizon_probability(payload, horizon)
        if prob is None:
            continue

        weight = float(weights.get(horizon, 0.0) or 0.0)
        if weight <= 0:
            continue

        base_weight_sum += weight
        point = (horizon, prob, weight)
        if model_status.get(status_key, True):
            validated_points.append(point)
            validated_weight_sum += weight
        else:
            fallback_points.append(point)

    active_points = validated_points if validated_points else fallback_points

    if not active_points:
        total_score = payload.get("total_score", payload.get("score", 3.0))
        try:
            total_score = float(total_score)
        except Exception:
            total_score = 3.0
        mapped = 50.0 + ((total_score - 3.0) * 12.0)
        trend_score = round(_clip(mapped), 2)
        trend_direction = "neutral" if 47.0 <= trend_score <= 53.0 else ("bullish" if trend_score > 53.0 else "bearish")
        return {
            "trend_score": 50.0 if payload == {} else trend_score,
            "trend_direction": "neutral" if payload == {} else trend_direction,
            "trend_confidence": 20.0 if payload == {} else round(35.0 + abs(trend_score - 50.0), 2),
            "dominant_horizon": None,
            "agreement": "unknown",
            "summary": "多周期数据不足，当前按中性信号处理",
        }

    weighted_sum = sum(prob * weight for _, prob, weight in active_points)
    active_weight_sum = sum(weight for _, _, weight in active_points) or 1.0
    trend_score = round(weighted_sum / active_weight_sum, 2)

    dominant_horizon = max(active_points, key=lambda x: abs(x[1] - 50.0) * x[2])[0]
    probs = [prob for _, prob, _ in active_points]
    dispersion = max(probs) - min(probs) if len(probs) > 1 else 0.0
    agreement_ratio = max(0.0, 1.0 - (dispersion / 35.0))
    reliability_ratio = (validated_weight_sum / base_weight_sum) if base_weight_sum > 0 else 0.0
    confidence = 35.0 + (abs(trend_score - 50.0) * 1.4) + (agreement_ratio * 18.0) + (reliability_ratio * 12.0)
    confidence = round(float(np.clip(confidence, 20.0, 95.0)), 2)

    if trend_score >= 70.0:
        trend_direction = "strong_bullish"
        summary = "多周期共振偏强，趋势明显向上"
    elif trend_score >= 55.0:
        trend_direction = "bullish"
        summary = "中长期趋势偏多，统一信号看涨"
    elif trend_score <= 30.0:
        trend_direction = "strong_bearish"
        summary = "多周期共振偏弱，趋势明显承压"
    elif trend_score <= 45.0:
        trend_direction = "bearish"
        summary = "中长期趋势偏空，统一信号偏谨慎"
    else:
        trend_direction = "neutral"
        summary = "多周期方向分化，统一信号中性"

    if dispersion <= 8.0:
        agreement = "high"
    elif dispersion <= 18.0:
        agreement = "medium"
    else:
        agreement = "low"

    return {
        "trend_score": trend_score,
        "trend_direction": trend_direction,
        "trend_confidence": confidence,
        "dominant_horizon": int(dominant_horizon),
        "agreement": agreement,
        "summary": summary,
    }


def derive_probabilities(rec, rec_type="other", calibrators=None):
    """优先使用推荐自带概率，否则按分资产非线性映射+动量修正生成概率。"""
    has_raw_probabilities = rec.get("up_probability_5d") is not None
    vol_level = str(rec.get("volatility_level", "medium")).lower()
    if has_raw_probabilities:
        p5 = _clip(rec.get("up_probability_5d", 50))
        p20 = _clip(rec.get("up_probability_20d", p5))
        p60 = _clip(rec.get("up_probability_60d", p20))
    else:
        params = {
            "active_fund": {"slope": 1.80, "prior": 0.66, "h5": 0.98, "h60": 1.38},
            "etf": {"slope": 1.90, "prior": 0.52, "h5": 1.00, "h60": 0.95},
            "gold": {"slope": 1.45, "prior": 0.50, "h5": 1.00, "h60": 0.90},
            "silver": {"slope": 1.35, "prior": 0.50, "h5": 1.05, "h60": 0.85},
            "other": {"slope": 1.60, "prior": 0.50, "h5": 1.00, "h60": 0.95},
        }
        p = params.get(rec_type, params["other"])

        score = rec.get("total_score", rec.get("score", 3.0))
        try:
            score = float(score)
        except Exception:
            score = 3.0

        z = score - 3.0
        sigmoid_prob = 1.0 / (1.0 + exp(-p["slope"] * z))
        p20 = (sigmoid_prob * 0.75 + p["prior"] * 0.25) * 100.0

        vol_scale = {"low": 1.10, "medium": 1.00, "high": 0.85}.get(vol_level, 1.00)
        conf = abs(p20 - 50.0) * vol_scale
        direction = 1.0 if p20 >= 50 else -1.0

        p5 = 50.0 + direction * conf * p["h5"]
        p60 = 50.0 + direction * conf * p["h60"]

    score = rec.get("total_score", rec.get("score", 3.0))
    try:
        score = float(score)
    except Exception:
        score = 3.0

    m_short = rec.get("return_5d", rec.get("ret_1m", 0))
    m_mid = rec.get("return_20d", rec.get("ret_3m", 0))
    try:
        m_short = float(m_short)
    except Exception:
        m_short = 0.0
    try:
        m_mid = float(m_mid)
    except Exception:
        m_mid = 0.0

    regime = _detect_market_regime(vol_level, m_short, m_mid)
    shift20, shift60 = _regime_shift(rec_type, regime)

    p5 += max(-3.0, min(3.0, m_short * 0.60))
    p20 += max(-3.0, min(3.0, m_mid * 0.30))
    p60 += max(-2.0, min(2.0, m_mid * 0.20))
    p20 += shift20
    p60 += shift60

    if rec_type == "active_fund":
        # 基金波动率自适应：低波动基金长期上升预期更强
        vol_level = str(rec.get("volatility_level", "medium")).lower()
        if vol_level == "low":
            # 低波动基金（多为固收/稳健型）长期趋势更平稳可信
            p20 += 4.5
            p60 += 12.5
        elif vol_level == "medium":
            # 中等波动（混合型）中等提升
            p20 += 3.0
            p60 += 9.5
        else:
            # 高波动基金（权益型）相对保守，长期预期趋向中性
            p20 += 1.5
            p60 += 5.0

    if has_raw_probabilities:
        # 模型原始概率在长周期易偏离，先做温和收敛再校准。
        shrink20 = {"low": 0.92, "medium": 0.86, "high": 0.80}.get(vol_level, 0.86)
        shrink60 = {"low": 0.86, "medium": 0.78, "high": 0.70}.get(vol_level, 0.78)
        if regime == "risk_off":
            shrink20 *= 0.95
            shrink60 *= 0.92
        elif regime == "risk_on":
            shrink20 *= 1.03
            shrink60 *= 1.05
        p20 = 50.0 + (p20 - 50.0) * shrink20
        p60 = 50.0 + (p60 - 50.0) * shrink60

    if calibrators:
        cals = calibrators.get(rec_type)
        if cals:
            p5 = cals[5](score, p5)
            p20 = cals[20](score, p20)
            p60 = cals[60](score, p60)

    return round(_clip(p5), 2), round(_clip(p20), 2), round(_clip(p60), 2)
