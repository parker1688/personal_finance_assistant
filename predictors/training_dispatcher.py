"""Asset training dispatcher for direct module execution.

This module decouples training execution from script files so callers can
invoke asset trainers directly while still accepting legacy CLI-style args.
"""

from __future__ import annotations

from typing import Dict, List, Optional


def _normalize_periods(periods: Optional[List[int]]) -> Optional[List[int]]:
    if not periods:
        return None
    normalized: List[int] = []
    for item in periods:
        try:
            value = int(item)
        except Exception:
            continue
        if value in (5, 20, 60) and value not in normalized:
            normalized.append(value)
    return normalized or None


def parse_legacy_args(asset_type: str, args: Optional[List[str]]) -> Dict[str, object]:
    """Parse legacy script args like ['--periods', '5,20,60'] into kwargs."""
    args = list(args or [])
    parsed: Dict[str, object] = {
        "periods": None,
        "val_start": "2026-04-01",
        "val_end": "2026-04-24",
        "selected_assets": None,
    }

    i = 0
    while i < len(args):
        token = args[i]
        next_token = args[i + 1] if i + 1 < len(args) else None

        if token == "--period" and next_token is not None:
            try:
                parsed["periods"] = [int(next_token)]
            except Exception:
                pass
            i += 2
            continue

        if token == "--periods" and next_token is not None:
            if str(next_token).strip().lower() == "all":
                parsed["periods"] = [5, 20, 60]
            else:
                chunks = [x.strip() for x in str(next_token).split(",") if x.strip()]
                values: List[int] = []
                for chunk in chunks:
                    try:
                        values.append(int(chunk))
                    except Exception:
                        continue
                parsed["periods"] = values
            i += 2
            continue

        if token == "--asset" and next_token is not None:
            asset_text = str(next_token).strip().lower()
            if asset_text == "both":
                parsed["selected_assets"] = ["gold", "silver"]
            elif asset_text in {"gold", "silver"}:
                parsed["selected_assets"] = [asset_text]
            i += 2
            continue

        if token == "--val-start" and next_token is not None:
            parsed["val_start"] = str(next_token)
            i += 2
            continue

        if token == "--val-end" and next_token is not None:
            parsed["val_end"] = str(next_token)
            i += 2
            continue

        i += 1

    parsed["periods"] = _normalize_periods(parsed.get("periods"))

    if asset_type in {"gold", "silver"} and not parsed.get("selected_assets"):
        parsed["selected_assets"] = [asset_type]

    return parsed


def run_asset_training(asset_type: str, args: Optional[List[str]] = None) -> bool:
    """Run one asset trainer directly with legacy-compatible args."""
    parsed = parse_legacy_args(asset_type, args)
    periods = parsed.get("periods")

    if asset_type == "a_stock":
        from predictors.a_stock_trainer import AStockTrainer

        trainer = AStockTrainer()
        return bool(trainer.run(periods=periods))

    if asset_type == "fund":
        from predictors.fund_trainer import FundTrainer

        trainer = FundTrainer()
        return bool(
            trainer.run(
                strict_val_start=str(parsed.get("val_start") or "2026-04-01"),
                strict_val_end=str(parsed.get("val_end") or "2026-04-24"),
            )
        )

    if asset_type in {"gold", "silver"}:
        from predictors.precious_metal_trainer import GoldTrainer

        trainer = GoldTrainer()
        selected_assets = parsed.get("selected_assets") or [asset_type]
        horizons = periods or [5, 20, 60]
        return bool(trainer.run(selected_assets=selected_assets, horizons=horizons))

    if asset_type == "etf":
        from predictors.etf_trainer import ETFTrainer

        trainer = ETFTrainer()
        return bool(trainer.run(horizons=periods or [5, 20, 60]))

    if asset_type == "hk_stock":
        from predictors.hk_stock_trainer import HKStockTrainer

        trainer = HKStockTrainer()
        return bool(trainer.run(periods=periods))

    if asset_type == "us_stock":
        from predictors.us_stock_trainer import USStockTrainer

        trainer = USStockTrainer()
        return bool(trainer.run(periods=periods))

    raise ValueError(f"Unsupported asset_type: {asset_type}")
