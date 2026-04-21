"""Lightweight data validation utilities used by collectors."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import math
from typing import Any, Dict, List, Optional


@dataclass
class ValidationResult:
    """Validation output compatible with existing collector expectations."""

    valid: bool
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {"valid": self.valid, "errors": self.errors}


class DataValidator:
    """Rule-based validator for record dictionaries."""

    def __init__(self, rules: Optional[Dict[str, Dict[str, Any]]] = None):
        self.rules = rules or {}

    def validate(self, record: Dict[str, Any]) -> Dict[str, Any]:
        errors: List[str] = []

        for field, rule in self.rules.items():
            value = record.get(field)
            required = bool(rule.get("required", False))

            if required and self._is_missing(value):
                errors.append(f"{field}: required")
                continue

            if self._is_missing(value):
                continue

            expected_type = rule.get("type")
            parsed_num = None

            if expected_type == "string":
                if not isinstance(value, str):
                    errors.append(f"{field}: must be string")
                    continue
                min_length = rule.get("min_length")
                if min_length is not None and len(value.strip()) < int(min_length):
                    errors.append(f"{field}: length < {min_length}")

            elif expected_type == "int":
                if isinstance(value, bool):
                    errors.append(f"{field}: must be int")
                    continue
                try:
                    parsed_num = int(value)
                except (TypeError, ValueError):
                    errors.append(f"{field}: must be int")
                    continue

            elif expected_type == "float":
                try:
                    parsed_num = float(value)
                except (TypeError, ValueError):
                    errors.append(f"{field}: must be float")
                    continue

            elif expected_type == "bool":
                if isinstance(value, bool):
                    pass
                elif str(value).strip().lower() in {"true", "false", "1", "0", "yes", "no"}:
                    pass
                else:
                    errors.append(f"{field}: must be bool")
                    continue

            elif expected_type == "datetime":
                if isinstance(value, datetime):
                    continue
                if not self._can_parse_datetime(value):
                    errors.append(f"{field}: invalid datetime")
                    continue

            if parsed_num is not None:
                min_value = rule.get("min")
                max_value = rule.get("max")
                if min_value is not None and parsed_num < float(min_value):
                    errors.append(f"{field}: must be >= {min_value}")
                if max_value is not None and parsed_num > float(max_value):
                    errors.append(f"{field}: must be <= {max_value}")

        return ValidationResult(valid=not errors, errors=errors).to_dict()

    def detect_and_handle_outliers(self, values: List[float], method: str = "iqr") -> Dict[str, Any]:
        """兼容旧接口：检测异常值并返回清洗摘要。"""
        series = [float(v) for v in (values or []) if v is not None]
        if not series:
            return {
                "method": method,
                "original_count": 0,
                "outlier_indices": [],
                "cleaned_values": [],
            }

        if method == "iqr" and len(series) >= 4:
            ordered = sorted(series)
            q1 = ordered[len(ordered) // 4]
            q3 = ordered[(len(ordered) * 3) // 4]
            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
        else:
            mean = sum(series) / len(series)
            variance = sum((x - mean) ** 2 for x in series) / max(len(series), 1)
            std = variance ** 0.5
            lower = mean - 3 * std
            upper = mean + 3 * std

        outlier_indices = [idx for idx, v in enumerate(series) if v < lower or v > upper]
        cleaned_values = [v for idx, v in enumerate(series) if idx not in outlier_indices]
        return {
            "method": method,
            "original_count": len(series),
            "outlier_indices": outlier_indices,
            "cleaned_values": cleaned_values,
            "bounds": {"lower": lower, "upper": upper},
        }

    @staticmethod
    def _is_missing(value: Any) -> bool:
        """统一识别空值: None/空串/NaN/NaT。"""
        if value is None:
            return True

        if isinstance(value, str):
            txt = value.strip().lower()
            return txt in {"", "nan", "nat", "none", "null"}

        try:
            if isinstance(value, float) and math.isnan(value):
                return True
        except Exception:
            pass

        # pandas.NaT / numpy.nan 等对象通常可通过 x != x 识别
        try:
            if value != value:
                return True
        except Exception:
            pass

        return False

    @staticmethod
    def _can_parse_datetime(value: Any) -> bool:
        if value is None:
            return False

        text = str(value).strip()
        if not text:
            return False

        for fmt in (
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d",
            "%Y/%m/%d %H:%M:%S",
            "%Y%m%d",
            "%Y%m%d %H:%M:%S",
        ):
            try:
                datetime.strptime(text, fmt)
                return True
            except ValueError:
                continue

        try:
            datetime.fromisoformat(text.replace("Z", "+00:00"))
            return True
        except ValueError:
            return False
