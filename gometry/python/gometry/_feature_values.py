"""Admission of optional feature scalar providers."""

from __future__ import annotations

import sys
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np

from gometry._lib import (
    ArrowValue,
    Geometry,
)


def temporal_as_py(
    kind: str, value: int, zone: str | None, unit: str = 'ns'
) -> date | datetime | timedelta | time:
    """Convert an Arrow temporal value only when Python can represent it exactly."""
    if kind == 'date32':
        return date(1970, 1, 1) + timedelta(days=value)
    if kind == 'date64':
        days, remainder = divmod(value, 86_400_000)
        if remainder:
            raise ValueError('date value is not an exact number of days')
        return date(1970, 1, 1) + timedelta(days=days)
    if unit == 'ns':
        microseconds, remainder = divmod(value, 1000)
    else:
        microseconds = value * {'s': 1_000_000, 'ms': 1000, 'us': 1}[unit]
        remainder = 0
    if remainder:
        raise ValueError('nanosecond value cannot be represented exactly in Python')
    if kind == 'duration':
        return timedelta(microseconds=microseconds)
    if kind == 'time':
        seconds, micros = divmod(microseconds, 1_000_000)
        hours, seconds = divmod(seconds, 3600)
        minutes, seconds = divmod(seconds, 60)
        return time(hours, minutes, seconds, micros)
    origin = datetime(1970, 1, 1)  # ruff: ignore[call-datetime-without-tzinfo] — Arrow timestamps may be timezone-free.
    if zone is None:
        return origin + timedelta(microseconds=microseconds)
    if zone.startswith(('+', '-')):
        target = datetime.fromisoformat(f'2000-01-01T00:00:00{zone}').tzinfo
    else:
        target = ZoneInfo(zone)
    return (
        origin.replace(tzinfo=UTC) + timedelta(microseconds=microseconds)
    ).astimezone(target)


def check_arrow_type(pa: Any, data_type: Any, path: str) -> None:
    if isinstance(data_type, pa.BaseExtensionType):
        raise TypeError(f'{path}: unsupported Arrow extension value')
    if pa.types.is_dictionary(data_type):
        check_arrow_type(pa, data_type.value_type, path)
    for index in range(data_type.num_fields):
        check_arrow_type(pa, data_type.field(index).type, path)


def column_value(scalar: Any) -> Any:
    """Expose a typed column scalar using the common feature value model."""
    if scalar is None or isinstance(scalar, Geometry):
        return scalar
    if isinstance(scalar, np.generic):
        normalized, _ = normalize_scalar(scalar, 'column')
        return normalized
    return scalar


def normalize_scalar(value: Any, path: str) -> tuple[Any, bool]:
    """Admit non-builtin scalar providers; container traversal stays native."""
    if isinstance(value, ArrowValue):
        return value, True
    if isinstance(value, np.generic):
        if isinstance(value, (np.datetime64, np.timedelta64)):
            return value, True
        normalized = np.generic.item(value)
        if isinstance(normalized, np.generic):
            return value, True
        return normalized, False
    if type(value) in (date, timedelta) or isinstance(value, Geometry):
        return value, True
    if type(value) in (datetime, time):
        if value.tzinfo is not None and type(value.tzinfo) not in (timezone, ZoneInfo):
            raise TypeError(f'{path}: unsupported timezone; convert it explicitly')
        return value, True
    if type(value) is Decimal:
        return value, True
    pa = sys.modules.get('pyarrow')
    if pa is not None and isinstance(value, pa.Scalar):
        check_arrow_type(pa, value.type, path)
        from gometry._lib import _snapshot_arrow_array

        array = pa.array([value], type=value.type)
        return _snapshot_arrow_array(array)[0], True
    raise TypeError(f'{path}: unsupported property value {type(value).__name__}')
