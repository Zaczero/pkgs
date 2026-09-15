"""Typed benchmark observations shared by producers and consumers.

The metric-specific raw and estimate keys are deliberate.  A duration cannot
be decoded as a ratio, count, or memory sample by position or by a generic
``value`` field.
"""

from __future__ import annotations

import math
import statistics
from typing import Any

from benchmark_identity import validate_producer_identity

SCHEMA_VERSION = 1
OBSERVATION_TYPE = 'gometry-benchmark-observation'

_METRICS: dict[str, tuple[str, str, str, str]] = {
    'duration': ('seconds', 'raw_seconds', 'median_seconds', 'lower_is_better'),
    'scaling_ratio': ('ratio', 'raw_ratios', 'median_ratio', 'lower_is_better'),
    'count': ('count', 'raw_counts', 'median_count', 'lower_is_better'),
    'work': ('operations', 'raw_operations', 'median_operations', 'lower_is_better'),
    'memory': ('bytes', 'raw_bytes', 'median_bytes', 'lower_is_better'),
}


def _fail(message: str) -> ValueError:
    return ValueError(f'invalid benchmark observation: {message}')


def _work(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _fail(f'{label} must be an object')
    count = value.get('count')
    unit = value.get('unit')
    if not isinstance(count, int) or isinstance(count, bool) or count <= 0:
        raise _fail(f'{label}.count must be a positive integer')
    if not isinstance(unit, str) or not unit.strip():
        raise _fail(f'{label}.unit must be a non-empty string')
    return {'count': count, 'unit': unit}


def _validate_raw(measurement: dict[str, Any]) -> tuple[str, str, str, list[float]]:
    metric = measurement.get('metric')
    if metric not in _METRICS:
        raise _fail(f'unknown metric {metric!r}')
    unit, raw_key, estimate_key, direction = _METRICS[metric]
    if measurement.get('unit') != unit:
        raise _fail(f'{metric} must use unit {unit!r}')
    if measurement.get('direction') != direction:
        raise _fail(f'{metric} must use direction {direction!r}')
    raw = measurement.get('raw')
    if not isinstance(raw, dict) or set(raw) != {raw_key}:
        raise _fail(f'{metric} raw data must contain only {raw_key!r}')
    values = raw[raw_key]
    if not isinstance(values, list) or not values:
        raise _fail(f'{raw_key} must be a non-empty list')
    numeric: list[float] = []
    for value in values:
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise _fail(f'{raw_key} contains a non-numeric value')
        number = float(value)
        if not math.isfinite(number) or number < 0.0:
            raise _fail(f'{raw_key} contains a non-finite or negative value')
        if metric in {'duration', 'scaling_ratio'} and number == 0.0:
            raise _fail(f'{raw_key} contains a zero value')
        numeric.append(number)
    estimate = measurement.get('estimate')
    if not isinstance(estimate, dict) or set(estimate) != {estimate_key}:
        raise _fail(f'{metric} estimate must contain only {estimate_key!r}')
    expected = statistics.median(numeric)
    actual = estimate[estimate_key]
    if not isinstance(actual, (int, float)) or isinstance(actual, bool):
        raise _fail(f'{estimate_key} must be numeric')
    if not math.isfinite(float(actual)) or float(actual) != expected:
        raise _fail(f'{estimate_key} does not equal the raw median')
    return metric, unit, estimate_key, numeric


def validate_observation(value: Any) -> dict[str, Any]:
    """Validate and return one complete typed observation."""
    if not isinstance(value, dict):
        raise _fail('top level must be an object')
    if value.get('schema_version') != SCHEMA_VERSION:
        raise _fail('unsupported schema version')
    if value.get('observation_type') != OBSERVATION_TYPE:
        raise _fail('unsupported observation type')
    metric, _unit, _estimate_key, _raw = _validate_raw(value.get('measurement', {}))
    useful_work = value.get('useful_work')
    if metric == 'scaling_ratio':
        if not isinstance(useful_work, dict) or set(useful_work) != {
            'numerator',
            'denominator',
        }:
            raise _fail('scaling_ratio useful_work must name numerator and denominator')
        _work(useful_work['numerator'], label='useful_work.numerator')
        _work(useful_work['denominator'], label='useful_work.denominator')
    else:
        _work(useful_work, label='useful_work')
    phase = value.get('phase')
    if phase not in {'first-use', 'warm', 'steady'}:
        raise _fail('phase must be first-use, warm, or steady')
    for name in ('warmup', 'calibration'):
        section = value.get(name)
        if not isinstance(section, dict):
            raise _fail(f'{name} must be an object')
        iterations = section.get('iterations')
        if (
            not isinstance(iterations, int)
            or isinstance(iterations, bool)
            or iterations < 0
        ):
            raise _fail(f'{name}.iterations must be a non-negative integer')
        if section.get('completed') is not True:
            raise _fail(f'{name}.completed must be true')
    warmup_iterations = value['warmup']['iterations']
    if phase == 'warm' and warmup_iterations < 1:
        raise _fail(
            'warm phase requires at least one explicit untimed prewarm iteration'
        )
    if phase == 'first-use' and warmup_iterations != 0:
        raise _fail('first-use phase cannot contain prewarm iterations')
    oracle = value.get('oracle')
    if (
        not isinstance(oracle, dict)
        or set(oracle) != {'status', 'identity'}
        or oracle.get('status') != 'passed'
        or not isinstance(oracle.get('identity'), str)
        or not oracle['identity'].strip()
    ):
        raise _fail('oracle must have passed status and a non-empty identity')
    producer_identity = value.get('producer_identity')
    if producer_identity is not None:
        try:
            validate_producer_identity(producer_identity)
        except ValueError as exc:
            raise _fail(f'producer identity rejected: {exc}') from exc
    return value


def make_observation(
    *,
    metric: str,
    raw_values: list[float],
    useful_work: dict[str, Any],
    oracle_identity: str,
    phase: str = 'warm',
    warmup_iterations: int = 0,
    calibration_iterations: int = 0,
    calibration_strategy: str = 'fixed-repetitions',
) -> dict[str, Any]:
    """Construct one observation using the metric-specific schema.

    ``warm`` requires explicit untimed prewarming; ``steady`` describes a
    case whose setup already establishes its reusable state. ``first-use`` is
    reserved for an isolated producing process and is not emitted by the
    shared-process paired worker.
    """
    if metric not in _METRICS:
        raise _fail(f'unknown metric {metric!r}')
    unit, raw_key, estimate_key, direction = _METRICS[metric]
    observation = {
        'schema_version': SCHEMA_VERSION,
        'observation_type': OBSERVATION_TYPE,
        'measurement': {
            'metric': metric,
            'unit': unit,
            'direction': direction,
            'raw': {raw_key: raw_values},
            'estimate': {estimate_key: statistics.median(raw_values)},
        },
        'useful_work': useful_work,
        'phase': phase,
        'warmup': {'iterations': warmup_iterations, 'completed': True},
        'calibration': {
            'strategy': calibration_strategy,
            'iterations': calibration_iterations,
            'completed': True,
        },
        'oracle': {'status': 'passed'},
    }
    observation['oracle']['identity'] = oracle_identity
    return validate_observation(observation)


def measurement_summary(
    observation: dict[str, Any],
) -> tuple[str, str, float, list[float]]:
    """Return metric, unit, median estimate, and raw values after validation."""
    validated = validate_observation(observation)
    measurement = validated['measurement']
    metric = measurement['metric']
    unit, raw_key, estimate_key, _direction = _METRICS[metric]
    return (
        metric,
        unit,
        float(measurement['estimate'][estimate_key]),
        [float(value) for value in measurement['raw'][raw_key]],
    )


def metric_spec(metric: str) -> tuple[str, str, str, str]:
    """Return the public metric schema for focused consumers and tests."""
    try:
        return _METRICS[metric]
    except KeyError as exc:
        raise _fail(f'unknown metric {metric!r}') from exc
