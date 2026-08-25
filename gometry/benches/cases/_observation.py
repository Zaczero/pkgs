"""Small stdout adapter for typed native A/B case observations."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

_SUPPORT = Path(__file__).resolve().parents[1] / 'support'
if str(_SUPPORT) not in sys.path:
    sys.path.insert(0, str(_SUPPORT))

from benchmark_observation import make_observation


def emit(
    *,
    metric: str,
    raw_values: list[float],
    useful_work: dict[str, Any],
    oracle_identity: str,
    phase: str = 'warm',
    warmup_iterations: int = 0,
    calibration_iterations: int = 0,
    calibration_strategy: str = 'fixed-repetitions',
) -> None:
    observation = make_observation(
        metric=metric,
        raw_values=raw_values,
        useful_work=useful_work,
        oracle_identity=oracle_identity,
        phase=phase,
        warmup_iterations=warmup_iterations,
        calibration_iterations=calibration_iterations,
        calibration_strategy=calibration_strategy,
    )
    print(json.dumps({'observation': observation}, sort_keys=True))
