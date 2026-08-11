"""bench_ab case: held CRS.info receiver-cache warm pass.

Prints median wall-clock of one full pass over 1,000 held CRS objects reading
``.info`` after a warm fill. Receiver-local generation-stamped cache keeps this
ms-class; a re-resolve-every-time path is hundreds of ms. Timing claims belong
here, not in pytest.
"""

from __future__ import annotations

import statistics
import time

import gometry as gm


def _held_crs(n: int = 1000) -> list[gm.CRS]:
    gm.crs_clear_cache()
    held: list[gm.CRS] = []
    for code in range(2000, 5000):
        try:
            held.append(gm.CRS(code))
        except Exception:  # noqa: S112 — skip unconstructible EPSG codes
            continue
        if len(held) >= n:
            break
    if len(held) < n:
        raise RuntimeError(f'could only construct {len(held)} CRS objects')
    return held


def _median_info_pass(held: list[gm.CRS], *, warm: int = 2, reps: int = 7) -> float:
    for _ in range(warm):
        for crs in held:
            _ = crs.info
    samples: list[float] = []
    last_info = None
    for _ in range(reps):
        t0 = time.perf_counter()
        for crs in held:
            last_info = crs.info
        samples.append(time.perf_counter() - t0)
    # Untimed postconditions: every receiver returns a non-empty info mapping.
    assert last_info is not None and isinstance(last_info, dict)
    assert all(isinstance(crs.info, dict) and crs.info for crs in held)
    return statistics.median(samples)


def main() -> None:
    held = _held_crs()
    print(_median_info_pass(held))


if __name__ == '__main__':
    main()
