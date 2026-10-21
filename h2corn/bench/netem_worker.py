"""Run one RTT-shaped benchmark phase inside its private network namespace."""

from __future__ import annotations

import argparse
import signal
import sys
from pathlib import Path
from typing import Any

from bench._core import BenchmarkError


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--job', type=Path, required=True)
    args = parser.parse_args()

    def interrupted(_signum: int, _frame: Any) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    from bench.adaptive import execute_rtt50_phase

    try:
        execute_rtt50_phase(args.job)
    except KeyboardInterrupt:
        print('RTT benchmark phase interrupted', file=sys.stderr, flush=True)
        return 1
    except (BenchmarkError, OSError, ValueError) as error:
        print(f'RTT benchmark phase failed: {error}', file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
