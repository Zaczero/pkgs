"""Untimed cross-library oracle for the public RELEASE benchmark set.

CLI:
  benches/python/bench_oracle.py --all
  benches/python/bench_oracle.py --operations <comma-separated-gometry-row-names>

Validates builder maps against the selected manifest, builds and verifies each
operation sequentially, prints ``PASS <row> [<kind>]``, and exits nonzero on
the first mismatch. Fixture caches and timed closures are cleared between
operations so peak RSS stays bounded.

An empty or incomplete ``PUBLIC_CASE_BUILDERS`` map **fails closed** at planning
(``validate_builders``) — never silently reports success with zero ops. Lane 2
registers 32 public PairCase builders.
"""

from __future__ import annotations

import argparse
import gc
import sys
from pathlib import Path

_PYTHON = Path(__file__).resolve().parent
_SUPPORT = _PYTHON.parent / 'support'
for _path in (_PYTHON, _SUPPORT):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from _bench_oracles import (
    PUBLIC_CASE_BUILDERS,
    OracleContext,
    OracleMismatch,
)
from _bench_registry import RELEASE_OPERATIONS, ReleaseOperation

# Lane 2: register the 32 public PairCase builders (side-effect import).
import _bench_public_cases as _public_cases  # noqa: F401


def _selected_operations(names: set[str] | None) -> list[ReleaseOperation]:
    if names is None:
        return list(RELEASE_OPERATIONS)
    known = {op.gometry: op for op in RELEASE_OPERATIONS}
    by_any = {row: op for op in RELEASE_OPERATIONS for row in op.rows}
    unknown: list[str] = []
    seen: set[str] = set()
    selected: list[ReleaseOperation] = []
    for name in names:
        op = known.get(name) or by_any.get(name)
        if op is None:
            unknown.append(name)
            continue
        if op.gometry in seen:
            continue
        seen.add(op.gometry)
        selected.append(op)
    if unknown:
        raise SystemExit(
            'unknown oracle operation name(s): ' + ', '.join(sorted(unknown))
        )
    order = {op.gometry: i for i, op in enumerate(RELEASE_OPERATIONS)}
    selected.sort(key=lambda op: order[op.gometry])
    return selected


def validate_builders(
    operations: list[ReleaseOperation],
    builders: dict[str, object],
) -> None:
    """Require exact bijection: every selected op has a builder; no orphans.

    Called before any timing or verification so a missing builder fails closed
    at planning/oracle entry rather than silently SKIPping.
    """
    public = {op.gometry for op in RELEASE_OPERATIONS}
    orphans = sorted(set(builders) - public)
    if orphans:
        raise SystemExit(
            'PUBLIC_CASE_BUILDERS has keys outside RELEASE_OPERATIONS: '
            + ', '.join(orphans)
        )
    selected = {op.gometry for op in operations}
    missing = sorted(selected - set(builders))
    if missing:
        raise SystemExit(
            'PUBLIC_CASE_BUILDERS missing builders for selected ops: '
            + ', '.join(missing)
        )


def run_oracle(
    operations: list[ReleaseOperation],
    *,
    builders: dict | None = None,
) -> int:
    builders = PUBLIC_CASE_BUILDERS if builders is None else builders
    # Fail closed before any verification work when the map is incomplete.
    validate_builders(operations, builders)

    from _bench_public_cases import PUBLIC_TIMED
    from _bench_public_fixtures import clear_public_fixture_cache
    from _bench_real_world_layers import clear_real_world_cache

    verified = 0
    for op in operations:
        builder = builders[op.gometry]
        case = builder()
        try:
            context = OracleContext(
                operation=op,
                kind=getattr(case, 'kind', None) or 'oracle',
                unit=None,
            )
            kind = context.kind
            case.verify(context)
            print(f'PASS {op.gometry} [{kind}]', flush=True)
            verified += 1
        except OracleMismatch as exc:
            print(f'FAIL {op.gometry}: {exc}', file=sys.stderr, flush=True)
            return 1
        except Exception as exc:
            print(
                f'FAIL {op.gometry}: unexpected {type(exc).__name__}: {exc}',
                file=sys.stderr,
                flush=True,
            )
            return 1
        finally:
            # Drop case + timed closures + fixture memo so the next op does not
            # retain the previous op's multi-100 MB working set.
            del case
            for row in op.rows:
                PUBLIC_TIMED.pop(row, None)
            clear_public_fixture_cache()
            clear_real_world_cache()
            gc.collect()

    print(f'oracle complete: {verified} passed, 0 skipped', flush=True)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description='Untimed cross-library oracle for public RELEASE benchmarks.'
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--all',
        action='store_true',
        help='verify every RELEASE_OPERATIONS entry that has a builder',
    )
    group.add_argument(
        '--operations',
        help='comma-separated gometry (or competitor) row names to verify',
    )
    args = parser.parse_args(argv)

    if args.all:
        operations = _selected_operations(None)
    else:
        names = {part.strip() for part in args.operations.split(',') if part.strip()}
        if not names:
            parser.error('--operations must contain at least one operation name')
        operations = _selected_operations(names)

    return run_oracle(operations)


if __name__ == '__main__':
    raise SystemExit(main())
