"""Untimed cross-library oracle for the public RELEASE benchmark set.

CLI:
  benches/python/bench_oracle.py --all
  benches/python/bench_oracle.py --operations <comma-separated-gometry-row-names>

Validates builder maps against the selected manifest, builds and verifies each
operation sequentially, prints ``PASS <row> [<kind>]``, and exits nonzero on
the first mismatch. Fixture caches and timed closures are cleared between
operations so peak RSS stays bounded.

An empty or incomplete public builder map **fails closed** at planning
(``validate_builders``) — never silently reports success with zero ops. The
public case modules register one builder for every declared operation.
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
from pathlib import Path

_PYTHON = Path(__file__).resolve().parent
_SUPPORT = _PYTHON.parent / 'support'
for _path in (_PYTHON, _SUPPORT):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from _bench_oracles import (
    PUBLIC_CASE_BUILDERS,
    PUBLIC_SCALING_CASE_BUILDERS,
    OracleContext,
    OracleMismatch,
    public_oracle_identity,
)
from _bench_registry import ALL_PUBLIC_OPERATIONS, RELEASE_OPERATIONS, ScalingOperation

# Register the public PairCase builders (side-effect import).
import _bench_public_cases as _public_cases  # ruff: ignore[unused-import]
import _bench_scaling_cases as _scaling_cases  # ruff: ignore[unused-import]


def _selected_operations(names: set[str] | None) -> list[object]:
    if names is None:
        return list(ALL_PUBLIC_OPERATIONS)
    known = {op.gometry: op for op in ALL_PUBLIC_OPERATIONS}
    by_any = {row: op for op in ALL_PUBLIC_OPERATIONS for row in op.rows}
    unknown: list[str] = []
    seen: set[str] = set()
    selected: list[object] = []
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
    order = {op.gometry: i for i, op in enumerate(ALL_PUBLIC_OPERATIONS)}
    selected.sort(key=lambda op: order[op.gometry])
    return selected


def validate_builders(
    operations: list[object],
    builders: dict[str, object],
    *,
    public_operations: tuple[object, ...] = tuple(RELEASE_OPERATIONS),
) -> None:
    """Require exact bijection: every selected op has a builder; no orphans.

    Called before any timing or verification so a missing builder fails closed
    at planning/oracle entry rather than silently SKIPping.
    """
    public = {op.gometry for op in public_operations}
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
    operations: list[object],
    *,
    builders: dict | None = None,
    output: Path | None = None,
) -> int:
    base = [op for op in operations if not isinstance(op, ScalingOperation)]
    scaling = [op for op in operations if isinstance(op, ScalingOperation)]
    if builders is not None:
        builder_maps = (builders,)
        validate_builders(
            operations,
            builders,
            public_operations=tuple(operations),
        )
    else:
        builder_maps = (PUBLIC_CASE_BUILDERS, PUBLIC_SCALING_CASE_BUILDERS)
        # Fail closed before any verification work when the map is incomplete.
        validate_builders(base, PUBLIC_CASE_BUILDERS)
        if scaling:
            (
                validate_builders(
                    scaling,
                    PUBLIC_SCALING_CASE_BUILDERS,
                    public_operations=tuple(
                        op
                        for op in ALL_PUBLIC_OPERATIONS
                        if isinstance(op, ScalingOperation)
                    ),
                ),
            )

    from _bench_public_cases import PUBLIC_TIMED
    from _bench_public_fixtures import clear_public_fixture_cache
    from _bench_real_world_layers import clear_real_world_cache

    verified = 0
    for op in operations:
        builder = next(
            builder_map[op.gometry]
            for builder_map in builder_maps
            if op.gometry in builder_map
        )
        case = builder()
        try:
            context = OracleContext(
                operation=op,
                kind=getattr(case, 'kind', None) or 'oracle',
                unit=None,
                oracle_identity=public_oracle_identity(op),
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
    if output is not None:
        payload = {
            'schema_version': 1,
            'artifact_type': 'gometry-public-oracle',
            'operations': [op.gometry for op in operations],
            'identities': [public_oracle_identity(op) for op in operations],
            'verified': verified,
            'state_scope': 'external-fresh-process',
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open('x', encoding='utf-8') as stream:
            json.dump(payload, stream, indent=2, sort_keys=True)
            stream.write('\n')
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description='Untimed cross-library oracle for public RELEASE benchmarks.'
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--all',
        action='store_true',
        help='verify every declared public operation that has a builder',
    )
    parser.add_argument('--output', type=Path)
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

    return run_oracle(operations, output=args.output)


if __name__ == '__main__':
    raise SystemExit(main())
