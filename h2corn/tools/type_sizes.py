#!/usr/bin/env python3
"""Capture and compare Rust ``-Zprint-type-sizes`` output for h2corn.

    tools/type_sizes.py capture /tmp/main.type-sizes
    tools/type_sizes.py capture /tmp/head.type-sizes
    tools/type_sizes.py diff /tmp/main.type-sizes /tmp/head.type-sizes

The capture build uses its own target directory so it cannot invalidate the
normal development build cache.  It retains every printed type, including
``{async fn body ...}`` state machines and other futures: a smaller struct is
not a win if the future that owns it grows.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[1]
DEFAULT_TARGET_DIRECTORY = REPOSITORY / 'target' / 'type-sizes'
TYPE_SIZE = re.compile(
    r'^print-type-size type: `(?P<name>.+)`: (?P<size>\d+) bytes, '
    r'alignment: (?P<alignment>\d+) bytes$'
)


@dataclass(frozen=True, slots=True)
class Layout:
    size: int
    alignment: int


def capture(output: Path, target_directory: Path) -> None:
    """Build the crate in an isolated target directory and retain rustc output."""
    environment = os.environ.copy()
    prior_flags = environment.get('RUSTFLAGS', '')
    environment['RUSTFLAGS'] = f'{prior_flags} -Zprint-type-sizes'.strip()
    environment['CARGO_TARGET_DIR'] = str(target_directory)
    # A dead local sccache daemon must not make the layout oracle unavailable.
    environment.pop('RUSTC_WRAPPER', None)
    result = subprocess.run(
        ['cargo', 'build', '--release', '--lib'],
        cwd=REPOSITORY,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(result.stdout + result.stderr)
    if result.returncode:
        raise RuntimeError(f'capture build failed; compiler output is in {output}')
    if not parse_capture(output):
        raise RuntimeError(
            f'capture contained no type sizes; compiler output is in {output}'
        )


def parse_capture(path: Path) -> dict[str, Layout]:
    """Read every rustc type layout record, including anonymous async futures."""
    layouts: dict[str, Layout] = {}
    for line in path.read_text().splitlines():
        match = TYPE_SIZE.match(line)
        if match is None:
            continue
        layouts[match['name']] = Layout(int(match['size']), int(match['alignment']))
    return layouts


def future_or_async(name: str) -> bool:
    """Whether rustc identified this layout as an async state machine or future."""
    lowered = name.lower()
    return 'async fn body' in lowered or 'future' in lowered or 'coroutine' in lowered


def diff(control_path: Path, candidate_path: Path) -> int:
    """Report layout deltas, with async/future state machines called out."""
    control = parse_capture(control_path)
    candidate = parse_capture(candidate_path)
    if not control:
        raise RuntimeError(f'no type-size records in {control_path}')
    if not candidate:
        raise RuntimeError(f'no type-size records in {candidate_path}')

    changes = [
        (name, control[name], candidate[name])
        for name in control.keys() & candidate.keys()
        if control[name] != candidate[name]
    ]
    changes.sort(key=lambda change: (-abs(change[2].size - change[1].size), change[0]))
    if not changes:
        print('No shared type changed size or alignment.')
    else:
        print('Changed type layouts (candidate relative to control):')
        for name, before, after in changes:
            size_change = after.size - before.size
            alignment = (
                ''
                if before.alignment == after.alignment
                else (f', alignment {before.alignment} -> {after.alignment}')
            )
            kind = ' [async/future]' if future_or_async(name) else ''
            print(
                f'  {size_change:+5d} B  {before.size:>5} -> {after.size:<5} {name}{kind}{alignment}'
            )

    futures = sorted(
        name for name in control.keys() & candidate.keys() if future_or_async(name)
    )
    if futures:
        print('\nAsync/future state machines (including unchanged):')
        for name in futures:
            before, after = control[name], candidate[name]
            print(f'  {before.size:>5} -> {after.size:<5} B  {name}')

    only_control = sorted(control.keys() - candidate.keys())
    only_candidate = sorted(candidate.keys() - control.keys())
    if only_control:
        print(f'\nOnly in control ({len(only_control)}):')
        for name in only_control:
            print(f'  {control[name].size:>5} B  {name}')
    if only_candidate:
        print(f'\nOnly in candidate ({len(only_candidate)}):')
        for name in only_candidate:
            print(f'  {candidate[name].size:>5} B  {name}')
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description='capture and compare h2corn rustc type layouts',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    commands = parser.add_subparsers(dest='command', required=True)
    capture_parser = commands.add_parser('capture')
    capture_parser.add_argument('output', type=Path)
    capture_parser.add_argument(
        '--target-dir', type=Path, default=DEFAULT_TARGET_DIRECTORY
    )
    diff_parser = commands.add_parser('diff')
    diff_parser.add_argument('control', type=Path)
    diff_parser.add_argument('candidate', type=Path)
    args = parser.parse_args()

    try:
        if args.command == 'capture':
            capture(args.output, args.target_dir)
            print(f'Captured type sizes in {args.output}')
            return 0
        return diff(args.control, args.candidate)
    except (OSError, RuntimeError) as error:
        print(f'type-size tool failed: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
