"""Validate exact worker inputs and freeze a useful-work-weighted LLVM profile."""

from __future__ import annotations

import hashlib
import json
import math
import re
import shutil
import subprocess
from fractions import Fraction
from functools import reduce
from pathlib import Path


def integer_weights(shares: list[Fraction]) -> list[int]:
    if not shares or any(share <= 0 for share in shares):
        raise ValueError('profile shares must be positive')
    scale = math.lcm(*(share.denominator for share in shares))
    coefficients = [int(share * scale) for share in shares]
    divisor = reduce(math.gcd, coefficients)
    return [coefficient // divisor for coefficient in coefficients]


def profdata_tool() -> Path:
    compiler = subprocess.check_output(['rustc', '-vV'], text=True, timeout=15)
    host = next(
        line.removeprefix('host: ')
        for line in compiler.splitlines()
        if line.startswith('host: ')
    )
    sysroot = subprocess.check_output(
        ['rustc', '--print', 'sysroot'], text=True, timeout=15
    ).strip()
    tool = Path(sysroot) / 'lib/rustlib' / host / 'bin/llvm-profdata'
    if tool.is_file():
        return tool
    path = shutil.which('llvm-profdata')
    if path is None:
        raise RuntimeError('LLVM profile merger is unavailable')
    return Path(path).resolve()


def checked(tool: Path, arguments: list[str], output: Path) -> str:
    command = [str(tool), *arguments]
    result = subprocess.run(
        command, capture_output=True, text=True, timeout=120, check=False
    )
    output.write_text(result.stdout)
    output.with_suffix(output.suffix + '.stderr').write_text(result.stderr)
    if result.returncode or result.stderr:
        raise RuntimeError(
            f'profile tool failed or diagnosed invalid data: {command}\n{result.stderr}'
        )
    return result.stdout


def merge(receipt: Path, output: Path, *, build: dict) -> Path:
    root = receipt.parent
    collection = json.loads(receipt.read_text())
    plan_bytes = (root / 'plan.json').read_bytes()
    if hashlib.sha256(plan_bytes).hexdigest() != collection['plan_sha256']:
        raise RuntimeError('collection plan changed')
    plan = json.loads(plan_bytes)
    leaves = collection['leaves']
    by_name = {record['name']: record for record in leaves}
    if len(by_name) != len(leaves) or set(by_name) != {
        item['name'] for item in plan['leaves']
    }:
        raise RuntimeError('collection does not match its finite leaf plan')
    for leaf in plan['leaves']:
        if any(
            by_name[leaf['name']][key] != leaf[key]
            for key in ('weight', 'units', 'expected_functions')
        ):
            raise RuntimeError(
                f'{leaf["name"]}: collection differs from its planned work'
            )
    if sum(Fraction(record['weight']) for record in leaves) != 100:
        raise RuntimeError('training mixture weights must sum to 100')
    tool = profdata_tool()
    merged = root / 'merged'
    merged.mkdir()
    seen = set()
    image = None
    profiles = []
    for record in leaves:
        name = record['name']
        if not isinstance(record['units'], int) or record['units'] <= 0:
            raise RuntimeError(f'{name}: missing verified useful-work denominator')
        native = record['image_sha256']
        if native != build['generator_image_sha256']:
            raise RuntimeError('training leaf did not use the verified generator')
        if image is not None and image != native:
            raise RuntimeError('training leaves used different native images')
        image = native
        inputs = []
        for raw in record['raw_files']:
            path = Path(raw['path'])
            if path in seen or path.parent != root / name:
                raise RuntimeError(f'duplicate or misplaced profile input: {path}')
            seen.add(path)
            if hashlib.sha256(path.read_bytes()).hexdigest() != raw['sha256']:
                raise RuntimeError(f'worker profile changed after joining: {path}')
            covered = checked(
                tool, ['show', '--covered', str(path)], path.with_suffix('.covered.txt')
            )
            if not covered.strip():
                raise RuntimeError(f'worker profile lacks serving work: {path}')
            inputs.append(str(path))
        if not inputs:
            raise RuntimeError(f'{name}: no accepted profile inputs')
        profile = merged / f'{name}.profdata'
        checked(
            tool,
            [
                'merge',
                '--instr',
                '--sparse=false',
                '--failure-mode=any',
                *inputs,
                '-o',
                str(profile),
            ],
            profile.with_suffix('.merge.txt'),
        )
        covered = checked(
            tool,
            ['show', '--covered', str(profile)],
            profile.with_suffix('.covered.txt'),
        )
        families = record['expected_functions']
        if any(family not in covered for family in families):
            raise RuntimeError(
                f'{name}: expected native subsystem is absent: {families}'
            )
        summary = checked(
            tool,
            ['show', '--detailed-summary', str(profile)],
            profile.with_suffix('.summary.txt'),
        )
        maximum = re.search(r'Maximum internal block count: (\d+)', summary)
        if maximum is None or int(maximum[1]) == 0:
            raise RuntimeError(f'{name}: zero-contributing profile')
        profiles.append((profile, int(maximum[1])))
    coefficients = integer_weights([
        Fraction(record['weight']) / record['units'] for record in leaves
    ])
    if (
        sum(
            maximum * coefficient
            for (_, maximum), coefficient in zip(profiles, coefficients, strict=True)
        )
        >= 2**64
    ):
        raise RuntimeError('weighted profile counters could overflow')
    pending = merged / 'mixture.profdata'
    checked(
        tool,
        [
            'merge',
            '--instr',
            '--sparse=false',
            '--failure-mode=any',
            *(
                f'--weighted-input={coefficient},{path}'
                for (path, _), coefficient in zip(profiles, coefficients, strict=True)
            ),
            '-o',
            str(pending),
        ],
        merged / 'mixture.merge.txt',
    )
    digest = hashlib.sha256(pending.read_bytes()).hexdigest()
    accepted = output / digest
    accepted.mkdir(parents=True)
    profile = accepted / 'profile.profdata'
    profile.write_bytes(pending.read_bytes())
    profile.chmod(0o444)
    (accepted / 'profile.json').write_text(
        json.dumps(
            {
                **build,
                'sha256': digest,
                'generator_image_sha256': image,
                'collection_sha256': hashlib.sha256(receipt.read_bytes()).hexdigest(),
                'plan_sha256': collection['plan_sha256'],
                'compiler': subprocess.check_output(
                    ['rustc', '-vV'], text=True, timeout=15
                ),
                'merger': {
                    'path': str(tool),
                    'sha256': hashlib.sha256(tool.read_bytes()).hexdigest(),
                    'version': subprocess.check_output(
                        [str(tool), '--version'], text=True, timeout=15
                    ),
                },
                'leaves': [
                    {
                        'name': record['name'],
                        'units': record['units'],
                        'coefficient': coefficient,
                    }
                    for record, coefficient in zip(leaves, coefficients, strict=True)
                ],
            },
            indent=2,
        )
        + '\n'
    )
    return profile
