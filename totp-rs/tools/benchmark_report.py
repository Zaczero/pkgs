"""Validate and render the matched PyOTP comparison and native-only cases."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pyperf
from benchroom import Report, Series
from benchroom.plot import save
from benchroom.pyperf import load
from matplotlib.ticker import EngFormatter

from benchmark_cases import case_definitions

_COMMON_METADATA = (
    'benchmark_sha256',
    'cpu_model_name',
    'native_extension_sha256',
    'native_package_version',
    'platform',
    'pyotp_version',
    'pyperf_version',
    'python_cflags',
    'python_compiler',
    'python_config_args',
    'python_implementation',
    'python_version',
    'source_revision',
    'comparison_workload',
)
_WORKER_RUNS = 12


def _time(value: float) -> str:
    for scale, unit in ((1e-9, 'ns'), (1e-6, 'µs'), (1e-3, 'ms'), (1, 's')):
        if value < scale * 1000 or unit == 's':
            return f'{value / scale:.3g} {unit}'
    raise AssertionError('time formatter exhausted its units')


def _read(path: str, implementation: str) -> tuple[dict, tuple[Series, ...]]:
    suite = pyperf.BenchmarkSuite.load(path)
    metadata = suite.get_metadata()
    if metadata.get('implementation_name') != implementation:
        raise ValueError(
            f'{path}: expected {implementation!r}, got '
            f'{metadata.get("implementation_name")!r}'
        )
    if not metadata.get('implementation_version'):
        raise ValueError(f'{path}: missing implementation version')
    expected_version = metadata.get(
        'native_package_version' if implementation == 'totp-rs' else 'pyotp_version'
    )
    if metadata['implementation_version'] != expected_version:
        raise ValueError(f'{path}: implementation version metadata is inconsistent')
    try:
        definitions = json.loads(metadata['case_definitions'])
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError(f'{path}: missing valid case_definitions metadata') from exc

    missing = [field for field in _COMMON_METADATA if not metadata.get(field)]
    if missing:
        raise ValueError(f'{path}: missing identity metadata: {", ".join(missing)}')
    package_root = Path(__file__).resolve().parents[1]
    expected_hashes = {
        'benchmark_sha256': hashlib.sha256(
            (package_root / 'benchmark.py').read_bytes()
        ).hexdigest(),
    }
    for field, expected_hash in expected_hashes.items():
        if metadata[field] != expected_hash:
            raise ValueError(f'{path}: {field} does not match the current source')

    expected = case_definitions(implementation)
    if definitions != list(expected):
        raise ValueError(f'{path}: benchmark cases do not match the current suite')
    loaded = load(path)
    by_name = {item.case: item for item in loaded}
    expected_names = {case['name'] for case in expected}
    if set(by_name) != expected_names or len(loaded) != len(expected_names):
        raise ValueError(f'{path}: result file does not contain every benchmark case')

    label_by_name = {case['name']: case['label'] for case in expected}
    display_name = f'{implementation} {metadata["implementation_version"]}'
    results = tuple(
        Series(label_by_name[item.case], display_name, item.values) for item in loaded
    )
    for item in results:
        if len(item.values) != _WORKER_RUNS:
            raise ValueError(
                f'{path}: {item.case!r} has {len(item.values)} worker runs; '
                f'expected {_WORKER_RUNS}'
            )
    return metadata, results


def _admit(
    pyotp_path: str, native_path: str
) -> tuple[dict, dict, tuple[Series, ...], tuple[Series, ...]]:
    pyotp_metadata, pyotp_series = _read(pyotp_path, 'pyotp')
    native_metadata, native_series = _read(native_path, 'totp-rs')
    for field in _COMMON_METADATA:
        if pyotp_metadata.get(field) != native_metadata.get(field):
            raise ValueError(
                f'incompatible {field} metadata: '
                f'{pyotp_metadata.get(field)!r} != {native_metadata.get(field)!r}'
            )

    groups = {case['label']: case['group'] for case in case_definitions('totp-rs')}
    pyotp_names = {item.case: item for item in pyotp_series}
    native_comparison = tuple(
        item for item in native_series if groups[item.case] == 'comparison'
    )
    if set(pyotp_names) != {item.case for item in native_comparison}:
        raise ValueError('PyOTP and totp-rs do not contain matching comparison cases')
    native_only = tuple(
        item for item in native_series if groups[item.case] == 'native-only'
    )
    if len(native_only) != 7:
        raise ValueError(f'expected 7 native-only results, got {len(native_only)}')
    return (
        pyotp_metadata,
        native_metadata,
        pyotp_series + native_comparison,
        native_only,
    )


def _caption(metadata: dict) -> str:
    return f'{metadata["cpu_model_name"]} · CPython {metadata["python_version"]}'


def _markdown(
    metadata: dict,
    comparison: Report,
    native_comparison: Report,
) -> str:
    summaries = {(item.case, item.name): item for item in comparison.results}
    native_name = next(
        item.name for item in comparison.results if item.name.startswith('totp-rs ')
    )
    pyotp_name = next(
        item.name for item in comparison.results if item.name.startswith('pyotp ')
    )
    rows = [
        '# TOTP generation and verification',
        '',
        _caption(metadata),
        '',
        'Fixed timestamp · Six digits · 30-second period · verification window ±1.',
        'Median time per call; lower is faster.',
        '',
        '## Code generation and verification',
        '',
        '| Workload | ' + native_name + ' | ' + pyotp_name + ' |',
        '| --- | ---: | ---: |',
    ]
    for case in case_definitions('pyotp'):
        values = [
            _time(summaries[case['label'], name].median)
            for name in (native_name, pyotp_name)
        ]
        rows.append(f'| {case["label"]} | ' + ' | '.join(values) + ' |')

    rows.extend([
        '',
        f'## Raw-key and time-counter calls · {native_name}',
        '',
        '| Workload | Median time per call |',
        '| --- | ---: |',
    ])
    native_summaries = {item.case: item for item in native_comparison.results}
    for case in case_definitions('totp-rs'):
        if case['group'] != 'native-only':
            continue
        summary = native_summaries[case['label']]
        rows.append(f'| {case["label"]} | {_time(summary.median)} |')
    rows.append('')
    return '\n'.join(rows)


def report(
    pyotp_path: str,
    native_path: str,
    output: str = 'benchmarks/results.svg',
) -> None:
    """Write matched comparison charts and tables from compatible pyperf files."""
    pyotp_metadata, _native_metadata, shared_series, native_only = _admit(
        pyotp_path, native_path
    )
    comparison = Report(
        'TOTP generation and verification by algorithm',
        shared_series,
        unit='seconds',
        caption=_caption(pyotp_metadata),
    )
    native_comparison = Report(
        'Native-only TOTP input forms',
        native_only,
        unit='seconds',
        caption=_caption(pyotp_metadata),
    )
    destination = Path(output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    for dark in (False, True):
        figure = comparison.figure(dark=dark)
        figure.set_size_inches(11, max(5, 1.8 + len(comparison.results) * 0.38))
        axes = figure.axes[0]
        axes.set_xscale('log')
        axes.xaxis.set_major_formatter(EngFormatter(unit='s'))
        axes.set_xlabel('Median time per call · log scale · lower is faster')
        lower = min(
            item.interval[0] if item.interval is not None else item.median
            for item in comparison.results
        )
        upper = max(
            item.interval[1] if item.interval is not None else item.median
            for item in comparison.results
        )
        axes.set_xlim(lower * 0.7, upper * 1.8)
        case_order = {
            case['label']: index for index, case in enumerate(case_definitions('pyotp'))
        }
        ordered = sorted(comparison.results, key=lambda item: case_order[item.case])
        for index, item in enumerate(ordered):
            axes.annotate(
                _time(item.median),
                (item.median, index),
                xytext=(5, 0),
                textcoords='offset points',
                ha='left',
                va='center',
                fontsize=8,
                color=axes.xaxis.label.get_color(),
            )
        suffix = '-dark' if dark else ''
        report_path = destination.with_name(
            f'{destination.stem}{suffix}{destination.suffix}'
        )
        save(figure, report_path)

    destination.with_suffix('.md').write_text(
        _markdown(pyotp_metadata, comparison, native_comparison), encoding='utf-8'
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Validate and render TOTP pyperf results.'
    )
    parser.add_argument('pyotp_results')
    parser.add_argument('totp_rs_results')
    parser.add_argument(
        '-o',
        '--output',
        default='benchmarks/results.svg',
        help='path for light-theme SVG; dark SVG and Markdown use the same stem',
    )
    args = parser.parse_args()
    report(args.pyotp_results, args.totp_rs_results, args.output)


if __name__ == '__main__':
    main()
