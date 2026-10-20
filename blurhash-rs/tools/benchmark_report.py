"""Validate and render the complete Pillow-image benchmark."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pyperf
from benchroom import Report, Series
from benchroom.plot import curves, save
from benchroom.pyperf import load
from matplotlib.ticker import EngFormatter

from benchmark import CASE_DEFINITIONS

_IMPLEMENTATIONS = {'blurhash-rs', 'blurhash', 'blurhash-python'}
_COMMON_METADATA = (
    'benchmark_sha256',
    'case_definitions',
    'cpu_model_name',
    'native_extension_sha256',
    'native_package_version',
    'numpy_version',
    'pillow_version',
    'platform',
    'pyperf_version',
    'python_cflags',
    'python_compiler',
    'python_config_args',
    'python_implementation',
    'python_version',
    'report_sha256',
    'source_revision',
    'workload',
)
_WORKER_RUNS = 12


def _metadata(path: str) -> tuple[dict, tuple[Series, ...]]:
    suite = pyperf.BenchmarkSuite.load(path)
    metadata = suite.get_metadata()
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
        'report_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    for field, expected_hash in expected_hashes.items():
        if metadata[field] != expected_hash:
            raise ValueError(f'{path}: {field} does not match the current source')

    expected = {case['name']: case for case in CASE_DEFINITIONS}
    actual = {case['name']: case for case in definitions}
    if actual != expected or len(definitions) != len(expected):
        raise ValueError(f'{path}: benchmark cases do not match the current suite')

    loaded = load(path)
    by_name = {item.case: item for item in loaded}
    if set(by_name) != set(expected) or len(loaded) != len(expected):
        raise ValueError(f'{path}: result file does not contain every benchmark case')
    implementation = metadata.get('implementation_name')
    if implementation not in _IMPLEMENTATIONS:
        raise ValueError(f'{path}: unsupported implementation {implementation!r}')
    version = metadata.get('implementation_version')
    if not version:
        raise ValueError(f'{path}: missing implementation version')
    if (
        implementation == 'blurhash-rs'
        and version != metadata['native_package_version']
    ):
        raise ValueError(f'{path}: native package version metadata is inconsistent')
    try:
        dependencies = json.loads(metadata['implementation_dependencies'])
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError(f'{path}: missing valid implementation_dependencies') from exc
    if not isinstance(dependencies, dict):
        raise TypeError(f'{path}: implementation_dependencies must be an object')
    if dependencies.get('Pillow') != metadata['pillow_version']:
        raise ValueError(f'{path}: Pillow dependency metadata is inconsistent')
    if dependencies.get('NumPy') != metadata['numpy_version']:
        raise ValueError(f'{path}: NumPy dependency metadata is inconsistent')
    if dependencies.get('pyperf') != metadata['pyperf_version']:
        raise ValueError(f'{path}: pyperf dependency metadata is inconsistent')
    if (
        implementation == 'blurhash-python'
        and not {
            'cffi',
            'six',
        }
        <= dependencies.keys()
    ):
        raise ValueError(f'{path}: blurhash-python dependency identity is incomplete')

    names = {case['name']: f'{implementation} {version}' for case in definitions}
    labels = {case['name']: case['label'] for case in definitions}
    results = tuple(
        Series(labels[item.case], names[item.case], item.values) for item in loaded
    )
    for item in results:
        if len(item.values) != _WORKER_RUNS:
            raise ValueError(
                f'{path}: {item.case!r} has {len(item.values)} worker runs; '
                f'expected {_WORKER_RUNS}'
            )
    return metadata, results


def _admit(paths: list[str]) -> tuple[dict[str, dict], tuple[Series, ...]]:
    if len(paths) != len(_IMPLEMENTATIONS):
        raise ValueError(
            f'expected one result file for each implementation '
            f'({", ".join(sorted(_IMPLEMENTATIONS))})'
        )

    loaded = [_metadata(path) for path in paths]
    by_implementation: dict[str, tuple[dict, tuple[Series, ...]]] = {}
    for metadata, results in loaded:
        implementation = metadata['implementation_name']
        if implementation in by_implementation:
            raise ValueError(f'duplicate result file for {implementation}')
        by_implementation[implementation] = metadata, results
    if set(by_implementation) != _IMPLEMENTATIONS:
        missing = ', '.join(sorted(_IMPLEMENTATIONS - set(by_implementation)))
        raise ValueError(f'missing result file for {missing}')

    reference = by_implementation['blurhash-rs'][0]
    for implementation, (metadata, _) in by_implementation.items():
        for field in _COMMON_METADATA:
            if metadata.get(field) != reference.get(field):
                raise ValueError(
                    f'{implementation}: incompatible {field} metadata '
                    f'({metadata.get(field)!r} != {reference.get(field)!r})'
                )

    definitions = json.loads(reference['case_definitions'])
    series = tuple(
        item
        for case in definitions
        for implementation in ('blurhash-rs', 'blurhash', 'blurhash-python')
        for item in by_implementation[implementation][1]
        if item.case == case['label']
    )
    return by_implementation, series


def _time(value: float) -> str:
    for scale, unit in ((1e-9, 'ns'), (1e-6, 'µs'), (1e-3, 'ms'), (1, 's')):
        if value < scale * 1000 or unit == 's':
            return f'{value / scale:.3g} {unit}'
    raise AssertionError('time formatter exhausted its units')


def _markdown(results: Report, definitions: tuple[dict, ...], metadata: dict) -> str:
    captions = (
        f'{metadata["cpu_model_name"]} · {metadata["platform"]} · '
        f'{metadata["python_implementation"]} {metadata["python_version"]}\n'
        f'Pillow {metadata["pillow_version"]} · NumPy {metadata["numpy_version"]}\n'
        'Each value is a median of 12 independent worker-process means; '
        'intervals are 95% percentile bootstrap intervals across those workers.'
    )
    summaries = {(item.case, item.name): item for item in results.results}
    rows = [f'# {results.title}', '', captions, '']
    for operation, heading in (
        ('encode', 'Encode a Pillow image'),
        ('decode', 'Decode to a Pillow image'),
    ):
        rows.extend([
            f'## {heading}',
            '',
            '| Workload | Implementation | Median time | 95% interval | Worker processes |',
            '| --- | --- | ---: | ---: | ---: |',
        ])
        for case in definitions:
            if case['operation'] != operation:
                continue
            for implementation in (
                'blurhash-rs',
                'blurhash',
                'blurhash-python',
            ):
                summary = next(
                    item
                    for (label, name), item in summaries.items()
                    if label == case['label'] and name.startswith(implementation + ' ')
                )
                interval = (
                    '—'
                    if summary.interval is None
                    else f'{_time(summary.interval[0])} to {_time(summary.interval[1])}'
                )
                rows.append(
                    f'| {case["label"]} | {summary.name} | '
                    f'{_time(summary.median)} | {interval} | {summary.runs} |'
                )
        rows.append('')
    rows.extend([
        'All three implementations use the same image dimensions and component counts. '
        'Pillow-to-NumPy conversion, the RGB image copy required by blurhash-python, '
        'and Pillow output construction are included where their interfaces require them.',
        '',
    ])
    return '\n'.join(rows)


def _environment_caption(metadata: dict) -> str:
    return (
        f'{metadata["cpu_model_name"]} · {metadata["python_implementation"]} '
        f'{metadata["python_version"]} · Pillow {metadata["pillow_version"]} · '
        f'NumPy {metadata["numpy_version"]}\n'
        '12 independent worker-process means per workload'
    )


def _scaling_caption(metadata: dict) -> str:
    return (
        f'{_environment_caption(metadata)}\n'
        'Dots: medians · Whiskers: 95% bootstrap intervals · lower is faster'
    )


def _save_scaling(
    results: Report,
    definitions: tuple[dict, ...],
    metadata: dict,
    root: Path,
    operation: str,
    *,
    dark: bool,
) -> None:
    cases = [
        case
        for case in definitions
        if case['operation'] == operation and case['group'] == 'square-size'
    ]
    names = tuple(dict.fromkeys(item.name for item in results.results))
    summaries = {(item.case, item.name): item for item in results.results}
    values = {
        name: {case['x']: summaries[case['label'], name].median for case in cases}
        for name in names
    }
    figure = curves(
        values,
        title=f'BlurHash {operation} · square RGB images',
        xlabel='Image side (pixels)',
        ylabel='Median time per image · logarithmic scale · lower is faster',
        dark=dark,
    )
    axes = figure.axes[0]
    axes.set_xticks([case['x'] for case in cases])
    axes.set_yscale('log')
    axes.yaxis.set_major_formatter(EngFormatter(unit='s'))
    for name in names:
        for case in cases:
            item = summaries[case['label'], name]
            if item.interval is None:
                continue
            low, high = item.interval
            axes.errorbar(
                case['x'],
                item.median,
                yerr=[[item.median - low], [high - item.median]],
                fmt='none',
                color=next(
                    line.get_color() for line in axes.lines if line.get_label() == name
                ),
                capsize=4,
            )
    figure.supxlabel(
        _scaling_caption(metadata),
        fontsize=9,
        color=axes.xaxis.label.get_color(),
    )
    suffix = '-dark' if dark else ''
    save(figure, root / f'{operation}{suffix}.svg')


def _save_variants(
    metadata: dict,
    by_implementation: dict[str, tuple[dict, tuple[Series, ...]]],
    definitions: tuple[dict, ...],
    root: Path,
    operation: str,
    *,
    dark: bool,
) -> None:
    cases = [
        case
        for case in definitions
        if case['operation'] == operation and case['group'] != 'square-size'
    ]
    series = tuple(
        item
        for case in cases
        for implementation in ('blurhash-rs', 'blurhash', 'blurhash-python')
        for item in by_implementation[implementation][1]
        if item.case == case['label']
    )
    comparison = Report(
        f'BlurHash {operation} · other workloads',
        series,
        unit='seconds',
        caption=_environment_caption(metadata),
    )
    figure = comparison.figure(dark=dark)
    figure.set_size_inches(11, max(4, 1.9 + len(comparison.results) * 0.42))
    axes = figure.axes[0]
    axes.set_xscale('log')
    axes.xaxis.set_major_formatter(EngFormatter(unit='s'))
    axes.set_xlabel('Median time per image · logarithmic scale · lower is faster')
    suffix = '-dark' if dark else ''
    save(figure, root / f'{operation}-workloads{suffix}.svg')


def report(paths: list[str], output_directory: str = 'benchmarks') -> None:
    """Write validated Markdown tables and light/dark charts from pyperf files."""
    by_implementation, series = _admit(paths)
    metadata = by_implementation['blurhash-rs'][0]
    definitions = tuple(json.loads(metadata['case_definitions']))
    comparison = Report(
        'BlurHash image benchmarks',
        series,
        unit='seconds',
        caption=_environment_caption(metadata),
    )
    root = Path(output_directory)
    root.mkdir(parents=True, exist_ok=True)
    root.joinpath('results.md').write_text(
        _markdown(comparison, definitions, metadata), encoding='utf-8'
    )
    for operation in ('encode', 'decode'):
        for dark in (False, True):
            _save_scaling(comparison, definitions, metadata, root, operation, dark=dark)
            _save_variants(
                metadata,
                by_implementation,
                definitions,
                root,
                operation,
                dark=dark,
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Validate and render BlurHash pyperf results.'
    )
    parser.add_argument(
        'results',
        nargs=3,
        metavar='PYPerf-JSON',
        help='one result file each for blurhash-rs, blurhash, and blurhash-python',
    )
    parser.add_argument(
        '-o',
        '--output-directory',
        default='benchmarks',
        help='directory for Markdown tables and SVG charts',
    )
    args = parser.parse_args()
    report(args.results, args.output_directory)


if __name__ == '__main__':
    main()
