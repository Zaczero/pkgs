from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests._support import load_tool

GOMETRY_ROOT = Path(__file__).resolve().parents[2]

quality_check = load_tool('quality_check', GOMETRY_ROOT / 'tools/check.py')
gatelib = sys.modules['_gatelib']


def test_run_missing_executable_returns_a_failed_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv('PATH', str(tmp_path))

    assert not quality_check.run(('cargo', '--version'))

    assert 'could not execute cargo' in capsys.readouterr().err


def test_strict_docs_build_does_not_inherit_markdown_exec_auto(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('MARKDOWN_EXEC_AUTO', 'python')
    monkeypatch.setattr(
        quality_check,
        '_runtime_command',
        lambda _arguments: [
            sys.executable,
            '-c',
            (
                'import os; '
                "assert 'MARKDOWN_EXEC_AUTO' not in os.environ, "
                "'MARKDOWN_EXEC_AUTO leaked'"
            ),
        ],
    )

    assert quality_check.run(quality_check.DOCS_BUILD.command, require_runtime=True)


def test_doctest_pyright_uses_the_selected_interpreter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = load_tool(
        '_check_doctest_types',
        GOMETRY_ROOT / 'tools/gates/_check_doctest_types.py',
        GOMETRY_ROOT / 'tools/gates',
    )
    selected_python = tmp_path / 'selected-python'
    calls: list[tuple[list[str], dict[str, object]]] = []

    def run(command: list[str], **kwargs: object) -> SimpleNamespace:
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout='', stderr='')

    monkeypatch.setattr(gate, 'sys', SimpleNamespace(executable=str(selected_python)))
    monkeypatch.setattr(gate.subprocess, 'run', run)
    errors: list[str] = []
    target = tmp_path / 'corpus.py'
    target.write_text('pass\n', encoding='utf-8')

    gate._check_file('corpus.py', target, errors)

    assert not errors
    assert calls[0][0] == [
        str(selected_python),
        '-m',
        'pyright',
        str(target),
        '--pythonpath',
        str(selected_python),
    ]


def test_typing_runtime_pyright_uses_the_selected_interpreter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = load_tool(
        '_check_typing_runtime',
        GOMETRY_ROOT / 'tools/gates/_check_typing_runtime.py',
        GOMETRY_ROOT / 'tools/gates',
    )
    selected_python = tmp_path / 'selected-python'
    calls: list[tuple[list[str], dict[str, object]]] = []

    def run(command: list[str], **kwargs: object) -> SimpleNamespace:
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout='', stderr='')

    monkeypatch.setattr(gate, 'sys', SimpleNamespace(executable=str(selected_python)))
    monkeypatch.setattr(gate.subprocess, 'run', run)
    errors: list[str] = []

    gate._check_pyright(errors, 'fixture', 'corpus.py')

    assert not errors
    assert calls[0][0] == [
        str(selected_python),
        '-m',
        'pyright',
        'corpus.py',
        '--pythonpath',
        str(selected_python),
    ]


def test_no_fix_keeps_source_checks_running_when_runtime_admission_fails(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, 'argv', ['tools/check.py', '--no-fix'])
    admissions: list[Path] = []

    def stale(_root: Path) -> None:
        admissions.append(_root)
        raise SystemExit('runtime is stale')

    monkeypatch.setattr(
        quality_check,
        '_quality_runtime_module',
        lambda: pytest.fail('--no-fix must not load the quality-preparation owner'),
    )
    monkeypatch.setattr(
        gatelib,
        'require_runtime',
        stale,
    )
    calls: list[tuple[tuple[str, ...], bool]] = []

    def run(
        arguments: tuple[str, ...],
        *,
        environment: dict[str, str] | None = None,
        require_runtime: bool = False,
    ) -> bool:
        calls.append((arguments, require_runtime))
        return True

    monkeypatch.setattr(quality_check, 'run', run)
    environment_before = os.environ.copy()

    status = quality_check.main()

    output = capsys.readouterr().err
    expected_source_checks = [
        (check.command, False)
        for check in quality_check.CHECKS
        if not check.requires_runtime
    ]
    expected_skips = [
        check.label for check in quality_check.CHECKS if check.requires_runtime
    ]
    assert status == 1
    assert admissions == [GOMETRY_ROOT]
    assert calls == expected_source_checks
    assert output.count('quality runtime prerequisite failed:') == 1
    assert 'quality runtime prerequisite failed: runtime is stale' in output
    assert 'Skipped dependent quality checks:' in output
    assert all(f'  {label}' in output for label in expected_skips)
    assert 'TOTAL QUALITY FAILURES: 1' in output
    assert f'TOTAL QUALITY SKIPPED: {len(expected_skips)}' in output
    assert os.environ == environment_before


def test_contracts_only_selects_readonly_public_checks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        sys,
        'argv',
        ['tools/check.py', '--no-fix', '--contracts-only'],
    )
    monkeypatch.setattr(
        quality_check,
        '_quality_runtime_module',
        lambda: pytest.fail('contract checks must not prepare a runtime'),
    )
    monkeypatch.setattr(gatelib, 'require_runtime', lambda _root: None)
    calls: list[tuple[tuple[str, ...], bool]] = []

    def run(
        command: tuple[str, ...],
        *,
        environment: dict[str, str] | None = None,
        require_runtime: bool = False,
    ) -> bool:
        calls.append((command, require_runtime))
        return True

    monkeypatch.setattr(quality_check, 'run', run)

    assert quality_check.main() == 0
    expected = [
        (check.command, check.requires_runtime)
        for check in quality_check.CHECKS
        if check.category is quality_check.CheckCategory.CONTRACT
    ]
    assert calls == expected
    commands = {command for command, _ in calls}
    assert {
        ('-m', 'pyo3stubs', 'check-all', '--config', 'tools/stubs/stubconfig.py'),
        ('tools/gates/_check_typing_runtime.py',),
        ('tools/stubs/_doc_coverage.py',),
        ('tools/gates/_check_doc_integrity.py',),
        ('tools/gates/_check_doc_model.py',),
        ('tools/gates/_check_docstring_examples.py',),
        ('tools/gates/_check_doctest_types.py',),
        ('tools/gates/_check_examples.py',),
        ('tools/docs/check_nav.py',),
        ('-m', 'properdocs', 'build', '--strict'),
        ('tools/docs/check.py',),
    } <= commands
    assert {
        ('ruff', 'check', '.'),
        ('ruff', 'format', '--check', '.'),
        ('cargo', 'fmt', '--package', 'gometry', '--', '--check'),
        ('tools/gates/_check_algebraic_float.py',),
        ('tools/gates/_check_packed_execution.py',),
        ('tools/gates/_check_typed_returns.py',),
        (
            'cargo',
            'clippy',
            '--all-targets',
            '--locked',
            '-p',
            'gometry',
            '--',
            '-D',
            'warnings',
        ),
    }.isdisjoint(commands)
    assert not any(command in quality_check.FIXES for command, _ in calls)


def test_contracts_only_stale_runtime_fails_without_preparation(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, 'argv', ['tools/check.py', '--contracts-only'])

    def stale(_root: Path) -> None:
        raise SystemExit('runtime is stale')

    monkeypatch.setattr(gatelib, 'require_runtime', stale)
    monkeypatch.setattr(
        quality_check,
        '_quality_runtime_module',
        lambda: pytest.fail('contract checks must not prepare a runtime'),
    )
    calls: list[tuple[str, ...]] = []

    def run(
        command: tuple[str, ...],
        *,
        environment: dict[str, str] | None = None,
        require_runtime: bool = False,
    ) -> bool:
        calls.append(command)
        return True

    monkeypatch.setattr(quality_check, 'run', run)

    assert quality_check.main() == 1
    output = capsys.readouterr().err
    assert 'quality runtime prerequisite failed: runtime is stale' in output
    assert calls == [
        check.command
        for check in quality_check.CHECKS
        if check.category is quality_check.CheckCategory.CONTRACT
        and not check.requires_runtime
    ]
    assert 'properdocs build --strict' in output
    assert 'tools/docs/check.py' in output
    assert 'TOTAL QUALITY FAILURES: 1' in output


def test_no_fix_preserves_the_selected_runtime_for_child_checks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, 'argv', ['tools/check.py', '--no-fix'])
    monkeypatch.setattr(
        quality_check,
        '_quality_runtime_module',
        lambda: pytest.fail('--no-fix must not load the quality-preparation owner'),
    )
    package_root = tmp_path / 'packages' / 'identified-runtime'
    monkeypatch.setenv('PYTHONPATH', str(package_root))
    admissions: list[Path] = []

    def admit(root: Path) -> None:
        admissions.append(root)

    monkeypatch.setattr(gatelib, 'require_runtime', admit)
    calls: list[tuple[tuple[str, ...], bool, dict[str, str] | None]] = []

    def run(
        arguments: tuple[str, ...],
        *,
        environment: dict[str, str] | None = None,
        require_runtime: bool = False,
    ) -> bool:
        calls.append((arguments, require_runtime, environment))
        return True

    monkeypatch.setattr(quality_check, 'run', run)

    status = quality_check.main()

    assert status == 0
    assert admissions == [GOMETRY_ROOT]
    assert len(calls) == len(quality_check.CHECKS)
    runtime_calls = [call for call in calls if call[1]]
    assert len(runtime_calls) == sum(
        check.requires_runtime for check in quality_check.CHECKS
    )
    assert [call[0] for call in calls[-2:]] == [
        quality_check.DOCS_BUILD.command,
        quality_check.DOCS_CHECK.command,
    ]
    assert all(
        call[2] is not None
        and call[2]['PYTHONPATH'].split(os.pathsep)[0] == str(package_root)
        for call in runtime_calls
    )


def test_prepare_runs_preparation_sequence_without_checks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, 'argv', ['tools/check.py', '--prepare'])
    package_root = tmp_path / 'prepared' / 'gometry'
    receipt = {'package_root': str(package_root)}
    events: list[tuple[str, str]] = []
    runtime_calls: list[bool] = []

    def run(
        arguments: tuple[str, ...],
        *,
        environment: dict[str, str] | None = None,
        require_runtime: bool = False,
    ) -> bool:
        events.append(('run', ' '.join(arguments)))
        runtime_calls.append(require_runtime)
        return True

    def prepare(root: Path, *, scratch: Path) -> dict[str, str]:
        events.append(('prepare', str(root)))
        assert scratch == GOMETRY_ROOT / 'target' / 'quality-runtime'
        return receipt

    def read(root: Path) -> dict[str, str]:
        events.append(('read', str(root)))
        return receipt

    def refresh(root: Path) -> dict[str, str]:
        events.append(('refresh', str(root)))
        return receipt

    monkeypatch.setattr(quality_check, 'run', run)
    monkeypatch.setattr(
        quality_check,
        '_quality_runtime_module',
        lambda: SimpleNamespace(
            prepare_quality_runtime=prepare,
            read_quality_receipt=read,
            refresh_quality_runtime=refresh,
        ),
    )

    status = quality_check.main()

    assert status == 0
    assert capsys.readouterr().out == f'{package_root}\n'
    assert events == [
        ('prepare', str(GOMETRY_ROOT)),
        ('read', str(GOMETRY_ROOT)),
        ('run', ' '.join(quality_check.GENERATE_STUBS)),
        ('run', 'ruff format --quiet python/gometry/_lib.pyi'),
        ('refresh', str(GOMETRY_ROOT)),
        ('read', str(GOMETRY_ROOT)),
    ]
    assert runtime_calls == [True, False]


def test_default_mode_runs_fixers_before_shared_runtime_preparation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, 'argv', ['tools/check.py'])
    package_root = tmp_path / 'prepared' / 'gometry'
    receipt = {'package_root': str(package_root)}
    events: list[tuple[str, str]] = []
    runtime_calls: list[bool] = []

    def run(
        arguments: tuple[str, ...],
        *,
        environment: dict[str, str] | None = None,
        require_runtime: bool = False,
    ) -> bool:
        events.append(('run', ' '.join(arguments)))
        runtime_calls.append(require_runtime)
        return True

    def prepare(root: Path, *, scratch: Path) -> dict[str, str]:
        events.append(('prepare', str(root)))
        assert scratch == GOMETRY_ROOT / 'target' / 'quality-runtime'
        return receipt

    def read(root: Path) -> dict[str, str]:
        events.append(('read', str(root)))
        return receipt

    def refresh(root: Path) -> dict[str, str]:
        events.append(('refresh', str(root)))
        return receipt

    monkeypatch.setattr(quality_check, 'run', run)
    monkeypatch.setattr(
        quality_check,
        '_quality_runtime_module',
        lambda: SimpleNamespace(
            prepare_quality_runtime=prepare,
            read_quality_receipt=read,
            refresh_quality_runtime=refresh,
        ),
    )

    status = quality_check.main()

    assert status == 0
    assert capsys.readouterr().out == ''
    assert events == [
        *(('run', ' '.join(step)) for step in quality_check.FIXES),
        ('prepare', str(GOMETRY_ROOT)),
        ('read', str(GOMETRY_ROOT)),
        ('run', ' '.join(quality_check.GENERATE_STUBS)),
        ('run', 'ruff format --quiet python/gometry/_lib.pyi'),
        ('refresh', str(GOMETRY_ROOT)),
        ('read', str(GOMETRY_ROOT)),
        *(('run', check.label) for check in quality_check.CHECKS),
    ]
    assert runtime_calls == [
        *([False] * len(quality_check.FIXES)),
        True,
        False,
        *(check.requires_runtime for check in quality_check.CHECKS),
    ]
    assert ('run', 'ruff check .') in events


@pytest.mark.parametrize(
    ('failed_step', 'expected_site_calls'),
    [
        pytest.param(
            'build',
            [quality_check.DOCS_BUILD.command],
            id='build-fails',
        ),
        pytest.param(
            'check',
            [
                quality_check.DOCS_BUILD.command,
                quality_check.DOCS_CHECK.command,
            ],
            id='audit-fails',
        ),
    ],
)
def test_site_checks_run_in_order_and_propagate_failures(
    failed_step: str,
    expected_site_calls: list[tuple[str, ...]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, 'argv', ['tools/check.py', '--no-fix'])
    monkeypatch.setattr(quality_check, 'ROOT', tmp_path)
    monkeypatch.setattr(gatelib, 'require_runtime', lambda _root: None)
    stale_page = tmp_path / 'site' / 'api' / 'index.html'
    if failed_step == 'build':
        stale_page.parent.mkdir(parents=True)
        stale_page.write_text('<html><body>stale passing API site</body></html>')
    calls: list[tuple[tuple[str, ...], bool]] = []

    def run(
        arguments: tuple[str, ...],
        *,
        environment: dict[str, str] | None = None,
        require_runtime: bool = False,
    ) -> bool:
        del environment
        calls.append((arguments, require_runtime))
        if failed_step == 'build' and arguments == quality_check.DOCS_BUILD.command:
            return False
        return not (
            failed_step == 'check' and arguments == quality_check.DOCS_CHECK.command
        )

    monkeypatch.setattr(quality_check, 'run', run)

    status = quality_check.main()

    output = capsys.readouterr().err
    assert status == 1
    assert [arguments for arguments, _ in calls[-len(expected_site_calls) :]] == (
        expected_site_calls
    )
    assert all(required for _, required in calls[-len(expected_site_calls) :])
    checks_before_site = [
        check
        for check in quality_check.CHECKS
        if check not in {quality_check.DOCS_BUILD, quality_check.DOCS_CHECK}
    ]
    assert calls == [
        *((check.command, check.requires_runtime) for check in checks_before_site),
        *((command, True) for command in expected_site_calls),
    ]
    if failed_step == 'build':
        assert stale_page.is_file()
        assert 'properdocs build --strict' in output
        assert 'tools/docs/check.py' in output
        assert output.count('quality prerequisite failed:') == 0
    else:
        assert 'tools/docs/check.py' in output


def test_properdocs_strict_build_fails_on_a_real_broken_document_link(
    tmp_path: Path,
) -> None:
    docs = tmp_path / 'docs'
    docs.mkdir()
    (docs / 'index.md').write_text(
        '# Fixture\n\n[Missing page](missing.md)\n',
        encoding='utf-8',
    )
    config = tmp_path / 'properdocs.yml'
    config.write_text(
        'site_name: Strict fixture\n'
        'site_url: https://example.test/\n'
        'docs_dir: docs\n'
        'site_dir: site\n'
        'theme:\n'
        '  name: material\n'
        'plugins: []\n'
        'nav:\n'
        '  - Home: index.md\n',
        encoding='utf-8',
    )

    result = subprocess.run(
        [
            sys.executable,
            '-m',
            'properdocs',
            'build',
            '--strict',
            '--config-file',
            str(config),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    output = result.stdout + result.stderr
    assert result.returncode != 0
    assert 'missing.md' in output
    assert 'strict mode' in output


def test_prepare_and_no_fix_are_mutually_exclusive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        sys,
        'argv',
        ['tools/check.py', '--prepare', '--no-fix'],
    )

    with pytest.raises(SystemExit) as error:
        quality_check.main()

    assert error.value.code == 2
