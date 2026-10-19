from __future__ import annotations

import importlib
import io
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pytest

SCRIPT_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = SCRIPT_DIR.parents[1]
original_path = sys.path[:]
try:
    sys.path.insert(0, str(SCRIPT_DIR))
    route_packages = importlib.import_module('route_packages')
finally:
    sys.path[:] = original_path


def test_selection_contains_only_package_names(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(REPO_ROOT)
    monkeypatch.setattr(route_packages.sys, 'stdin', io.StringIO('h2corn/src/lib.rs\n'))

    route_packages.main()

    selection = json.loads(capsys.readouterr().out)

    assert 'h2corn' in {item['package'] for item in selection}
    assert all(set(item) == {'package'} for item in selection)
