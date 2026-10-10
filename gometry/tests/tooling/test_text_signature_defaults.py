from __future__ import annotations

import pytest
from stubconfig import config


def test_source_owned_text_signature_default_presentations_match_rust_source() -> None:
    pytest.importorskip(
        'tree_sitter_rust',
        reason='tree-sitter-rust==0.24.2 has no CPython 3.14t wheel',
    )
    from pyo3stubs.text_signature import collect_errors

    assert collect_errors(config()) == []
