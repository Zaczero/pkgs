from __future__ import annotations

from pyo3stubs.text_signature import collect_errors
from stubconfig import config


def test_source_owned_text_signature_default_presentations_match_rust_source() -> None:
    assert collect_errors(config()) == []
