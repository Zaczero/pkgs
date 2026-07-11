"""Opt-in project-specific stub parity plugins."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyo3stubs.plugins.namespace_facade import NamespaceFacadeCheck
from pyo3stubs.plugins.rust_text_signature import RustTextSignatureCheck
from pyo3stubs.plugins.token_vocabulary import TokenVocabularyCheck

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig

__all__ = [
    'NamespaceFacadeCheck',
    'RustTextSignatureCheck',
    'TokenVocabularyCheck',
    'collect_errors',
]


def collect_errors(cfg: StubConfig) -> list[str]:
    """Run every project-specific check from ``cfg.plugins``."""
    from pyo3stubs.context import CheckContext

    ctx = CheckContext(cfg)
    errors: list[str] = []
    for plugin in cfg.plugins:
        errors.extend(plugin.collect(cfg, ctx))
    return errors
