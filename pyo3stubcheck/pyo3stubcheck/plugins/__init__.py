"""Opt-in project-specific stub parity plugins."""

from pyo3stubcheck.plugins.namespace_facade import NamespaceFacadeCheck
from pyo3stubcheck.plugins.rust_text_signature import RustTextSignatureCheck
from pyo3stubcheck.plugins.token_vocabulary import TokenVocabularyCheck

__all__ = [
    'NamespaceFacadeCheck',
    'RustTextSignatureCheck',
    'TokenVocabularyCheck',
]