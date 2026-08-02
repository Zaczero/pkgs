"""Shared state built once per gate invocation.

The stub is read, parsed and indexed once. Four gates used to re-derive the
same class index — one of them three times in a single run — which is both
waste and a place for two spellings of "the stub's classes" to drift apart.
"""

from __future__ import annotations

import ast
import importlib
from dataclasses import dataclass, field
from functools import cached_property
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType

    from pyo3stubs.config import StubConfig


@dataclass
class CheckContext:
    """Lazy-loaded artifacts reused across gates."""

    cfg: StubConfig
    _runtime_module: ModuleType | None = field(default=None, repr=False)

    @property
    def runtime_module(self) -> ModuleType:
        """The compiled extension, imported on first use."""
        if self._runtime_module is None:
            self._runtime_module = importlib.import_module(self.cfg.module)
        return self._runtime_module

    @cached_property
    def stub_text(self) -> str:
        """Source of the hand-authored stub.

        UTF-8 explicitly, matching what `gen-docs` writes and what
        `gen-docs-sync` compares — a locale-dependent read here would decode
        the file the generator had just written differently from the gate.
        """
        return self.cfg.stub_path.read_text(encoding='utf-8')

    @cached_property
    def stub_ast(self) -> ast.Module:
        """Parsed stub."""
        return ast.parse(self.stub_text)

    @cached_property
    def stub_classes(self) -> dict[str, ast.ClassDef]:
        """Top-level stub classes by name, in declaration order."""
        return {
            node.name: node
            for node in self.stub_ast.body
            if isinstance(node, ast.ClassDef)
        }
