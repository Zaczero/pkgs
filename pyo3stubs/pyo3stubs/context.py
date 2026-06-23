"""Shared state built once per gate invocation."""

from __future__ import annotations

import ast
import importlib
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path
    from types import ModuleType

    from pyo3stubs.config import StubConfig


@dataclass
class CheckContext:
    """Lazy-loaded artifacts reused across universal checks and plugins."""

    cfg: StubConfig
    _runtime_module: ModuleType | None = None
    _stub_ast: ast.Module | None = None

    @property
    def runtime_module(self) -> ModuleType:
        if self._runtime_module is None:
            self._runtime_module = importlib.import_module(self.cfg.module)
        return self._runtime_module

    @property
    def stub_ast(self) -> ast.Module:
        if self._stub_ast is None:
            self._stub_ast = ast.parse(self.cfg.stub_path.read_text())
        return self._stub_ast

    @property
    def stub_text(self) -> str:
        return self.cfg.stub_path.read_text()

    @property
    def src_root(self) -> Path:
        return self.cfg.src_root
