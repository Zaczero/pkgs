"""Protocols for opt-in stub-check plugins."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig
    from pyo3stubs.context import CheckContext


class Check(Protocol):
    """Project-specific gate plugged into :func:`stub_parity.collect_errors`."""

    def collect(self, cfg: StubConfig, ctx: CheckContext) -> list[str]:
        """Return parity violations (empty when clean)."""
        ...
