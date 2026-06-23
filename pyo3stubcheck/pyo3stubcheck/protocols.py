"""Protocols for opt-in stub-check plugins."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from pyo3stubcheck.config import StubCheckConfig
    from pyo3stubcheck.context import CheckContext


class Check(Protocol):
    """Project-specific gate plugged into :func:`stub_parity.collect_errors`."""

    def collect(self, cfg: StubCheckConfig, ctx: CheckContext) -> list[str]:
        """Return parity violations (empty when clean)."""
        ...