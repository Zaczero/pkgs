"""pyo3stubs configuration for h2corn."""

from pathlib import Path

from pyo3stubs import StubConfig

ROOT = Path(__file__).resolve().parent.parent


def config() -> StubConfig:
    return StubConfig(
        module='h2corn._lib',
        stub_path=ROOT / 'python' / 'h2corn' / '_lib.pyi',
        src_root=ROOT / 'src',
    )
