"""pyo3stubs configuration for lrucache_rs."""

from pathlib import Path

from pyo3stubs import StubConfig

ROOT = Path(__file__).resolve().parent.parent


def config() -> StubConfig:
    return StubConfig(
        module='lrucache_rs._lib',
        stub_path=ROOT / 'python' / 'lrucache_rs' / '_lib.pyi',
        src_root=ROOT / 'src',
    )
