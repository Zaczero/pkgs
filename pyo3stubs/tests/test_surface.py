"""Cross-surface option-signature comparisons."""

from __future__ import annotations

from types import ModuleType

from pyo3stubs.config import SurfaceConfig
from pyo3stubs.surface import collect_errors
from tests._support import make_config


def test_uninspectable_configured_callable_is_reported(tmp_path):
    class Left:
        def operation(self, value: int, enabled: bool = True) -> None: ...

    class Right:
        def operation(self, value: int, enabled: bool = True) -> None: ...

    Left.operation.__signature__ = object()  # type: ignore[attr-defined]
    cfg = make_config(
        tmp_path,
        surface=SurfaceConfig(targets=(('Left', Left), ('Right', Right))),
    )

    errors = collect_errors(cfg)

    assert len(errors) == 1, errors
    assert 'Left.operation' in errors[0]
    assert 'inspect.signature' in errors[0]
    assert 'valid __signature__' in errors[0]


def test_resolved_registered_divergence_requires_removal(tmp_path):
    class Left:
        def operation(self, value: int, enabled: bool = True) -> None: ...

    class Right:
        def operation(self, value: int, enabled: bool = True) -> None: ...

    cfg = make_config(
        tmp_path,
        surface=SurfaceConfig(
            targets=(('Left', Left), ('Right', Right)),
            known_divergences={'operation': 'the old defaults intentionally differ'},
        ),
    )

    errors = collect_errors(cfg)

    assert len(errors) == 1, errors
    assert 'operation: registered divergence now agrees' in errors[0]
    assert 'drop the known_divergences entry' in errors[0]


def test_surface_discovery_does_not_invoke_custom_descriptors(tmp_path):
    accesses = []

    class SideEffectDescriptor:
        def __get__(self, instance, owner=None):
            accesses.append((instance, owner))
            raise AssertionError('descriptor access must remain passive')

    class Left:
        operation = SideEffectDescriptor()

    class Right:
        def operation(self, value: int, enabled: bool = True) -> None: ...

    cfg = make_config(
        tmp_path,
        surface=SurfaceConfig(targets=(('Left', Left), ('Right', Right))),
    )

    assert collect_errors(cfg) == []
    assert accesses == []


def test_module_surface_does_not_compare_class_constructors(tmp_path):
    left = ModuleType('left')
    left.PublicError = ValueError
    right = ModuleType('right')
    right.PublicError = RuntimeError
    cfg = make_config(
        tmp_path,
        surface=SurfaceConfig(targets=(('left', left), ('right', right))),
    )

    assert collect_errors(cfg) == []
