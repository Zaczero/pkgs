"""The doc-structure rules, against both docstring dialects.

`_check` takes the prose and the object, so each rule is exercised directly
rather than through a fixture that would have to carry one violation per rule.
"""

from __future__ import annotations

import ast
from typing import cast

import pytest

from pyo3stubs.doc_structure import _check, _stub_parameters

NUMPYDOC = """\
Do the thing.

Parameters
----------
value : int
    The input.
scale : float, default 2.0
    How much.

Returns
-------
int
    The output.
"""

GOOGLE = """\
Do the thing.

Args:
    value (int): The input.
    scale (float): How much, defaults to 2.0.

Returns:
    int: The output.
"""


def sample(value: int, scale: float = 2.0) -> int:
    return int(value * scale)


@pytest.mark.parametrize(
    ('dialect', 'doc'),
    [pytest.param('numpydoc', NUMPYDOC), pytest.param('google', GOOGLE)],
)
def test_a_complete_docstring_passes_in_either_dialect(dialect, doc):
    assert _check('sample', sample, doc) == [], dialect


def test_an_undocumented_parameter_is_flagged():
    doc = NUMPYDOC.replace('scale : float, default 2.0\n    How much.\n', '')
    assert _check('sample', sample, doc) == [
        "sample: parameter 'scale' is not documented"
    ]


def test_a_documented_parameter_that_left_the_signature_is_flagged():
    doc = NUMPYDOC.replace('value : int', 'value : int\ngone : str')
    assert any(
        'gone' in error and 'not in the signature' in error
        for error in _check('sample', sample, doc)
    )


def test_a_missing_returns_section_is_flagged():
    doc = NUMPYDOC.split('Returns', maxsplit=1)[0]
    assert any('no Returns section' in error for error in _check('sample', sample, doc))


def test_a_documented_default_must_match_the_runtime():
    """Prose that states a default is a claim the runtime has to honour."""
    doc = NUMPYDOC.replace('default 2.0', 'default 3.0')
    assert _check('sample', sample, doc) == [
        'sample.scale: documented default 3.0 != runtime 2.0'
    ]


class _RuntimeBase:
    def inherited(self, value: int, flag: bool = True) -> int:
        return value if flag else -value


class _RuntimeChild(_RuntimeBase):
    pass


STUB_OVERRIDE = cast(
    'ast.FunctionDef',
    ast.parse(
        '''\
@pytest.overload
def inherited(self, value: int, flag: bool = False) -> int: ...
def inherited(self, value: int, flag: bool = False) -> int:
    """An inherited override.

    Parameters
    ----------
    value : int
        The value.
    flag : bool, default False
        Whether to keep the sign.

    Returns
    -------
    int
        The result.
    """
'''
    ).body[-1],
)


def test_stub_only_inherited_override_uses_final_stub_signature() -> None:
    assert _check(
        'Child.inherited',
        _RuntimeChild.inherited,
        ast.get_docstring(STUB_OVERRIDE) or '',
        parameters=_stub_parameters(STUB_OVERRIDE),
    ) == []


def test_stub_only_inherited_override_flags_stub_default_mismatch() -> None:
    doc = (ast.get_docstring(STUB_OVERRIDE) or '').replace('default False', 'default True')
    assert _check(
        'Child.inherited',
        _RuntimeChild.inherited,
        doc,
        parameters=_stub_parameters(STUB_OVERRIDE),
    ) == ['Child.inherited.flag: documented default True != runtime False']
