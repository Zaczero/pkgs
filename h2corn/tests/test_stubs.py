import sysconfig

import pytest

if sysconfig.get_config_var('Py_GIL_DISABLED'):

    @pytest.mark.skip(
        reason=(
            'pyo3stubs gates require the tree-sitter native parser, which has '
            'no free-threaded CPython build'
        )
    )
    def test_pyo3stubs_gate() -> None:
        pass

else:
    from pyo3stubs.testing import gate_test

    test_pyo3stubs_gate = gate_test('tools/stubconfig.py')
