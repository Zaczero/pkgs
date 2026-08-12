"""pyo3stubs gates for lrucache_rs (one test per gate)."""

import pytest

gate_test = pytest.importorskip('pyo3stubs.testing').gate_test

test_pyo3stubs_gate = gate_test('tools/stubconfig.py')
