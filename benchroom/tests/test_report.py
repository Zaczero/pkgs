import re
import xml.etree.ElementTree as ET
from typing import Literal, cast

import pytest

from benchroom import Report, Series


def test_report_exports_chart_and_matching_table(tmp_path):
    report = Report(
        'Encode',
        (Series('short', 'A', (2, 4, 6)), Series('short', 'B', (8,))),
        'milliseconds',
    )
    output = tmp_path / 'compare.svg'
    report.save(output)
    assert ET.parse(output).getroot().tag.endswith('svg')  # ruff: ignore[suspicious-xml-element-tree-usage] - our own generated figure
    text = output.with_suffix('.md').read_text()
    assert '| short | A | — | 4 | 2 to 6 | 3 |' in text
    assert '| short | B | — | 8 | — | 1 |' in text


def test_single_run_has_no_uncertainty_interval():
    assert Series('encode', 'A', (3,)).interval is None


def test_report_exposes_run_summary():
    report = Report('Encode', (Series('short', 'A', (2, 4, 6)),), 'milliseconds')
    (result,) = report.results
    assert result.median == 4
    assert result.interval == (2, 6)
    assert result.runs == 3


@pytest.mark.parametrize(
    ('values', 'message'),
    [
        pytest.param((), 'values must not be empty', id='empty'),
        pytest.param(
            (float('nan'),),
            'values must contain only finite measurements, got nan',
            id='nan',
        ),
        pytest.param(
            (float('inf'),),
            'values must contain only finite measurements, got inf',
            id='positive-infinity',
        ),
    ],
)
def test_invalid_samples_name_the_parameter_and_value(values, message):
    with pytest.raises(ValueError, match=re.escape(message)) as caught:
        Series('encode', 'A', values)
    assert str(caught.value) == message


def test_empty_report_names_series():
    with pytest.raises(ValueError, match='series must not be empty'):
        Report('Encode', (), 'seconds')


def test_invalid_better_names_the_value():
    message = "better must be 'lower' or 'higher', got 'faster'"
    invalid_better = cast('Literal["lower", "higher"]', 'faster')
    with pytest.raises(ValueError, match=re.escape(message)) as caught:
        Report(
            'Encode',
            (Series('short', 'A', (2,)),),
            'seconds',
            better=invalid_better,
        )
    assert str(caught.value) == message


def test_duplicate_series_names_its_key():
    item = Series('short', 'A', (2,))
    message = (
        "series contains a duplicate (case, name, x) key, got ('short', 'A', None)"
    )
    with pytest.raises(ValueError, match=re.escape(message)) as caught:
        Report('Encode', (item, item), 'seconds')
    assert str(caught.value) == message


def test_invalid_chart_path_names_the_path(tmp_path):
    path = tmp_path / 'compare.pdf'
    report = Report('Encode', (Series('short', 'A', (2,)),), 'seconds')

    message = f"chart path must end in '.svg' or '.png', got {path}"
    with pytest.raises(ValueError, match=re.escape(message)) as caught:
        report.save(path)

    assert str(caught.value) == message
