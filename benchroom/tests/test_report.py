import xml.etree.ElementTree as ET

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
    assert ET.parse(output).getroot().tag.endswith('svg')  # noqa: S314 - our own generated figure
    text = output.with_suffix('.md').read_text()
    assert '| short | A | — | 4 | 2 to 6 | 3 |' in text
    assert '| short | B | — | 8 | — | 1 |' in text


def test_single_run_has_no_uncertainty_interval():
    assert Series('encode', 'A', (3,)).interval is None


@pytest.mark.parametrize('values', [(), (float('nan'),), (float('inf'),)])
def test_invalid_samples_rejected(values):
    with pytest.raises(ValueError, match='finite measurements'):
        Series('encode', 'A', values)
