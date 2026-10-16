"""Render pyperf timing files collected with benchmark.py.

From the totp-rs checkout, collect both implementations with ``benchmark.py
--impl pyotp -o pyotp.json`` and ``--impl totp-rs -o totp-rs.json``, using the
bench dependency group. Then call ``report`` with those two paths. Result
metadata supplies the implementation versions; file names label older results.
"""

from benchroom import Report
from benchroom.pyperf import load


def report(baseline: str, candidate: str, output: str = 'benchmark.svg') -> None:
    """Write a TOTP timing chart and table from two pyperf result files."""
    series = load(baseline)
    series += load(candidate)
    Report(
        'TOTP generation and verification',
        series,
        unit='seconds',
        caption='Fixed secret and timestamp; setup excluded.',
    ).save(output)
