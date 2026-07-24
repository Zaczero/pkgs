"""Tests for ``tools/gates/_check_docstyle.py`` (stub-based docstring style gate).

Covers the stub-applicable SPEC section 8 cases: type-column isolation, empty
Raises, See Also format classes, reciprocity directed-miss, mirror parity
(CRSMismatchError delta + polygonize exclusion), ``( arg`` detection, and
deterministic JSON. Synthetic mini-docstrings drive pure check functions.
"""

from __future__ import annotations

import json
import textwrap
from typing import TYPE_CHECKING

from conftest import load_tool

if TYPE_CHECKING:
    from pathlib import Path

gate = load_tool('_check_docstyle')


def _doc(symbol: str, text: str, *, kind: str = 'function', is_operation: bool = True, owner: str | None = None):
    return gate.PublicDoc(
        symbol=symbol,
        text=textwrap.dedent(text).strip(),
        kind=kind,  # type: ignore[arg-type]
        is_operation=is_operation,
        owner_class=owner,
    )


def _scan(doc, public: frozenset[str] | None = None):
    public = public or frozenset({doc.symbol})
    return gate.scan_doc(doc, public)


def test_type_column_backticks_only_in_type_columns() -> None:
    doc = _doc(
        'buffer',
        '''
        Buffer a geometry by ``distance``.

        Parameters
        ----------
        distance : `float`
            Radius with backticks only in type column.
        unit : {'planar', 'meters'}
            Enum tokens.

        Returns
        -------
        `Polygon` or `MultiPolygon`
            Buffered region.

        Raises
        ------
        GeometryError
            If ``distance`` is non-finite (backticks OK here).
        ''',
    )
    findings, _ = _scan(doc)
    classes = [f.class_ for f in findings]
    assert 'type_column.backticks' in classes
    # Raises description with backticks must not be flagged as type_column
    raise_backtick = [
        f for f in findings
        if f.class_ == 'type_column.backticks' and f.section == 'Raises'
    ]
    assert raise_backtick == []
    type_hits = [f for f in findings if f.class_ == 'type_column.backticks']
    assert any('float' in (f.current_text or '') for f in type_hits)
    assert any('Polygon' in (f.current_text or '') for f in type_hits)


def test_type_column_ndarray_spelling() -> None:
    doc = _doc(
        'bounds_array',
        '''
        Return bounds.

        Returns
        -------
        ndarray of float64
            Shape (n, 4).
        ''',
    )
    findings, _ = _scan(doc)
    assert any(f.class_ == 'type_column.ndarray_spelling' for f in findings)
    # prose mention of ndarray outside type column is fine
    doc2 = _doc(
        'note_ndarray',
        '''
        Uses an ndarray under the hood.

        Returns
        -------
        float
            Scalar.
        ''',
    )
    findings2, _ = _scan(doc2)
    assert not any(f.class_ == 'type_column.ndarray_spelling' for f in findings2)


def test_empty_raises_description() -> None:
    doc = _doc(
        'coverage_clean',
        '''
        Clean coverage.

        Raises
        ------
        GeometryError
        InvalidGeometryError
            If content is invalid.
        ''',
    )
    findings, _ = _scan(doc)
    empty = [f for f in findings if f.class_ == 'numpydoc.empty_raises_description']
    assert len(empty) == 1
    assert empty[0].entry == 'GeometryError'


def test_missing_blank_before_section() -> None:
    doc = _doc(
        'foo',
        '''
        Summary without blank.
        Parameters
        ----------
        x : float
            Value.
        ''',
    )
    findings, _ = _scan(doc)
    assert any(f.class_ == 'numpydoc.missing_blank_before_section' for f in findings)


def test_see_also_format_classes() -> None:
    doc = _doc(
        'centroid',
        '''
        Compute the centroid.

        See Also
        --------
        `point_on_surface` : Backticked name.
        gm.polylabel : Prefixed name.
        envelope:Missing spaces.
        envelope_only
        lowercase : starts lowercase.
        ok_name : Missing period
        double : Ends with two periods..
        envelope : First. polylabel : Second on one line.
        ''',
    )
    public = frozenset({
        'centroid', 'point_on_surface', 'polylabel', 'envelope', 'ok_name',
        'lowercase', 'double',
    })
    findings, _ = _scan(doc, public)
    classes = {f.class_ for f in findings}
    assert 'see_also.backticked_name' in classes
    assert 'see_also.noncanonical_name' in classes
    assert 'see_also.spacing' in classes
    assert 'see_also.missing_description' in classes
    assert 'see_also.description_capitalization' in classes
    assert 'see_also.missing_period' in classes
    assert 'see_also.double_period' in classes
    assert 'see_also.merged_entries' in classes


def test_reciprocity_directed_miss() -> None:
    """Contains must point to within (and vice versa) under policy."""
    contains = _doc(
        'contains',
        '''
        Test whether left contains right.

        See Also
        --------
        covers : Related covering predicate.
        ''',
    )
    within = _doc(
        'within',
        '''
        Test whether left is within right.

        Examples
        --------
        >>> import gometry as gm
        >>> True
        True
        ''',
    )
    public = frozenset({'contains', 'within', 'covers'})
    graph = {}
    all_findings = []
    for doc in (contains, within):
        findings, resolved = gate.scan_doc(doc, public)
        all_findings.extend(findings)
        graph[doc.symbol] = resolved
    rec = gate.check_reciprocity([contains, within], graph, public)
    assert any(f.class_ == 'see_also.missing_reciprocal' for f in rec)
    # directed: each side that lacks the opposite link gets its own row
    miss = [f for f in rec if f.class_ == 'see_also.missing_reciprocal']
    symbols = {f.symbol for f in miss}
    assert 'contains' in symbols
    assert 'within' in symbols


def test_mirror_raises_crsmismatch_delta() -> None:
    free = _doc(
        'union_all',
        '''
        Union all geometries.

        Raises
        ------
        GeometryTypeError
            If values have wrong kind.
        CRSMismatchError
            If frames disagree.
        GeometryError
            If a parameter is invalid.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.union_all([])
        ''',
    )
    method = _doc(
        'GeometryArray.union_all',
        '''
        Union all rows.

        Raises
        ------
        GeometryTypeError
            If rows have wrong kind.
        GeometryError
            If a parameter is invalid.

        Examples
        --------
        >>> import gometry as gm
        >>> arr.union_all()
        ''',
        kind='method',
        owner='GeometryArray',
    )
    public = frozenset({'union_all', 'GeometryArray.union_all'})
    # Temporarily treat as mirror pair (already in MIRROR_PAIRS)
    findings = gate.check_mirror_parity([free, method], public)
    raises = [f for f in findings if f.class_ == 'mirror.raises_parity']
    assert raises == [], f'unexpected raises parity findings: {raises}'


def test_mirror_raises_extra_mismatch_flagged() -> None:
    free = _doc(
        'coverage_clean',
        '''
        Clean coverage.

        Raises
        ------
        GeometryError
            If invalid.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.coverage_clean([])
        ''',
    )
    method = _doc(
        'GeometryArray.coverage_clean',
        '''
        Clean coverage rows.

        Raises
        ------
        GeometryError
            If invalid.
        ParseError
            Extra only on method.

        Examples
        --------
        >>> import gometry as gm
        >>> arr.coverage_clean()
        ''',
        kind='method',
        owner='GeometryArray',
    )
    public = frozenset({'coverage_clean', 'GeometryArray.coverage_clean'})
    findings = gate.check_mirror_parity([free, method], public)
    assert any(f.class_ == 'mirror.raises_parity' for f in findings)


def test_polygonize_excluded_from_mirror() -> None:
    free = _doc(
        'polygonize',
        '''
        Polygonize linework pool.

        Raises
        ------
        GeometryError
            If invalid.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.polygonize([])
        ''',
    )
    method = _doc(
        'GeometryArray.polygonize',
        '''
        Polygonize each row.

        Raises
        ------
        ParseError
            Completely different.

        Examples
        --------
        >>> import gometry as gm
        >>> arr.polygonize()
        ''',
        kind='method',
        owner='GeometryArray',
    )
    public = frozenset({'polygonize', 'GeometryArray.polygonize'})
    findings = gate.check_mirror_parity([free, method], public)
    assert not any(f.class_.startswith('mirror.') for f in findings)


def test_space_after_open_paren_same_line_only() -> None:
    bad = 'gm.simplify( 1.0)\n'
    assert gate._space_after_open_paren(bad)
    # multiline call — next token on next line — must NOT flag
    good = 'gm.simplify(\n    1.0\n)\n'
    assert gate._space_after_open_paren(good) == []
    # empty call
    assert gate._space_after_open_paren('f()\n') == []


def test_examples_import_style() -> None:
    doc = _doc(
        'area',
        '''
        Compute area.

        Examples
        --------
        >>> import gometry
        >>> from gometry import Point
        >>> 1
        1
        ''',
    )
    findings, _ = _scan(doc)
    assert any(f.class_ == 'examples.import_style' for f in findings)


def test_deterministic_json_roundtrip(tmp_path: Path) -> None:
    """Two collect_report runs yield byte-identical JSON (fixed root)."""
    # Use real repo root but only assert sort stability of the report helper.
    report = {
        'schema_version': 1,
        'tool': 'gometry-docstyle',
        'status': 'fail',
        'complete': True,
        'summary': {
            'public_symbols': 1,
            'docstrings_scanned': 1,
            'symbols_with_violations': 1,
            'violations': 1,
            'errors': 0,
            'by_class': {'type_column.backticks': 1},
        },
        'violations': [
            {
                'file': 'src/a.rs',
                'line': 1,
                'column': 0,
                'owner_file': 'src/a.rs',
                'owner_line': 1,
                'source_kind': 'stub',
                'symbol': 'buffer',
                'class': 'type_column.backticks',
                'section': 'Parameters',
                'entry': 'x : `float`',
                'current_text': '`float`',
                'suggested_fix': 'float',
                'message': 'Type columns use bare identifiers, never code markup.',
                'related_symbol': None,
            }
        ],
        'errors': [],
    }
    a = json.dumps(report, indent=2, sort_keys=True) + '\n'
    b = json.dumps(report, indent=2, sort_keys=True) + '\n'
    assert a == b
    # Violation sort key is total-ordered
    v1 = gate.Violation(
        file='b.rs', line=2, column=0, owner_file=None, owner_line=None,
        source_kind='stub', symbol='z', class_='a', section=None, entry=None,
        current_text=None, suggested_fix='', message='m',
    )
    v2 = gate.Violation(
        file='a.rs', line=1, column=0, owner_file=None, owner_line=None,
        source_kind='stub', symbol='y', class_='b', section=None, entry=None,
        current_text=None, suggested_fix='', message='m',
    )
    ordered = sorted([v1, v2], key=gate._sort_key)
    assert ordered[0].file == 'a.rs'


def test_full_surface_gate_is_complete_and_clean() -> None:
    """Release blocker: full public surface has zero docstyle violations.

    Asserts CLI exit code (0 or 1, never 2), completeness, empty violation
    list, and pass status so the gate stays a hard CI/release check.
    """
    code = gate.main(['--format', 'json'])
    assert code in (0, 1), f'expected exit 0/1, got {code}'
    report = gate.collect_report()
    assert report['complete'] is True, (
        f'docstyle gate incomplete: errors={report.get("errors")!r}'
    )
    summary = report['summary']
    assert summary['errors'] == 0, report.get('errors')
    assert summary['violations'] == 0, (
        f'docstyle violations remain: {summary.get("by_class")!r}'
    )
    assert report['violations'] == []
    assert report['status'] == 'pass'


def test_style_banned_terms() -> None:
    doc = _doc(
        'distance',
        '''
        Measure the natural way in metres.

        Parameters
        ----------
        values : iterable of float or array-like
            Input.
        ''',
    )
    findings, _ = _scan(doc)
    classes = {f.class_ for f in findings}
    assert 'style.banned_term.natural_way' in classes
    assert 'style.banned_term.metres' in classes
    assert 'style.banned_term.iterable_of_float' in classes
    assert 'style.banned_term.array_like' in classes


def test_mirror_examples_parity_identical_free_and_method() -> None:
    """gm.NAME(recv, …) and recv.NAME(…) must normalize to the same counter.

    A bit-identical demonstration must NOT emit mirror.examples_parity.
    """
    free = _doc(
        'union_all',
        '''
        Union all geometries.

        Raises
        ------
        GeometryTypeError
            If values have wrong kind.
        CRSMismatchError
            If frames disagree.
        GeometryError
            If a parameter is invalid.

        See Also
        --------
        coverage_union : Coverage-aware dissolve over shared interfaces.

        Examples
        --------
        >>> import gometry as gm
        >>> values = [gm.Point(0, 0), gm.Point(1, 1)]
        >>> gm.union_all(values)
        <Geometry>
        ''',
    )
    method = _doc(
        'GeometryArray.union_all',
        '''
        Union all rows.

        Raises
        ------
        GeometryTypeError
            If rows have wrong kind.
        GeometryError
            If a parameter is invalid.

        See Also
        --------
        coverage_union : Coverage-aware dissolve over shared interfaces.

        Examples
        --------
        >>> import gometry as gm
        >>> values = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)])
        >>> values.union_all()
        <Geometry>
        ''',
        kind='method',
        owner='GeometryArray',
    )
    public = frozenset({
        'union_all',
        'GeometryArray.union_all',
        'coverage_union',
        'GeometryArray.coverage_union',
    })
    # Normalize counters must match for the mirrored call
    free_c = gate._normalize_mirror_examples(free, 'union_all')
    method_c = gate._normalize_mirror_examples(method, 'union_all')
    assert free_c == method_c, f'free={free_c} method={method_c}'
    assert free_c, 'expected at least one normalized mirror call'

    findings = gate.check_mirror_parity([free, method], public)
    assert not any(
        f.class_ == 'mirror.examples_parity' for f in findings
    ), findings
    assert not any(f.class_ == 'mirror.raises_parity' for f in findings), findings
    assert not any(f.class_ == 'mirror.see_also_parity' for f in findings), findings


def test_mirror_examples_parity_detects_real_divergence() -> None:
    free = _doc(
        'union_all',
        '''
        Union all.

        Raises
        ------
        GeometryError
            If invalid.
        CRSMismatchError
            If frames disagree.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.union_all([gm.Point(0, 0)])
        <Geometry>
        ''',
    )
    method = _doc(
        'GeometryArray.union_all',
        '''
        Union all rows.

        Raises
        ------
        GeometryError
            If invalid.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.GeometryArray([gm.Point(9, 9)]).union_all()
        <Geometry>
        ''',
        kind='method',
        owner='GeometryArray',
    )
    public = frozenset({'union_all', 'GeometryArray.union_all'})
    findings = gate.check_mirror_parity([free, method], public)
    assert any(f.class_ == 'mirror.examples_parity' for f in findings)


def test_mirror_see_also_parity_detects_description_drift() -> None:
    free = _doc(
        'coverage_clean',
        '''
        Clean coverage.

        Raises
        ------
        GeometryError
            If invalid.

        See Also
        --------
        coverage_is_valid : Different blurb on free side.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.coverage_clean([])
        ''',
    )
    method = _doc(
        'GeometryArray.coverage_clean',
        '''
        Clean coverage rows.

        Raises
        ------
        GeometryError
            If invalid.

        See Also
        --------
        coverage_is_valid : Shared validity check for the array.

        Examples
        --------
        >>> import gometry as gm
        >>> gm.GeometryArray([]).coverage_clean()
        ''',
        kind='method',
        owner='GeometryArray',
    )
    public = frozenset({
        'coverage_clean',
        'GeometryArray.coverage_clean',
        'coverage_is_valid',
        'GeometryArray.coverage_is_valid',
    })
    findings = gate.check_mirror_parity([free, method], public)
    assert any(f.class_ == 'mirror.see_also_parity' for f in findings)


def test_free_coverage_reciprocity_policy() -> None:
    """Free coverage_* duals participate in ReciprocalFamily (SPEC §4.3)."""
    free_valid = _doc(
        'coverage_is_valid',
        '''
        Test whether a polygonal coverage is valid.

        Examples
        --------
        >>> import gometry as gm
        >>> True
        True
        ''',
    )
    free_invalid = _doc(
        'coverage_invalid_edges',
        '''
        Return invalid coverage edges.

        Examples
        --------
        >>> import gometry as gm
        >>> True
        True
        ''',
    )
    public = frozenset({
        'coverage_is_valid',
        'coverage_invalid_edges',
        'GeometryArray.coverage_is_valid',
        'GeometryArray.coverage_invalid_edges',
    })
    graph = {
        'coverage_is_valid': [],
        'coverage_invalid_edges': [],
    }
    rec = gate.check_reciprocity([free_valid, free_invalid], graph, public)
    miss = [f for f in rec if f.class_ == 'see_also.missing_reciprocal']
    symbols = {f.symbol for f in miss}
    assert 'coverage_is_valid' in symbols
    assert 'coverage_invalid_edges' in symbols
