"""Revert-sensitive regressions for unbounded Python iterable/mapping drains.

Covers D15 (lying-key mapping), D11 (known-cardinality coordinate/broadcast
iterables), D10 (untrusted iterable materialization), D12 (Features side-data),
D13 (from_geoparquet columns=), D14/D23 (GeoParquet/viz attributes Mapping).
Each negative test must TERMINATE under a hard wall-clock bound — hang
(timeout rc 124) is a failure. Honest finite inputs remain valid.
"""

from __future__ import annotations

import collections
import collections.abc
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

import gometry as gm
import pytest


def _run_child(
    script: str, *, timeout: float = 8.0
) -> subprocess.CompletedProcess[str]:
    """Run *script* in a fresh interpreter under ``timeout`` wall seconds."""
    return subprocess.run(
        [sys.executable, '-c', textwrap.dedent(script)],
        capture_output=True,
        check=False,
        text=True,
        timeout=timeout,
    )


def _assert_terminates(
    script: str, *, timeout: float = 8.0, allow_sigabrt: bool = False
) -> subprocess.CompletedProcess[str]:
    """Assert the child finishes (raises or returns) inside *timeout*.

    ``subprocess.run(..., timeout=)`` raises ``TimeoutExpired`` on hang — that
    is the gate. Exit code may be non-zero (exception); only hang fails.
    """
    try:
        completed = _run_child(script, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        pytest.fail(
            f'child hung past {timeout}s (rc would be 124 under GNU timeout):\n'
            f'stdout={exc.stdout!r}\nstderr={exc.stderr!r}'
        )
    if not allow_sigabrt:
        # Never accept a process abort (SIGABRT = 134 on Linux).
        assert completed.returncode != -6, f'SIGABRT:\n{completed.stderr}'
        assert completed.returncode != 134, f'abort exit 134:\n{completed.stderr}'
    return completed


# ---------------------------------------------------------------------------
# D15 — lying mapping key stream must not hang mapping_as_dict consumers
# ---------------------------------------------------------------------------


_LYING_MAPPING = """\
import collections.abc, itertools
import gometry as gm

class M(collections.abc.Mapping):
    def __len__(self):
        return 1
    def __iter__(self):
        return itertools.repeat("type")
    def __getitem__(self, k):
        return "Point"
"""


@pytest.mark.parametrize(
    'call',
    [
        'gm.from_geojson(M())',
        'gm.GeometryArray([M()])',
        'gm.require(M())',
    ],
)
def test_d15_lying_mapping_terminates(call: str) -> None:
    script = (
        _LYING_MAPPING
        + f'\ntry:\n    {call}\nexcept Exception as e:\n    print(type(e).__name__, e)\n'
    )
    completed = _assert_terminates(script)
    # Must raise (lying mapping is not valid GeoJSON geometry content).
    assert completed.returncode != 0 or 'Error' in completed.stdout
    # Prefer the lying-length message when the boundary sees it first.
    blob = completed.stdout + completed.stderr
    assert 'PanicException' not in blob
    assert 'mapping reports length' in blob or 'Error' in blob


def test_d15_honest_mapping_from_geojson() -> None:
    g = gm.from_geojson({'type': 'Point', 'coordinates': [1.0, 2.0]})
    assert g.x == 1.0 and g.y == 2.0


def test_d15_honest_userdict_properties() -> None:
    props = collections.UserDict({'name': 'a'})
    feat = {
        'type': 'Feature',
        'geometry': {'type': 'Point', 'coordinates': [0.0, 1.0]},
        'properties': props,
    }
    features = gm.from_features([feat])
    assert len(features.geometries) == 1
    assert features.properties[0]['name'] == 'a'


# ---------------------------------------------------------------------------
# N4 — mapping protocol: keys()-only accept; repeated-key reject immediately
# ---------------------------------------------------------------------------


_KEYS_ONLY_POINT = """\
class K:
    def keys(self):
        return ("type", "coordinates")
    def __getitem__(self, key):
        return {"type": "Point", "coordinates": [1.0, 2.0]}[key]
"""

_REPEAT_KEY_MAPPING = """\
import collections.abc, itertools

class M(collections.abc.Mapping):
    def __len__(self):
        raise TypeError("unknown")
    def __iter__(self):
        return itertools.repeat("type")
    def __getitem__(self, key):
        return "Point"
"""


def test_n4_keys_only_mapping_from_geojson() -> None:
    """keys()-only duck that dict() accepts must parse as POINT (1 2)."""
    script = (
        _KEYS_ONLY_POINT
        + """
import gometry as gm
g = gm.from_geojson(K())
print(g.to_wkt())
assert g.x == 1.0 and g.y == 2.0
"""
    )
    completed = _assert_terminates(script)
    assert completed.returncode == 0, completed.stderr
    assert 'POINT (1 2)' in completed.stdout


def test_n4_repeated_key_mapping_rejects_immediately() -> None:
    """Infinite repeated-key stream must raise typed error under timeout 8."""
    script = (
        _REPEAT_KEY_MAPPING
        + """
import gometry as gm
try:
    gm.from_geojson(M())
except Exception as e:
    print(type(e).__name__, e)
    raise
"""
    )
    completed = _assert_terminates(script, timeout=8.0)
    assert completed.returncode != 0
    blob = completed.stdout + completed.stderr
    assert 'PanicException' not in blob
    assert 'duplicate key' in blob
    assert 'GeometryError' in blob


@pytest.mark.parametrize(
    'call',
    [
        'gm.from_geojson(M())',
        'gm.GeometryArray([M()])',
        'gm.to_feature(gm.Point(0, 0), properties=M())',
        "gm.from_features([{'type': 'Feature', 'geometry': {'type': 'Point', 'coordinates': [0, 0]}, 'properties': M()}])",
        'gm.Features(gm.GeometryArray([gm.Point(0, 0)]), properties=M())',
    ],
)
def test_n4_repeated_key_rejects_across_sites(call: str) -> None:
    script = (
        _REPEAT_KEY_MAPPING
        + f"""
import gometry as gm
try:
    {call}
except Exception as e:
    print(type(e).__name__, e)
"""
    )
    completed = _assert_terminates(script, timeout=8.0)
    blob = completed.stdout + completed.stderr
    assert 'PanicException' not in blob
    assert 'duplicate key' in blob
    assert 'GeometryError' in blob


def test_n4_keys_only_properties_to_feature_and_features() -> None:
    """keys()-only properties mapping accepted by to_feature / Features."""

    class Props:
        def keys(self):
            return ('name',)

        def __getitem__(self, key):
            return {'name': 'ok'}[key]

    feat = gm.to_feature(gm.Point(0, 0), properties=Props())
    assert feat['properties']['name'] == 'ok'
    features = gm.Features(gm.GeometryArray([gm.Point(0, 0)]), properties=Props())
    assert features.properties[0]['name'] == 'ok'

    # from_features feature body with keys()-only properties
    class Feat:
        def keys(self):
            return ('type', 'geometry', 'properties')

        def __getitem__(self, key):
            return {
                'type': 'Feature',
                'geometry': {'type': 'Point', 'coordinates': [3.0, 4.0]},
                'properties': {'k': 1},
            }[key]

    parsed = gm.from_features([Feat()])
    assert parsed.geometries[0].to_wkt() == 'POINT (3 4)'
    assert parsed.properties[0]['k'] == 1


def test_n4_honest_dict_and_userdict_unchanged() -> None:
    g = gm.from_geojson({'type': 'Point', 'coordinates': [1.0, 2.0]})
    assert g.to_wkt() == 'POINT (1 2)'
    props = collections.UserDict({'name': 'a'})
    feat = gm.to_feature(gm.Point(0, 1), properties=props)
    assert feat['properties']['name'] == 'a'
    from types import MappingProxyType

    proxy = MappingProxyType({'type': 'Point', 'coordinates': [5.0, 6.0]})
    assert gm.from_geojson(proxy).to_wkt() == 'POINT (5 6)'


# ---------------------------------------------------------------------------
# F3 — nested non-dict Mapping must convert (not over-reject as unsupported)
# ---------------------------------------------------------------------------


def test_f3_nested_userdict_geometry_from_geojson() -> None:
    """Nested UserDict geometry in a Feature converts (top-level already did)."""
    g = gm.from_geojson({
        'type': 'Feature',
        'geometry': collections.UserDict({'type': 'Point', 'coordinates': [1.0, 2.0]}),
        'properties': {},
    })
    assert g.x == 1.0 and g.y == 2.0


def test_f3_nested_mapping_proxy_and_keys_duck() -> None:
    from types import MappingProxyType

    class Geom:
        def keys(self):
            return ('type', 'coordinates')

        def __getitem__(self, key):
            return {'type': 'Point', 'coordinates': [7.0, 8.0]}[key]

    proxy_geom = MappingProxyType({'type': 'Point', 'coordinates': [3.0, 4.0]})
    assert (
        gm.from_geojson({
            'type': 'Feature',
            'geometry': proxy_geom,
            'properties': {},
        }).to_wkt()
        == 'POINT (3 4)'
    )
    assert (
        gm.from_geojson({
            'type': 'Feature',
            'geometry': Geom(),
            'properties': {},
        }).to_wkt()
        == 'POINT (7 8)'
    )
    # GeometryArray / require / from_features nested geometry
    arr = gm.GeometryArray([
        collections.UserDict({'type': 'Point', 'coordinates': [9.0, 10.0]})
    ])
    assert arr[0].to_wkt() == 'POINT (9 10)'
    assert (
        gm.require(
            collections.UserDict({'type': 'Point', 'coordinates': [11.0, 12.0]})
        ).to_wkt()
        == 'POINT (11 12)'
    )
    feats = gm.from_features([
        {
            'type': 'Feature',
            'geometry': collections.UserDict({
                'type': 'Point',
                'coordinates': [13.0, 14.0],
            }),
            'properties': {'k': 1},
        }
    ])
    assert feats.geometries[0].to_wkt() == 'POINT (13 14)'


# ---------------------------------------------------------------------------
# F1 — to_feature_collection must retain one-shot properties (no silent {})
# ---------------------------------------------------------------------------


class _OnceMap:
    """keys() returns a one-shot iterator; re-enumeration yields nothing."""

    def __init__(self, data: dict):
        self.data = data
        self._k = iter(data)

    def keys(self):
        return self._k

    def __getitem__(self, k):
        return self.data[k]


def test_f1_to_feature_collection_preserves_once_map_properties() -> None:
    fc = gm.to_feature_collection(
        [gm.Point(0, 0), gm.Point(1, 1)],
        properties=_OnceMap({'name': 'ok'}),
    )
    props = [f['properties'] for f in fc['features']]
    assert props == [{'name': 'ok'}, {'name': 'ok'}]


def test_f1_from_features_accepts_keys_only_mapping() -> None:
    """Top-level keys()-only Feature (not only abc.Mapping) is accepted."""

    class Feat:
        def keys(self):
            return ('type', 'geometry', 'properties')

        def __getitem__(self, key):
            return {
                'type': 'Feature',
                'geometry': {'type': 'Point', 'coordinates': [2.0, 3.0]},
                'properties': {'name': 'ok'},
            }[key]

    parsed = gm.from_features(Feat())
    assert parsed.geometries[0].to_wkt() == 'POINT (2 3)'
    assert parsed.properties[0]['name'] == 'ok'


# ---------------------------------------------------------------------------
# F2 — Python twin: legacy getitem keys + advisory __len__
# ---------------------------------------------------------------------------


def test_f2_mapping_duck_legacy_getitem_keys_features() -> None:
    class Keys:
        def __getitem__(self, i):
            if i == 0:
                return 'name'
            raise IndexError

    class MappingDuck:
        def keys(self):
            return Keys()

        def __getitem__(self, k):
            return {'name': 'ok'}[k]

    # Builtin dict() accepts this; Features must too.
    assert dict(MappingDuck()) == {'name': 'ok'}
    features = gm.Features(gm.GeometryArray([gm.Point(0, 0)]), properties=MappingDuck())
    assert features.properties[0]['name'] == 'ok'


def test_f2_lying_short_len_remains_advisory() -> None:
    class ShortLen:
        def __len__(self):
            return 1

        def keys(self):
            return ('a', 'b', 'c')

        def __getitem__(self, k):
            return k

    features = gm.Features(gm.GeometryArray([gm.Point(0, 0)]), properties=ShortLen())
    assert features.properties[0] == {'a': 'a', 'b': 'b', 'c': 'c'}


def test_n4_geoparquet_and_viz_repeated_key_attributes() -> None:
    script = (
        _REPEAT_KEY_MAPPING
        + """
import tempfile
from pathlib import Path
import gometry as gm
from gometry._viz import _attribute_table
from gometry._geoparquet import _normalize_attribute_columns
a = gm.GeometryArray([gm.Point(0, 0, crs=4326)])
errs = []
try:
    _attribute_table(M(), 1)
except Exception as e:
    errs.append(("viz", type(e).__name__, str(e)))
try:
    _normalize_attribute_columns(M(), 1)
except Exception as e:
    errs.append(("geoparquet", type(e).__name__, str(e)))
for tag, name, msg in errs:
    print(tag, name, msg)
assert all("duplicate key" in msg for _, _, msg in errs)
assert all(name == "GeometryError" for _, name, _ in errs)
"""
    )
    completed = _assert_terminates(script, timeout=8.0)
    assert completed.returncode == 0, completed.stdout + completed.stderr


# ---------------------------------------------------------------------------
# D11 — known-cardinality float/int iterables must not hang
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    'call',
    [
        'gm.crs_transform(4326, 4326, itertools.repeat(0.0), [0.0])',
        'gm.Point(0, 0).set_coordinates(x=itertools.repeat(1.0), y=[2.0])',
        'gm.points(itertools.repeat(0.0), [0.0])',
        'gm.h3_cells([0.0], [0.0], resolution=itertools.repeat(1))',
        'gm.GeometryArray([gm.Point(0, 0)]).buffer(itertools.repeat(1.0))',
    ],
)
def test_d11_known_cardinality_repeat_terminates(call: str) -> None:
    script = f"""\
import itertools
import gometry as gm
try:
    {call}
except Exception as e:
    print(type(e).__name__, e)
"""
    completed = _assert_terminates(script)
    assert completed.returncode != 0 or 'Error' in completed.stdout
    blob = completed.stdout + completed.stderr
    assert 'PanicException' not in blob
    assert 'length' in blob or 'Error' in blob


def test_d11_finite_mismatch_message_stable() -> None:
    with pytest.raises(gm.InvalidGeometryError, match='same length'):
        gm.points([1, 2], [1, 2, 3])


def test_d11_honest_points_and_buffer() -> None:
    assert gm.points([1.0, 2.0], [3.0, 4.0]).to_wkt() == [
        'POINT (1 3)',
        'POINT (2 4)',
    ]
    arr = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)])
    out = arr.buffer(1.0)
    assert len(out) == 2


# ---------------------------------------------------------------------------
# D10 / N3 — untrusted iterable materialization: fallible only, no hard cap
# ---------------------------------------------------------------------------


def _d10_rlimit_script(body: str, *, as_bytes: int = 256 * 1024 * 1024) -> str:
    """Child script: tight RLIMIT_AS so infinite collect → catchable MemoryError.

    N3 removed the artificial ``MAX_BARE_COLLECT`` ceiling. Valid large finite
    generators succeed; infinite streams rely on proportional fallible growth.
    Under Linux overcommit that only cleanly surfaces with a process rlimit.
    """
    indented = textwrap.indent(textwrap.dedent(body).strip(), '    ')
    return f"""\
import itertools
import resource
import sys
import gometry as gm

# Pre-load NumPy before the address-space cap. gometry imports NumPy at package
# import time (core dependency), but under RLIMIT_AS a first NumPy/OpenBLAS init
# can still abort with a non-MemoryError message and miss the fallible-collection
# path under test if init is deferred past the cap.
import numpy  # noqa: F401

# This disposable child never needs to raise its address-space ceiling again.
_, hard = resource.getrlimit(resource.RLIMIT_AS)
limit = {as_bytes} if hard == resource.RLIM_INFINITY else min({as_bytes}, hard)
resource.setrlimit(resource.RLIMIT_AS, (limit, limit))

try:
{indented}
    print("ok-no-raise")
    raise SystemExit(2)
except MemoryError as exc:
    print("MemoryError", exc)
    raise SystemExit(0)
except BaseException as exc:
    name = type(exc).__name__
    print(name, exc)
    # SIGSEGV / OOM-killer are not clean; PanicException is a hard fail.
    raise SystemExit(1 if name == "PanicException" else 0)
"""


_POLYGON_HOLES_REPEAT = (
    'gm.Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)], '
    'holes=itertools.repeat([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]))'
)


@pytest.mark.parametrize(
    ('body', 'inner_arc_oom'),
    [
        (
            'gm.from_features(itertools.repeat({"type": "Feature", "geometry": {"type": "Point", "coordinates": [0, 0]}, "properties": {}}))',
            False,
        ),
        ('gm.GeometryCollection(itertools.repeat(gm.Point(0, 0)))', False),
        ('gm.line_strings(itertools.repeat([(0, 0), (1, 1)]))', False),
        pytest.param(_POLYGON_HOLES_REPEAT, True, id='polygon-holes-inner-arc-oom'),
        ('gm.crs_transform_bounds(4326, 3857, itertools.repeat((0, 0, 1, 1)))', False),
        ('gm.h3_union(itertools.repeat(gm.H3Cell(0.0, 0.0, resolution=5)), [])', False),
    ],
)
def test_d10_infinite_iterable_memoryerror_not_abort(
    body: str, inner_arc_oom: bool
) -> None:
    """Under RLIMIT_AS: infinite bare stream → MemoryError (not hang/SIGABRT)."""
    completed = _assert_terminates(
        _d10_rlimit_script(body), timeout=30.0, allow_sigabrt=inner_arc_oom
    )
    if inner_arc_oom:
        # The dedicated non-strict xfail below owns this one known inner
        # allocation failure. This parameter remains the strict no-hang check.
        return
    assert completed.returncode in (0, 1), (
        f'unexpected rc {completed.returncode}: {completed.stdout!r} {completed.stderr!r}'
    )
    blob = completed.stdout + completed.stderr
    assert 'PanicException' not in blob
    assert completed.returncode != 134
    assert completed.returncode != -6  # SIGABRT
    assert 'MemoryError' in blob


def test_d10_polygon_holes_inner_arc_oom_memoryerror() -> None:
    """Only the documented allocator abort remains an expected limitation."""
    completed = _assert_terminates(
        _d10_rlimit_script(_POLYGON_HOLES_REPEAT), timeout=30.0, allow_sigabrt=True
    )
    blob = completed.stdout + completed.stderr
    if (
        completed.returncode in (-6, 134)
        and 'memory allocation of 56 bytes failed' in blob
    ):
        pytest.xfail('documented proportional OOM in Polygon hole Arc<[f64]> ingress')
    assert completed.returncode in (0, 1), (
        f'unexpected rc {completed.returncode}: {completed.stdout!r} {completed.stderr!r}'
    )
    assert 'PanicException' not in blob
    assert 'MemoryError' in blob


@pytest.mark.parametrize(
    'body',
    [
        """
cell = gm.H3Cell(0.0, 0.0, resolution=5)
vid = int(cell.vertices[0])
gm.H3VertexArray(itertools.repeat(vid))
""",
        """
cell = gm.H3Cell(0.0, 0.0, resolution=5)
eid = int(cell.edges[0])
gm.H3EdgeArray(itertools.repeat(eid))
""",
    ],
)
def test_d10_h3_vertex_edge_array_repeat_terminates(body: str) -> None:
    """Sister path: H3VertexArray/H3EdgeArray id collect (was SIGABRT)."""
    completed = _assert_terminates(_d10_rlimit_script(body), timeout=30.0)
    blob = completed.stdout + completed.stderr
    assert 'PanicException' not in blob
    assert completed.returncode != 134
    assert completed.returncode != -6
    assert 'MemoryError' in blob


def test_d10_honest_collection_and_features() -> None:
    assert gm.GeometryCollection([gm.Point(0, 0)]).to_wkt() == (
        'GEOMETRYCOLLECTION (POINT (0 0))'
    )
    feats = gm.from_features([
        {
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [1.0, 2.0]},
            'properties': {},
        }
    ])
    assert len(feats.geometries) == 1
    assert feats.geometries[0].to_wkt() == 'POINT (1 2)'
    # Length-reporting multi-million-scale lists still work.
    n = 10_000
    arr = gm.H3VertexArray([int(gm.H3Cell(0.0, 0.0, resolution=5).vertices[0])] * n)
    assert len(arr) == n


def test_n3_crs_configure_generator_2_20_matches_list() -> None:
    """N3: bare generator of 2^20 paths succeeds like the same-size list.

    Former MAX_BARE_COLLECT rejected generators at 1 Mi while lists of the
    same length succeeded — the list-vs-generator asymmetry that marks a
    hard-cap violation of the no-artificial-cap binding rule.
    """
    n = 1 << 20
    try:
        from_list = gm.crs_configure(search_paths=['/tmp'] * n)  # noqa: S108  # intentional stress path
        assert from_list['search_paths'] is not None
        assert len(from_list['search_paths']) == n

        from_gen = gm.crs_configure(search_paths=('/tmp' for _ in range(n)))  # noqa: S108  # intentional stress path
        assert from_gen['search_paths'] is not None
        assert len(from_gen['search_paths']) == n
        assert from_gen['search_paths'] == from_list['search_paths']
    finally:
        gm.crs_reset()


def test_n3_infinite_crs_configure_memoryerror_under_rlimit() -> None:
    """N3: infinite search_paths generator → catchable MemoryError under rlimit."""
    completed = _assert_terminates(
        _d10_rlimit_script(
            "gm.crs_configure(search_paths=itertools.repeat('/tmp'))",
            as_bytes=128 * 1024 * 1024,
        ),
        timeout=30.0,
    )
    blob = completed.stdout + completed.stderr
    assert 'PanicException' not in blob
    assert completed.returncode != 134
    assert completed.returncode != -6
    assert 'MemoryError' in blob


# ---------------------------------------------------------------------------
# D12 — Features properties/ids bounded by known geometry row count
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    'call',
    [
        'gm.Features(rows, ids=itertools.repeat(None))',
        'gm.Features(rows, properties=itertools.repeat(None))',
    ],
)
def test_d12_features_infinite_side_data_terminates(call: str) -> None:
    script = f"""\
import itertools
import gometry as gm
rows = gm.GeometryArray([gm.Point(0, 0)])
try:
    {call}
except Exception as e:
    print(type(e).__name__, e)
"""
    completed = _assert_terminates(script)
    blob = completed.stdout + completed.stderr
    assert 'PanicException' not in blob
    assert 'ValueError' in blob
    assert 'length' in blob


def test_d12_features_honest_aligned_side_data() -> None:
    rows = gm.GeometryArray([gm.Point(0, 0)])
    features = gm.Features(rows, properties=[{'a': 1}], ids=[7])
    assert features.properties == ({'a': 1},)
    assert features.ids == (7,)
    # One-shot iterators of correct length still work (consumed once).
    features2 = gm.Features(
        rows,
        properties=iter([{'b': 2}]),
        ids=iter([9]),
    )
    assert features2.properties == ({'b': 2},)
    assert features2.ids == (9,)


# ---------------------------------------------------------------------------
# D23 + D14 — attributes Mapping accept non-dict; bound infinite columns
# ---------------------------------------------------------------------------


def test_d23_to_geoparquet_userdict_attributes_succeeds() -> None:
    attrs = collections.UserDict({'v': [1]})
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / 'userdict.parquet')
        gm.GeometryArray([gm.Point(0, 0, crs=4326)]).to_geoparquet(
            path, attributes=attrs
        )
        assert Path(path).is_file()
        restored, table = gm.from_geoparquet(path)
        assert len(restored) == 1
        assert table.to_pydict() == {'v': [1]}


def test_d14_to_geoparquet_infinite_attribute_column_terminates() -> None:
    script = """\
import itertools, tempfile
from pathlib import Path
import gometry as gm
path = str(Path(tempfile.mkdtemp()) / "hang.parquet")
a = gm.GeometryArray([gm.Point(0, 0, crs=4326)])
try:
    a.to_geoparquet(path, attributes={"x": itertools.repeat(1)})
except Exception as e:
    print(type(e).__name__, e)
"""
    completed = _assert_terminates(script)
    blob = completed.stdout + completed.stderr
    assert 'PanicException' not in blob
    assert 'GeometryError' in blob or 'length' in blob
    assert 'does not match geometry length' in blob


def test_d14_viz_attribute_table_infinite_column_terminates() -> None:
    script = """\
import itertools
from gometry._viz import _attribute_table
try:
    _attribute_table({"x": itertools.repeat(1)}, 1)
except Exception as e:
    print(type(e).__name__, e)
"""
    completed = _assert_terminates(script)
    blob = completed.stdout + completed.stderr
    assert 'PanicException' not in blob
    assert 'ValueError' in blob
    assert 'does not match geometry length' in blob


def test_d14_honest_finite_attributes() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / 'honest.parquet')
        arr = gm.GeometryArray([
            gm.Point(0, 0, crs=4326),
            gm.Point(1, 1, crs=4326),
        ])
        arr.to_geoparquet(path, attributes={'x': [1, 2]})
        restored, table = gm.from_geoparquet(path)
        assert len(restored) == 2
        assert table.to_pydict() == {'x': [1, 2]}
    from gometry._viz import _attribute_table

    table = _attribute_table({'x': [1]}, 1)
    assert table is not None
    assert table.num_rows == 1


# ---------------------------------------------------------------------------
# D13 — from_geoparquet columns= bounded by finite schema attribute set
# ---------------------------------------------------------------------------


def test_d13_from_geoparquet_infinite_columns_terminates() -> None:
    script = """\
import itertools, tempfile
from pathlib import Path
import gometry as gm
path = str(Path(tempfile.mkdtemp()) / "cols.parquet")
gm.GeometryArray([gm.Point(0, 0, crs=4326)]).to_geoparquet(
    path, attributes={"id": [1]}
)
try:
    gm.from_geoparquet(path, columns=itertools.repeat("id"))
except Exception as e:
    print(type(e).__name__, e)
"""
    completed = _assert_terminates(script)
    blob = completed.stdout + completed.stderr
    assert 'PanicException' not in blob
    assert 'GeometryError' in blob
    assert 'duplicate' in blob


def test_d13_from_geoparquet_honest_columns_selection() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / 'cols.parquet')
        gm.GeometryArray([gm.Point(0, 0, crs=4326)]).to_geoparquet(
            path, attributes={'id': [1], 'label': ['a']}
        )
        _geoms, attrs = gm.from_geoparquet(path, columns=['id'])
        assert attrs.column_names == ['id']
        assert attrs.to_pydict() == {'id': [1]}
        # One-shot generator still works.
        _geoms, attrs2 = gm.from_geoparquet(path, columns=(c for c in ['id', 'label']))
        assert attrs2.column_names == ['id', 'label']
