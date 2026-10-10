"""Source-only tests for reachable API reference type definitions."""

from __future__ import annotations

import ast
import sys
from types import SimpleNamespace
from typing import TYPE_CHECKING

import gometry as gm
import pytest

from tests._support import GOMETRY_ROOT, load_tool

if TYPE_CHECKING:
    from pathlib import Path

_REFERENCE_TYPES = None


def _reference_types():
    global _REFERENCE_TYPES
    if _REFERENCE_TYPES is None:
        _REFERENCE_TYPES = load_tool(
            'reference_types',
            GOMETRY_ROOT / 'tools/docs/reference_types.py',
        )
    return _REFERENCE_TYPES


def _reference_type_page():
    return load_tool(
        'reference_types_page',
        GOMETRY_ROOT / 'tools/docs/reference_types_page.py',
    )


def _write_package(
    package: Path,
    *,
    initializer: str,
    stub: str,
    types: str,
) -> tuple[Path, Path, Path]:
    package.mkdir()
    init_path = package / '__init__.py'
    stub_path = package / '_lib.pyi'
    types_path = package / '_types.py'
    init_path.write_text(initializer, encoding='utf-8')
    stub_path.write_text(stub, encoding='utf-8')
    types_path.write_text(types, encoding='utf-8')
    return init_path, stub_path, types_path


def _graph_for_public_function(
    package: Path,
    stub_path: Path,
    function_name: str,
    *,
    source_classes: dict[str, object] | None = None,
):
    module = ast.parse(stub_path.read_text(encoding='utf-8'))
    function = next(
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name == function_name
    )
    reference_types = _reference_types()
    subject = SimpleNamespace(
        qualname=function_name,
        declarations=(function,),
        source=SimpleNamespace(path=str(stub_path)),
    )
    return reference_types.build_type_graph(
        package,
        subjects=(subject,),
        source_classes=source_classes or {},
        public_names={'describe', 'Record'},
    )


def test_reachable_definitions_keep_identity_schema_contract_and_typevar_default(
    tmp_path: Path,
) -> None:
    package = tmp_path / 'gometry'
    _, stub_path, types_path = _write_package(
        package,
        initializer="""\
from ._types import Record

__all__ = ['describe', 'Record']
""",
        stub="""\
from ._types import Coordinate, GeoJsonFeature, Metadata

def describe(value: Coordinate, metadata: Metadata) -> GeoJsonFeature: ...
""",
        types="""\
from typing import Generic, Literal, NamedTuple, NotRequired, Required, TypeAlias, TypeVar, TypedDict

if TYPE_CHECKING:
    import gometry_optional_peer_for_reference_test as peer

_Coordinate: TypeAlias = tuple[float, float]
Coordinate: TypeAlias = tuple[_Coordinate, ...]
Format: TypeAlias = Literal['wkt', 'wkb']
ItemT = TypeVar('ItemT', default=int, covariant=True)

class Record(NamedTuple, Generic[ItemT]):
    required: str
    value: ItemT = 7

class Base(TypedDict, total=False):
    label: NotRequired[str | None]
    record: Required[Record]
    format: NotRequired[Format]
    source: NotRequired[peer.Source]

class GeoJsonFeature(Base):
    geometry: Record | None
    child: NotRequired['GeoJsonFeature | None']
    "A recursive nullable feature reference."

Metadata: TypeAlias = Record | None
"Metadata remains nullable while Record is a public result class."

raise RuntimeError('type discovery executed the declaration module')
""",
    )
    type_tree = ast.parse(types_path.read_text(encoding='utf-8'))
    record = next(
        node
        for node in type_tree.body
        if isinstance(node, ast.ClassDef) and node.name == 'Record'
    )
    source_classes = {
        'Record': SimpleNamespace(
            node=record,
            source=SimpleNamespace(path=str(types_path)),
        )
    }

    graph = _graph_for_public_function(
        package,
        stub_path,
        'describe',
        source_classes=source_classes,
    )
    rendered = _reference_type_page().render_type_page(graph)
    paths = {declaration.path for declaration in graph.definitions}

    assert {
        'gometry._types.Base',
        'gometry._types.Coordinate',
        'gometry._types.GeoJsonFeature',
        'gometry._types.Metadata',
        'gometry._types.ItemT',
    } <= paths
    assert 'gometry._types._Coordinate' not in paths
    assert 'gometry._types.Format' not in paths
    assert graph.expansions['gometry._types.Format']
    assert (
        '**Annotation shape:** <code>tuple[tuple[float, float], ...]</code>' in rendered
    )
    assert '**Default element type:** <code>int</code>' in rendered
    assert '**Variance:** covariant' in rendered
    assert (
        'Metadata remains nullable while Record is a public result class.' in rendered
    )
    assert '| `label` | <code>str &#124; None</code> | no |  |' in rendered
    assert '| `record` | <code>Record</code> | yes |  |' in rendered
    assert "| `format` | <code>Literal['wkt', 'wkb']</code> | no |  |" in rendered
    assert '| `source` | <code>peer.Source</code> | no |  |' in rendered
    assert 'A recursive nullable feature reference.' in rendered
    assert 'gometry._types.GeoJsonFeature' in rendered
    assert '[Record][gometry.Record]' in rendered
    assert '[Base mapping][gometry.describe]' in rendered
    assert '## `gometry._types.' not in rendered
    assert '**Source:**' not in rendered
    assert 'GeoJsonFeature: TypeAlias' not in rendered
    assert 'descriptive labels are not additional import names.' in rendered
    assert 'gometry_optional_peer_for_reference_test' not in sys.modules

    original_types = types_path.read_text(encoding='utf-8')
    types_path.write_text(
        original_types.replace(
            'label: NotRequired[str | None]',
            'label: Required[str | None]',
        ),
        encoding='utf-8',
    )
    required_graph = _graph_for_public_function(
        package,
        stub_path,
        'describe',
        source_classes=source_classes,
    )
    required_page = _reference_type_page().render_type_page(required_graph)
    assert '| `label` | <code>str &#124; None</code> | yes |  |' in required_page
    assert '| `label` | <code>str &#124; None</code> | no |  |' not in required_page

    types_path.write_text(
        types_path.read_text(encoding='utf-8').replace(
            'class GeoJsonFeature(Base):',
            'class GeoJsonFeature(TypedDict):',
        ),
        encoding='utf-8',
    )
    without_inheritance = _graph_for_public_function(
        package,
        stub_path,
        'describe',
        source_classes=source_classes,
    )
    assert 'gometry._types.Base' not in {
        declaration.path for declaration in without_inheritance.definitions
    }


def test_unresolved_in_package_reference_fails_with_source_chain(
    tmp_path: Path,
) -> None:
    package = tmp_path / 'gometry'
    _, stub_path, _ = _write_package(
        package,
        initializer="__all__ = ['describe']\n",
        stub="""\
from ._types import Input

def describe(value: Input) -> None: ...
""",
        types="""\
from typing import TypeAlias

Input: TypeAlias = MissingLocalType
""",
    )

    with pytest.raises(
        _reference_types().TypeReferenceError,
        match=r'_types\.py:3.*gometry\._types\.MissingLocalType',
    ):
        _graph_for_public_function(package, stub_path, 'describe')


def test_private_alias_only_cycle_fails_with_its_source_chain(tmp_path: Path) -> None:
    package = tmp_path / 'gometry'
    _, stub_path, _ = _write_package(
        package,
        initializer="__all__ = ['describe']\n",
        stub="""\
from ._types import PublicAlias

def describe(value: PublicAlias) -> None: ...
""",
        types="""\
from typing import TypeAlias

_First: TypeAlias = _Second
_Second: TypeAlias = _First
PublicAlias: TypeAlias = _First
""",
    )

    with pytest.raises(
        _reference_types().TypeReferenceError,
        match=r'private alias cycle: gometry\._types\._First.*gometry\._types\._First',
    ):
        _graph_for_public_function(package, stub_path, 'describe')


def test_same_spelling_resolves_to_the_imported_declaration_identity(
    tmp_path: Path,
) -> None:
    package = tmp_path / 'gometry'
    init_path = package / '__init__.py'
    stub_path = package / '_lib.pyi'
    types_path = package / '_types.py'
    other_path = package / '_other.py'
    package.mkdir()
    init_path.write_text("__all__ = ['describe']\n", encoding='utf-8')
    stub_path.write_text(
        """\
from ._types import Size

def describe(value: Size) -> None: ...
""",
        encoding='utf-8',
    )
    types_path.write_text(
        'from typing import TypeAlias\n\nSize: TypeAlias = int\n',
        encoding='utf-8',
    )
    other_path.write_text(
        'from typing import TypeAlias\n\nSize: TypeAlias = str\n',
        encoding='utf-8',
    )
    function = next(
        node
        for node in ast.parse(stub_path.read_text(encoding='utf-8')).body
        if isinstance(node, ast.FunctionDef)
    )
    subject = SimpleNamespace(
        qualname='describe',
        declarations=(function,),
        source=SimpleNamespace(path=str(stub_path)),
    )
    graph_builder = _reference_types().build_type_graph
    graph = graph_builder(
        package,
        subjects=(subject,),
        public_names={'describe'},
    )
    assert {item.path for item in graph.definitions} == {'gometry._types.Size'}

    stub_path.write_text(
        stub_path.read_text(encoding='utf-8').replace('_types import', '_other import'),
        encoding='utf-8',
    )
    function = next(
        node
        for node in ast.parse(stub_path.read_text(encoding='utf-8')).body
        if isinstance(node, ast.FunctionDef)
    )
    subject = SimpleNamespace(
        qualname='describe',
        declarations=(function,),
        source=SimpleNamespace(path=str(stub_path)),
    )
    redirected = graph_builder(
        package,
        subjects=(subject,),
        public_names={'describe'},
    )
    assert {item.path for item in redirected.definitions} == {'gometry._other.Size'}


def test_live_public_subjects_resolve_reachable_reference_definitions() -> None:
    graph = _reference_types().build_public_type_graph(
        GOMETRY_ROOT / 'python/gometry',
        gm,
    )
    paths = {declaration.path for declaration in graph.definitions}
    rendered = _reference_type_page().render_type_page(graph)

    assert not {f'gometry.{name}' for name in gm.__all__} & paths
    assert {
        'gometry._types.FloatScalar',
        'gometry._types.ScalarFloatArray',
        'gometry._types.GeoJsonFeature',
        'gometry._types.GeoJsonGeometryCollectionGeometry',
        'gometry._types.CrsCfInfo',
        'gometry._types._ExtremeT',
    } <= paths
    assert 'gometry._lib._FloatScalarInput' in graph.expansions
    assert 'gometry._types.ParseFormat' in graph.expansions
    assert '## Generic element type' in rendered
    assert '## `gometry._types.' not in rendered
    assert '**Default element type:** <code>Point</code>' in rendered
    assert 'gometry.Point' in graph.references['gometry._types._ExtremeT']
    assert '[Point][gometry.Point]' in rendered
    assert (
        'gometry._types.GeoJsonGeometry'
        in graph.references['gometry._types.GeoJsonGeometryCollectionGeometry']
    )
    assert 'gometry._types._FloatScalarInput' not in rendered
    assert graph.public_owners['gometry._types.FloatScalar']
    assert all(
        not owner.endswith(('.__new__', '.__init__'))
        for owners in graph.public_owners.values()
        for owner in owners
    )
