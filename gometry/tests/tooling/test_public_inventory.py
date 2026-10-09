from __future__ import annotations

import ast
import types
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple, TypedDict

from public_inventory import load_public_symbols
from pyo3stubs.config import StubConfig
from pyo3stubs.context import CheckContext
from pyo3stubs.doc_structure import (
    check_doc_subject,
    iter_doc_subjects,
    parameter_entries,
    parse_doc,
    raises_entries,
)


def test_facade_source_classes_feed_shared_subjects_with_provenance(
    tmp_path: Path,
) -> None:
    package = tmp_path / 'fixturepkg'
    package.mkdir()
    init_path = package / '__init__.py'
    init_path.write_text(
        """\
from fixturepkg.models import Child as Public, DataRecord, Record, Schema

__all__ = ['Public', 'DataRecord', 'Record', 'Schema']
""",
        encoding='utf-8',
    )
    models_path = package / 'models.py'
    models_path.write_text(
        '''\
from dataclasses import dataclass
from typing import NamedTuple, TypedDict


class Base:
    @property
    def inherited(self) -> int:
        """The base-only value.

        Returns
        -------
        int
        """

    @property
    def mutable(self) -> int:
        """The inherited mutable value.

        Returns
        -------
        int
        """

    @mutable.setter
    def mutable(self, value: int) -> None:
        pass

    @property
    def label(self) -> int:
        """The base label.

        Returns
        -------
        int
        """


class Child(Base):
    """A public source-defined class."""

    def __init__(self, value: int) -> None:
        """Build a public value.

        Parameters
        ----------
        value : int
            The value to retain.

        Returns
        -------
        Public
        """

    @property
    def label(self) -> str:
        """The overridden label.

        Returns
        -------
        str
        """

    @Base.mutable.setter
    def mutable(self, value: int) -> None:
        pass

    value: int
    """The retained value."""


class Record(NamedTuple):
    item: str
    """The record's item."""


class Schema(TypedDict, total=False):
    key: str
    """The optional schema key."""


@dataclass
class DataRecord:
    count: int
    """The number of values."""
''',
        encoding='utf-8',
    )
    stub_path = package / '_lib.pyi'
    stub_path.write_text('', encoding='utf-8')
    source_root = tmp_path / 'src'
    source_root.mkdir()

    inventory = load_public_symbols(stub_path, init_path)

    assert {
        'Public',
        'Public.value',
        'Public.inherited',
        'Public.label',
        'Record.item',
        'Schema.key',
        'DataRecord.count',
    } <= inventory
    public = inventory.classes['Public']
    declaration = public.declaration
    assert public.imported.declared_name == 'Child'
    assert public.imported.alias_node.asname == 'Public'
    assert declaration.node.name == 'Child'
    assert declaration.source.path == str(models_path)
    assert ast.unparse(declaration.members['label'].node.returns) == 'str'

    class RuntimeBase:
        @property
        def inherited(self) -> int:
            """The base-only value.

            Returns
            -------
            int
            """
            return 1

        @property
        def mutable(self) -> int:
            """The inherited mutable value.

            Returns
            -------
            int
            """
            return 1

        @mutable.setter
        def mutable(self, value: int) -> None:
            self._mutable = value

        @property
        def label(self) -> int:
            """The base label.

            Returns
            -------
            int
            """
            return 1

    class RuntimePublic(RuntimeBase):
        def __init__(self, value: int) -> None:
            self.value = value

        @property
        def label(self) -> str:
            """The overridden label.

            Returns
            -------
            str
            """
            return str(self.value)

        @RuntimeBase.mutable.setter
        def mutable(self, value: int) -> None:
            self._mutable = value

    class RuntimeRecord(NamedTuple):
        item: str

    class RuntimeSchema(TypedDict, total=False):
        key: str

    @dataclass
    class RuntimeDataRecord:
        count: int

    runtime = types.ModuleType('fixturepkg.runtime')
    runtime.__all__ = ['Public', 'DataRecord', 'Record', 'Schema']
    runtime.Public = RuntimePublic  # type: ignore[attr-defined]
    runtime.DataRecord = RuntimeDataRecord  # type: ignore[attr-defined]
    runtime.Record = RuntimeRecord  # type: ignore[attr-defined]
    runtime.Schema = RuntimeSchema  # type: ignore[attr-defined]
    ctx = CheckContext(
        StubConfig(
            module='fixturepkg.runtime',
            stub_path=stub_path,
            src_root=source_root,
        ),
        _runtime_module=runtime,
    )
    subjects = tuple(
        iter_doc_subjects(
            ctx,
            runtime_module=runtime,
            constructor_sources={},
            source_classes=inventory.source_classes,
        )
    )
    by_name = {(subject.qualname, subject.kind): subject for subject in subjects}

    constructor = by_name[('Public.__new__', 'constructor')]
    value = by_name[('Public.value', 'attribute')]
    inherited = by_name[('Public.inherited', 'property')]
    mutable = by_name[('Public.mutable', 'property')]
    label = by_name[('Public.label', 'property')]
    record_field = by_name[('Record.item', 'attribute')]
    schema_field = by_name[('Schema.key', 'schema')]
    dataclass_field = by_name[('DataRecord.count', 'attribute')]

    assert constructor.source.path == str(models_path)
    assert constructor.prose is not None
    assert 'Build a public value.' in constructor.prose
    assert constructor.source.line == declaration.members['__init__'].source.line
    assert check_doc_subject(constructor) == []
    assert (value.prose, value.source.path) == (
        'The retained value.',
        str(models_path),
    )
    assert inherited.source.path == str(models_path)
    assert mutable.source.path == str(models_path)
    assert mutable.source == declaration.members['mutable'].source
    assert mutable.source.line < label.source.line
    assert label.source.path == str(models_path)
    assert label.source.line == declaration.members['label'].source.line
    assert record_field.prose == "The record's item."
    assert schema_field.prose == 'The optional schema key.'
    assert dataclass_field.prose == 'The number of values.'
    assert all(
        check_doc_subject(subject) == []
        for subject in (value, record_field, schema_field, dataclass_field)
    )
    assert sum(subject.qualname == 'Public.label' for subject in subjects) == 1
    assert sum(subject.qualname == 'Public.inherited' for subject in subjects) == 1
    assert sum(subject.qualname == 'Public.mutable' for subject in subjects) == 1


def test_features_constructor_and_field_prose_matches_the_python_declaration() -> None:
    root = Path(__file__).resolve().parents[2]
    inventory = load_public_symbols(
        root / 'python' / 'gometry' / '_lib.pyi',
        root / 'python' / 'gometry' / '__init__.py',
    )
    features = inventory.source_classes['Features']
    initializer = features.members['__init__']
    assert features.initialization is not None
    assert features.initialization.node is initializer.node
    assert initializer.prose is not None
    parameters = {
        entry.names[0]: entry
        for entry in parameter_entries(parse_doc(initializer.prose))
    }
    assert parameters['geometries'].type_text == 'GeometryArray[Geometry | None]'
    assert parameters['properties'].default_text is None
    assert parameters['ids'].default_text is None
    assert {
        name
        for entry in raises_entries(parse_doc(initializer.prose))
        for name in entry.names
    } == {
        'TypeError',
        'GeometryError',
    }
    assert features.members['geometries'].kind == 'attribute'
    assert 'One parsed geometry' in features.members['geometries'].prose
    assert 'shallow-copied for each row' in features.members['properties'].prose
    assert 'independent shallow copy per row' in initializer.prose
    assert 'non-boolean integer' in initializer.prose
    assert 'frozen slots' not in (ast.get_docstring(features.node) or '').lower()
    assert 'cold import' not in (ast.get_docstring(features.node) or '').lower()
