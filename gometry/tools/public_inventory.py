"""Source-only inventory of gometry's declared public API.

The inventory reads the literal facade ``__all__``, top-level declarations in
the generated native stub, and Python classes directly re-exported by that
facade.  Exported class members include public methods, properties, annotated
attributes, and members inherited from declared bases.  It never imports the
extension or optional dependencies, and it does not attempt to infer names
created by arbitrary execution.
"""

from __future__ import annotations

import ast
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyo3stubs.ast_util import decorator_names
from pyo3stubs.doc_structure import (
    DocClassSource,
    DocInitializationSource,
    DocLocation,
    DocMemberSource,
    parse_doc,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping
    from pathlib import Path


@dataclass(frozen=True, slots=True)
class FacadeImport:
    """One public facade import and its source declaration provenance."""

    public_name: str
    declared_name: str
    source_path: Path
    import_node: ast.ImportFrom
    alias_node: ast.alias


@dataclass(frozen=True, slots=True)
class FacadeClass:
    """A re-exported class declaration and the import that publishes it."""

    imported: FacadeImport
    declaration: DocClassSource


@dataclass(frozen=True, slots=True, eq=False)
class PublicInventory(AbstractSet[str]):
    """Set-compatible public names plus source declarations for facade classes."""

    symbols: frozenset[str]
    classes: Mapping[str, FacadeClass]

    def __contains__(self, value: object) -> bool:
        return value in self.symbols

    def __iter__(self) -> Iterator[str]:
        return iter(self.symbols)

    def __len__(self) -> int:
        return len(self.symbols)

    @property
    def source_classes(self) -> dict[str, DocClassSource]:
        """Declarations keyed by the public name used by the shared subjects."""
        return {name: source.declaration for name, source in self.classes.items()}


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding='utf-8'), filename=str(path))


def load_public_all(init_path: Path) -> frozenset[str]:
    """Read the literal top-level export list from ``__init__.py``."""
    tree = _tree(init_path)
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AnnAssign):
            targets = (node.target,)
        else:
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == '__all__'
            for target in targets
        ):
            continue
        if node.value is None:
            raise RuntimeError('__all__ has no value')
        try:
            value = ast.literal_eval(node.value)
        except Exception as exc:
            raise RuntimeError(f'cannot eval __all__: {exc}') from exc
        if not isinstance(value, (list, tuple)):
            raise TypeError('__all__ is not a list')
        if any(not isinstance(item, str) for item in value):
            raise RuntimeError('__all__ must contain only strings')
        return frozenset(value)
    raise RuntimeError('__all__ not found in __init__.py')


def _is_public_name(name: str) -> bool:
    return not name.startswith('_') or (name.startswith('__') and name.endswith('__'))


def _base_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Call):
        return _base_name(node.func)
    if isinstance(node, ast.Subscript):
        return _base_name(node.value)
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _class_declarations(tree: ast.Module) -> dict[str, ast.ClassDef]:
    return {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}


def _source_location(path: Path, node: ast.AST) -> DocLocation:
    line = node.lineno
    if (
        isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
        and isinstance(node.body[0].value.value, str)
    ):
        line = node.body[0].lineno
    return DocLocation(str(path), line)


def _has_initialization_contract(prose: str | None) -> bool:
    if not prose:
        return False
    sections = {section.name for section in parse_doc(prose).sections}
    return bool(
        sections
        & {
            'Parameters',
            'Other Parameters',
            'Keyword Arguments',
            'Raises',
            'Returns',
            'Yields',
        }
    )


def _class_members(
    name: str,
    declarations: dict[str, ast.ClassDef],
    source_path: Path,
    visiting: set[str] | None = None,
) -> dict[str, DocMemberSource]:
    """Resolve each member to its nearest declaration through the class bases."""
    if visiting is None:
        visiting = set()
    if name in visiting:
        return {}
    cls = declarations.get(name)
    if cls is None:
        return {}

    visiting.add(name)
    members: dict[str, DocMemberSource] = {}
    for base in cls.bases:
        base_name = _base_name(base)
        if base_name is not None:
            for member_name, member in _class_members(
                base_name,
                declarations,
                source_path,
                visiting,
            ).items():
                members.setdefault(member_name, member)
    typed_dict = any(_base_name(base) == 'TypedDict' for base in cls.bases) or any(
        member.kind == 'schema' for member in members.values()
    )

    functions: dict[str, list[ast.FunctionDef | ast.AsyncFunctionDef]] = {}
    fields: dict[str, tuple[int, DocMemberSource]] = {}
    for index, node in enumerate(cls.body):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if _is_public_name(node.name):
                functions.setdefault(node.name, []).append(node)
        elif (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and _is_public_name(node.target.id)
        ):
            prose = None
            line = node.lineno
            if index + 1 < len(cls.body):
                following = cls.body[index + 1]
                if (
                    isinstance(following, ast.Expr)
                    and isinstance(following.value, ast.Constant)
                    and isinstance(following.value.value, str)
                ):
                    prose = following.value.value
                    line = following.lineno
            member_name = node.target.id
            fields[member_name] = (
                node.lineno,
                DocMemberSource(
                    node=node,
                    kind='schema' if typed_dict else 'attribute',
                    prose=prose,
                    source=DocLocation(str(source_path), line),
                ),
            )

    own: dict[str, tuple[int, DocMemberSource]] = {}
    for member_name, defs in functions.items():
        selected = next(
            (node for node in defs if decorator_names(node) & {'property', 'getter'}),
            None,
        )
        if selected is None:
            inherited = members.get(member_name)
            if (
                inherited is not None
                and inherited.kind == 'property'
                and all('setter' in decorator_names(node) for node in defs)
            ):
                continue
            selected = defs[-1]
        own[member_name] = (
            selected.lineno,
            DocMemberSource(
                node=selected,
                kind=(
                    'property'
                    if decorator_names(selected) & {'property', 'getter'}
                    else 'callable'
                ),
                prose=ast.get_docstring(selected),
                source=_source_location(source_path, selected),
            ),
        )
    for member_name, field in fields.items():
        current = own.get(member_name)
        if current is None or field[0] > current[0]:
            own[member_name] = field
    members.update({member: declaration for member, (_, declaration) in own.items()})
    visiting.remove(name)
    return members


def _module_path(
    module: str | None,
    *,
    init_path: Path,
    stub_path: Path,
) -> Path | None:
    if not module:
        return None
    package_name = init_path.parent.name
    prefix = f'{package_name}.'
    if not module.startswith(prefix):
        return None
    if module == f'{package_name}.{stub_path.stem}' and stub_path.is_file():
        return stub_path

    relative = module[len(prefix) :].replace('.', '/')
    candidates = (
        init_path.parent / f'{relative}.py',
        init_path.parent / f'{relative}.pyi',
        init_path.parent / relative / '__init__.py',
    )
    return next((path for path in candidates if path.is_file()), None)


def _facade_imports(
    init_path: Path,
    stub_path: Path,
    public_all: frozenset[str],
) -> list[FacadeImport]:
    """Retain the import node, alias, and resolved declaration source."""
    imports: list[FacadeImport] = []
    for node in _tree(init_path).body:
        if not isinstance(node, ast.ImportFrom):
            continue
        source_path = _module_path(
            node.module, init_path=init_path, stub_path=stub_path
        )
        if source_path is None:
            continue
        for alias in node.names:
            public_name = alias.asname or alias.name
            if public_name in public_all and alias.name != '*':
                imports.append(
                    FacadeImport(
                        public_name=public_name,
                        declared_name=alias.name,
                        source_path=source_path,
                        import_node=node,
                        alias_node=alias,
                    )
                )
    return imports


def load_public_symbols(stub_path: Path, init_path: Path) -> PublicInventory:
    """Return set-compatible public names with source-backed class declarations."""
    public_all = load_public_all(init_path)
    symbols: set[str] = set(public_all)

    stub_tree = _tree(stub_path)
    stub_declarations = _class_declarations(stub_tree)
    for name in public_all:
        if name in stub_declarations:
            members = _class_members(name, stub_declarations, stub_path)
            symbols.update(f'{name}.{member}' for member in members)

    source_declarations: dict[Path, dict[str, ast.ClassDef]] = {
        stub_path: stub_declarations
    }
    classes: dict[str, FacadeClass] = {}
    for imported in _facade_imports(init_path, stub_path, public_all):
        declarations = source_declarations.get(imported.source_path)
        if declarations is None:
            declarations = _class_declarations(_tree(imported.source_path))
            source_declarations[imported.source_path] = declarations
        declaration = declarations.get(imported.declared_name)
        if declaration is None:
            continue
        members = _class_members(
            imported.declared_name,
            declarations,
            imported.source_path,
        )
        symbols.update(f'{imported.public_name}.{member}' for member in members)
        class_prose = ast.get_docstring(declaration)
        source_initializer = members.get('__init__') or members.get('__new__')
        if source_initializer is not None and _has_initialization_contract(
            source_initializer.prose
        ):
            initialization = DocInitializationSource(
                node=source_initializer.node,
                prose=source_initializer.prose,
                source=source_initializer.source,
            )
        elif _has_initialization_contract(class_prose):
            initialization = DocInitializationSource(
                node=declaration,
                prose=class_prose,
                source=_source_location(imported.source_path, declaration),
            )
        elif source_initializer is not None:
            initialization = DocInitializationSource(
                node=source_initializer.node,
                prose=source_initializer.prose,
                source=source_initializer.source,
            )
        else:
            initialization = None
        classes.setdefault(
            imported.public_name,
            FacadeClass(
                imported=imported,
                declaration=DocClassSource(
                    node=declaration,
                    source=_source_location(imported.source_path, declaration),
                    members=members,
                    initialization=initialization,
                ),
            ),
        )
    return PublicInventory(frozenset(symbols), classes)


def unknown_public_symbols(
    symbols: Iterable[str], public_symbols: Iterable[str]
) -> tuple[str, ...]:
    """Return sorted symbols absent from the canonical inventory."""
    return tuple(sorted(set(symbols) - set(public_symbols)))
