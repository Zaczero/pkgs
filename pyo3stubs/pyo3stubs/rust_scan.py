"""Structural Rust scanning: the ONE parsed view every Rust-facing gate uses.

Sources are parsed with tree-sitter rather than matched with regexes over raw
text, and each file is read and parsed once per run. That is what makes several
classes of defect unrepresentable rather than merely unlikely:

* a ``#[pyclass]`` inside a comment or a string literal is a comment or a
  string, not an export;
* an item's body is the body the grammar gives it, so a unit struct cannot
  inherit the next struct's ``#[pyo3(get)]`` fields;
* delimiters inside literals cannot derail a scan, so a ``(`` in a Rust default
  no longer raises ``IndexError`` out of a gate;
* a ``macro_rules!`` *definition* declares nothing — only its invocations do;
* the export view drops items provably disabled when ``test = false`` while
  retaining unknown cfg expressions; the raw view also includes test items.

Items are collected at item positions (module, ``impl`` and ``trait`` bodies,
struct fields, enum variants). Function bodies remain on the retained tree for
consumers that need expression-level syntax.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, replace
from functools import lru_cache
from typing import TYPE_CHECKING, Literal

import tree_sitter_rust
from tree_sitter import Language, Parser

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from tree_sitter import Node, Tree

    from pyo3stubs.config import StubConfig

# Use Point indexing: tree-sitter 0.26's row/column getters return borrowed references.
_LANGUAGE = Language(tree_sitter_rust.language())
#: One parser per thread: a tree-sitter parser is mutable state, and
#: ``run_all`` runs the Rust-reading gates concurrently.
_THREAD = threading.local()
#: The first gate to need the crate parses it; the others wait and reuse it.
_PARSE_LOCK = threading.Lock()


def _parser() -> Parser:
    parser: Parser | None = getattr(_THREAD, 'parser', None)
    if parser is None:
        parser = _THREAD.parser = Parser(_LANGUAGE)
    return parser


#: Nodes whose children are items in declaration order.
_CONTAINERS = frozenset({
    'source_file',
    'declaration_list',
    'field_declaration_list',
    'enum_variant_list',
})

#: Children that neither are items nor end an attribute run. Comments matter:
#: ``#[pyclass]\n// note\nstruct X;`` must keep the attribute attached.
_TRIVIA = frozenset({
    'line_comment',
    'block_comment',
    'inner_attribute_item',
    '{',
    '}',
})


@dataclass(frozen=True, slots=True)
class Attr:
    """One ``#[...]`` attribute, arguments already split.

    ``params`` holds top-level ``(key, value)`` pairs; a bare flag has an empty
    value, so ``#[pyclass(name = "Sealed", frozen)]`` is
    ``(('name', '"Sealed"'), ('frozen', ''))``.
    """

    name: str
    text: str
    line: int
    params: tuple[tuple[str, str], ...]
    #: Evaluation of this ``cfg`` with ``test = false``; None means unknown.
    cfg_test: bool | None = None

    def value(self, key: str) -> str | None:
        """Source of ``key = <value>``, ``''`` for a bare flag, None if absent."""
        return next((value for name, value in self.params if name == key), None)

    def has(self, key: str) -> bool:
        """Whether ``key`` appears at all, as a flag or an assignment."""
        return any(name == key for name, _ in self.params)


@dataclass(frozen=True, slots=True)
class Item:
    """One attributed Rust item, and the items its body declares."""

    kind: str
    name: str
    #: A function's return type, or a field's declared type; ``''`` when neither.
    ty: str
    node: Node
    type_node: Node | None
    line: int
    #: The ``///`` block above the item, dedented; ``''`` when it has none.
    doc: str
    attrs: tuple[Attr, ...]
    children: tuple[Item, ...]

    def attr(self, name: str) -> Attr | None:
        """The first attribute spelled ``name``, or None."""
        return next((attr for attr in self.attrs if attr.name == name), None)

    @property
    def text(self) -> str:
        """The item's source, viewed from its retained syntax node."""
        return _text(self.node)


@dataclass(frozen=True, slots=True)
class RustSource:
    """One parsed ``.rs`` file."""

    #: Path relative to ``src_root`` — the key form gates store in maps.
    rel: str
    #: Path relative to ``src_root``'s parent — the form that reads well in a
    #: violation message (``src/lib.rs``).
    label: str
    #: Owns the source bytes behind every node from the file's original parse.
    tree: Tree
    #: Owns the source bytes behind nodes recovered from macro invocations.
    macro_trees: tuple[Tree, ...]
    items: tuple[Item, ...]

    def walk(self) -> Iterator[Item]:
        """Every item in the file, outermost first."""

        def descend(items: tuple[Item, ...]) -> Iterator[Item]:
            for item in items:
                yield item
                yield from descend(item.children)

        return descend(self.items)


@dataclass(frozen=True, slots=True)
class PyClassExport:
    """One Python class the extension exports, and where it is declared."""

    rust: str
    python: str
    source: RustSource
    item: Item


@dataclass(frozen=True, slots=True)
class ConstructorDocSource:
    """A constructor source, including explicit absence or rejection."""

    python_class: str | None
    prose: str | None
    path: str
    line: int
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class ConstructorDocInventory:
    """Effective class carriers and every unsupported macro-source record."""

    by_class: dict[str, ConstructorDocSource]
    issues: tuple[ConstructorDocSource, ...]


def token_params(source: str) -> tuple[tuple[str, str], ...]:
    """Top-level ``key = value`` pairs of a delimited Rust token tree.

    Bare tokens carry an empty value, so ``(a, b = "x")`` is
    ``(('a', ''), ('b', '"x"'))``. Splitting is the grammar's job, so a
    delimiter or comma inside a literal — ``(a, b = "(,)")`` — is literal text
    and still yields two parameters.
    """
    body = source.encode()
    prefix = b'_!'
    node = _outermost(_parser().parse(prefix + body).root_node, 'token_tree')
    if node is None:
        return ()
    offset = len(prefix)
    return tuple(_assignment(group, body, offset) for group in _groups(node) if group)


def iter_sources(
    cfg: StubConfig, scope: Literal['export', 'all'] = 'export'
) -> tuple[RustSource, ...]:
    """Every ``.rs`` file under ``src_root``, parsed once per run.

    A ``check-all`` used to read and re-scan the tree once per Rust-facing
    gate. The parse is shared, keyed on what the files look like right now, so
    a suite that mutates a fixture between gates still sees its own edit.

    ``export`` excludes only items provably disabled with ``test = false``.
    Unknown cfg predicates remain visible. ``all`` returns the raw attributed
    items, including test-only declarations.
    """
    stamp: list[tuple[Path, int, int]] = []
    for path in sorted(cfg.src_root.rglob('*.rs')):
        stat = path.stat()
        stamp.append((path, stat.st_mtime_ns, stat.st_size))
    return _parsed(cfg.src_root, tuple(stamp), scope)


def pyclass_exports(cfg: StubConfig) -> tuple[PyClassExport, ...]:
    """Every class the extension exports, from attributes and macro patterns.

    A ``#[pyclass]`` exports under its ``name = "..."`` argument when it has
    one and its Rust identifier otherwise — an unnamed one is just as public.
    """
    exports: list[PyClassExport] = []
    for source in iter_sources(cfg):
        for item in source.walk():
            if item.kind in ('struct_item', 'enum_item'):
                attr = item.attr('pyclass')
                if attr is None:
                    continue
                named = attr.value('name')
                python = unquote(named) if named else item.name
                exports.append(PyClassExport(item.name, python, source, item))
            elif item.kind == 'macro_invocation':
                for spec in cfg.macro_exports:
                    if item.name != spec.macro:
                        continue
                    args = [
                        unquote(key)
                        for key, _ in token_params(item.text.split('!', 1)[1])
                    ]
                    if spec.python >= len(args):
                        continue
                    python = args[spec.python]
                    rust = python if spec.rust is None else args[spec.rust]
                    exports.append(PyClassExport(rust, python, source, item))
    return tuple(exports)


def pyclass_names(cfg: StubConfig) -> dict[str, str]:
    """Exported Python class name -> defining path, relative to ``src_root``."""
    names: dict[str, str] = {}
    for export in pyclass_exports(cfg):
        names.setdefault(export.python, export.source.rel)
    return names


def rust_class_map(cfg: StubConfig) -> dict[str, str]:
    """Rust struct/enum identifier -> exported Python class name."""
    mapping: dict[str, str] = {}
    for export in pyclass_exports(cfg):
        mapping.setdefault(export.rust, export.python)
    return mapping


def _constructor_macro_rust_name(item: Item) -> str | None:
    """Class identifier in the configured macro's direct constructor header."""
    if item.kind == 'impl_item':
        return item.name or None
    if item.kind != 'ERROR':
        return None
    children = item.node.children
    if len(children) < 3 or tuple(child.type for child in children[:3]) != (
        'impl',
        'type_identifier',
        '{',
    ):
        return None
    return _text(children[1])


def constructor_doc_sources(cfg: StubConfig) -> ConstructorDocInventory:
    """Resolve direct and configured-macro constructor carriers without loss.

    PyO3 does not attach a ``#[new]``'s doc comment to anything Python can read:
    the class docstring comes from the ``#[pyclass]``, and ``__new__`` carries
    CPython's inherited boilerplate. Without this the prose is written,
    maintained, and unreachable — 16 of gometry's 20 constructors, several with
    full numpydoc parameter tables. A configured macro may carry that same
    prose on the class ``impl`` in its invocation when the expanded ``#[new]``
    is not present in the retained syntax tree. Missing and unsupported
    carriers remain records so consumers cannot mistake them for no source.
    """
    classes = rust_class_map(cfg)
    direct: dict[str, ConstructorDocSource] = {}
    macro_candidates: dict[str, list[ConstructorDocSource]] = {}
    issues: list[ConstructorDocSource] = []
    for source in iter_sources(cfg):
        for item in source.walk():
            if item.kind == 'impl_item':
                py_class = classes.get(item.name)
                if py_class is not None:
                    for child in item.children:
                        if child.attr('new') is not None:
                            prose = child.doc.strip() or None
                            direct.setdefault(
                                py_class,
                                ConstructorDocSource(
                                    python_class=py_class,
                                    prose=prose,
                                    path=source.label,
                                    line=child.line,
                                    reason=(
                                        None
                                        if prose is not None
                                        else 'direct #[new] carrier has no authored prose'
                                    ),
                                ),
                            )
            elif (
                item.kind == 'macro_invocation'
                and item.name in cfg.constructor_doc_macros
            ):
                for carrier in item.children:
                    rust_name = _constructor_macro_rust_name(carrier)
                    if rust_name is None:
                        prose = carrier.doc.strip() or None
                        if prose is not None:
                            issues.append(
                                ConstructorDocSource(
                                    python_class=None,
                                    prose=prose,
                                    path=source.label,
                                    line=carrier.line,
                                    reason=(
                                        f'configured constructor-doc macro {item.name!r} '
                                        'has an unsupported documented item; expected '
                                        'the `impl Type {` carrier header'
                                    ),
                                )
                            )
                        continue
                    py_class = classes.get(rust_name)
                    if py_class is None:
                        issues.append(
                            ConstructorDocSource(
                                python_class=None,
                                prose=carrier.doc.strip() or None,
                                path=source.label,
                                line=carrier.line,
                                reason=(
                                    f'constructor-doc macro type {rust_name!r} does not '
                                    'map to an exported PyO3 class'
                                ),
                            )
                        )
                        continue
                    prose = carrier.doc.strip() or None
                    candidate = ConstructorDocSource(
                        python_class=py_class,
                        prose=prose,
                        path=source.label,
                        line=carrier.line,
                        reason=(
                            None
                            if prose is not None
                            else 'configured constructor-doc macro carrier has no authored prose'
                        ),
                    )
                    macro_candidates.setdefault(py_class, []).append(candidate)

    macro_carriers: dict[str, ConstructorDocSource] = {}
    for py_class, candidates in macro_candidates.items():
        issues.extend(candidate for candidate in candidates if candidate.reason)
        if len(candidates) == 1:
            macro_carriers[py_class] = candidates[0]
            continue
        locations = ', '.join(
            f'{carrier.path}:{carrier.line}' for carrier in candidates
        )
        duplicate = ConstructorDocSource(
            python_class=py_class,
            prose=None,
            path=candidates[0].path,
            line=candidates[0].line,
            reason=(
                f'ambiguous duplicate constructor-doc carriers for {py_class}: '
                f'{locations}'
            ),
        )
        macro_carriers[py_class] = duplicate
        issues.append(duplicate)

    return ConstructorDocInventory(macro_carriers | direct, tuple(issues))


def unquote(source: str) -> str:
    """A Rust string literal's contents, or the source unchanged.

    Attribute arguments arrive as source text, so `name = "Sealed"` and
    `#[getter("name")]` carry their quotes while a bare token does not.
    """
    return source[1:-1] if source.startswith('"') and source.endswith('"') else source


def _text(node: Node) -> str:
    """A node's source. tree-sitter types this optional; a parsed node has it."""
    return node.text.decode() if node.text is not None else ''


def _cfg_value(nodes: list[Node]) -> bool | None:
    """Evaluate supported cfg syntax with ``test = false``; unknown stays unknown."""
    nodes = [
        node for node in nodes if node.type not in {'line_comment', 'block_comment'}
    ]
    if len(nodes) == 1 and nodes[0].type == 'identifier':
        return False if _text(nodes[0]) == 'test' else None
    if (
        len(nodes) != 2
        or nodes[0].type != 'identifier'
        or nodes[1].type != 'token_tree'
    ):
        return None

    operator = _text(nodes[0])
    arguments = [group for group in _groups(nodes[1]) if group]
    values = [_cfg_value(group) for group in arguments]
    if operator == 'all':
        if False in values:
            return False
        return True if all(value is True for value in values) else None
    if operator == 'any':
        if True in values:
            return True
        return False if all(value is False for value in values) else None
    if operator == 'not' and len(values) == 1:
        return None if values[0] is None else not values[0]
    return None


def _first(node: Node, kind: str) -> Node | None:
    return next((child for child in node.children if child.type == kind), None)


def _outermost(node: Node, kind: str) -> Node | None:
    """The shallowest, leftmost node of ``kind`` at or below ``node``."""
    if node.type == kind:
        return node
    for child in node.children:
        found = _outermost(child, kind)
        if found is not None:
            return found
    return None


def _groups(tree: Node) -> list[list[Node]]:
    """Direct children of a token tree, split on top-level commas."""
    groups: list[list[Node]] = [[]]
    for child in tree.children[1:-1]:  # drop the outer delimiters
        if child.type == ',':
            groups.append([])
        else:
            groups[-1].append(child)
    return groups


def _span(nodes: list[Node], src: bytes, offset: int) -> str:
    return src[nodes[0].start_byte - offset : nodes[-1].end_byte - offset].decode()


def _assignment(group: list[Node], src: bytes, offset: int) -> tuple[str, str]:
    """``(key, value)`` of one comma group, split on its top-level ``=``."""
    for index, node in enumerate(group):
        if node.type == '=':
            rest = group[index + 1 :]
            return (
                _span(group[:index], src, offset) if index else '',
                _span(rest, src, offset) if rest else '',
            )
    return _span(group, src, offset), ''


def _excluded_from_export(attrs: tuple[Attr, ...]) -> bool:
    return any(attr.name == 'cfg' and attr.cfg_test is False for attr in attrs)


def _doc_line(node: Node) -> str | None:
    """One ``///`` line's content, or None when the comment is an ordinary ``//``.

    The grammar makes the distinction: a doc comment carries a ``doc_comment``
    child, a plain one does not.
    """
    inner = _first(node, 'doc_comment')
    return None if inner is None else _text(inner).rstrip('\n').removeprefix(' ')


def _attribute(node: Node) -> Attr | None:
    inner = _first(node, 'attribute')
    if inner is None or not inner.named_children:
        return None
    tree = _first(inner, 'token_tree')
    name = _text(inner.named_children[0]).rsplit('::', 1)[-1]
    return Attr(
        name=name,
        text=_text(node),
        line=node.start_point[0] + 1,
        params=token_params(_text(tree)) if tree is not None else (),
        cfg_test=(
            _cfg_value(tree.children[1:-1])
            if name == 'cfg' and tree is not None
            else None
        ),
    )


def _name(node: Node) -> str:
    """An item's bare identifier, without its module path or type parameters."""
    return _text(node).split('<', 1)[0].rsplit('::', 1)[-1].strip()


def _name_and_type(node: Node) -> tuple[Node | None, Node | None]:
    """``(name, type)`` nodes for one item, by the grammar's field names."""
    if node.type == 'impl_item':
        return node.child_by_field_name('type'), None
    if node.type == 'macro_invocation':
        return node.child_by_field_name('macro'), None
    return (
        node.child_by_field_name('name'),
        node.child_by_field_name('return_type') or node.child_by_field_name('type'),
    )


def _macro_body_items(node: Node, trees: list[Tree]) -> tuple[Item, ...]:
    """Items declared inside a macro *invocation*'s token tree.

    The grammar hands a macro body back as opaque tokens, so an
    ``frozen_pymethods! { impl PyCrs { … } }`` wrapper hid its whole ``impl``
    from every scan — 29 of gometry's 195 ``#[getter]``s and 3 of its
    constructors. Re-parsing the body as Rust recovers them. A body that is not
    Rust items simply yields none, so this cannot go wrong noisily; a
    ``macro_rules!`` *definition* is a different node and is never reached here.
    """
    tree = _first(node, 'token_tree')
    if tree is None:
        return ()
    # Pad with the lines that precede the body so every recovered item reports
    # its real line in the file rather than an offset into the macro.
    inner = '\n' * tree.start_point[0] + _text(tree)[1:-1]
    parsed = _parser().parse(inner.encode())
    trees.append(parsed)
    return _items(parsed.root_node, trees)


def _item(
    node: Node, attrs: tuple[Attr, ...], doc: str, macro_trees: list[Tree]
) -> Item:
    name_node, type_node = _name_and_type(node)
    if node.type == 'macro_invocation':
        return Item(
            kind=node.type,
            name=_name(name_node) if name_node else '',
            ty='',
            node=node,
            type_node=None,
            line=node.start_point[0] + 1,
            doc=doc,
            attrs=attrs,
            children=_macro_body_items(node, macro_trees),
        )
    body = next((child for child in node.children if child.type in _CONTAINERS), None)
    return Item(
        kind=node.type,
        # `impl<T> crate::Foo<T>` names `Foo`, and `crate::tokens::token_enum!`
        # names `token_enum`: neither the path nor the type parameters are part
        # of the identifier a scan matches against.
        name=_name(name_node) if name_node else '',
        ty=_text(type_node) if type_node else '',
        node=node,
        type_node=type_node,
        line=node.start_point[0] + 1,
        doc=doc,
        attrs=attrs,
        children=_items(body, macro_trees) if body is not None else (),
    )


def _items(container: Node, macro_trees: list[Tree]) -> tuple[Item, ...]:
    items: list[Item] = []
    attrs: list[Attr] = []
    doc: list[str] = []
    for child in container.children:
        if child.type == 'attribute_item':
            attr = _attribute(child)
            if attr is not None:
                attrs.append(attr)
            continue
        if child.type == 'line_comment':
            line = _doc_line(child)
            if line is not None:
                doc.append(line)
            continue
        if child.type in _TRIVIA:
            continue
        node = child
        if node.type == 'expression_statement' and node.named_children:
            node = node.named_children[0]  # `foo!(A, "B");` at item position
        pending, attrs = tuple(attrs), []
        prose, doc = '\n'.join(doc).strip(), []
        if node.is_named:
            items.append(_item(node, pending, prose, macro_trees))
    return tuple(items)


def _parse_file(path: Path, src_root: Path) -> RustSource:
    tree = _parser().parse(path.read_bytes())
    macro_trees: list[Tree] = []
    items = _items(tree.root_node, macro_trees)
    return RustSource(
        rel=path.relative_to(src_root).as_posix(),
        label=path.relative_to(src_root.parent).as_posix(),
        tree=tree,
        macro_trees=tuple(macro_trees),
        items=items,
    )


def _export_items(items: tuple[Item, ...]) -> tuple[Item, ...]:
    exported: list[Item] = []
    for item in items:
        if _excluded_from_export(item.attrs):
            continue
        children = _export_items(item.children)
        unchanged = len(children) == len(item.children) and all(
            child is original
            for child, original in zip(children, item.children, strict=True)
        )
        exported.append(item if unchanged else replace(item, children=children))
    return tuple(exported)


def _parsed(
    src_root: Path,
    stamp: tuple[tuple[Path, int, int], ...],
    scope: Literal['export', 'all'],
) -> tuple[RustSource, ...]:
    with _PARSE_LOCK:
        return _parsed_once(src_root, stamp, scope)


@lru_cache(maxsize=8)
def _parsed_once(
    src_root: Path,
    stamp: tuple[tuple[Path, int, int], ...],
    scope: Literal['export', 'all'],
) -> tuple[RustSource, ...]:
    raw = _parsed_raw(src_root, stamp)
    if scope == 'all':
        return raw
    return tuple(
        source
        if (items := _export_items(source.items)) == source.items
        else replace(source, items=items)
        for source in raw
    )


@lru_cache(maxsize=8)
def _parsed_raw(
    src_root: Path, stamp: tuple[tuple[Path, int, int], ...]
) -> tuple[RustSource, ...]:
    return tuple(_parse_file(path, src_root) for path, _, _ in stamp)
