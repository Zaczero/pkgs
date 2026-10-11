"""Source-derived reference definitions for types used by the public API."""

from __future__ import annotations

import ast
import builtins
import copy
import sys
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Literal

_PACKAGE = 'gometry'
_BUILTINS = frozenset({
    'None',
    'bool',
    'bytes',
    'bytearray',
    'complex',
    'dict',
    'ellipsis',
    'float',
    'frozenset',
    'int',
    'list',
    'memoryview',
    'object',
    'property',
    'range',
    'set',
    'slice',
    'str',
    'tuple',
    'type',
})
_TYPEVAR_FACTORIES = frozenset({'ParamSpec', 'TypeVar', 'TypeVarTuple'})


class TypeReferenceError(RuntimeError):
    """A source annotation refers to a missing in-package type declaration."""


@dataclass(slots=True)
class _Module:
    name: str
    path: Path
    tree: ast.Module
    is_package: bool = False
    imports: dict[str, str] = field(default_factory=dict)
    module_imports: dict[str, str] = field(default_factory=dict)
    declarations: dict[str, _Declaration] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class _Declaration:
    path: str
    name: str
    kind: Literal['alias', 'typevar', 'typed-dict', 'protocol', 'namedtuple', 'class']
    module: _Module
    node: ast.AST
    expression: ast.expr | None
    prose: str | None
    line: int


@dataclass(frozen=True, slots=True)
class TypeExpression:
    """An unevaluated expression and the module that owns its names."""

    expression: ast.expr
    module: _Module
    source_path: Path
    line: int


@dataclass(frozen=True, slots=True)
class TypeGraph:
    """Reachable declarations, inline expansions, and canonical references."""

    definitions: tuple[_Declaration, ...]
    expansions: dict[str, TypeExpression]
    references: dict[str, tuple[str, ...]]
    public_targets: dict[str, str]
    public_owners: dict[str, tuple[str, ...]]


def build_public_type_graph(package_root: Path, runtime_module: Any) -> TypeGraph:
    """Build the graph from the shared public subjects and facade declarations."""
    tools_root = package_root.parent.parent / 'tools'
    if str(tools_root) not in sys.path:
        sys.path.insert(0, str(tools_root))
    from public_inventory import load_public_symbols
    from pyo3stubs.cli import load_config
    from pyo3stubs.context import CheckContext
    from pyo3stubs.doc_structure import iter_doc_subjects

    inventory = load_public_symbols(
        package_root / '_lib.pyi',
        package_root / '__init__.py',
    )
    config = load_config(tools_root / 'stubs' / 'stubconfig.py')
    subjects = tuple(
        iter_doc_subjects(
            CheckContext(config),
            runtime_module=runtime_module,
            constructor_sources={},
            source_classes=inventory.source_classes,
        )
    )
    return build_type_graph(
        package_root,
        subjects=subjects,
        source_classes=inventory.classes,
        public_names=set(runtime_module.__all__),
    )


def _is_type_checking(expression: ast.expr) -> bool:
    if isinstance(expression, ast.Name):
        return expression.id == 'TYPE_CHECKING'
    if isinstance(expression, ast.Attribute):
        return expression.attr == 'TYPE_CHECKING'
    if isinstance(expression, ast.BoolOp):
        return any(_is_type_checking(value) for value in expression.values)
    return False


def _module_statements(body: list[ast.stmt]) -> list[ast.stmt]:
    result: list[ast.stmt] = []
    for statement in body:
        if isinstance(statement, ast.If):
            if _is_type_checking(statement.test):
                result.extend(_module_statements(statement.body))
            else:
                result.extend(_module_statements(statement.body))
                result.extend(_module_statements(statement.orelse))
        else:
            result.append(statement)
    return result


def _module_name(path: Path, package_root: Path) -> str:
    relative = path.resolve().relative_to(package_root.resolve())
    parts = list(relative.with_suffix('').parts)
    if parts and parts[-1] == '__init__':
        parts.pop()
    return '.'.join((_PACKAGE, *parts))


def _relative_import(module: _Module, node: ast.ImportFrom) -> str:
    if not node.level:
        return node.module or ''
    package_parts = (
        module.name.split('.') if module.is_package else module.name.split('.')[:-1]
    )
    keep = len(package_parts) - node.level + 1
    prefix = package_parts[:keep]
    suffix = (node.module or '').split('.')
    return '.'.join(part for part in (*prefix, *suffix) if part)


def _record_imports(module: _Module) -> None:
    for statement in _module_statements(module.tree.body):
        if isinstance(statement, ast.ImportFrom):
            imported_module = _relative_import(module, statement)
            for alias in statement.names:
                if alias.name == '*':
                    continue
                local_name = alias.asname or alias.name
                module.imports.setdefault(
                    local_name,
                    f'{imported_module}.{alias.name}'
                    if imported_module
                    else alias.name,
                )
        elif isinstance(statement, ast.Import):
            for alias in statement.names:
                if alias.asname:
                    module.module_imports.setdefault(alias.asname, alias.name)
                else:
                    root = alias.name.split('.', 1)[0]
                    module.module_imports.setdefault(root, root)


def _dotted_name(expression: ast.expr) -> str | None:
    if isinstance(expression, ast.Name):
        return expression.id
    if isinstance(expression, ast.Attribute):
        parent = _dotted_name(expression.value)
        return f'{parent}.{expression.attr}' if parent else None
    return None


def _resolve_name(module: _Module, expression: ast.expr) -> str | None:
    if isinstance(expression, ast.Name):
        if expression.id in module.imports:
            return module.imports[expression.id]
        if expression.id in _BUILTINS or hasattr(builtins, expression.id):
            return None
        return f'{module.name}.{expression.id}'

    dotted = _dotted_name(expression)
    if dotted is None:
        return None
    head, separator, tail = dotted.partition('.')
    if head in module.module_imports:
        return module.module_imports[head] + (separator + tail if separator else '')
    if head in module.imports:
        return module.imports[head] + (separator + tail if separator else '')
    if dotted == _PACKAGE or dotted.startswith(f'{_PACKAGE}.'):
        return dotted
    return None


def _last_name(expression: ast.expr | None) -> str | None:
    name = _dotted_name(expression) if expression is not None else None
    return name.rsplit('.', 1)[-1] if name else None


def _is_type_alias(annotation: ast.expr | None, module: _Module) -> bool:
    if annotation is None:
        return False
    return (
        _resolve_name(module, annotation)
        in {
            'typing.TypeAlias',
            'typing_extensions.TypeAlias',
        }
        or _last_name(annotation) == 'TypeAlias'
    )


def _is_typevar_call(expression: ast.expr | None, module: _Module) -> bool:
    if not isinstance(expression, ast.Call):
        return False
    target = _resolve_name(module, expression.func)
    return (
        target is not None and target.rsplit('.', 1)[-1] in _TYPEVAR_FACTORIES
    ) or _last_name(expression.func) in {'_DefaultTypeVar', *_TYPEVAR_FACTORIES}


def _leading_comments(path: Path, node: ast.AST) -> str | None:
    lines = path.read_text(encoding='utf-8').splitlines()
    index = getattr(node, 'lineno', 1) - 2
    comments = []
    while index >= 0:
        line = lines[index].strip()
        if not line.startswith('#'):
            break
        comments.append(line[1:].removeprefix(':').strip())
        index -= 1
    if not comments:
        return None
    return '\n'.join(reversed(comments)).strip() or None


def _following_docstring(module: _Module, node: ast.AST) -> str | None:
    for parent in ast.walk(module.tree):
        body = getattr(parent, 'body', None)
        if not isinstance(body, list):
            continue
        try:
            index = body.index(node)
        except ValueError:
            continue
        if index + 1 >= len(body):
            continue
        following = body[index + 1]
        if (
            isinstance(following, ast.Expr)
            and isinstance(following.value, ast.Constant)
            and isinstance(following.value.value, str)
        ):
            return following.value.value
    return None


def _class_kind(node: ast.ClassDef, module: _Module) -> str:
    targets = [_resolve_name(module, base) for base in node.bases]
    bases = {_last_name(base) for base in node.bases}
    if (
        'TypedDict' in bases
        or any(
            target in {'typing.TypedDict', 'typing_extensions.TypedDict'}
            for target in targets
        )
        or any(
            (declaration := module.declarations.get(target.rsplit('.', 1)[-1]))
            is not None
            and declaration.kind == 'typed-dict'
            for target in targets
            if target is not None and target.startswith(f'{module.name}.')
        )
    ):
        return 'typed-dict'
    if (
        'Protocol' in bases
        or any(
            target in {'typing.Protocol', 'typing_extensions.Protocol'}
            for target in targets
        )
        or any(
            (declaration := module.declarations.get(target.rsplit('.', 1)[-1]))
            is not None
            and declaration.kind == 'protocol'
            for target in targets
            if target is not None and target.startswith(f'{module.name}.')
        )
    ):
        return 'protocol'
    if 'NamedTuple' in bases or any(
        target in {'typing.NamedTuple', 'typing_extensions.NamedTuple'}
        for target in targets
    ):
        return 'namedtuple'
    return 'class'


def _add_declaration(
    module: _Module,
    name: str,
    kind: str,
    node: ast.AST,
    expression: ast.expr | None,
) -> None:
    if isinstance(node, ast.ClassDef):
        prose = ast.get_docstring(node)
    else:
        prose = (
            '\n\n'.join(
                item
                for item in (
                    _leading_comments(module.path, node),
                    _following_docstring(module, node),
                )
                if item
            )
            or None
        )
    declaration = _Declaration(
        path=f'{module.name}.{name}',
        name=name,
        kind=kind,  # type: ignore[arg-type]
        module=module,
        node=node,
        expression=expression,
        prose=prose,
        line=getattr(node, 'lineno', 1),
    )
    module.declarations[name] = declaration


def _index_declarations(module: _Module) -> None:
    for statement in _module_statements(module.tree.body):
        if isinstance(statement, ast.ClassDef):
            _add_declaration(
                module,
                statement.name,
                _class_kind(statement, module),  # type: ignore[arg-type]
                statement,
                None,
            )
        elif isinstance(statement, ast.AnnAssign) and isinstance(
            statement.target, ast.Name
        ):
            if _is_type_alias(statement.annotation, module):
                _add_declaration(
                    module,
                    statement.target.id,
                    'alias',
                    statement,
                    statement.value,
                )
        elif isinstance(statement, ast.Assign):
            names = [
                target.id
                for target in statement.targets
                if isinstance(target, ast.Name)
            ]
            if _is_typevar_call(statement.value, module):
                for name in names:
                    _add_declaration(
                        module, name, 'typevar', statement, statement.value
                    )
        elif isinstance(statement, ast.TypeAlias):
            name = _dotted_name(statement.name)
            if name is not None:
                _add_declaration(module, name, 'alias', statement, statement.value)
    for name, declaration in tuple(module.declarations.items()):
        if isinstance(declaration.node, ast.ClassDef):
            module.declarations[name] = replace(
                declaration,
                kind=_class_kind(declaration.node, module),  # type: ignore[arg-type]
            )


def _load_modules(package_root: Path) -> tuple[dict[str, _Module], dict[Path, _Module]]:
    modules_by_name: dict[str, _Module] = {}
    modules_by_path: dict[Path, _Module] = {}
    for path in sorted((*package_root.rglob('*.py'), *package_root.rglob('*.pyi'))):
        if '__pycache__' in path.parts:
            continue
        module = _Module(
            _module_name(path, package_root),
            path,
            ast.parse(path.read_text(encoding='utf-8'), filename=str(path)),
            is_package=path.stem == '__init__',
        )
        modules_by_path[path.resolve()] = module
        previous = modules_by_name.get(module.name)
        if previous is None or path.suffix == '.pyi':
            modules_by_name[module.name] = module

    for module in modules_by_path.values():
        _record_imports(module)
    for module in modules_by_path.values():
        _index_declarations(module)
    return modules_by_name, modules_by_path


def _public_names(init_path: Path) -> set[str]:
    tree = ast.parse(init_path.read_text(encoding='utf-8'), filename=str(init_path))
    for statement in tree.body:
        if not isinstance(statement, (ast.Assign, ast.AnnAssign)):
            continue
        targets = (
            statement.targets
            if isinstance(statement, ast.Assign)
            else (statement.target,)
        )
        if not any(
            isinstance(target, ast.Name) and target.id == '__all__'
            for target in targets
        ):
            continue
        try:
            names = ast.literal_eval(statement.value)
        except (ValueError, TypeError):
            break
        if isinstance(names, (tuple, list)) and all(
            isinstance(name, str) for name in names
        ):
            return set(names)
    raise TypeReferenceError(f'{init_path}: cannot read literal __all__')


def _public_targets(
    init_module: _Module,
    public_names: set[str],
) -> dict[str, str]:
    result = {}
    for statement in _module_statements(init_module.tree.body):
        if not isinstance(statement, ast.ImportFrom):
            continue
        source_module = _relative_import(init_module, statement)
        for alias in statement.names:
            public_name = alias.asname or alias.name
            if public_name not in public_names or alias.name == '*':
                continue
            result[f'{source_module}.{alias.name}'] = f'{_PACKAGE}.{public_name}'
    return result


def _module_for_path(
    path: str | Path,
    package_root: Path,
    modules_by_path: dict[Path, _Module],
    modules_by_name: dict[str, _Module],
) -> _Module | None:
    source = Path(path)
    if not source.is_absolute():
        candidates = (
            package_root / source,
            package_root.parent.parent / source,
        )
        source = next(
            (candidate for candidate in candidates if candidate.exists()), source
        )
    module = modules_by_path.get(source.resolve())
    if module is not None:
        return module
    if source.suffix in {'.rs', '.md'}:
        return modules_by_name.get(f'{_PACKAGE}._lib')
    return None


def _function_annotations(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
) -> list[ast.expr]:
    args = [
        *node.args.posonlyargs,
        *node.args.args,
        *node.args.kwonlyargs,
    ]
    if node.args.vararg is not None:
        args.append(node.args.vararg)
    if node.args.kwarg is not None:
        args.append(node.args.kwarg)
    annotations = [argument.annotation for argument in args if argument.annotation]
    if node.returns is not None:
        annotations.append(node.returns)
    return annotations


def _class_annotations(node: ast.ClassDef) -> list[ast.expr]:
    expressions = [*node.bases]
    expressions.extend(
        keyword.value for keyword in node.keywords if keyword.arg != 'metaclass'
    )
    for member in node.body:
        if isinstance(member, ast.AnnAssign) and member.annotation is not None:
            expressions.append(member.annotation)
        elif isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
            expressions.extend(_function_annotations(member))
    for parameter in getattr(node, 'type_params', ()):
        for attribute in ('bound', 'default_value'):
            value = getattr(parameter, attribute, None)
            if isinstance(value, ast.expr):
                expressions.append(value)
    return expressions


def _subject_declarations(module: _Module, qualname: str) -> tuple[ast.AST, ...]:
    parts = qualname.split('.')
    statements = _module_statements(module.tree.body)
    if len(parts) == 1:
        return tuple(
            node
            for node in statements
            if isinstance(
                node,
                (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
            )
            and node.name == parts[0]
        )

    class_node = next(
        (
            node
            for node in statements
            if isinstance(node, ast.ClassDef) and node.name == parts[0]
        ),
        None,
    )
    if class_node is None:
        return ()
    member_name = parts[-1]
    return tuple(
        node
        for node in class_node.body
        if (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == member_name
        )
        or (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == member_name
        )
    )


def _root_expressions(
    subjects: tuple[Any, ...],
    source_classes: Any,
    public_names: set[str],
    package_root: Path,
    modules_by_name: dict[str, _Module],
    modules_by_path: dict[Path, _Module],
) -> list[tuple[ast.expr, _Module, Path, int, str]]:
    roots = []
    seen: set[tuple[str, int, str]] = set()

    def add(node: ast.AST, module: _Module, owner: str) -> None:
        if isinstance(node, ast.ClassDef):
            expressions = _class_annotations(node)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            expressions = _function_annotations(node)
        elif isinstance(node, ast.AnnAssign) and node.annotation is not None:
            expressions = [node.annotation]
        else:
            return
        for expression in expressions:
            key = (
                module.name,
                getattr(expression, 'lineno', node.lineno),
                ast.dump(expression),
            )
            if key not in seen:
                seen.add(key)
                roots.append((
                    expression,
                    module,
                    module.path,
                    getattr(expression, 'lineno', node.lineno),
                    owner,
                ))

    for subject in subjects:
        qualname = getattr(subject, 'qualname', '')
        public_name = qualname.split('.', 1)[0]
        if public_name not in public_names:
            continue
        source = getattr(getattr(subject, 'source', None), 'path', '')
        module = _module_for_path(
            source,
            package_root,
            modules_by_path,
            modules_by_name,
        ) or modules_by_name.get(f'{_PACKAGE}._lib')
        if module is None:
            continue
        declarations = tuple(getattr(subject, 'declarations', ()))
        if not declarations and source:
            declarations = _subject_declarations(module, qualname)
        for declaration in declarations:
            if isinstance(
                declaration,
                (
                    ast.ClassDef,
                    ast.FunctionDef,
                    ast.AsyncFunctionDef,
                    ast.AnnAssign,
                ),
            ):
                add(declaration, module, qualname)

    for public_name, source_class in getattr(source_classes, 'items', lambda: ())():
        if public_name not in public_names:
            continue
        class_node = getattr(source_class, 'node', None)
        location = getattr(source_class, 'source', None)
        if (
            class_node is None
            and getattr(source_class, 'declaration', None) is not None
        ):
            class_node = source_class.declaration.node
            location = source_class.declaration.source
        if not isinstance(class_node, ast.ClassDef):
            continue
        source = getattr(location, 'path', '')
        module = _module_for_path(
            source,
            package_root,
            modules_by_path,
            modules_by_name,
        )
        if module is not None:
            add(class_node, module, public_name)
    return roots


def _is_literal_alias(declaration: _Declaration) -> bool:
    expression = declaration.expression
    if not isinstance(expression, ast.Subscript):
        return False
    target = _resolve_name(declaration.module, expression.value)
    return target in {'typing.Literal', 'typing_extensions.Literal'}


def _expression_references(
    expression: ast.expr,
    module: _Module,
) -> list[str]:
    references: list[str] = []

    def visit(node: ast.expr) -> None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            try:
                parsed = ast.parse(node.value, mode='eval').body
            except SyntaxError:
                return
            visit(parsed)
            return
        if isinstance(node, (ast.Name, ast.Attribute)):
            target = _resolve_name(module, node)
            if target is not None:
                references.append(target)
            return
        if isinstance(node, ast.Subscript):
            target = _resolve_name(module, node.value)
            if target in {'typing.Literal', 'typing_extensions.Literal'}:
                visit(node.value)
                return
            visit(node.value)
            visit(node.slice)
            return
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.expr):
                visit(child)

    visit(expression)
    return list(dict.fromkeys(references))


def _declaration_expressions(declaration: _Declaration) -> list[ast.expr]:
    if declaration.kind == 'alias':
        return [declaration.expression] if declaration.expression is not None else []
    if declaration.kind == 'typevar':
        call = declaration.expression
        if not isinstance(call, ast.Call):
            return []
        return [*call.args[1:], *(keyword.value for keyword in call.keywords)]
    if isinstance(declaration.node, ast.ClassDef):
        return _class_annotations(declaration.node)
    return []


def _missing_reference(
    target: str,
    declaration: _Declaration | None,
    source_path: Path,
    source_line: int,
    chain: tuple[str, ...],
) -> TypeReferenceError:
    location = f'{source_path}:{source_line}'
    path = ' -> '.join((*chain, target))
    owner = f' in {declaration.path}' if declaration is not None else ''
    return TypeReferenceError(
        f'{location}{owner}: unresolved in-package type {target!r} '
        f'(reference chain: {path})'
    )


def build_type_graph(
    package_root: Path,
    *,
    subjects: tuple[Any, ...] = (),
    source_classes: Any = None,
    public_names: set[str] | None = None,
) -> TypeGraph:
    """Resolve public annotation roots without importing or evaluating modules."""
    init_path = package_root / '__init__.py'
    if public_names is None:
        public_names = _public_names(init_path)
    modules_by_name, modules_by_path = _load_modules(package_root)
    init_module = modules_by_path.get(init_path.resolve())
    if init_module is None:
        raise TypeReferenceError(f'{init_path}: source package initializer is missing')
    public_targets = _public_targets(init_module, public_names)
    roots = _root_expressions(
        subjects,
        source_classes,
        public_names,
        package_root,
        modules_by_name,
        modules_by_path,
    )

    declarations = {
        declaration.path: declaration
        for module in modules_by_name.values()
        for declaration in module.declarations.values()
    }
    definitions: dict[str, _Declaration] = {}
    expansions: dict[str, TypeExpression] = {}
    references: dict[str, set[str]] = {}
    public_owners: dict[str, set[str]] = {}

    def visit_expression(
        expression: ast.expr,
        module: _Module,
        source_path: Path,
        source_line: int,
        owner: str | None,
        chain: tuple[str, ...] = (),
        public_owner: str | None = None,
    ) -> None:
        for target in _expression_references(expression, module):
            public_target = public_targets.get(target)
            if public_target is None and target.startswith(f'{_PACKAGE}.'):
                exported_name = target.removeprefix(f'{_PACKAGE}.')
                if '.' not in exported_name and exported_name in public_names:
                    public_target = target
            if public_target is not None:
                if owner is not None:
                    references.setdefault(owner, set()).add(public_target)
                continue
            if not target.startswith(f'{_PACKAGE}.'):
                continue
            if public_owner is not None:
                public_owners.setdefault(target, set()).add(public_owner)
            declaration = declarations.get(target)
            if declaration is None:
                raise _missing_reference(
                    target,
                    None,
                    source_path,
                    source_line,
                    chain,
                )
            if declaration.kind == 'alias' and (
                declaration.name.startswith('_') or _is_literal_alias(declaration)
            ):
                if target in chain:
                    raise TypeReferenceError(
                        f'{source_path}:{source_line}: private alias cycle: '
                        + ' -> '.join((*chain, target))
                    )
                if declaration.expression is None:
                    raise _missing_reference(
                        target,
                        declaration,
                        source_path,
                        source_line,
                        chain,
                    )
                expansions[target] = TypeExpression(
                    declaration.expression,
                    declaration.module,
                    declaration.module.path,
                    declaration.line,
                )
                if not _is_literal_alias(declaration):
                    visit_expression(
                        declaration.expression,
                        declaration.module,
                        declaration.module.path,
                        declaration.line,
                        owner,
                        (*chain, target),
                        public_owner,
                    )
                continue

            if owner is not None:
                references.setdefault(owner, set()).add(target)
            if target in definitions:
                continue
            definitions[target] = declaration
            for child in _declaration_expressions(declaration):
                visit_expression(
                    child,
                    declaration.module,
                    declaration.module.path,
                    getattr(child, 'lineno', declaration.line),
                    target,
                    public_owner=public_owner,
                )

    for expression, module, source_path, source_line, owner in roots:
        public_owner = (
            owner if owner.startswith(f'{_PACKAGE}.') else f'{_PACKAGE}.{owner}'
        )
        if public_owner.endswith(('.__new__', '.__init__')):
            public_owner = public_owner.rsplit('.', 1)[0]
        visit_expression(
            expression,
            module,
            source_path,
            source_line,
            None,
            (owner,),
            public_owner,
        )

    return TypeGraph(
        tuple(definitions[path] for path in sorted(definitions)),
        expansions,
        {path: tuple(sorted(targets)) for path, targets in references.items()},
        public_targets,
        {path: tuple(sorted(owners)) for path, owners in public_owners.items()},
    )


def _expanded_expression(
    expression: ast.expr,
    module: _Module,
    graph: TypeGraph,
    stack: frozenset[str] = frozenset(),
) -> ast.expr:
    class Expand(ast.NodeTransformer):
        def visit_Name(self, node: ast.Name) -> ast.expr:
            return replace(node)

        def visit_Attribute(self, node: ast.Attribute) -> ast.expr:
            return replace(node)

        def visit_Subscript(self, node: ast.Subscript) -> ast.expr:
            target = _resolve_name(module, node.value)
            if target in {'typing.Literal', 'typing_extensions.Literal'}:
                return node
            return self.generic_visit(node)

        def visit_Constant(self, node: ast.Constant) -> ast.expr:
            if not isinstance(node.value, str):
                return node
            try:
                parsed = ast.parse(node.value, mode='eval').body
            except SyntaxError:
                return node
            return replace(parsed)

    def replace(node: ast.expr) -> ast.expr:
        target = _resolve_name(module, node)
        expansion = graph.expansions.get(target or '')
        if expansion is None:
            return node
        if target in stack:
            raise TypeReferenceError(
                f'{expansion.source_path}:{expansion.line}: private alias cycle: '
                + ' -> '.join((*stack, target or ''))
            )
        return _expanded_expression(
            expansion.expression,
            expansion.module,
            graph,
            stack | {target},
        )

    return Expand().visit(copy.deepcopy(expression))


def _expression_source(
    expression: ast.expr,
    module: _Module,
    graph: TypeGraph,
) -> str:
    return ast.unparse(_expanded_expression(expression, module, graph))


def _typed_dict_fields(
    declaration: _Declaration,
    declarations: dict[str, _Declaration],
    seen: frozenset[str] = frozenset(),
) -> list[tuple[str, ast.expr, bool, str | None, _Module]]:
    if declaration.path in seen:
        raise TypeReferenceError(
            f'{declaration.module.path}:{declaration.line}: TypedDict inheritance '
            f'cycle: {" -> ".join((*seen, declaration.path))}'
        )
    if not isinstance(declaration.node, ast.ClassDef):
        return []
    node = declaration.node
    total = True
    for keyword in node.keywords:
        if keyword.arg == 'total' and isinstance(keyword.value, ast.Constant):
            total = bool(keyword.value.value)

    fields: dict[str, tuple[str, ast.expr, bool, str | None, _Module]] = {}
    for base in node.bases:
        target = _resolve_name(declaration.module, base)
        base_declaration = declarations.get(target or '')
        if base_declaration is not None and base_declaration.kind == 'typed-dict':
            fields.update({
                field[0]: field
                for field in _typed_dict_fields(
                    base_declaration,
                    declarations,
                    seen | {declaration.path},
                )
            })

    for index, member in enumerate(node.body):
        if not isinstance(member, ast.AnnAssign) or not isinstance(
            member.target, ast.Name
        ):
            continue
        name = member.target.id
        if name.startswith('_') or member.annotation is None:
            continue
        annotation = member.annotation
        required = total
        if isinstance(annotation, ast.Subscript):
            wrapper = _resolve_name(declaration.module, annotation.value)
            if wrapper in {'typing.Required', 'typing_extensions.Required'}:
                required = True
                annotation = annotation.slice
            elif wrapper in {
                'typing.NotRequired',
                'typing_extensions.NotRequired',
            }:
                required = False
                annotation = annotation.slice
        description = None
        if index + 1 < len(node.body):
            following = node.body[index + 1]
            if (
                isinstance(following, ast.Expr)
                and isinstance(following.value, ast.Constant)
                and isinstance(following.value.value, str)
            ):
                description = following.value.value
        fields[name] = (name, annotation, required, description, declaration.module)
    return list(fields.values())
