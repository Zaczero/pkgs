"""Generate the API reference from one exact page manifest.

The handwritten stub owns signatures, member order, and documentation.  This
module only assigns public symbols to pages and emits mkdocstrings directives.
"""

from __future__ import annotations

import pathlib
import sys

import gometry as gm
import mkdocs_gen_files

_HERE = pathlib.Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from api_structure import (
    CLASS_EXPORTS,
    GEOMETRY_LEAVES,
    OPTIONAL_EXPORTS,
    PAGES,
    Page,
    Section,
    resolved_symbols,
)

# Executable guide figures import ``_figures`` directly.  The docs build runs
# every page in this process, so one stable examples path serves all snippets.
_EXAMPLES = pathlib.Path(__file__).resolve().parents[2] / 'examples'
if str(_EXAMPLES) not in sys.path:
    sys.path.insert(0, str(_EXAMPLES))

_CORE_EXPORTS = set(gm.__all__)
_OPTIONAL_EXPORTS = set(OPTIONAL_EXPORTS)
_EXPORTS = _CORE_EXPORTS | _OPTIONAL_EXPORTS


def _validate_manifest() -> None:
    placements: dict[str, list[str]] = {}
    for page in PAGES:
        for name in resolved_symbols(page, _EXPORTS):
            placements.setdefault(name, []).append(page.path)

    duplicate = {name: pages for name, pages in placements.items() if len(pages) != 1}
    missing = _EXPORTS - placements.keys()
    stale = placements.keys() - _EXPORTS
    if duplicate or missing or stale:
        raise RuntimeError(
            'API page manifest must partition gometry.__all__ exactly: '
            f'duplicate={duplicate!r}, missing={sorted(missing)!r}, '
            f'stale={sorted(stale)!r}'
        )

    optional = _OPTIONAL_EXPORTS
    interop = next(page for page in PAGES if page.path == 'interop')
    placed_optional = set(resolved_symbols(interop, _EXPORTS))
    if optional != placed_optional:
        raise RuntimeError(
            'optional API partition drift: '
            f'missing={sorted(optional - placed_optional)!r}, '
            f'extra={sorted(placed_optional - optional)!r}'
        )


_validate_manifest()

_FUNCTION_OPTIONS = (
    '    options:\n'
    '      heading_level: 2\n'
    '      show_root_heading: true\n'
    '      show_symbol_type_toc: true\n'
)
_CLASS_OPTIONS = (
    '    options:\n'
    '      heading_level: 2\n'
    '      show_root_heading: true\n'
    '      inherited_members: true\n'
    '      members_order: alphabetical\n'
    '      show_symbol_type_toc: true\n'
)
_OWN_CLASS_OPTIONS = (
    '    options:\n'
    '      heading_level: 2\n'
    '      show_root_heading: true\n'
    '      inherited_members: false\n'
    '      members_order: alphabetical\n'
    '      show_symbol_type_toc: true\n'
)


def _options(name: str) -> str:
    if name not in CLASS_EXPORTS:
        return _FUNCTION_OPTIONS
    if name in GEOMETRY_LEAVES:
        return _OWN_CLASS_OPTIONS
    return _CLASS_OPTIONS


def _write_symbol(file: object, name: str) -> None:
    file.write(f'::: gometry.{name}\n')
    file.write(_options(name))
    file.write('\n')


def _write_section(file: object, section_spec: Section) -> None:
    if section_spec.title:
        file.write(f'## {section_spec.title}\n\n')
    for name in (
        *section_spec.symbols,
        *(
            name
            for name in sorted(_EXPORTS)
            if section_spec.prefixes and name.startswith(section_spec.prefixes)
        ),
    ):
        _write_symbol(file, name)


def _write_page(page: Page) -> None:
    with mkdocs_gen_files.open(f'api/{page.path}.md', 'w') as file:
        description = page.description.replace('"', "'")
        file.write(
            '---\n'
            'api_reference: true\n'
            f'description: "{description}"\n'
            '---\n\n'
            f'# {page.title}\n\n'
            f'{page.description}\n\n'
        )
        if page.related:
            links = ' · '.join(f'[{label}]({target})' for label, target in page.related)
            file.write(f'**Related:** {links}\n\n')
        for section_spec in page.sections:
            _write_section(file, section_spec)


for _page in PAGES:
    _write_page(_page)


with mkdocs_gen_files.open('api/index.md', 'w') as file:
    file.write(
        '---\n'
        'api_reference: true\n'
        'description: "Complete typed reference for gometry functions and classes."\n'
        '---\n\n'
        '# API reference\n\n'
        'Signatures and prose come from the handwritten stub and native '
        'docstrings. Each public symbol appears on exactly one page.\n\n'
        '| Page | Contents |\n'
        '|---|---|\n'
    )
    for page in PAGES:
        file.write(f'| [{page.title}]({page.path}.md) | {page.description} |\n')
    file.write(
        '\n'
        '## Route by result shape\n\n'
        'Use the operation pages above for signatures. These links route by the '
        'shape and boundary contract of the result without repeating those signatures.\n\n'
        '| Need | Start here | Contract |\n'
        '|---|---|---|\n'
        '| One scalar geometry or scalar fact | [Geometry](geometry/geometry.md) | A scalar input returns a scalar value or geometry. |\n'
        '| Packed array and dense numeric result | [GeometryArray](geometry/geometryarray.md) | Array operations return row-aligned read-only NumPy arrays or a `GeometryArray`. |\n'
        '| Missing rows | [Missing rows](../guide/arrays.md#missing-rows) | Missing rows remain row-aligned; fixed-width lanes use sentinels and `.is_missing` is authoritative. |\n'
        '| Coordinate axes | [Coordinates & dimensions](geometry/coordinates.md) | Scalar `coordinate_axes` is one layout; array `coordinate_axes` is per row and `common_coordinate_axes` is shared-or-`None`. |\n'
        '| Ragged matches or grouped output | [Groups](geometry/coordinates.md#gometry.Groups) · [Features](results.md#gometry.Features) | `Groups` stores row offsets; named result records keep related fields together. |\n'
        '| Optional dataframe, storage, or map interop | [Optional interoperability](interop.md) | Optional adapters are explicit extras; each page states copy, CRS, epoch, missing-row, and loss behavior. |\n'
        '\n'
        '## Route by operation\n\n'
        '- [Measure or navigate](toplevel/measurement.md) returns scalar or array numeric values; '
        'geodesy and metric units are described in [CRS, units & measurement](../guide/crs.md).\n'
        '- [Build, transform, or edit geometry](toplevel/constructors-bulk-factories.md) starts with '
        'constructors and continues through [Geometry](geometry/geometry.md).\n'
        '- [Relate geometries](toplevel/predicates.md) uses free predicates and strict scalar/array broadcasting.\n'
        '- [Find candidates, refine, or join](toplevel/index-join.md) returns integer arrays or '
        '[`Groups`](geometry/coordinates.md#gometry.Groups); candidates are not exact matches.\n'
        '- [Exchange data](toplevel/io-interop.md) covers text, binary, GeoJSON, and Arrow; '
        '[optional interop](interop.md) covers dataframe and map adapters.\n'
    )
