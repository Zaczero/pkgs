from functools import wraps
from html import unescape
from random import getrandbits

from htmlmin import Minifier

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Collection
    from typing import TypedDict, TypeVar

    try:
        from typing import Unpack  # type: ignore
    except ImportError:
        try:
            from typing_extensions import Unpack  # type: ignore
        except ImportError:
            from typing import Any as Unpack

    from htmlmin.parser import HTMLMinParser
    from jinja2 import BaseLoader

    _Loader = TypeVar('_Loader', bound=BaseLoader)

    class _HTMLMinKwargs(TypedDict, total=False):
        remove_comments: bool
        remove_empty_space: bool
        remove_all_empty_space: bool
        reduce_empty_attributes: bool
        reduce_boolean_attributes: bool
        remove_optional_attribute_quotes: bool
        convert_charrefs: bool
        keep_pre: bool
        pre_tags: Collection[str]
        pre_attr: str
        cls: type[HTMLMinParser]


__version__ = '1.0.1'


def _make_placeholder_prefix(source: str) -> str:
    """Return a per-template placeholder prefix absent from raw and decoded source."""
    if '&' in source:
        source = unescape(source)

    while True:
        prefix = f'j2h{getrandbits(32):08x}`'
        if prefix not in source:
            return prefix


def _restore_placeholders(
    source: str,
    placeholder_prefix: str,
    lookup: list[str],
) -> str:
    """Restore placeholders created with a prefix unique to this loader call."""
    parts = iter(source.split(placeholder_prefix))
    out = [next(parts)]
    append = out.append

    for part in parts:
        index, _, rest = part.partition('z')
        append(lookup[int(index)])
        append(rest)

    return ''.join(out)


def _protect_jinja_syntax(
    source: str,
    environment,
    template: str,
    path: 'str | None',
) -> tuple[str, list[str], str]:
    """Replace lexer-owned Jinja spans with placeholders before minification."""
    placeholder_prefix = ''
    lookup: list[str] = []
    out: list[str] = []
    parts: list[str] | None = None
    end_token: str | None = None

    def add_placeholder(value: str) -> str:
        nonlocal placeholder_prefix
        if not placeholder_prefix:
            placeholder_prefix = _make_placeholder_prefix(source)

        id_ = len(lookup)
        lookup.append(value)
        return f'{placeholder_prefix}{id_:05d}z'

    for _, token, value in environment.lex(source, template, path):
        if parts is not None:
            parts.append(value)
            if token == end_token:
                out.append(add_placeholder(''.join(parts)))
                parts = None
                end_token = None
            continue

        if token == 'data':
            out.append(value)
        elif token.endswith('_begin'):
            if token in {'linestatement_begin', 'linecomment_begin'} and out:
                line_start = out[-1].rfind('\n')
                if line_start != -1:
                    parts = [out[-1][line_start:], value]
                    out[-1] = out[-1][:line_start]
                else:
                    parts = [value]
            else:
                parts = [value]
            end_token = f'{token[:-5]}end'
        else:
            raise RuntimeError(f'unexpected Jinja token outside tag: {token}')

    return ''.join(out), lookup, placeholder_prefix


def minify_loader(
    loader: '_Loader',
    /,
    **kwargs: 'Unpack[_HTMLMinKwargs]',
) -> '_Loader':
    """
    Enhance a Jinja2 loader to automatically minify HTML templates.

    Wraps the loader's get_source method to apply HTML minification while
    preserving Jinja2 syntax. The minification occurs at template load time,
    before Jinja2 compilation.

    :param loader: Jinja2 loader instance to enhance
    :param kwargs: Minifier configuration options (see
                   https://htmlmin.readthedocs.io/en/latest/reference.html)
    :return: The same loader instance with minification enabled

    Example::

        from jinja2 import FileSystemLoader
        from jinja2_htmlmin import minify_loader

        loader = minify_loader(
            FileSystemLoader("templates"),
            remove_comments=True,
            remove_empty_space=True,
            remove_all_empty_space=True,
            reduce_boolean_attributes=True,
        )
    """
    minifier = Minifier(**kwargs)

    super_get_source = loader.get_source

    @wraps(super_get_source)
    def get_source(environment, template):
        source, path, up_to_date = super_get_source(environment, template)
        source, lookup, placeholder_prefix = _protect_jinja_syntax(
            source,
            environment,
            template,
            path,
        )
        source = minifier.minify(source)

        if lookup:
            source = _restore_placeholders(source, placeholder_prefix, lookup)

        return source, path, up_to_date

    loader.get_source = get_source
    return loader
