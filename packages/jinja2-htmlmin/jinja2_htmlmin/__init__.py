import re
from functools import wraps
from typing import TypedDict

from htmlmin import Minifier

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Collection
    from typing import TypeVar

    try:
        from typing import Unpack  # type: ignore
    except ImportError:
        try:
            from typing_extensions import Unpack  # type: ignore
        except ImportError:
            from typing import Any as Unpack

    from htmlmin.parser import HTMLMinParser
    from jinja2 import BaseLoader

    _Loader = TypeVar("_Loader", bound=BaseLoader)

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


__version__ = "1.0.0"

_ESCAPE_RE = re.compile(r"\{(?:\{.*?\}|%.*?%|#.*?#)\}")
_UNESCAPE_RE = re.compile(r"__jinja2_htmlmin (\d+)__")


def minify_loader(loader: _Loader, /, **kwargs: "Unpack[_HTMLMinKwargs]") -> _Loader:
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

        # Replace Jinja syntax with unique entities
        def repl_escape(match: re.Match[str]) -> str:
            id_ = len(lookup)
            lookup.append(match[0])
            return f"__jinja2_htmlmin {id_:05d}__"

        lookup: list[str] = []
        source = _ESCAPE_RE.sub(repl_escape, source)

        # Minify the source
        source = minifier.minify(source)

        # Restore Jinja syntax
        def repl_unescape(match: re.Match[str]) -> str:
            return lookup[int(match[1])]

        source = _UNESCAPE_RE.sub(repl_unescape, source)

        return source, path, up_to_date

    loader.get_source = get_source
    return loader
