"""Python constructor facts for h2corn's generated configuration reference."""

from __future__ import annotations

import inspect
import types
from functools import cache
from typing import Any, Literal, Union, get_args, get_origin, get_type_hints

from h2corn._config import Config, ConfigOption, config_options


def format_type(annotation: Any) -> str:
    if annotation is Any:
        return 'Any'
    if annotation is type(None):
        return 'None'

    origin = get_origin(annotation)
    arguments = get_args(annotation)
    if origin in {Union, types.UnionType}:
        return ' | '.join(format_type(argument) for argument in arguments)
    if origin is Literal:
        return 'Literal[' + ', '.join(repr(argument) for argument in arguments) + ']'
    if origin is tuple and len(arguments) == 2 and arguments[1] is Ellipsis:
        return f'tuple[{format_type(arguments[0])}, ...]'
    if origin is not None:
        name = getattr(origin, '__qualname__', getattr(origin, '__name__', str(origin)))
        return (
            f'{name}['
            + ', '.join(format_type(argument) for argument in arguments)
            + ']'
        )
    if isinstance(annotation, type):
        return annotation.__qualname__
    return str(annotation).removeprefix('typing.')


@cache
def _stored_types() -> dict[str, Any]:
    return get_type_hints(Config)


def stored_type(name: str) -> Any:
    return _stored_types()[name]


def python_input_type(option: ConfigOption) -> Any:
    parameters = inspect.signature(option.metadata.normalize).parameters
    parameter = next(iter(parameters.values()), None)
    if parameter is None or parameter.annotation is inspect.Parameter.empty:
        return stored_type(option.name)

    try:
        annotation = get_type_hints(option.metadata.normalize).get(
            parameter.name, parameter.annotation
        )
    except (NameError, TypeError):
        annotation = parameter.annotation
    if annotation is Any or annotation is object:
        return stored_type(option.name)
    return annotation


def python_constructor_description() -> str:
    parameters = inspect.signature(Config).parameters
    if any(
        option.name not in parameters
        or parameters[option.name].kind is not inspect.Parameter.KEYWORD_ONLY
        for option in config_options()
    ):
        raise RuntimeError('Config option reference expects keyword-only fields')
    return (
        '`Config(...)` takes keyword-only options. Each entry lists the accepted '
        'Python input type, the normalized stored type, and any conversion the '
        'constructor applies.'
    )
