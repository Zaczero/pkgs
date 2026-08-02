"""Config-activated gate: ``Literal`` tokens vs runtime ``token_enum!``.

Activated by ``StubConfig.tokens``; a reported no-op when unset.
"""

from __future__ import annotations

import importlib
import re
import typing
from typing import TYPE_CHECKING

from pyo3stubs.context import CheckContext
from pyo3stubs.report import Findings
from pyo3stubs.rust_scan import iter_sources

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig

#: The enum a token macro declares, inside that macro's own token tree. The
#: search is bounded by the invocation, so it cannot adopt an unrelated enum
#: from further down the file.
_ENUM = re.compile(r'\benum\s+(\w+)')


def _declared_enums(cfg: StubConfig, macro: str) -> set[str]:
    return {
        match.group(1)
        for source in iter_sources(cfg)
        for item in source.walk()
        if item.kind == 'macro_invocation'
        and item.name == macro
        and (match := _ENUM.search(item.text))
    }


def collect_errors(cfg: StubConfig) -> Findings:
    """Compare stub ``Literal`` unions to runtime token-enum exports."""
    conf = cfg.tokens
    if conf is None:
        return Findings()

    ctx = CheckContext(cfg)
    types_mod = importlib.import_module(conf.types_module)
    vocabulary_fn = getattr(ctx.runtime_module, conf.vocabulary_export, None)
    if vocabulary_fn is None:
        return Findings([
            f'token vocabulary: {cfg.module}.{conf.vocabulary_export} missing'
        ])
    vocabulary = vocabulary_fn()

    declared = _declared_enums(cfg, conf.enum_macro)
    exported = {name for name, _, _ in vocabulary}
    errors: list[str] = [
        f'token vocabulary: {conf.enum_macro}! {name} not in {conf.vocabulary_export}()'
        for name in sorted(declared - exported)
    ]
    errors.extend(
        f'token vocabulary: {conf.vocabulary_export}() entry {name} '
        f'has no {conf.enum_macro}!'
        for name in sorted(exported - declared)
    )

    types_label = conf.types_module.rsplit('.', 1)[-1]
    for name, alias, tokens in vocabulary:
        if alias is None:
            continue
        literal = getattr(types_mod, alias, None)
        if literal is None:
            errors.append(
                f'token vocabulary: no {types_label}.{alias} Literal for {name}'
            )
            continue
        stub_tokens = list(typing.get_args(literal))
        if stub_tokens != list(tokens):
            errors.append(
                f'token vocabulary {alias}: stub Literal {stub_tokens} '
                f'!= runtime {list(tokens)}'
            )
    return Findings(errors, examined=len(declared | exported))
