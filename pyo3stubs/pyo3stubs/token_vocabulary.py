"""Config-activated gate: ``Literal`` tokens vs runtime ``token_enum!``.

Activated by ``StubConfig.tokens``; no-op when unset.
"""

from __future__ import annotations

import importlib
import re
import typing
from typing import TYPE_CHECKING

from pyo3stubs.context import CheckContext

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig


def collect_errors(cfg: StubConfig) -> list[str]:
    """Compare stub ``Literal`` unions to runtime token-enum exports."""
    conf = cfg.tokens
    if conf is None:
        return []

    ctx = CheckContext(cfg)
    types_mod = importlib.import_module(conf.types_module)
    runtime = ctx.runtime_module
    vocabulary_fn = getattr(runtime, conf.vocabulary_export, None)
    if vocabulary_fn is None:
        return [
            f'token vocabulary: {cfg.module}.{conf.vocabulary_export} missing'
        ]
    vocabulary = vocabulary_fn()
    macro = re.escape(conf.enum_macro)

    declared = set()
    for path in sorted(cfg.src_root.rglob('*.rs')):
        text = path.read_text()
        for match in re.finditer(rf'{macro}\s*\{{', text):
            enum = re.search(r'enum\s+(\w+)\s*\(', text[match.end() :])
            if enum:
                declared.add(enum.group(1))
    exported = {name for name, _, _ in vocabulary}
    errors: list[str] = []
    errors.extend(
        f'token vocabulary: {conf.enum_macro} {name} not in '
        f'{conf.vocabulary_export}()'
        for name in sorted(declared - exported)
    )
    errors.extend(
        f'token vocabulary: {conf.vocabulary_export}() entry {name} '
        f'has no {conf.enum_macro}'
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
    return errors
