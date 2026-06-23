"""Check ``Literal`` token aliases against runtime ``token_enum!`` vocabularies."""

from __future__ import annotations

import importlib
import re
import typing
from dataclasses import dataclass

if typing.TYPE_CHECKING:
    from pyo3stubs.config import StubConfig
    from pyo3stubs.context import CheckContext


@dataclass(frozen=True)
class TokenVocabularyCheck:
    """Compare stub ``Literal`` unions to runtime token-enum exports."""

    def collect(self, cfg: StubConfig, ctx: CheckContext) -> list[str]:
        if not cfg.types_module or not cfg.token_vocabulary_export:
            return []

        types_mod = importlib.import_module(cfg.types_module)
        runtime = ctx.runtime_module
        vocabulary_fn = getattr(runtime, cfg.token_vocabulary_export, None)
        if vocabulary_fn is None:
            return [
                f'token vocabulary: {cfg.module}.{cfg.token_vocabulary_export} missing'
            ]
        vocabulary = vocabulary_fn()
        macro = re.escape(cfg.token_enum_macro)

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
            f'token vocabulary: {cfg.token_enum_macro} {name} not in '
            f'{cfg.token_vocabulary_export}()'
            for name in sorted(declared - exported)
        )
        errors.extend(
            f'token vocabulary: {cfg.token_vocabulary_export}() entry {name} '
            f'has no {cfg.token_enum_macro}'
            for name in sorted(exported - declared)
        )

        for name, alias, tokens in vocabulary:
            if alias is None:
                continue
            literal = getattr(types_mod, alias, None)
            if literal is None:
                types_label = (
                    cfg.types_module.rsplit('.', 1)[-1]
                    if cfg.types_module
                    else '_types'
                )
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
