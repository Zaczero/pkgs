"""Check manual PyO3 ``text_signature`` overrides against real ``signature`` attrs."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig
    from pyo3stubs.context import CheckContext

#: Defaults comparable across the two attribute languages: Python/Rust shared
#: literals (numbers, quoted strings, ``None``, booleans). Anything else (Rust
#: expressions like ``f64::NAN`` or ``Vec::new()``) has a legitimately
#: different spelling in ``text_signature`` and is structure-checked only.
_SIMPLE_DEFAULT = re.compile(
    r'^(?:-?\d+(?:\.\d+)?|"[^"]*"|\'[^\']*\'|None|True|False|true|false)$'
)


def _attr_params(inner: str) -> list[tuple[str, str | None]]:
    """Top-level ``(token, default)`` pairs of a signature/text_signature body.

    Tokens keep their ``*`` / ``**`` / ``/`` markers so parameter kinds are
    compared, not just names.
    """
    parts, depth, cur = [], 0, ''
    for ch in inner:
        if ch in '([{':
            depth += 1
        elif ch in ')]}':
            depth -= 1
        if ch == ',' and depth == 0:
            parts.append(cur)
            cur = ''
        else:
            cur += ch
    if cur.strip():
        parts.append(cur)
    params: list[tuple[str, str | None]] = []
    for part in parts:
        token, _, default = part.partition('=')
        token = token.strip()
        if token in ('$self', 'self'):
            continue
        params.append((token, default.strip() or None))
    return params


def _defaults_comparable(rust: str | None, text: str | None) -> bool:
    """False only when both defaults are simple literals that disagree.

    A Rust default of ``None`` with a concrete text literal is the deliberate
    resolve-inside idiom: the parameter is ``Option<T>`` resolved to its
    effective default in the body, and ``text_signature`` documents that
    effective value (``clip='padded'``) — better DX than a bare ``None``.
    """
    if rust is None or text is None:
        return (rust is None) == (text is None)
    if rust == 'None' or text == '...':
        return True
    if _SIMPLE_DEFAULT.match(rust) and _SIMPLE_DEFAULT.match(text):
        return rust.lower() == text.lower() if rust in ('true', 'false') else (
            rust == text
        )
    return True


@dataclass(frozen=True)
class RustTextSignatureCheck:
    """A manual ``text_signature`` must not contradict the real ``signature``:
    same parameter tokens (names and ``*``/``**``/``/`` markers), same
    has-default shape, and equal defaults where both are simple literals.
    """

    def collect(self, cfg: StubConfig, ctx: CheckContext) -> list[str]:
        errors: list[str] = []
        root = cfg.src_root
        repo_root = root.parent
        for path in sorted(root.rglob('*.rs')):
            source = path.read_text()
            for match in re.finditer(r'#\[(?:pyo3|pyfunction)\(', source):
                depth, end = 0, match.end() - 1
                while True:
                    if source[end] == '(':
                        depth += 1
                    elif source[end] == ')':
                        depth -= 1
                    if depth == 0:
                        break
                    end += 1
                block = source[match.start() : end + 1]
                text_match = re.search(
                    r'text_signature = "\((.*?)\)"', block, re.DOTALL
                )
                sig_match = re.search(r'signature = \(', block)
                if text_match is None or sig_match is None:
                    continue
                depth, sig_end = 1, sig_match.end()
                while depth:
                    if block[sig_end] == '(':
                        depth += 1
                    elif block[sig_end] == ')':
                        depth -= 1
                    sig_end += 1
                sig_body = ' '.join(block[sig_match.end() : sig_end - 1].split())
                sig = _attr_params(sig_body)
                text = _attr_params(' '.join(text_match.group(1).split()))
                # ``copy.replace`` surfaces ``**kwargs`` at runtime; the manual
                # text_signature documents the public keyword names instead.
                if '**kwargs' in sig_body:
                    continue
                line = source.count('\n', 0, match.start()) + 1
                location = f'{path.relative_to(repo_root)}:{line}'
                if [t for t, _ in sig] != [t for t, _ in text]:
                    errors.append(
                        f'{location}: text_signature params '
                        f'{[t for t, _ in text]} != signature params '
                        f'{[t for t, _ in sig]}'
                    )
                    continue
                for (token, rust_default), (_, text_default) in zip(
                    sig, text, strict=True
                ):
                    if (rust_default is None) != (text_default is None):
                        errors.append(
                            f'{location}: {token}: text_signature default '
                            f'{text_default!r} != signature default '
                            f'{rust_default!r} (has-default shape differs)'
                        )
                    elif not _defaults_comparable(rust_default, text_default):
                        errors.append(
                            f'{location}: {token}: text_signature default '
                            f'{text_default} != signature default {rust_default}'
                        )
        return errors
