"""Check manual PyO3 ``text_signature`` overrides against real ``signature`` attrs."""

from __future__ import annotations

import re
from dataclasses import dataclass

from pyo3stubcheck.config import StubCheckConfig
from pyo3stubcheck.context import CheckContext


def _attr_params(inner: str) -> list[str]:
    """Top-level parameter tokens of a signature/text_signature body."""
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
    names = []
    for part in parts:
        token = part.split('=', 1)[0].strip()
        if token in ('$self', 'self'):
            continue
        names.append(token)
    return names


@dataclass(frozen=True)
class RustTextSignatureCheck:
    """A manual ``text_signature`` must not contradict the real ``signature``."""

    def collect(self, cfg: StubCheckConfig, ctx: CheckContext) -> list[str]:
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
                sig = _attr_params(
                    ' '.join(block[sig_match.end() : sig_end - 1].split())
                )
                text = _attr_params(' '.join(text_match.group(1).split()))
                if sig != text:
                    line = source.count('\n', 0, match.start()) + 1
                    errors.append(
                        f'{path.relative_to(repo_root)}:{line}: text_signature '
                        f'params {text} != signature params {sig}'
                    )
        return errors