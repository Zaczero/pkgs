"""Shared error handling for lazily imported optional dependencies."""

from __future__ import annotations


def missing_optional_dependency(
    error: ModuleNotFoundError,
    target: str,
    message: str,
) -> ModuleNotFoundError:
    """Rewrite absence of ``target`` without hiding broken dependencies."""
    if error.name != target and not (error.name or '').startswith(f'{target}.'):
        raise error
    return ModuleNotFoundError(message, name=target)
