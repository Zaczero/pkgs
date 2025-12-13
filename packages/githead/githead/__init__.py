from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from os import PathLike


def githead(git_dir: PathLike[str] | str = '.git') -> str:
    """Get the current git commit hash.

    >>> githead()
    'bca663418428d603eea8243d08a5ded19eb19a34'
    """
    dir_path = Path(git_dir)
    head_path = dir_path.joinpath('HEAD')
    head = head_path.read_text().strip()

    is_symbolic = head[:5] == 'ref: '
    if not is_symbolic:
        return head

    dir_abs = dir_path.resolve()
    ref_abs = dir_abs.joinpath(head[5:]).resolve()
    if not ref_abs.is_relative_to(dir_abs):
        raise ValueError(f'HEAD references outside of {git_dir} directory')

    ref = ref_abs.read_text().strip()
    return ref


__all__ = ('githead',)
