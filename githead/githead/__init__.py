from __future__ import annotations

__version__ = '1.2.2'

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from os import PathLike


def githead(git_dir: PathLike[str] | str = '.git') -> str:
    """Read detached HEAD text or a loose symbolic reference.

    Parameters
    ----------
    git_dir
        Path to the Git metadata directory itself.

    Returns
    -------
    str
        The stripped contents of ``HEAD`` or of its referenced loose file.
        Detached contents are not validated as an object ID.

    Raises
    ------
    FileNotFoundError
        If ``HEAD`` or its loose reference file is missing.
    ValueError
        If a symbolic reference resolves outside ``git_dir``.
    OSError
        For other filesystem read errors.
    UnicodeError
        If the file contents cannot be decoded as text.

    Notes
    -----
    This function does not discover repository metadata, invoke Git, read
    packed refs or reftable, or follow a worktree's common-directory file.
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
