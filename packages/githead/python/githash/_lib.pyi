from os import PathLike

def githash(dir_: PathLike[str] | str = '.git') -> str:
    """
    Get the current git commit hash.

    >>> githash()
    'bca663418428d603eea8243d08a5ded19eb19a34'
    """
