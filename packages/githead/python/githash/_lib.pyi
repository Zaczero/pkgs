from os import PathLike

def githead(dir_: PathLike[str] | str = '.git') -> str:
    """
    Get the current git commit hash.

    >>> githead()
    'bca663418428d603eea8243d08a5ded19eb19a34'
    """
