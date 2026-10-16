from math import isfinite, log

__version__ = '1.2.1'

_SUFFIXES = (' KiB', ' MiB', ' GiB', ' TiB', ' PiB', ' EiB', ' ZiB')


def sizestr(size: float) -> str:
    """Format a byte count as a human-readable string.

    Sub-KiB magnitudes are truncated toward zero to whole bytes. Larger values
    use powers of 1024 with suffixes from KiB through ZiB. Scaled magnitudes
    below 10 use two decimal places; other scaled values use one, with ordinary
    float rounding and no carry into the next suffix. The sign is handled
    separately, so a negative fraction smaller than one byte displays as
    ``-0 B``.

    Non-finite floating-point values are returned in parentheses. Values whose
    computed binary exponent exceeds ZiB return ``(too large to display)``.

    Parameters
    ----------
    size
        Byte count as an integer or floating-point value.

    Returns
    -------
    str
        Formatted display text, not a machine-readable number.

    Raises
    ------
    TypeError
        If ``size`` is not a supported numeric value.
    """
    # Handle non-finite values
    if not isinstance(size, int) and not isfinite(size):
        return f'({size})'

    # Handle sign
    prefix = '-' if size < 0 else ''
    size = abs(size)

    # Fast path for small values
    if size < 1024:
        return f'{prefix}{int(size)} B'

    exp = int(log(size, 1024))
    if exp > 7:
        return '(too large to display)'

    size /= 1024**exp
    precision = 2 if size < 10 else 1
    return f'{prefix}{size:.{precision}f}{_SUFFIXES[exp - 1]}'
