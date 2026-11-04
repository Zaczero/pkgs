from math import frexp, isfinite

__version__ = '2.0.0'

_UNITS = tuple(
    (1024**exponent, suffix)
    for exponent, suffix in enumerate(
        (' KiB', ' MiB', ' GiB', ' TiB', ' PiB', ' EiB', ' ZiB'), 1
    )
)


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

    binary_exponent = size.bit_length() if isinstance(size, int) else frexp(size)[1]
    exp = (binary_exponent - 1) // 10
    if exp > len(_UNITS):
        return '(too large to display)'

    divisor, suffix = _UNITS[exp - 1]
    size /= divisor
    format_spec = '.2f' if size < 10 else '.1f'
    return f'{prefix}{size:{format_spec}}{suffix}'
