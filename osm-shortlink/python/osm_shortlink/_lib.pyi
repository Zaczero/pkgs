def shortlink_encode(lon: float, lat: float, zoom: int) -> str:
    """
    Encode a coordinate pair and zoom level into a shortlink code.

    Longitude and latitude must be finite. Longitude wraps around the world;
    latitude reflects across the poles with a corresponding 180-degree
    longitude shift. The north pole maps to the highest latitude cell. Zoom
    must be between 0 and 22 inclusive. Coordinate values are quantized to the
    selected shortlink cell. After Python argument conversion, zoom range is
    checked before coordinate finiteness.

    Raises:
        ValueError: If a coordinate is not finite or zoom is greater than 22.
        TypeError: If an argument has an incompatible Python type.
        OverflowError: If a coordinate cannot be converted to a float or zoom
            cannot be extracted as an unsigned byte.

    >>> shortlink_encode(0.055, 51.511, 9)
    '0EEQjE--'
    """

def shortlink_decode(s: str, /) -> tuple[float, float, int]:
    """
    Decode a shortlink code into a quantized (longitude, latitude, zoom) tuple.

    The decoded coordinate identifies the encoded grid cell; it is not
    necessarily the original coordinate supplied to ``shortlink_encode``.
    Characters outside the shortlink alphabet are ignored, and legacy ``@``
    and ``=`` spellings remain accepted. This function decodes a code, not a
    complete URL.

    Raises:
        ValueError: If the token has too few or too many encoded coordinate
            bits.
        TypeError: If ``s`` is not a string.

    >>> shortlink_decode('0EEQjE--')
    (0.054931640625, 51.510772705078125, 9)
    """
