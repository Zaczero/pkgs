from collections.abc import Iterable

def encode_lonlat(coordinates: Iterable[Iterable[float]], precision: int = 5) -> str:
    """Encode coordinate rows whose first two values are (longitude, latitude).

    Extra row values are ignored. Coordinates are rounded half away from zero
    after scaling. The rounded scaled coordinates and their successive deltas
    must fit signed 32-bit integers. Precision is accepted when its decimal
    scale and inverse are finite and positive and the decoded signed 32-bit
    coordinate range remains finite.

    Raises:
        ValueError: If a row is too short, a coordinate or delta is not
            representable, or precision cannot be represented by the codec.
        TypeError: If a coordinate row is not iterable or a value cannot be
            converted to a float. Exceptions raised by user-defined iterators
            propagate.
        OverflowError: If precision or a coordinate value is outside its
            supported Python-to-native numeric range.
    """

def encode_latlon(coordinates: Iterable[Iterable[float]], precision: int = 5) -> str:
    """Encode coordinate rows whose first two values are (latitude, longitude).

    Extra row values are ignored. Coordinates are rounded half away from zero
    after scaling. The rounded scaled coordinates and their successive deltas
    must fit signed 32-bit integers. Precision is accepted when its decimal
    scale and inverse are finite and positive and the decoded signed 32-bit
    coordinate range remains finite.

    Raises:
        ValueError: If a row is too short, a coordinate or delta is not
            representable, or precision cannot be represented by the codec.
        TypeError: If a coordinate row is not iterable or a value cannot be
            converted to a float. Exceptions raised by user-defined iterators
            propagate.
        OverflowError: If precision or a coordinate value is outside its
            supported Python-to-native numeric range.
    """

def decode_lonlat(polyline: str, precision: int = 5) -> list[tuple[float, float]]:
    """Decode a polyline string to (longitude, latitude) coordinate pairs.

    Surrounding whitespace is stripped. The decoder accepts only the encoded
    ASCII value range and rejects malformed or overflowing values.

    Raises:
        ValueError: If precision is not representable or the encoded values
            are invalid, incomplete, or overflow signed 32-bit coordinates.
        TypeError: If ``polyline`` is not a string.
        OverflowError: If precision is outside the signed 32-bit integer range.
    """

def decode_latlon(polyline: str, precision: int = 5) -> list[tuple[float, float]]:
    """Decode a polyline string to (latitude, longitude) coordinate pairs.

    Surrounding whitespace is stripped. The decoder accepts only the encoded
    ASCII value range and rejects malformed or overflowing values.

    Raises:
        ValueError: If precision is not representable or the encoded values
            are invalid, incomplete, or overflow signed 32-bit coordinates.
        TypeError: If ``polyline`` is not a string.
        OverflowError: If precision is outside the signed 32-bit integer range.
    """
