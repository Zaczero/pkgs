def encode_rgb(
    rgb: bytes,
    width: int,
    height: int,
    x_components: int,
    y_components: int,
) -> str: ...
def decode_rgb(
    blurhash: str,
    width: int,
    height: int,
    punch: float,
) -> bytes: ...
