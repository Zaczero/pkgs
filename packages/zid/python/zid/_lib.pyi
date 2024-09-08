def zid() -> int:
    """
    Generate a unique identifier.
    """

def zids(n: int, /) -> list[int]:
    """
    Generate a list of unique identifiers.
    """

def parse_zid_timestamp(zid: int) -> int:
    """
    Extract the UNIX timestamp in milliseconds from a ZID.
    """

def set_random_buffer_size(size: int) -> None:
    """
    Set the size of the buffer when requesting random bytes from the operating system.

    Default: 128 KiB
    """
