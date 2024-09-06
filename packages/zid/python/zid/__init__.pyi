RANDOM_BUFFER_SIZE: int
"""
The size of the buffer when requesting random bytes from the operating system.
"""

def zid() -> int:
    """
    Generate a unique identifier.
    """

def parse_zid_timestamp(zid: int) -> int:
    """
    Extract the UNIX timestamp in milliseconds from a ZID.
    """
