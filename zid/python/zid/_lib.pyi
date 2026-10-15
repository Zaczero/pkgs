def zid() -> int:
    """Reserve one increasing identifier in this process's shared state.

    Logical time advances on sequence exhaustion and does not move backward.
    Raises OverflowError when the 47-bit timestamp horizon is exhausted.
    Independently initialized processes can collide.
    Forked children must exec before generation; there is no automatic reset.
    """

def zids(n: int, /) -> list[int]:
    """Reserve 0 through 65536 contiguous identifiers at one logical timestamp.

    Zero returns [] without reserving. Negative counts raise OverflowError;
    counts above 65536 raise ValueError. Timestamp exhaustion raises OverflowError.
    Counts beyond the native platform's unsigned pointer-sized range also raise
    OverflowError during argument conversion. Non-integer inputs raise TypeError.
    Concurrent calls reserve disjoint intervals in reservation order.
    Forked children must exec before generation; there is no automatic reset.
    """

def parse_zid_timestamp(zid: int) -> int:
    """Extract the upper 48 bits of an unsigned 64-bit integer as milliseconds.

    Generated ZIDs use only 47 timestamp bits and may lead physical time.
    Out-of-range integers raise OverflowError. This does not validate provenance.
    """
