from typing import Literal

try:
    from typing import TypeAlias  # type: ignore
except ImportError:
    try:
        from typing_extensions import TypeAlias  # type: ignore
    except ImportError:
        from typing import Any as TypeAlias

Algorithm: TypeAlias = Literal['sha1', 'sha256', 'sha512']

def totp_time_window(
    time: float | None = None,
    *,
    step_seconds: int = 30,
    t0: int = 0,
) -> int:
    """Return the TOTP time counter (T) for the given UNIX time (seconds)."""

def totp_generate(
    secret: bytes | str,
    *,
    digits: int = 6,
    algorithm: Algorithm = 'sha1',
    time: float | None = None,
    time_window: int | None = None,
    step_seconds: int = 30,
    t0: int = 0,
) -> str:
    """
    Generate a TOTP code for the provided secret.

    `secret` can be raw bytes, or a base32-encoded string.
    For base32 strings, `=` padding is optional and lowercase is accepted.

    If `time_window` is provided, it is used directly and `time` is ignored.
    """

def totp_verify(
    secret: bytes | str,
    code: str | int,
    *,
    digits: int = 6,
    algorithm: Algorithm = 'sha1',
    time: float | None = None,
    time_window: int | None = None,
    step_seconds: int = 30,
    t0: int = 0,
    window: int = 1,
) -> bool:
    """
    Verify a TOTP code against the secret.

    `secret` can be raw bytes, or a base32-encoded string.
    For base32 strings, `=` padding is optional and lowercase is accepted.

    If `time_window` is provided, it is used directly and `time` is ignored.

    The `window` parameter controls allowed counter drift: `window=1` accepts T-1, T, T+1.
    """
