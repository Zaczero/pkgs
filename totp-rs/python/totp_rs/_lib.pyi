from typing import Literal, TypeAlias

_Algorithm: TypeAlias = Literal['sha1', 'sha256', 'sha512']

def totp_time_window(
    time: float | None = None,
    *,
    step_seconds: int = 30,
    t0: int = 0,
) -> int:
    """Return the TOTP counter for a Unix timestamp in seconds.

    Finite ``time`` values are floored to whole seconds. If ``time`` is omitted,
    the current system time is used. The counter is
    ``floor((floor(time) - t0) / step_seconds)`` for a positive step.
    Negative, nonzero steps are also accepted and use Euclidean integer
    division. ``step_seconds`` and ``t0`` are signed 64-bit integers. Nonfinite
    or out-of-range floating times convert to signed 64-bit seconds using
    saturating float-to-integer conversion. The default of 30 seconds is the
    common RFC 6238 configuration.

    Raises:
        ValueError: If ``step_seconds`` is zero or the counter does not fit a
            signed 64-bit integer.
        TypeError: If a typed time or integer argument has an incompatible
            Python type.
        OverflowError: If ``step_seconds`` or ``t0`` is outside the signed
            64-bit integer range.
    """

def totp_generate(
    secret: bytes | str,
    *,
    digits: int = 6,
    algorithm: _Algorithm = 'sha1',
    time: float | None = None,
    time_window: int | None = None,
    step_seconds: int = 30,
    t0: int = 0,
) -> str:
    """
    Generate a zero-padded TOTP code for the supplied counter or time.

    ``secret`` may be raw bytes or Base32 text. Base32 text is case-insensitive;
    ASCII whitespace and ``=`` are ignored. ``algorithm`` is exactly one of
    ``"sha1"``, ``"sha256"``, or ``"sha512"``, and ``digits`` must be from 1
    through 9.

    If ``time_window`` is supplied, it is the signed counter used directly;
    ``step_seconds`` and ``t0`` are then unused. ``time`` and ``time_window``
    are mutually exclusive. When neither is supplied, the current system time
    is used with ``step_seconds`` and ``t0``. ``step_seconds`` must be nonzero
    when deriving a counter; the default is 30 seconds. Time, origin, and
    explicit counter values use signed 64-bit integers.

    Raises:
        ValueError: If the secret is not bytes or valid Base32 text, the
            algorithm or digit count is unsupported, the time arguments
            conflict, a derived counter uses a zero step, or the derived
            counter is out of range.
        TypeError: If a typed option has an incompatible Python type.
        OverflowError: If ``digits`` is outside the unsigned-byte range or an
            ``i64`` time/counter option is outside its signed range.
    """

def totp_verify(
    secret: bytes | str,
    code: str | int,
    *,
    digits: int = 6,
    algorithm: _Algorithm = 'sha1',
    time: float | None = None,
    time_window: int | None = None,
    step_seconds: int = 30,
    t0: int = 0,
    window: int = 1,
) -> bool:
    """
    Verify a candidate TOTP code against the supplied counter or time.

    ``secret`` may be raw bytes or Base32 text. Base32 text is case-insensitive;
    ASCII whitespace and ``=`` are ignored. ``algorithm`` is exactly one of
    ``"sha1"``, ``"sha256"``, or ``"sha512"``, and ``digits`` must be from 1
    through 9.

    If ``time_window`` is supplied, it is the signed counter used directly;
    ``step_seconds`` and ``t0`` are then unused. ``time`` and ``time_window``
    are mutually exclusive. When neither is supplied, the current system time
    is used with ``step_seconds`` and ``t0``. ``step_seconds`` must be nonzero
    when deriving a counter; the default is 30 seconds. Time, origin, and
    explicit counter values use signed 64-bit integers.

    ``window`` allows counter drift: with ``window=1``, candidates are checked
    in this order: T, T-1, T+1. It must fit an unsigned byte. String codes use
    their ASCII decimal digits; other bytes are ignored, and exactly ``digits``
    decimal digits are required. Integer codes must satisfy
    ``0 <= code < 10**digits``. A malformed candidate returns ``False``. The secret is
    parsed only after the candidate has the required shape, so a malformed
    candidate returns ``False`` even if the secret is malformed.

    The candidate code comparison uses a constant-time byte comparison. The
    complete verification is not constant-time: validation and counter search
    can return early. Verification is stateless and does not prevent replay or
    limit attempts.

    Raises:
        ValueError: If the secret is not bytes or valid Base32 text, the
            algorithm or digit count is unsupported, the time arguments
            conflict, a derived counter uses a zero step, or the derived
            counter is out of range.
        TypeError: If a typed option has an incompatible Python type. A code
            with an unsupported type returns ``False``.
        OverflowError: If ``digits`` or ``window`` is outside the unsigned-byte
            range or an ``i64`` time/counter option is outside its signed range.
    """
