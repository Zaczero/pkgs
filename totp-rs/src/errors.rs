use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum TotpError {
    #[error("secret contains an invalid Base32 character at index {index}")]
    InvalidSecretChar { index: usize },
    #[error("digits must be between 1 and 9, got {digits}")]
    DigitsOutOfRange { digits: u8 },
    #[error("step_seconds must be positive")]
    StepSecondsMustBePositive,
    #[error("time must be finite and fit in signed 64-bit seconds")]
    TimeSecondsOutOfRange,
    #[error("time window is outside the signed 64-bit counter range")]
    TimeWindowOutOfRange,
    #[error("algorithm must be sha1, sha256, or sha512")]
    InvalidAlgorithm,
    #[error("time and time_window cannot both be set")]
    TimeAndTimeWindowBothSet,
}
