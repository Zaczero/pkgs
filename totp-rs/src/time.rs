use std::hint::unlikely;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::errors::TotpError;

fn time_seconds_or_default(time: Option<f64>) -> Result<i64, TotpError> {
    if let Some(value) = time {
        // i64::MAX rounds up to 2^63 in binary64, so the upper bound is exclusive.
        if !(i64::MIN as f64..-(i64::MIN as f64)).contains(&value) {
            return Err(TotpError::TimeSecondsOutOfRange);
        }
        Ok(value.floor() as i64)
    } else {
        let seconds = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => i128::from(duration.as_secs()),
            Err(error) => {
                let duration = error.duration();
                i128::from(duration.as_secs())
                    .wrapping_neg()
                    .wrapping_sub(i128::from(duration.subsec_nanos() != 0))
            },
        };
        i64::try_from(seconds).map_err(|_| TotpError::TimeSecondsOutOfRange)
    }
}

pub(crate) fn time_window_from_time(
    time: Option<f64>,
    step_seconds: i64,
    t0: i64,
) -> Result<i64, TotpError> {
    if unlikely(step_seconds <= 0) {
        return Err(TotpError::StepSecondsMustBePositive);
    }

    let diff = i128::from(time_seconds_or_default(time)?).wrapping_sub(i128::from(t0));
    let counter = diff.div_euclid(i128::from(step_seconds));
    i64::try_from(counter).map_err(|_| TotpError::TimeWindowOutOfRange)
}

pub(crate) fn resolve_counter(
    time: Option<f64>,
    time_window: Option<i64>,
    step_seconds: i64,
    t0: i64,
) -> Result<i64, TotpError> {
    if unlikely(time.is_some() && time_window.is_some()) {
        return Err(TotpError::TimeAndTimeWindowBothSet);
    }

    match time_window {
        Some(window) => Ok(window),
        None => Ok(time_window_from_time(time, step_seconds, t0)?),
    }
}
