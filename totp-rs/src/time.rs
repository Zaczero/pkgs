use std::hint::unlikely;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::errors::Error;

fn time_seconds_or_default(time: Option<f64>) -> i64 {
    match time {
        Some(value) => value.floor() as i64,
        None => {
            let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
                return 0;
            };
            i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
        }
    }
}

pub(crate) fn time_window_from_time(
    time: Option<f64>,
    step_seconds: i64,
    t0: i64,
) -> Result<i64, Error> {
    if unlikely(step_seconds == 0) {
        return Err(Error::StepSecondsMustBeNonZero);
    }

    let diff = time_seconds_or_default(time) - t0;
    Ok(diff.div_euclid(step_seconds))
}

pub(crate) fn resolve_counter(
    time: Option<f64>,
    time_window: Option<i64>,
    step_seconds: i64,
    t0: i64,
) -> Result<i64, Error> {
    if unlikely(time.is_some() && time_window.is_some()) {
        return Err(Error::TimeAndTimeWindowBothSet);
    }

    match time_window {
        Some(window) => Ok(window),
        None => Ok(time_window_from_time(time, step_seconds, t0)?),
    }
}
