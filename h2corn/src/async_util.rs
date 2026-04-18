use tokio::sync::mpsc::{self, error::TrySendError};

use crate::error::{ErrorExt, H2CornError};

pub(crate) enum TryPush<T> {
    Sent,
    Full(T),
    Closed(T),
}

pub(crate) fn try_push<T>(tx: &mpsc::Sender<T>, value: T) -> TryPush<T> {
    match tx.try_send(value) {
        Ok(()) => TryPush::Sent,
        Err(TrySendError::Full(value)) => TryPush::Full(value),
        Err(TrySendError::Closed(value)) => TryPush::Closed(value),
    }
}

pub(crate) async fn send_with_backpressure<T, F, E>(
    tx: &mpsc::Sender<T>,
    value: T,
    closed_error: F,
) -> Result<(), H2CornError>
where
    F: Fn() -> E + Copy,
    E: Into<H2CornError>,
{
    match try_push(tx, value) {
        TryPush::Sent => Ok(()),
        TryPush::Full(value) => tx
            .send(value)
            .await
            .map_err(|_| closed_error().into_error()),
        TryPush::Closed(_) => Err(closed_error().into_error()),
    }
}

pub(crate) async fn send_if_open<T>(tx: &mpsc::Sender<T>, value: T) -> bool {
    match try_push(tx, value) {
        TryPush::Sent => true,
        TryPush::Full(value) => tx.send(value).await.is_ok(),
        TryPush::Closed(_) => false,
    }
}

pub(crate) async fn send_best_effort<T>(tx: &mpsc::Sender<T>, value: T) {
    match try_push(tx, value) {
        TryPush::Sent | TryPush::Closed(_) => {}
        TryPush::Full(value) => {
            let _ = tx.send(value).await;
        }
    }
}
