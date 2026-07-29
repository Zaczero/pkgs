use super::actions::{ResponseAction, ResponseActions};
use crate::error::H2CornError;

pub(crate) trait HttpResponseTransport {
    /// Put one action on the wire.
    ///
    /// The action enum is the whole vocabulary of a response, so a transport
    /// says how it writes each kind and nothing else. Everything the response
    /// machinery emits arrives here.
    async fn apply_response_action(&mut self, action: ResponseAction) -> Result<(), H2CornError>;

    /// Push anything still buffered to the peer.
    ///
    /// Called once a batch of actions has been applied, which is exactly the
    /// moment the application has nothing more ready to send. Chunks within a
    /// batch still coalesce, so throughput streaming keeps its one write per
    /// batch, while a server-sent-events app that emits one small event and
    /// then waits does not leave that event sitting in a buffer.
    async fn flush_buffered(&mut self) -> Result<(), H2CornError>;

    async fn apply_response_actions(
        &mut self,
        actions: &mut ResponseActions,
    ) -> Result<(), H2CornError> {
        let applied = !actions.is_empty();
        for action in actions.drain(..) {
            self.apply_response_action(action).await?;
        }
        if applied {
            self.flush_buffered().await?;
        }
        Ok(())
    }
}
