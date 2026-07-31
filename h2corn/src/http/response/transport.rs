use super::actions::{ResponseAction, ResponseActions};
use crate::bridge::HttpOutboundEvent;
use crate::error::H2CornError;
use crate::http::app::{AdmittedHttpOutboundEvent, HttpSendBuffer, HttpSendState};

pub(crate) trait HttpResponseTransport {
    /// HTTP/1 has no cross-stream body backlog. HTTP/2 overrides this with a
    /// connection-wide byte ledger before the ASGI callable is constructed.
    fn new_http_send_state(&self) -> (HttpSendState, HttpSendBuffer) {
        HttpSendState::unbounded()
    }

    /// Charge an event that did not come through `HttpSendState` to the
    /// connection's byte budget.
    ///
    /// Only the WebSocket handshake takes this route — a denial response is
    /// driven straight from the handshake loop, so it never passes the ASGI
    /// admission that charges an ordinary response body. HTTP/1 has no
    /// connection budget and keeps the default.
    async fn admit_outbound_event(
        &self,
        event: HttpOutboundEvent,
    ) -> Result<AdmittedHttpOutboundEvent, H2CornError> {
        Ok(AdmittedHttpOutboundEvent::unbounded(event))
    }

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
