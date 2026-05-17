use super::actions::{FinalResponseBody, ResponseAction, ResponseActions, ResponseStart};
use crate::bridge::PayloadBytes;
use crate::console::ResponseLogState;
use crate::error::H2CornError;
use crate::http::types::ResponseHeaders;

pub trait HttpResponseTransport {
    async fn send_final_response(
        &mut self,
        start: ResponseStart,
        body: FinalResponseBody,
    ) -> Result<(), H2CornError>;

    async fn start_streaming_response(&mut self, start: ResponseStart) -> Result<(), H2CornError>;

    async fn send_streaming_body(&mut self, body: PayloadBytes) -> Result<(), H2CornError>;

    async fn send_streaming_file(
        &mut self,
        file: tokio::fs::File,
        len: usize,
    ) -> Result<(), H2CornError>;

    async fn finish_streaming_response(&mut self) -> Result<(), H2CornError>;

    async fn finish_streaming_with_trailers(
        &mut self,
        trailers: ResponseHeaders,
    ) -> Result<(), H2CornError>;

    async fn send_internal_error_response(&mut self) -> Result<(), H2CornError>;

    async fn abort_incomplete_response(&mut self) -> Result<(), H2CornError>;

    fn response_log_state(&self) -> ResponseLogState;

    async fn apply_response_actions(
        &mut self,
        actions: &mut ResponseActions,
    ) -> Result<(), H2CornError> {
        for action in actions.drain(..) {
            match action {
                ResponseAction::Final { start, body } => {
                    self.send_final_response(start, body).await?;
                },
                ResponseAction::Start { start } => {
                    self.start_streaming_response(start).await?;
                },
                ResponseAction::Body(body) => self.send_streaming_body(body).await?,
                ResponseAction::File { file, len } => {
                    self.send_streaming_file(*file, len).await?;
                },
                ResponseAction::Finish => self.finish_streaming_response().await?,
                ResponseAction::FinishWithTrailers(trailers) => {
                    self.finish_streaming_with_trailers(trailers).await?;
                },
                ResponseAction::InternalError => {
                    self.send_internal_error_response().await?;
                },
                ResponseAction::AbortIncomplete => {
                    self.abort_incomplete_response().await?;
                },
            }
        }
        Ok(())
    }
}
