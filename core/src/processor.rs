use std::sync::Arc;

use async_trait::async_trait;

use crate::{error::IndexerResult, metrics::MetricsCollection};

#[async_trait]
/// Generic processing trait for transforming batches of inputs into an
/// output, with access to metrics.
pub trait Processor {
    type InputType;
    type OutputType;

    /// Process a batch of inputs and optionally produce an output.
    async fn process(
        &mut self,
        data: Vec<Self::InputType>,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<Self::OutputType>;
}
