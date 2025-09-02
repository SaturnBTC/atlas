use async_trait::async_trait;
use std::sync::Arc;

use crate::error::IndexerResult;

#[async_trait]
/// Pluggable metrics backend used by the indexing pipeline.
///
/// Implementors should make methods non-blocking and resilient to
/// backpressure. Errors should be rare and indicate misconfiguration.
pub trait Metrics: Send + Sync {
    async fn initialize(&self) -> IndexerResult<()>;

    async fn flush(&self) -> IndexerResult<()>;

    async fn shutdown(&self) -> IndexerResult<()>;

    /// Set a gauge to the provided value.
    async fn update_gauge(&self, name: &str, value: f64) -> IndexerResult<()>;

    /// Increment a counter by the provided amount.
    async fn increment_counter(&self, name: &str, value: u64) -> IndexerResult<()>;

    /// Record a single value into a histogram.
    async fn record_histogram(&self, name: &str, value: f64) -> IndexerResult<()>;
}

#[derive(Default)]
pub struct MetricsCollection {
    pub metrics: Vec<Arc<dyn Metrics>>,
}

impl MetricsCollection {
    pub fn new(metrics: Vec<Arc<dyn Metrics>>) -> Self {
        Self { metrics }
    }

    pub async fn initialize_metrics(&self) -> IndexerResult<()> {
        for metric in &self.metrics {
            metric.initialize().await?;
        }
        Ok(())
    }

    pub async fn shutdown_metrics(&self) -> IndexerResult<()> {
        for metric in &self.metrics {
            metric.shutdown().await?;
        }
        Ok(())
    }

    pub async fn flush_metrics(&self) -> IndexerResult<()> {
        for metric in &self.metrics {
            metric.flush().await?;
        }
        Ok(())
    }

    pub async fn update_gauge(&self, name: &str, value: f64) -> IndexerResult<()> {
        for metric in &self.metrics {
            metric.update_gauge(name, value).await?;
        }
        Ok(())
    }

    pub async fn increment_counter(&self, name: &str, value: u64) -> IndexerResult<()> {
        for metric in &self.metrics {
            metric.increment_counter(name, value).await?;
        }
        Ok(())
    }

    pub async fn record_histogram(&self, name: &str, value: f64) -> IndexerResult<()> {
        for metric in &self.metrics {
            metric.record_histogram(name, value).await?;
        }
        Ok(())
    }
}
