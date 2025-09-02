use crate::datasource::{BitcoinBlock, BlockDetails};
use crate::error::IndexerResult;
use crate::filter::Filter;
use crate::metrics::MetricsCollection;
use crate::processor::Processor;
use async_trait::async_trait;
use std::sync::Arc;

pub struct BlockDetailsPipe {
    pub processor: Box<dyn Processor<InputType = BlockDetails, OutputType = ()> + Send + Sync>,
    pub filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
}

#[async_trait]
/// Forwards block detail updates (height, hashes, times) to a processor.
pub trait BlockDetailsPipes: Send + Sync {
    async fn run(
        &mut self,
        block_details: Vec<BlockDetails>,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()>;

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>>;
}

#[async_trait]
impl BlockDetailsPipes for BlockDetailsPipe {
    async fn run(
        &mut self,
        block_details: Vec<BlockDetails>,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()> {
        log::trace!(
            "Block details::run(block_details: {:?}, metrics)",
            block_details,
        );

        self.processor.process(block_details, metrics).await?;

        Ok(())
    }

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>> {
        &self.filters
    }
}

pub struct BitcoinBlockPipe {
    pub processor: Box<dyn Processor<InputType = BitcoinBlock, OutputType = ()> + Send + Sync>,
    pub filters: Vec<Box<dyn Filter + Send + Sync + 'static>>,
}

#[async_trait]
/// Forwards Bitcoin block summaries to a processor.
pub trait BitcoinBlockPipes: Send + Sync {
    async fn run(
        &mut self,
        blocks: Vec<BitcoinBlock>,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()>;

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>>;
}

#[async_trait]
impl BitcoinBlockPipes for BitcoinBlockPipe {
    async fn run(
        &mut self,
        blocks: Vec<BitcoinBlock>,
        metrics: Arc<MetricsCollection>,
    ) -> IndexerResult<()> {
        log::trace!("Bitcoin block::run(blocks: {:?}, metrics)", blocks);

        self.processor.process(blocks, metrics).await?;

        Ok(())
    }

    fn filters(&self) -> &Vec<Box<dyn Filter + Send + Sync + 'static>> {
        &self.filters
    }
}
