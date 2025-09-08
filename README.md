## Atlas Core

Atlas Core is a modular, high-throughput indexing pipeline. It provides the building blocks to:

-   Ingest updates from one or more datasources
-   Decode and process accounts, instructions, and transactions
-   React to block-level metadata, Bitcoin blocks, and rollback/reapply events
-   Filter any stream by datasource or custom logic
-   Collect metrics and control shutdown and buffering behavior

The `datasources/*` and `checkpoint-stores/*` crates in this workspace are just implementations of the traits defined in the core crate.

## Crate name and dependency

Inside this workspace you can depend on the core crate via the workspace dependency:

```toml
[dependencies]
atlas-core = { path = "./core" }
```

```rust
use atlas_arch as core;
```

## Build and test

Build only the core crate and run its tests within the workspace:

```bash
cargo build -p atlas-core
```

## Core concepts

### Pipeline and builder

The `Pipeline` coordinates reading updates from datasources and dispatching them to registered pipes. Use `Pipeline::builder()` to assemble your pipeline.

-   Add one or more datasources: `builder.datasource(...)` or `builder.datasource_with_id(id, ...)`
-   Register any combination of pipes: accounts, account deletions, instructions, transactions, block details, Bitcoin blocks, rollback/reapply events
-   Configure metrics, shutdown strategy, channel buffer size, and optional providers

Key types: `pipeline::Pipeline`, `pipeline::PipelineBuilder`, `pipeline::ShutdownStrategy`.

### Datasource

Implement `datasource::Datasource` to push updates into the pipeline. A datasource is responsible for producing one or more `Updates` variants and sending them to the pipeline via the provided channel.

-   `consume(id, sender, cancellation_token, metrics) -> IndexerResult<()>`
-   `update_types() -> Vec<UpdateType>`

Updates you can emit:

-   `Updates::Accounts(Vec<AccountUpdate>)`
-   `Updates::Transactions(Vec<TransactionUpdate>)`
-   `Updates::AccountDeletions(Vec<AccountDeletion>)`
-   `Updates::BlockDetails(Vec<BlockDetails>)`
-   `Updates::BitcoinBlocks(Vec<BitcoinBlock>)`
-   `Updates::RolledbackTransactions(Vec<RolledbackTransactionsEvent>)`
-   `Updates::ReappliedTransactions(Vec<ReappliedTransactionsEvent>)`

Associated data structs: `AccountUpdate`, `TransactionUpdate`, `AccountDeletion`, `BlockDetails`, `BitcoinBlock`, and rollback/reapply event types.

### Pipes and processors

Each stream is handled by a pipe which decodes and forwards data to your `Processor` implementation:

-   Accounts: `account::AccountPipe` with your `AccountDecoder` and a `Processor<(AccountMetadata, DecodedAccount<T>, AccountInfo), ()>`
-   Account deletions: `account_deletion::AccountDeletionPipe` with a `Processor<AccountDeletion, ()>`
-   Instructions: `instruction::InstructionPipe` with your `InstructionDecoder` and a `Processor<(DecodedInstruction<T>, Instruction), ()>`
-   Transactions: `transaction::TransactionPipe<T, U>` with an optional `schema::TransactionSchema<T>` and a `Processor<(Arc<TransactionMetadata>, Vec<ParsedInstruction<T>>, Option<U>), ()>`
-   Block details: `block_details::BlockDetailsPipe` with a `Processor<BlockDetails, ()>`
-   Bitcoin blocks: `block_details::BitcoinBlockPipe` with a `Processor<BitcoinBlock, ()>`
-   Rollbacks/Reapplies: `rollback::{RolledbackTransactionsPipe, ReappliedTransactionsPipe}` with processors that return `HashSet<Pubkey>` to drive on-demand refreshes

Processors implement:

```rust
#[async_trait::async_trait]
pub trait Processor {
    type InputType;
    type OutputType;
    async fn process(
        &mut self,
        data: Vec<Self::InputType>,
        metrics: std::sync::Arc<atlas_arch::metrics::MetricsCollection>,
    ) -> atlas_arch::error::IndexerResult<Self::OutputType>;
}
```

### Filters

Every pipe accepts zero or more filters implementing `filter::Filter`. Use `filter::DatasourceFilter` to restrict a pipe to specific datasources via `DatasourceId`. You can add your own filters by implementing the trait methods you need (account, instruction, transaction, block details, bitcoin block, rollback/reapply events).

### Metrics

`metrics::MetricsCollection` fans out to one or more `metrics::Metrics` implementations you register via `builder.metrics(...)`. The pipeline records counters, gauges, and histograms for received/processed updates and timing. You can implement `Metrics` to integrate with your preferred backend.

### Optional providers

-   `datasource::BitcoinDatasource`: Optional provider used to batch fetch raw Bitcoin transactions by txid. When configured via `builder.bitcoin_datasource(...)`, the pipeline will enrich `TransactionMetadata` with `bitcoin_tx` when possible.
-   `datasource::AccountDatasource`: Optional provider used to batch fetch accounts by pubkey on demand. When rollbacks/reapplies occur, the pipeline can refresh accounts and emit synthesized account updates or deletions accordingly.

### Operational knobs

-   `ShutdownStrategy::{Immediate, ProcessPending}` controls how Ctrl-C or cancellation is handled
-   `metrics_flush_interval(seconds)` controls periodic metrics flushing
-   `channel_buffer_size(n)` tunes backpressure between datasources and the pipeline
-   `datasource_cancellation_token(token)` lets you cancel datasources on demand

## Quick start

Minimal example wiring a pipeline with one datasource, one instruction pipe, and metrics:

```rust
use std::sync::Arc;
use atlas_arch as core;
use core::pipeline::Pipeline;
use core::datasource::{Datasource, DatasourceId, Updates, UpdateType};
use core::instruction::{InstructionDecoder, DecodedInstruction};
use core::processor::Processor;
use core::metrics::{Metrics, MetricsCollection};
use arch_program::instruction::Instruction;
use async_trait::async_trait;

// 1) A tiny metrics impl
struct LogMetrics;
#[async_trait]
impl Metrics for LogMetrics {
    async fn initialize(&self) -> core::error::IndexerResult<()> { Ok(()) }
    async fn flush(&self) -> core::error::IndexerResult<()> { Ok(()) }
    async fn shutdown(&self) -> core::error::IndexerResult<()> { Ok(()) }
    async fn update_gauge(&self, _: &str, _: f64) -> core::error::IndexerResult<()> { Ok(()) }
    async fn increment_counter(&self, _: &str, _: u64) -> core::error::IndexerResult<()> { Ok(()) }
    async fn record_histogram(&self, _: &str, _: f64) -> core::error::IndexerResult<()> { Ok(()) }
}

// 2) Implement a simple instruction decoder and processor
struct MyIxDecoder;
impl<'a> InstructionDecoder<'a> for MyIxDecoder {
    type InstructionType = (); // your enum/struct here
    fn decode_instruction(&self, _ix: &'a Instruction) -> Option<DecodedInstruction<Self::InstructionType>> {
        None
    }
}

struct MyIxProcessor;
#[async_trait]
impl Processor for MyIxProcessor {
    type InputType = (DecodedInstruction<()>, Instruction);
    type OutputType = ();
    async fn process(&mut self, _data: Vec<Self::InputType>, _m: Arc<MetricsCollection>) -> core::error::IndexerResult<()> {
        Ok(())
    }
}

// 3) A stub datasource (send nothing, just to show wiring)
struct MyDatasource;
#[async_trait]
impl Datasource for MyDatasource {
    async fn consume(
        &self,
        _id: DatasourceId,
        _sender: tokio::sync::mpsc::Sender<(Updates, DatasourceId)>,
        _cancellation: tokio_util::sync::CancellationToken,
        _metrics: Arc<MetricsCollection>,
    ) -> core::error::IndexerResult<()> {
        Ok(())
    }
    fn update_types(&self) -> Vec<UpdateType> { vec![] }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> core::error::IndexerResult<()> {
    let mut pipeline = Pipeline::builder()
        .datasource(MyDatasource)
        .metrics(Arc::new(LogMetrics))
        .instruction(MyIxDecoder, MyIxProcessor)
        .build()?;
    pipeline.run().await
}
```

## Implementing a datasource

At a minimum, push updates through the `sender` channel supplied by `Datasource::consume`:

```rust
use atlas_arch::datasource::{Datasource, DatasourceId, Updates, AccountUpdate, UpdateType};
use async_trait::async_trait;

struct MyDs;
#[async_trait]
impl Datasource for MyDs {
    async fn consume(
        &self,
        id: DatasourceId,
        sender: tokio::sync::mpsc::Sender<(Updates, DatasourceId)>,
        cancellation: tokio_util::sync::CancellationToken,
        _metrics: std::sync::Arc<atlas_arch::metrics::MetricsCollection>,
    ) -> atlas_arch::error::IndexerResult<()> {
        // example: send a batch of accounts
        let updates = Updates::Accounts(vec![/* AccountUpdate { .. } */]);
        let _ = sender.send((updates, id)).await;
        // honor cancellation when producing a stream
        let _ = cancellation.cancelled().await;
        Ok(())
    }
    fn update_types(&self) -> Vec<UpdateType> { vec![UpdateType::AccountUpdate] }
}
```

## Using filters

To restrict a pipe to a particular datasource, use `filter::DatasourceFilter` with a named `DatasourceId`:

```rust
use atlas_arch::datasource::DatasourceId;
use atlas_arch::filter::DatasourceFilter;

let mainnet = DatasourceId::new_named("mainnet");
let filters = vec![Box::new(DatasourceFilter::new(mainnet)) as Box<dyn atlas_arch::filter::Filter + Send + Sync>];

let builder = atlas_arch::pipeline::Pipeline::builder()
    .instruction_with_filters(MyIxDecoder, MyIxProcessor, filters);
```

## Optional providers: Bitcoin and Accounts

-   Configure `bitcoin_datasource(...)` to batch-fetch Bitcoin transactions by txid. The pipeline will enrich `TransactionMetadata.bitcoin_tx` when the provider returns a match.
-   Configure `account_datasource(...)` to batch-fetch accounts by pubkey on rollback/reapply events. The pipeline will:
    -   Emit `AccountDeletion` when an account becomes system-owned with zero lamports or disappears
    -   Emit refreshed `AccountUpdate`s for still-existing accounts

## Operational notes

-   Use `ShutdownStrategy::Immediate` to stop on Ctrl-C without draining; use `ProcessPending` to finish queued updates first
-   Set `channel_buffer_size(n)` to tune throughput and memory
-   Set `metrics_flush_interval(seconds)` to adjust reporting cadence
-   Provide `datasource_cancellation_token(...)` if you want to cancel datasources independently of Ctrl-C

## Repository layout (focus)

-   `core/`: the `atlas-core` crate (this README focuses here)
-   `datasources/*`: concrete `Datasource` implementations (optional to use)
-   `checkpoint-stores/*`: storage implementations (unrelated to core API surface)
-   `deps/*`: external program/sdk dependencies used by the workspace (unrelated to core)

## Acknowledgements

Special thanks to the Carbon project for inspiration: [sevenlabs-hq/carbon](https://github.com/sevenlabs-hq/carbon).

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
