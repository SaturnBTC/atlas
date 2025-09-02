# Rollback/Reapply Pipe Example

This example demonstrates how to use Atlas Core's rollback/reapply pipes. It wires:

-   a minimal `Datasource` that emits one `RolledbackTransactionsEvent` and one `ReappliedTransactionsEvent`
-   a tiny in-memory `TransactionStore` and `RollbackService`
-   two `Processor` implementations that collect affected `Pubkey`s per event batch
-   a `Pipeline` that registers these pipes and runs to completion

## Files

-   `src/main.rs`: Complete runnable example
-   `Cargo.toml`: Example crate manifest

## Build

From the workspace root:

```bash
cargo build -p atlas-example-rollback-pipe
```

## Run

From the workspace root:

```bash
cargo run -p atlas-example-rollback-pipe
```

The demo datasource sends a rollback event for `tx-1` and a reapply event for `tx-2`. The processors look up these IDs in an in-memory store, gather related pubkeys (pool, optional position, and per-transaction writable accounts), and return them to the pipeline.

## What to look at

-   `RolledbackProcessor` and `ReappliedProcessor` implement `Processor` with `InputType` set to the respective event type and `OutputType = HashSet<Pubkey>`.
-   `Pipeline::builder()` registers both pipes via `.rolledback_transactions(...)` and `.reapplied_transactions(...)`.
-   `DemoDatasource` implements `Datasource` and sends `Updates::RolledbackTransactions` and `Updates::ReappliedTransactions` through the pipeline channel.

## Customize

-   Replace the in-memory store with your own repository or database adapter.
-   Use `filter::DatasourceFilter` to scope the pipes to specific datasources.
-   Provide an `AccountDatasource` to automatically refresh accounts affected by returned pubkeys.

## Requirements

-   Rust 1.70+ (edition 2021)
-   Tokio multi-thread runtime (enabled via workspace features)
