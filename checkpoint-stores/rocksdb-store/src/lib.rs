use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use atlas_arch::{error::IndexerResult, sync::CheckpointStore};
use rocksdb::{ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded, Options};

#[derive(thiserror::Error, Debug)]
pub enum CheckpointError {
    #[error("rocksdb error: {0}")]
    RocksDb(#[from] rocksdb::Error),
}

type Result<T> = std::result::Result<T, CheckpointError>;

const STATS_CF: &str = "stats";
const LAST_INDEXED_HEIGHT_KEY: &str = "last_indexed_height";
const BITCOIN_LATEST_HEIGHT_KEY: &str = "bitcoin_latest_height";

pub struct RocksCheckpointStore {
    db: Arc<DBWithThreadMode<MultiThreaded>>,
}

impl RocksCheckpointStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut cf_opts = Options::default();
        cf_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        let stats_cfd = ColumnFamilyDescriptor::new(STATS_CF, cf_opts);

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        db_opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);

        let db = DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(
            &db_opts,
            path,
            vec![stats_cfd],
        )?;
        Ok(Self { db: Arc::new(db) })
    }

    fn get_u64_cf(&self, cf: &str, key: &str) -> IndexerResult<u64> {
        let handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| atlas_arch::error::Error::Custom("Missing stats cf".to_string()))?;
        let val = self.db.get_cf(&handle, key);
        match val {
            Ok(Some(bytes)) => {
                let mut arr = [0u8; 8];
                let copy_len = bytes.len().min(8);
                arr[..copy_len].copy_from_slice(&bytes[..copy_len]);
                Ok(u64::from_le_bytes(arr))
            }
            Ok(None) => Ok(0),
            Err(e) => Err(atlas_arch::error::Error::Custom(format!(
                "rocksdb get error: {}",
                e
            ))),
        }
    }

    fn put_u64_cf(&self, cf: &str, key: &str, value: u64) -> IndexerResult<()> {
        let handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| atlas_arch::error::Error::Custom("Missing stats cf".to_string()))?;
        self.db
            .put_cf(&handle, key, value.to_le_bytes())
            .map_err(|e| atlas_arch::error::Error::Custom(format!("rocksdb put error: {}", e)))?;
        Ok(())
    }

    /// Returns the latest observed Bitcoin block height persisted by the Bitcoin height pipe.
    pub async fn latest_bitcoin_height(&self) -> IndexerResult<u64> {
        self.get_u64_cf(STATS_CF, BITCOIN_LATEST_HEIGHT_KEY)
    }

    /// Persists the latest observed Bitcoin block height.
    pub async fn set_latest_bitcoin_height(&self, height: u64) -> IndexerResult<()> {
        self.put_u64_cf(STATS_CF, BITCOIN_LATEST_HEIGHT_KEY, height)
    }
}

#[async_trait]
impl CheckpointStore for RocksCheckpointStore {
    async fn last_indexed_height(&self) -> IndexerResult<u64> {
        self.get_u64_cf(STATS_CF, LAST_INDEXED_HEIGHT_KEY)
    }

    async fn set_last_indexed_height(&self, height: u64) -> IndexerResult<()> {
        self.put_u64_cf(STATS_CF, LAST_INDEXED_HEIGHT_KEY, height)
    }
}
