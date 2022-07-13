use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use rocksdb::crocksdb_ffi::{
    CompactionPriority, DBCompressionType, DBInfoLogLevel as InfoLogLevel, DBRateLimiterMode,
    DBStatisticsHistogramType as HistogramType, DBStatisticsTickerType as TickerType,
};
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyOptions, CompactOptions, DBOptions, Env,
    FifoCompactionOptions, IndexType, LRUCacheOptions, ReadOptions, SeekKey, SliceTransform,
    Writable, WriteOptions, DB,
};

use super::tempdir_with_prefix;

#[test]
fn test_wal_sync() {
    // look at ManualLogSyncTest in fault_injection_test.cc
    // They prevent the flush job from running, but I don't think this is necessary
    // if I set manual_wal_flush to true
    
    let path = tempdir_with_prefix("_rust_wal_sync_test");
    let env = Arc::new(Env::new_fault_injection());
    let mut opts = DBOptions::new();
    opts.create_if_missing(true);
    opts.manual_wal_flush(true);
    opts.set_env(env.clone());

    let db = DB::open(opts, path.path().to_str().unwrap()).unwrap();

    let wopts = WriteOptions::new();

    db.put_opt(b"k0-1", b"a", &wopts).unwrap();
    db.flush_wal(true).unwrap();
    assert_eq!(db.get(b"k0-1").unwrap().unwrap(), b"a");
    db.put_opt(b"k0-2", b"b", &wopts).unwrap();
    
    drop(db);
    assert!(env.drop_unsynced_data().is_ok());

    let mut opts2 = DBOptions::new();
    opts2.create_if_missing(true);
    opts2.manual_wal_flush(true);
    opts2.set_env(env.clone());
    let db2 = DB::open(opts2, path.path().to_str().unwrap()).unwrap();
    assert_eq!(db2.get(b"k0-1").unwrap().unwrap(), b"a");
    assert!(db2.get(b"k0-2").unwrap().is_none());
    drop(db2);
}

#[test]
fn test_wal_disabled() {
    // NOTE: if set_fail_on_write is not set to true, RocksDB will write the
    // memtable to disk on shutdown because the WAL is disabled and it knows
    // there is unpersisted data. drop_unsynced_data() doesn't do anything here
    // because there is no WAL to truncate!
    let path = tempdir_with_prefix("_rust_wal_disabled");
    let env = Arc::new(Env::new_fault_injection());
    let mut opts = DBOptions::new();
    opts.set_fail_on_write(true);
    opts.create_if_missing(true);
    opts.manual_wal_flush(true);
    opts.set_env(env.clone());
    let db = DB::open(opts, path.path().to_str().unwrap()).unwrap();

    let mut wopts = WriteOptions::new();
    wopts.disable_wal(true);
    wopts.set_sync(false);
    db.put_opt(b"k1", b"a", &wopts).unwrap();
    db.put_opt(b"k2", b"b", &wopts).unwrap();
    db.put_opt(b"k3", b"c", &wopts).unwrap();
    assert_eq!(db.get(b"k1").unwrap().unwrap(), b"a");
    assert_eq!(db.get(b"k2").unwrap().unwrap(), b"b");
    assert_eq!(db.get(b"k3").unwrap().unwrap(), b"c");

    drop(db);
//    assert!(env.drop_unsynced_data().is_ok());

    let mut opts2 = DBOptions::new();
    opts2.create_if_missing(true);
    opts2.manual_wal_flush(true);
    opts2.set_env(env.clone());
    let db2 = DB::open(opts2, path.path().to_str().unwrap()).unwrap();    
    assert!(db2.get(b"k1").unwrap().is_none());
    assert!(db2.get(b"k2").unwrap().is_none());
    assert!(db2.get(b"k3").unwrap().is_none());
    drop(db2);
}
