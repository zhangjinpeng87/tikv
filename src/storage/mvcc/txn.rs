// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use super::lock::{Lock, LockType};
use super::metrics::*;
use super::reader::MvccReader;
use super::write::{Write, WriteType};
use super::{Error, Result};
use crate::storage::kv::{Modify, Snapshot};
use crate::storage::{
    Key, Mutation, Options, Statistics, Value, CF_HISTORY, CF_LATEST, CF_LOCK, CF_ROLLBACK,
};
use kvproto::kvrpcpb::IsolationLevel;
use std::fmt;

pub const MAX_TXN_WRITE_SIZE: usize = 32 * 1024;

pub struct GcInfo {
    pub found_versions: usize,
    pub deleted_versions: usize,
    pub is_completed: bool,
}

pub struct MvccTxn<S: Snapshot> {
    reader: MvccReader<S>,
    start_ts: u64,
    writes: Vec<Modify>,
    write_size: usize,
    // collapse continuous rollbacks.
    collapse_rollback: bool,
}

impl<S: Snapshot> fmt::Debug for MvccTxn<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "txn @{}", self.start_ts)
    }
}

impl<S: Snapshot> MvccTxn<S> {
    pub fn new(snapshot: S, start_ts: u64, fill_cache: bool) -> Result<Self> {
        Ok(Self {
            // Todo: use session variable to indicate fill cache or not
            // ScanMode is `None`, since in prewrite and other operations, keys are not given in
            // order and we use prefix seek for each key. An exception is GC, which uses forward
            // scan only.
            // IsolationLevel is `SI`, actually the method we use in MvccTxn does not rely on
            // isolation level, so it can be any value.
            reader: MvccReader::new(
                snapshot.clone(),
                None,
                fill_cache,
                None,
                None,
                IsolationLevel::Si,
            ),
            start_ts,
            writes: vec![],
            write_size: 0,
            collapse_rollback: true,
        })
    }

    pub fn collapse_rollback(&mut self, collapse: bool) {
        self.collapse_rollback = collapse;
    }

    pub fn into_modifies(self) -> Vec<Modify> {
        self.writes
    }

    pub fn take_statistics(&mut self) -> Statistics {
        let mut statistics = Statistics::default();
        self.reader.collect_statistics_into(&mut statistics);
        statistics
    }

    pub fn write_size(&self) -> usize {
        self.write_size
    }

    fn put_lock(&mut self, key: Key, lock: &Lock) {
        let lock = lock.to_bytes();
        self.write_size += CF_LOCK.len() + key.as_encoded().len() + lock.len();
        self.writes.push(Modify::Put(CF_LOCK, key, lock));
    }

    fn lock_key(
        &mut self,
        key: Key,
        lock_type: LockType,
        primary: Vec<u8>,
        value: Option<Value>,
        options: &Options,
    ) {
        let lock = Lock::new(
            lock_type,
            primary,
            self.start_ts,
            options.lock_ttl,
            value,
            options.for_update_ts,
            options.txn_size,
        );
        self.put_lock(key, &lock);
    }

    fn unlock_key(&mut self, key: Key) {
        self.write_size += CF_LOCK.len() + key.as_encoded().len();
        self.writes.push(Modify::Delete(CF_LOCK, key));
    }

    fn put_latest(&mut self, key: Key, value: Value) {
        self.write_size += CF_LATEST.len() + key.as_encoded().len() + value.len();
        self.writes.push(Modify::Put(CF_LATEST, key, value));
    }

    fn put_history(&mut self, key: Key, commit_ts: u64, value: Value) {
        let key = key.append_ts(commit_ts);
        self.write_size += CF_HISTORY.len() + key.as_encoded().len() + value.len();
        self.writes.push(Modify::Put(CF_HISTORY, key, value));
    }

    fn put_rollback(&mut self, key: Key, commit_ts: u64, value: Value) {
        let key = key.append_ts(commit_ts);
        self.write_size += CF_ROLLBACK.len() + key.as_encoded().len() + value.len();
        self.writes.push(Modify::Put(CF_ROLLBACK, key, value));
    }

    fn prewrite_key_value(
        &mut self,
        key: Key,
        lock_type: LockType,
        primary: Vec<u8>,
        value: Option<Value>,
        options: &Options,
    ) {
        self.lock_key(key, lock_type, primary, value, options);
    }

    // Pessimistic transactions only acquire pessimistic locks on row keys.
    // The corrsponding index keys are not locked until pessimistic prewrite.
    // It's possible that lock conflict occours on them, but the isolation is
    // guaranteed by pessimistic locks on row keys, so let TiDB resolves these
    // locks immediately.
    fn handle_non_pessimistic_lock_conflict(
        &self,
        key: Key,
        for_update_ts: u64,
        lock: Lock,
    ) -> Result<()> {
        // The previous pessimistic transaction has been committed or aborted.
        // Resolve it immediately.
        //
        // Because the row key is locked, the optimistic transaction will
        // abort. Resolve it immediately.
        // Optimistic lock's for_update_ts is zero.
        if for_update_ts > lock.for_update_ts {
            let mut info = kvproto::kvrpcpb::LockInfo::default();
            info.set_primary_lock(lock.primary);
            info.set_lock_version(lock.ts);
            info.set_key(key.into_raw()?);
            // Set ttl to 0 so TiDB will resolve lock immediately.
            info.set_lock_ttl(0);
            info.set_txn_size(lock.txn_size);
            Err(Error::KeyIsLocked(info))
        } else {
            Err(Error::Other("stale request".into()))
        }
    }

    pub fn acquire_pessimistic_lock(
        &mut self,
        key: Key,
        primary: &[u8],
        should_not_exist: bool,
        options: &Options,
    ) -> Result<()> {
        let for_update_ts = options.for_update_ts;
        if let Some(lock) = self.reader.load_lock(&key)? {
            if lock.ts != self.start_ts {
                let mut info = kvproto::kvrpcpb::LockInfo::default();
                info.set_primary_lock(lock.primary);
                info.set_lock_version(lock.ts);
                info.set_key(key.into_raw()?);
                info.set_lock_ttl(lock.ttl);
                info.set_txn_size(options.txn_size);
                return Err(Error::KeyIsLocked(info));
            }
            if lock.lock_type != LockType::Pessimistic {
                return Err(Error::LockTypeNotMatch {
                    start_ts: self.start_ts,
                    key: key.into_raw()?,
                    pessimistic: false,
                });
            }
            // Overwrite the lock with small for_update_ts
            if for_update_ts > lock.for_update_ts {
                self.lock_key(key, LockType::Pessimistic, primary.to_vec(), None, options);
            } else {
                MVCC_DUPLICATE_CMD_COUNTER_VEC
                    .acquire_pessimistic_lock
                    .inc();
            }
            return Ok(());
        }

        if let Some(latest) = self.reader.get_latest(&key)? {
            if latest.commit_ts > for_update_ts {
                MVCC_CONFLICT_COUNTER
                    .acquire_pessimistic_lock_conflict
                    .inc();
                return Err(Error::WriteConflict {
                    start_ts: self.start_ts,
                    conflict_start_ts: latest.start_ts,
                    conflict_commit_ts: latest.commit_ts,
                    key: key.into_raw()?,
                    primary: primary.to_vec(),
                });
            }
            if latest.commit_ts > self.start_ts {
                if let Some(rollback) = self.reader.get_rollback(&key, self.start_ts)? {
                    if rollback.write_type == WriteType::Rollback {
                        return Err(Error::PessimisticLockRollbacked {
                            start_ts: self.start_ts,
                            key: key.into_raw()?,
                        });
                    }
                }
            }

            if should_not_exist && latest.write_type == WriteType::Put {
                return Err(Error::AlreadyExist { key: key.to_raw()? });
            }
        }

        self.lock_key(key, LockType::Pessimistic, primary.to_vec(), None, options);

        Ok(())
    }

    pub fn pessimistic_prewrite(
        &mut self,
        mutation: Mutation,
        primary: &[u8],
        is_pessimistic_lock: bool,
        options: &Options,
    ) -> Result<()> {
        let lock_type = LockType::from_mutation(&mutation);
        let (key, value) = mutation.into_key_value();
        if let Some(lock) = self.reader.load_lock(&key)? {
            if lock.ts != self.start_ts {
                // Abort on lock belonging to other transaction if
                // prewrites a pessimistic lock.
                if is_pessimistic_lock {
                    warn!(
                        "prewrite failed (pessimistic lock not found)";
                        "start_ts" => self.start_ts,
                        "key" => %key,
                        "lock_ts" => lock.ts
                    );
                    return Err(Error::PessimisticLockNotFound {
                        start_ts: self.start_ts,
                        key: key.into_raw()?,
                    });
                }
                return self.handle_non_pessimistic_lock_conflict(key, options.for_update_ts, lock);
            } else {
                if lock.lock_type != LockType::Pessimistic {
                    // Duplicated command. No need to overwrite the lock and data.
                    MVCC_DUPLICATE_CMD_COUNTER_VEC.prewrite.inc();
                    return Ok(());
                }
                // The lock is pessimistic and owned by this txn, go through to overwrite it.
            }
        } else if is_pessimistic_lock {
            // Pessimistic lock does not exist, the transaction should be aborted.
            warn!(
                "prewrite failed (pessimistic lock not found)";
                "start_ts" => self.start_ts,
                "key" => %key
            );

            return Err(Error::PessimisticLockNotFound {
                start_ts: self.start_ts,
                key: key.into_raw()?,
            });
        }

        // No need to check data constraint, it's resolved by pessimistic locks.
        self.prewrite_key_value(key, lock_type, primary.to_vec(), value, options);
        Ok(())
    }

    pub fn prewrite(
        &mut self,
        mutation: Mutation,
        primary: &[u8],
        options: &Options,
    ) -> Result<()> {
        let lock_type = LockType::from_mutation(&mutation);
        // For the insert operation, the old key should not be in the system.
        let should_not_exist = mutation.is_insert();
        let (key, value) = mutation.into_key_value();
        if let Some(write) = self.reader.get_latest(&key)? {
            if write.commit_ts >= self.start_ts {
                MVCC_CONFLICT_COUNTER.prewrite_write_conflict.inc();
                return Err(Error::WriteConflict {
                    start_ts: self.start_ts,
                    conflict_start_ts: write.start_ts,
                    conflict_commit_ts: write.commit_ts,
                    key: key.into_raw()?,
                    primary: primary.to_vec(),
                });
            }
            if should_not_exist && write.write_type == WriteType::Put {
                return Err(Error::AlreadyExist { key: key.to_raw()? });
            }
        }
        // Get latest rollback/lock and check conflicts
        if let Some(rollback) = self.reader.seek_rollback(&key, u64::max_value())? {
            if rollback.commit_ts >= self.start_ts {
                MVCC_CONFLICT_COUNTER.prewrite_write_conflict.inc();
                return Err(Error::WriteConflict {
                    start_ts: self.start_ts,
                    conflict_start_ts: rollback.start_ts,
                    conflict_commit_ts: rollback.commit_ts,
                    key: key.into_raw()?,
                    primary: primary.to_vec(),
                });
            }
        }

        // Check whether the current key is locked at any timestamp.
        if let Some(lock) = self.reader.load_lock(&key)? {
            if lock.ts != self.start_ts {
                let mut info = kvproto::kvrpcpb::LockInfo::default();
                info.set_primary_lock(lock.primary);
                info.set_lock_version(lock.ts);
                info.set_key(key.into_raw()?);
                info.set_lock_ttl(lock.ttl);
                info.set_txn_size(lock.txn_size);
                return Err(Error::KeyIsLocked(info));
            }
            // TODO: remove it in future
            if lock.lock_type == LockType::Pessimistic {
                return Err(Error::LockTypeNotMatch {
                    start_ts: self.start_ts,
                    key: key.into_raw()?,
                    pessimistic: true,
                });
            }
            // Duplicated command. No need to overwrite the lock and data.
            MVCC_DUPLICATE_CMD_COUNTER_VEC.prewrite.inc();
            return Ok(());
        }

        self.prewrite_key_value(key, lock_type, primary.to_vec(), value, options);
        Ok(())
    }

    pub fn commit(&mut self, key: Key, commit_ts: u64) -> Result<bool> {
        let (lock_type, value, is_pessimistic_txn) = match self.reader.load_lock(&key)? {
            Some(ref mut lock) if lock.ts == self.start_ts => {
                // A pessimistic lock cannot be committed.
                if lock.lock_type == LockType::Pessimistic {
                    error!(
                        "trying to commit a pessimistic lock";
                        "key" => %key,
                        "start_ts" => self.start_ts,
                        "commit_ts" => commit_ts,
                    );
                    return Err(Error::LockTypeNotMatch {
                        start_ts: self.start_ts,
                        key: key.into_raw()?,
                        pessimistic: true,
                    });
                }
                (lock.lock_type, lock.value.take(), lock.for_update_ts != 0)
            }
            _ => {
                return match self.reader.get_txn_commit_info(&key, self.start_ts)? {
                    Some((_, WriteType::Rollback)) | None => {
                        MVCC_CONFLICT_COUNTER.commit_lock_not_found.inc();
                        // None: related Rollback has been collapsed.
                        // Rollback: rollback by concurrent transaction.
                        info!(
                            "txn conflict (lock not found)";
                            "key" => %key,
                            "start_ts" => self.start_ts,
                            "commit_ts" => commit_ts,
                        );
                        Err(Error::TxnLockNotFound {
                            start_ts: self.start_ts,
                            commit_ts,
                            key: key.into_raw()?,
                        })
                    }
                    // Committed by concurrent transaction.
                    Some((_, WriteType::Put))
                    | Some((_, WriteType::Delete))
                    | Some((_, WriteType::Lock)) => {
                        MVCC_DUPLICATE_CMD_COUNTER_VEC.commit.inc();
                        Ok(false)
                    }
                };
            }
        };
        let write = Write::new(
            WriteType::from_lock_type(lock_type).unwrap(),
            self.start_ts,
            commit_ts,
            value,
        );
        match lock_type {
            LockType::Put | LockType::Delete => {
                // move last version into history
                if let Some(latest) = self.reader.get_latest(&key)? {
                    self.put_history(key.clone(), latest.commit_ts, latest.to_bytes());
                }
                self.put_latest(key.clone(), write.to_bytes());
            }
            LockType::Lock => {
                // store Lock type in rollback cf
                self.put_rollback(key.clone(), write.commit_ts, write.to_bytes());
            }
            LockType::Pessimistic => panic!("unexpected lock type pessimistic in commit stage"),
        }
        self.unlock_key(key);
        Ok(is_pessimistic_txn)
    }

    pub fn rollback(&mut self, key: Key) -> Result<bool> {
        let is_pessimistic_txn = match self.reader.load_lock(&key)? {
            Some(ref lock) if lock.ts == self.start_ts => lock.for_update_ts != 0,
            _ => {
                return match self.reader.get_txn_commit_info(&key, self.start_ts)? {
                    Some((ts, write_type)) => {
                        if write_type == WriteType::Rollback {
                            // return Ok on Rollback already exist
                            MVCC_DUPLICATE_CMD_COUNTER_VEC.rollback.inc();
                            Ok(false)
                        } else {
                            MVCC_CONFLICT_COUNTER.rollback_committed.inc();
                            info!(
                                "txn conflict (committed)";
                                "key" => %key,
                                "start_ts" => self.start_ts,
                                "commit_ts" => ts,
                            );
                            Err(Error::Committed { commit_ts: ts })
                        }
                    }
                    None => {
                        // insert a Rollback to rollback CF when receives Rollback before Prewrite
                        let ts = self.start_ts;
                        let write = Write::new(WriteType::Rollback, ts, ts, None);
                        self.put_rollback(key, ts, write.to_bytes());
                        Ok(false)
                    }
                };
            }
        };
        let ts = self.start_ts;
        let write = Write::new(WriteType::Rollback, self.start_ts, ts, None);
        self.put_rollback(key.clone(), ts, write.to_bytes());
        self.unlock_key(key.clone());
        Ok(is_pessimistic_txn)
    }

    /// Delete any pessimistic lock with small for_update_ts belongs to this transaction.
    pub fn pessimistic_rollback(&mut self, key: Key, for_update_ts: u64) -> Result<()> {
        if let Some(lock) = self.reader.load_lock(&key)? {
            if lock.lock_type == LockType::Pessimistic
                && lock.ts == self.start_ts
                && lock.for_update_ts <= for_update_ts
            {
                self.unlock_key(key);
            }
        }
        Ok(())
    }

    /// Update a primary key's TTL if `advise_ttl > lock.ttl`.
    ///
    /// Returns the new TTL.
    pub fn txn_heart_beat(&mut self, primary_key: Key, advise_ttl: u64) -> Result<u64> {
        if let Some(mut lock) = self.reader.load_lock(&primary_key)? {
            if lock.ts == self.start_ts {
                if lock.ttl < advise_ttl {
                    lock.ttl = advise_ttl;
                    self.put_lock(primary_key, &lock);
                } else {
                    debug!(
                        "txn_heart_beat with advise_ttl not large than current ttl";
                        "primary_key" => %primary_key,
                        "start_ts" => self.start_ts,
                        "advise_ttl" => advise_ttl,
                        "current_ttl" => lock.ttl,
                    );
                }
                return Ok(lock.ttl);
            }
        }

        debug!(
            "txn_heart_beat invoked but lock is absent";
            "primary_key" => %primary_key,
            "start_ts" => self.start_ts,
            "advise_ttl" => advise_ttl,
        );
        Err(Error::TxnLockNotFound {
            start_ts: self.start_ts,
            commit_ts: 0,
            key: primary_key.into_raw()?,
        })
    }

    pub fn gc(&mut self, _key: Key, _safe_point: u64) -> Result<GcInfo> {
        let found_versions = 0;
        let deleted_versions = 0;
        let is_completed = true;

        Ok(GcInfo {
            found_versions,
            deleted_versions,
            is_completed,
        })
    }
}

#[cfg(test)]
mod tests {
    use kvproto::kvrpcpb::Context;

    use crate::storage::kv::Engine;
    use crate::storage::mvcc::tests::*;
    use crate::storage::mvcc::MvccTxn;
    use crate::storage::mvcc::WriteType;
    use crate::storage::{Key, Mutation, Options, TestEngineBuilder, SHORT_VALUE_MAX_LEN};

    use std::u64;

    fn test_mvcc_txn_read_imp(k1: &[u8], k2: &[u8], v: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();

        must_get_none(&engine, k1, 1);

        must_prewrite_put(&engine, k1, v, k1, 2);
        must_rollback(&engine, k1, 2);
        // should ignore rollback
        must_get_none(&engine, k1, 3);

        must_prewrite_lock(&engine, k1, k1, 3);
        must_commit(&engine, k1, 3, 4);
        // should ignore read lock
        must_get_none(&engine, k1, 5);

        must_prewrite_put(&engine, k1, v, k1, 5);
        must_prewrite_put(&engine, k2, v, k1, 5);
        // should not be affected by later locks
        must_get_none(&engine, k1, 4);
        // should read pending locks
        must_get_err(&engine, k1, 7);
        // should ignore the primary lock and get none when reading the latest record
        must_get_none(&engine, k1, u64::MAX);
        // should read secondary locks even when reading the latest record
        must_get_err(&engine, k2, u64::MAX);

        must_commit(&engine, k1, 5, 10);
        must_commit(&engine, k2, 5, 10);
        must_get_none(&engine, k1, 3);
        // should not read with ts < commit_ts
        must_get_none(&engine, k1, 7);
        // should read with ts > commit_ts
        must_get(&engine, k1, 13, v);
        // should read the latest record if `ts == u64::MAX`
        must_get(&engine, k1, u64::MAX, v);

        must_prewrite_delete(&engine, k1, k1, 15);
        // should ignore the lock and get previous record when reading the latest record
        must_get(&engine, k1, u64::MAX, v);
        must_commit(&engine, k1, 15, 20);
        must_get_none(&engine, k1, 3);
        must_get_none(&engine, k1, 7);
        must_get(&engine, k1, 13, v);
        must_get(&engine, k1, 17, v);
        must_get_none(&engine, k1, 23);

        // intersecting timestamps with pessimistic txn
        // T1: start_ts = 25, commit_ts = 27
        // T2: start_ts = 23, commit_ts = 31
        must_prewrite_put(&engine, k1, v, k1, 25);
        must_commit(&engine, k1, 25, 27);
        must_acquire_pessimistic_lock(&engine, k1, k1, 23, 29);
        must_get(&engine, k1, 30, v);
        must_pessimistic_prewrite_delete(&engine, k1, k1, 23, 29, true);
        must_commit(&engine, k1, 23, 31);
        must_get(&engine, k1, 30, v);
        must_get_none(&engine, k1, 32);
    }

    #[test]
    fn test_mvcc_txn_read() {
        test_mvcc_txn_read_imp(b"k1", b"k2", b"v1");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_mvcc_txn_read_imp(b"k1", b"k2", &long_value);
    }

    fn test_mvcc_txn_prewrite_imp(k: &[u8], v: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&engine, k, v, k, 5);
        // Key is locked.
        must_locked(&engine, k, 5);
        // Retry prewrite.
        must_prewrite_put(&engine, k, v, k, 5);
        // Conflict.
        must_prewrite_lock_err(&engine, k, k, 6);

        must_commit(&engine, k, 5, 10);
        must_written(&engine, k, 5, 10, WriteType::Put);
        // Delayed prewrite request after committing should do nothing.
        must_prewrite_put_err(&engine, k, v, k, 5);
        must_unlocked(&engine, k);
        // Write conflict.
        must_prewrite_lock_err(&engine, k, k, 6);
        must_unlocked(&engine, k);
        // Not conflict.
        must_prewrite_lock(&engine, k, k, 12);
        must_locked(&engine, k, 12);
        must_rollback(&engine, k, 12);
        must_unlocked(&engine, k);
        must_written(&engine, k, 12, 12, WriteType::Rollback);
        // Cannot retry Prewrite after rollback.
        must_prewrite_lock_err(&engine, k, k, 12);
        // Can prewrite after rollback.
        must_prewrite_delete(&engine, k, k, 13);
        must_rollback(&engine, k, 13);
        must_unlocked(&engine, k);
    }

    #[test]
    fn test_mvcc_txn_prewrite_insert() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let (k1, v1, v2, v3) = (b"k1", b"v1", b"v2", b"v3");
        must_prewrite_put(&engine, k1, v1, k1, 1);
        must_commit(&engine, k1, 1, 2);

        // "k1" already exist, returns AlreadyExist error.
        assert!(try_prewrite_insert(&engine, k1, v2, k1, 3).is_err());

        // Delete "k1"
        must_prewrite_delete(&engine, k1, k1, 4);
        must_commit(&engine, k1, 4, 5);

        // After delete "k1", insert returns ok.
        assert!(try_prewrite_insert(&engine, k1, v2, k1, 6).is_ok());
        must_commit(&engine, k1, 6, 7);

        // Rollback
        must_prewrite_put(&engine, k1, v3, k1, 8);
        must_rollback(&engine, k1, 8);

        assert!(try_prewrite_insert(&engine, k1, v3, k1, 9).is_err());

        // Delete "k1" again
        must_prewrite_delete(&engine, k1, k1, 10);
        must_commit(&engine, k1, 10, 11);

        // Rollback again
        must_prewrite_put(&engine, k1, v3, k1, 12);
        must_rollback(&engine, k1, 12);

        // After delete "k1", insert returns ok.
        assert!(try_prewrite_insert(&engine, k1, v2, k1, 13).is_ok());
        must_commit(&engine, k1, 13, 14);
    }

    #[test]
    fn test_rollback_lock() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (k, v) = (b"k1", b"v1");
        must_prewrite_put(&engine, k, v, k, 5);
        must_commit(&engine, k, 5, 10);

        // Lock
        must_prewrite_lock(&engine, k, k, 15);
        must_locked(&engine, k, 15);

        // Rollback lock
        must_rollback(&engine, k, 15);
    }

    #[test]
    fn test_rollback_del() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (k, v) = (b"k1", b"v1");
        must_prewrite_put(&engine, k, v, k, 5);
        must_commit(&engine, k, 5, 10);

        // Prewrite delete
        must_prewrite_delete(&engine, k, k, 15);
        must_locked(&engine, k, 15);

        // Rollback delete
        must_rollback(&engine, k, 15);
    }

    #[test]
    fn test_mvcc_txn_prewrite() {
        test_mvcc_txn_prewrite_imp(b"k1", b"v1");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_mvcc_txn_prewrite_imp(b"k2", &long_value);
    }

    fn test_mvcc_txn_commit_ok_imp(k1: &[u8], v1: &[u8], k2: &[u8], k3: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();
        must_prewrite_put(&engine, k1, v1, k1, 10);
        must_prewrite_lock(&engine, k2, k1, 10);
        must_prewrite_delete(&engine, k3, k1, 10);
        must_locked(&engine, k1, 10);
        must_locked(&engine, k2, 10);
        must_locked(&engine, k3, 10);
        must_commit(&engine, k1, 10, 15);
        must_commit(&engine, k2, 10, 15);
        must_commit(&engine, k3, 10, 15);
        must_written(&engine, k1, 10, 15, WriteType::Put);
        must_written(&engine, k2, 10, 15, WriteType::Lock);
        must_written(&engine, k3, 10, 15, WriteType::Delete);
        // commit should be idempotent
        must_commit(&engine, k1, 10, 15);
        must_commit(&engine, k2, 10, 15);
        must_commit(&engine, k3, 10, 15);
    }

    #[test]
    fn test_mvcc_txn_commit_ok() {
        test_mvcc_txn_commit_ok_imp(b"x", b"v", b"y", b"z");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_mvcc_txn_commit_ok_imp(b"x", &long_value, b"y", b"z");
    }

    fn test_mvcc_txn_commit_err_imp(k: &[u8], v: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();

        // Not prewrite yet
        must_commit_err(&engine, k, 1, 2);
        must_prewrite_put(&engine, k, v, k, 5);
        // start_ts not match
        must_commit_err(&engine, k, 4, 5);
        must_rollback(&engine, k, 5);
        // commit after rollback
        must_commit_err(&engine, k, 5, 6);
    }

    #[test]
    fn test_mvcc_txn_commit_err() {
        test_mvcc_txn_commit_err_imp(b"k", b"v");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_mvcc_txn_commit_err_imp(b"k2", &long_value);
    }

    #[test]
    fn test_mvcc_txn_rollback_after_commit() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k";
        let v = b"v";
        let t1 = 1;
        let t2 = 10;
        let t3 = 20;
        let t4 = 30;

        must_prewrite_put(&engine, k, v, k, t1);

        must_rollback(&engine, k, t2);
        must_rollback(&engine, k, t2);
        must_rollback(&engine, k, t4);

        must_commit(&engine, k, t1, t3);
        // The rollback should be failed since the transaction
        // was committed before.
        must_rollback_err(&engine, k, t1);
        must_get(&engine, k, t4, v);
    }

    fn test_mvcc_txn_rollback_imp(k: &[u8], v: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&engine, k, v, k, 5);
        must_rollback(&engine, k, 5);
        // Rollback should be idempotent
        must_rollback(&engine, k, 5);
        // Lock should be released after rollback
        must_unlocked(&engine, k);
        must_prewrite_lock(&engine, k, k, 10);
        must_rollback(&engine, k, 10);
        // data should be dropped after rollback
        must_get_none(&engine, k, 20);

        // Can't rollback committed transaction.
        must_prewrite_put(&engine, k, v, k, 25);
        must_commit(&engine, k, 25, 30);
        must_rollback_err(&engine, k, 25);
        must_rollback_err(&engine, k, 25);

        // Can't rollback other transaction's lock
        must_prewrite_delete(&engine, k, k, 35);
        must_rollback(&engine, k, 34);
        must_rollback(&engine, k, 36);
        must_written(&engine, k, 34, 34, WriteType::Rollback);
        must_written(&engine, k, 36, 36, WriteType::Rollback);
        must_locked(&engine, k, 35);
        must_commit(&engine, k, 35, 40);
        must_get(&engine, k, 39, v);
        must_get_none(&engine, k, 41);
    }

    #[test]
    fn test_mvcc_txn_rollback() {
        test_mvcc_txn_rollback_imp(b"k", b"v");

        let long_value = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_mvcc_txn_rollback_imp(b"k2", &long_value);
    }

    #[test]
    fn test_mvcc_txn_rollback_before_prewrite() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let key = b"key";
        must_rollback(&engine, key, 5);
        must_prewrite_lock_err(&engine, key, key, 5);
    }

    fn test_gc_imp(k: &[u8], v1: &[u8], v2: &[u8], v3: &[u8], v4: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&engine, k, v1, k, 5);
        must_commit(&engine, k, 5, 10);
        must_prewrite_put(&engine, k, v2, k, 15);
        must_commit(&engine, k, 15, 20);
        must_prewrite_delete(&engine, k, k, 25);
        must_commit(&engine, k, 25, 30);
        must_prewrite_put(&engine, k, v3, k, 35);
        must_commit(&engine, k, 35, 40);
        must_prewrite_lock(&engine, k, k, 45);
        must_commit(&engine, k, 45, 50);
        must_prewrite_put(&engine, k, v4, k, 55);
        must_rollback(&engine, k, 55);

        // Transactions:
        // startTS commitTS Command
        // --
        // 55      -        PUT "x55" (Rollback)
        // 45      50       LOCK
        // 35      40       PUT "x35"
        // 25      30       DELETE
        // 15      20       PUT "x15"
        //  5      10       PUT "x5"

        // CF data layout:
        // ts CFDefault   CFWrite
        // --
        // 55             Rollback(PUT,50)
        // 50             Commit(LOCK,45)
        // 45
        // 40             Commit(PUT,35)
        // 35   x35
        // 30             Commit(Delete,25)
        // 25
        // 20             Commit(PUT,15)
        // 15   x15
        // 10             Commit(PUT,5)
        // 5    x5

        must_gc(&engine, k, 12);
        must_get(&engine, k, 12, v1);

        must_gc(&engine, k, 22);
        must_get(&engine, k, 22, v2);
        must_get_none(&engine, k, 12);

        must_gc(&engine, k, 32);
        must_get_none(&engine, k, 22);
        must_get_none(&engine, k, 35);

        must_gc(&engine, k, 60);
        must_get(&engine, k, 62, v3);
    }

    #[test]
    fn test_gc() {
        test_gc_imp(b"k1", b"v1", b"v2", b"v3", b"v4");

        let v1 = "x".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v2 = "y".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v3 = "z".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v4 = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_gc_imp(b"k2", &v1, &v2, &v3, &v4);
    }

    fn test_write_imp(k: &[u8], v: &[u8], _k2: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&engine, k, v, k, 5);

        must_commit(&engine, k, 5, 10);
        must_get_commit_ts(&engine, k, 5, 10);

        must_prewrite_delete(&engine, k, k, 15);
        must_rollback(&engine, k, 15);
        must_get_commit_ts(&engine, k, 5, 10);
        must_get_commit_ts_none(&engine, k, 15);

        must_prewrite_lock(&engine, k, k, 25);
        must_commit(&engine, k, 25, 30);
        must_get_commit_ts(&engine, k, 25, 30);
    }

    #[test]
    fn test_write() {
        test_write_imp(b"kk", b"v1", b"k");

        let v2 = "x".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_write_imp(b"kk", &v2, b"k");
    }

    fn test_write_size_imp(k: &[u8], v: &[u8], pk: &[u8]) {
        let engine = TestEngineBuilder::new().build().unwrap();
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, 10, true).unwrap();
        let key = Key::from_raw(k);
        assert_eq!(txn.write_size, 0);

        txn.prewrite(
            Mutation::Put((key.clone(), v.to_vec())),
            pk,
            &Options::default(),
        )
        .unwrap();
        assert!(txn.write_size() > 0);
        engine.write(&ctx, txn.into_modifies()).unwrap();

        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, 10, true).unwrap();
        txn.commit(key, 15).unwrap();
        assert!(txn.write_size() > 0);
        engine.write(&ctx, txn.into_modifies()).unwrap();
    }

    #[test]
    fn test_write_size() {
        test_write_size_imp(b"key", b"value", b"pk");

        let v = "x".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_write_size_imp(b"key", &v, b"pk");
    }

    #[test]
    fn test_skip_constraint_check() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let (key, value) = (b"key", b"value");

        must_prewrite_put(&engine, key, value, key, 5);
        must_commit(&engine, key, 5, 10);

        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, 5, true).unwrap();
        assert!(txn
            .prewrite(
                Mutation::Put((Key::from_raw(key), value.to_vec())),
                key,
                &Options::default()
            )
            .is_err());

        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, 5, true).unwrap();
        let mut opt = Options::default();
        opt.skip_constraint_check = true;
        assert!(txn
            .prewrite(
                Mutation::Put((Key::from_raw(key), value.to_vec())),
                key,
                &opt
            )
            .is_ok());
    }

    #[test]
    fn test_read_commit() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let (key, v1, v2) = (b"key", b"v1", b"v2");

        must_prewrite_put(&engine, key, v1, key, 5);
        must_commit(&engine, key, 5, 10);
        must_prewrite_put(&engine, key, v2, key, 15);
        must_get_err(&engine, key, 20);
        must_get_rc(&engine, key, 12, v1);
        must_get_rc(&engine, key, 20, v1);
    }

    #[test]
    fn test_pessimistic_lock() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k1";
        let v = b"v1";

        // TODO: Some corner cases don't give proper results. Although they are not important, we
        // should consider whether they are better to be fixed.

        // Normal
        must_acquire_pessimistic_lock(&engine, k, k, 1, 1);
        must_pessimistic_locked(&engine, k, 1, 1);
        must_pessimistic_prewrite_put(&engine, k, v, k, 1, 1, true);
        must_locked(&engine, k, 1);
        must_commit(&engine, k, 1, 2);
        must_unlocked(&engine, k);

        // Lock conflict
        must_prewrite_put(&engine, k, v, k, 3);
        must_acquire_pessimistic_lock_err(&engine, k, k, 4, 4);
        must_rollback(&engine, k, 3);
        must_unlocked(&engine, k);
        must_acquire_pessimistic_lock(&engine, k, k, 5, 5);
        must_prewrite_lock_err(&engine, k, k, 6);
        must_acquire_pessimistic_lock_err(&engine, k, k, 6, 6);
        must_rollback(&engine, k, 5);
        must_unlocked(&engine, k);

        // Data conflict
        must_prewrite_put(&engine, k, v, k, 7);
        must_commit(&engine, k, 7, 9);
        must_unlocked(&engine, k);
        must_prewrite_lock_err(&engine, k, k, 8);
        must_acquire_pessimistic_lock_err(&engine, k, k, 8, 8);
        must_acquire_pessimistic_lock(&engine, k, k, 8, 9);
        must_pessimistic_prewrite_put(&engine, k, v, k, 8, 8, true);
        must_commit(&engine, k, 8, 10);
        must_unlocked(&engine, k);

        // Rollback
        must_acquire_pessimistic_lock(&engine, k, k, 11, 11);
        must_pessimistic_locked(&engine, k, 11, 11);
        must_rollback(&engine, k, 11);
        must_acquire_pessimistic_lock_err(&engine, k, k, 11, 11);
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 11, 11, true);
        must_prewrite_lock_err(&engine, k, k, 11);
        must_unlocked(&engine, k);

        must_acquire_pessimistic_lock(&engine, k, k, 12, 12);
        must_pessimistic_prewrite_put(&engine, k, v, k, 12, 12, true);
        must_locked(&engine, k, 12);
        must_rollback(&engine, k, 12);
        must_acquire_pessimistic_lock_err(&engine, k, k, 12, 12);
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 12, 12, true);
        must_prewrite_lock_err(&engine, k, k, 12);
        must_unlocked(&engine, k);

        // Duplicated
        must_acquire_pessimistic_lock(&engine, k, k, 13, 13);
        must_pessimistic_locked(&engine, k, 13, 13);
        must_acquire_pessimistic_lock(&engine, k, k, 13, 13);
        must_pessimistic_locked(&engine, k, 13, 13);
        must_pessimistic_prewrite_put(&engine, k, v, k, 13, 13, true);
        must_locked(&engine, k, 13);
        must_pessimistic_prewrite_put(&engine, k, v, k, 13, 13, true);
        must_locked(&engine, k, 13);
        must_commit(&engine, k, 13, 14);
        must_unlocked(&engine, k);
        must_commit(&engine, k, 13, 14);
        must_unlocked(&engine, k);

        // Pessimistic lock doesn't block reads.
        must_acquire_pessimistic_lock(&engine, k, k, 15, 15);
        must_pessimistic_locked(&engine, k, 15, 15);
        must_get(&engine, k, 16, v);
        must_pessimistic_prewrite_delete(&engine, k, k, 15, 15, true);
        must_get_err(&engine, k, 16);
        must_commit(&engine, k, 15, 17);

        // Rollback
        must_acquire_pessimistic_lock(&engine, k, k, 18, 18);
        must_rollback(&engine, k, 18);
        must_unlocked(&engine, k);
        must_prewrite_put(&engine, k, v, k, 19);
        must_commit(&engine, k, 19, 20);
        must_acquire_pessimistic_lock_err(&engine, k, k, 18, 21);
        must_unlocked(&engine, k);

        // Prewrite non-exist pessimistic lock
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 22, 22, true);

        // LockTypeNotMatch
        must_prewrite_put(&engine, k, v, k, 23);
        must_locked(&engine, k, 23);
        must_acquire_pessimistic_lock_err(&engine, k, k, 23, 23);
        must_rollback(&engine, k, 23);
        must_acquire_pessimistic_lock(&engine, k, k, 24, 24);
        must_pessimistic_locked(&engine, k, 24, 24);
        must_commit_err(&engine, k, 24, 25);
        must_rollback(&engine, k, 24);

        // Acquire lock on a prewritten key should fail.
        must_acquire_pessimistic_lock(&engine, k, k, 26, 26);
        must_pessimistic_locked(&engine, k, 26, 26);
        must_pessimistic_prewrite_delete(&engine, k, k, 26, 26, true);
        must_locked(&engine, k, 26);
        must_acquire_pessimistic_lock_err(&engine, k, k, 26, 26);
        must_locked(&engine, k, 26);

        // Acquire lock on a committed key should fail.
        must_commit(&engine, k, 26, 27);
        must_unlocked(&engine, k);
        must_get_none(&engine, k, 28);
        must_acquire_pessimistic_lock_err(&engine, k, k, 26, 26);
        must_unlocked(&engine, k);
        must_get_none(&engine, k, 28);
        // Pessimistic prewrite on a committed key should fail.
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 26, 26, true);
        must_unlocked(&engine, k);
        must_get_none(&engine, k, 28);
        // Currently we cannot avoid this.
        must_acquire_pessimistic_lock(&engine, k, k, 26, 29);
        must_pessimistic_rollback(&engine, k, 26, 29);
        must_unlocked(&engine, k);

        // Non pessimistic key in pessimistic transaction.
        must_pessimistic_prewrite_put(&engine, k, v, k, 30, 30, false);
        must_locked(&engine, k, 30);
        must_commit(&engine, k, 30, 31);
        must_unlocked(&engine, k);
        must_get_commit_ts(&engine, k, 30, 31);

        // Rollback collapsed.
        must_rollback_collapsed(&engine, k, 32);
        must_rollback_collapsed(&engine, k, 33);
        must_acquire_pessimistic_lock_err(&engine, k, k, 32, 32);
        // Currently we cannot avoid this.
        must_acquire_pessimistic_lock(&engine, k, k, 32, 34);
        must_pessimistic_rollback(&engine, k, 32, 34);
        must_unlocked(&engine, k);

        // Acquire lock when there is lock with different for_update_ts.
        must_acquire_pessimistic_lock(&engine, k, k, 35, 36);
        must_pessimistic_locked(&engine, k, 35, 36);
        must_acquire_pessimistic_lock(&engine, k, k, 35, 35);
        must_pessimistic_locked(&engine, k, 35, 36);
        must_acquire_pessimistic_lock(&engine, k, k, 35, 37);
        must_pessimistic_locked(&engine, k, 35, 37);

        // Cannot prewrite when there is another transaction's pessimistic lock.
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 36, 36, true);
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 36, 38, true);
        must_pessimistic_locked(&engine, k, 35, 37);
        // Cannot prewrite when there is another transaction's non-pessimistic lock.
        must_pessimistic_prewrite_put(&engine, k, v, k, 35, 37, true);
        must_locked(&engine, k, 35);
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 36, 38, true);
        must_locked(&engine, k, 35);

        // Commit pessimistic transaction's key but with smaller commit_ts than for_update_ts.
        // Currently not checked, so in this case it will actually be successfully committed.
        must_commit(&engine, k, 35, 36);
        must_unlocked(&engine, k);
        must_get_commit_ts(&engine, k, 35, 36);

        // Prewrite meets pessimistic lock on a non-pessimistic key.
        // Currently not checked, so prewrite will success.
        must_acquire_pessimistic_lock(&engine, k, k, 40, 40);
        must_pessimistic_locked(&engine, k, 40, 40);
        must_pessimistic_prewrite_put(&engine, k, v, k, 40, 40, false);
        must_locked(&engine, k, 40);
        must_commit(&engine, k, 40, 41);
        must_unlocked(&engine, k);

        // Prewrite with different for_update_ts.
        // Currently not checked.
        must_acquire_pessimistic_lock(&engine, k, k, 42, 45);
        must_pessimistic_locked(&engine, k, 42, 45);
        must_pessimistic_prewrite_put(&engine, k, v, k, 42, 43, true);
        must_locked(&engine, k, 42);
        must_commit(&engine, k, 42, 45);
        must_unlocked(&engine, k);

        must_acquire_pessimistic_lock(&engine, k, k, 46, 47);
        must_pessimistic_locked(&engine, k, 46, 47);
        must_pessimistic_prewrite_put(&engine, k, v, k, 46, 48, true);
        must_locked(&engine, k, 46);
        must_commit(&engine, k, 46, 49);
        must_unlocked(&engine, k);

        // Prewrite on non-pessimistic key meets write with larger commit_ts than current
        // for_update_ts (non-pessimistic data conflict).
        // Normally non-pessimistic keys in pessimistic transactions are used when we are sure that
        // there won't be conflicts. So this case is also not checked, and prewrite will succeeed.
        must_pessimistic_prewrite_put(&engine, k, v, k, 47, 48, false);
        must_locked(&engine, k, 47);
        must_rollback(&engine, k, 47);
        must_unlocked(&engine, k);

        // start_ts and commit_ts interlacing
        for start_ts in &[140, 150, 160] {
            let for_update_ts = start_ts + 48;
            let commit_ts = start_ts + 50;
            must_acquire_pessimistic_lock(&engine, k, k, *start_ts, for_update_ts);
            must_pessimistic_prewrite_put(&engine, k, v, k, *start_ts, for_update_ts, true);
            must_commit(&engine, k, *start_ts, commit_ts);
            must_get(&engine, k, commit_ts + 1, v);
        }

        must_rollback(&engine, k, 170);

        // Now the data should be like: (start_ts -> commit_ts)
        // 140 -> 190
        // 150 -> 200
        // 160 -> 210
        // 170 -> rollback
        must_get_commit_ts(&engine, k, 140, 190);
        must_get_commit_ts(&engine, k, 150, 200);
        must_get_commit_ts(&engine, k, 160, 210);
        must_get_rollback_ts(&engine, k, 170);
    }

    #[test]
    fn test_non_pessimistic_lock_conflict() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k1";
        let v = b"v1";

        must_prewrite_put(&engine, k, v, k, 2);
        must_locked(&engine, k, 2);
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 1, 1, false);
        must_pessimistic_prewrite_put_err(&engine, k, v, k, 3, 3, false);
    }

    #[test]
    fn test_pessimistic_rollback() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k1";
        let v = b"v1";

        // Normal
        must_acquire_pessimistic_lock(&engine, k, k, 1, 1);
        must_pessimistic_locked(&engine, k, 1, 1);
        must_pessimistic_rollback(&engine, k, 1, 1);
        must_unlocked(&engine, k);
        must_get_commit_ts_none(&engine, k, 1);
        // Pessimistic rollback is idempotent
        must_pessimistic_rollback(&engine, k, 1, 1);
        must_unlocked(&engine, k);
        must_get_commit_ts_none(&engine, k, 1);

        // Succeed if the lock doesn't exist.
        must_pessimistic_rollback(&engine, k, 2, 2);

        // Do nothing if meets other transaction's pessimistic lock
        must_acquire_pessimistic_lock(&engine, k, k, 2, 3);
        must_pessimistic_rollback(&engine, k, 1, 1);
        must_pessimistic_rollback(&engine, k, 1, 2);
        must_pessimistic_rollback(&engine, k, 1, 3);
        must_pessimistic_rollback(&engine, k, 1, 4);
        must_pessimistic_rollback(&engine, k, 3, 3);
        must_pessimistic_rollback(&engine, k, 4, 4);

        // Succeed if for_update_ts is larger; do nothing if for_update_ts is smaller.
        must_pessimistic_locked(&engine, k, 2, 3);
        must_pessimistic_rollback(&engine, k, 2, 2);
        must_pessimistic_locked(&engine, k, 2, 3);
        must_pessimistic_rollback(&engine, k, 2, 4);
        must_unlocked(&engine, k);

        // Do nothing if rollbacks a non-pessimistic lock.
        must_prewrite_put(&engine, k, v, k, 3);
        must_locked(&engine, k, 3);
        must_pessimistic_rollback(&engine, k, 3, 3);
        must_locked(&engine, k, 3);

        // Do nothing if meets other transaction's optimistic lock
        must_pessimistic_rollback(&engine, k, 2, 2);
        must_pessimistic_rollback(&engine, k, 2, 3);
        must_pessimistic_rollback(&engine, k, 2, 4);
        must_pessimistic_rollback(&engine, k, 4, 4);
        must_locked(&engine, k, 3);

        // Do nothing if committed
        must_commit(&engine, k, 3, 4);
        must_unlocked(&engine, k);
        must_get_commit_ts(&engine, k, 3, 4);
        must_pessimistic_rollback(&engine, k, 3, 3);
        must_pessimistic_rollback(&engine, k, 3, 4);
        must_pessimistic_rollback(&engine, k, 3, 5);
    }

    #[test]
    fn test_overwrite_pessimistic_lock() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k1";

        must_acquire_pessimistic_lock(&engine, k, k, 1, 2);
        must_pessimistic_locked(&engine, k, 1, 2);
        must_acquire_pessimistic_lock(&engine, k, k, 1, 1);
        must_pessimistic_locked(&engine, k, 1, 2);
        must_acquire_pessimistic_lock(&engine, k, k, 1, 3);
        must_pessimistic_locked(&engine, k, 1, 3);
    }

    #[test]
    fn test_txn_heart_beat() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let (k, v) = (b"k1", b"v1");

        let test = |ts| {
            // Do nothing if advise_ttl is less smaller than current TTL.
            must_txn_heart_beat(&engine, k, ts, 90, 100);
            // Return the new TTL if the TTL when the TTL is updated.
            must_txn_heart_beat(&engine, k, ts, 110, 110);
            // The lock's TTL is updated and persisted into the db.
            must_txn_heart_beat(&engine, k, ts, 90, 110);
            // Heart beat another transaction's lock will lead to an error.
            must_txn_heart_beat_err(&engine, k, ts - 1, 150);
            must_txn_heart_beat_err(&engine, k, ts + 1, 150);
            // The existing lock is not changed.
            must_txn_heart_beat(&engine, k, ts, 90, 110);
        };

        // No lock.
        must_txn_heart_beat_err(&engine, k, 5, 100);

        // Create a lock with TTL=100.
        // The initial TTL will be set to 0 after calling must_prewrite_put. Update it first.
        must_prewrite_put(&engine, k, v, k, 5);
        must_locked(&engine, k, 5);
        must_txn_heart_beat(&engine, k, 5, 100, 100);

        test(5);

        must_locked(&engine, k, 5);
        must_commit(&engine, k, 5, 10);
        must_unlocked(&engine, k);

        // No lock.
        must_txn_heart_beat_err(&engine, k, 5, 100);
        must_txn_heart_beat_err(&engine, k, 10, 100);

        must_acquire_pessimistic_lock(&engine, k, k, 8, 15);
        must_pessimistic_locked(&engine, k, 8, 15);
        must_txn_heart_beat(&engine, k, 8, 100, 100);

        test(8);

        must_pessimistic_locked(&engine, k, 8, 15);
    }

    #[test]
    fn test_constraint_check_with_overlapping_txn() {
        let engine = TestEngineBuilder::new().build().unwrap();

        let k = b"k1";
        let v = b"v1";

        must_prewrite_put(&engine, k, v, k, 10);
        must_commit(&engine, k, 10, 11);
        must_acquire_pessimistic_lock(&engine, k, k, 5, 12);
        must_pessimistic_prewrite_lock(&engine, k, k, 5, 12, true);
        must_commit(&engine, k, 5, 15);

        // Now in write cf:
        // start_ts = 10, commit_ts = 11, Put("v1")
        // start_ts = 5,  commit_ts = 15, Lock

        must_get(&engine, k, 19, v);
        assert!(try_prewrite_insert(&engine, k, v, k, 20).is_err());
    }
}
