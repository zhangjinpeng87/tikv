// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::process;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use super::transport::RaftStoreRouter;
use super::Result;
use crate::import::SSTImporter;
use crate::pd::{Error as PdError, PdClient, PdTask, INVALID_ID};
use crate::raftstore::coprocessor::dispatcher::CoprocessorHost;
use crate::raftstore::store::fsm::{RaftBatchSystem, SendCh};
use crate::raftstore::store::{
    self, keys, Config as StoreConfig, Engines, Peekable, ReadTask, SnapManager, Transport,
};
use crate::server::readpool::ReadPool;
use crate::server::Config as ServerConfig;
use crate::server::ServerRaftStoreRouter;
use crate::storage::{self, Config as StorageConfig, RaftKv, RocksEngine, Storage, CF_RAFT, CF_DEFAULT};
use crate::util::worker::{FutureWorker, Worker};
use crate::util::rocksdb_util;
use kvproto::metapb;
use kvproto::raft_serverpb::{StoreIdent, RegionLocalState};
use protobuf::RepeatedField;
use rocksdb::{Writable, WriteBatch, DB};

const MAX_CHECK_CLUSTER_BOOTSTRAPPED_RETRY_COUNT: u64 = 60;
const CHECK_CLUSTER_BOOTSTRAPPED_RETRY_SECONDS: u64 = 3;

const INIT_EPOCH_VER: u64 = 1;
const INIT_EPOCH_CONF_VER: u64 = 1;

pub const RAFT_INIT_LOG_TERM: u64 = 5;
pub const RAFT_INIT_LOG_INDEX: u64 = 5;
const MAX_SNAP_TRY_CNT: usize = 5;
const RAFT_LOG_MULTI_GET_CNT: u64 = 8;

pub struct SingleNode<C: PdClient + 'static> {
    cluster_id: u64,
    store: metapb::Store,
    store_cfg: StoreConfig,

    pd_client: Arc<C>,
}

impl<C> SingleNode<C>
where
    C: PdClient,
{
    /// Creates a new Node.
    pub fn new(cfg: &ServerConfig, store_cfg: &StoreConfig, pd_client: Arc<C>) -> Self<C> {
        let mut store = metapb::Store::new();
        store.set_id(INVALID_ID);
        if cfg.advertise_addr.is_empty() {
            store.set_address(cfg.addr.clone());
        } else {
            store.set_address(cfg.advertise_addr.clone())
        }
        store.set_version(env!("CARGO_PKG_VERSION").to_string());

        let mut labels = Vec::new();
        for (k, v) in &cfg.labels {
            let mut label = metapb::StoreLabel::new();
            label.set_key(k.to_owned());
            label.set_value(v.to_owned());
            labels.push(label);
        }
        store.set_labels(RepeatedField::from_vec(labels));

        Self {
            cluster_id: cfg.cluster_id,
            store,
            store_cfg: store_cfg.clone(),
            pd_client,
        }
    }

    /// Starts the Node. It tries to bootstrap cluster if the cluster is not
    /// bootstrapped yet.
    pub fn start(&mut self, db: Arc<DB>, pd_worker: FutureWorker<PdTask>) -> Result<()>
    {
        let bootstrapped = self.check_cluster_bootstrapped()?;
        let mut store_id = self.check_store(&db)?;
        if store_id == INVALID_ID {
            store_id = self.bootstrap_store(&db)?;
        } else if !bootstrapped {
            // We have saved data before, and the cluster must be bootstrapped.
            return Err(box_err!(
                "store {} is not empty, but cluster {} is not bootstrapped, \
                 maybe you connected a wrong PD or need to remove the TiKV data \
                 and start again",
                store_id,
                self.cluster_id
            ));
        }

        self.store.set_id(store_id);
        self.check_prepare_bootstrap_cluster(&db)?;
        if !bootstrapped {
            // cluster is not bootstrapped, and we choose first store to bootstrap
            // prepare bootstrap.
            let region = self.prepare_bootstrap_cluster(&db, store_id)?;
            self.bootstrap_cluster(&db, region)?;
        }

        // inform pd.
        self.pd_client.put_store(self.store.clone())?;
        Ok(())
    }

    /// Gets the store id.
    pub fn id(&self) -> u64 {
        self.store.get_id()
    }

    /// Gets a transmission end of a channel which is used to send `Msg` to the
    /// raftstore.
    pub fn get_sendch(&self) -> SendCh {
        SendCh::new(self.system.router(), "raftstore")
    }

    // check store, return store id for the engine.
    // If the store is not bootstrapped, use INVALID_ID.
    fn check_store(&self, db: &DB) -> Result<u64> {
        let res = db.get_msg::<StoreIdent>(keys::STORE_IDENT_KEY)?;
        if res.is_none() {
            return Ok(INVALID_ID);
        }

        let ident = res.unwrap();
        if ident.get_cluster_id() != self.cluster_id {
            error!(
                "cluster ID mismatch: local_id {} remote_id {}. \
                 you are trying to connect to another cluster, please reconnect to the correct PD",
                ident.get_cluster_id(),
                self.cluster_id
            );
            process::exit(1);
        }

        let store_id = ident.get_store_id();
        if store_id == INVALID_ID {
            return Err(box_err!("invalid store ident {:?}", ident));
        }

        Ok(store_id)
    }

    fn alloc_id(&self) -> Result<u64> {
        let id = self.pd_client.alloc_id()?;
        Ok(id)
    }

    fn bootstrap_store(&self, db: &DB) -> Result<u64> {
        let store_id = self.alloc_id()?;
        info!("alloc store id {} ", store_id);

        bootstrap_store(db, self.cluster_id, store_id)?;

        Ok(store_id)
    }

    // Exported for tests.
    #[doc(hidden)]
    pub fn prepare_bootstrap_cluster(&self, db: &DB, store_id: u64) -> Result<metapb::Region> {
        let region_id = self.alloc_id()?;
        info!(
            "alloc first region id {} for cluster {}, store {}",
            region_id, self.cluster_id, store_id
        );
        let peer_id = self.alloc_id()?;
        info!(
            "alloc first peer id {} for first region {}",
            peer_id, region_id
        );

        let region = prepare_bootstrap(db, store_id, region_id, peer_id)?;
        Ok(region)
    }

    fn check_prepare_bootstrap_cluster(&self, db: &DB) -> Result<()> {
        let res = db.get_msg::<metapb::Region>(keys::PREPARE_BOOTSTRAP_KEY)?;
        if res.is_none() {
            return Ok(());
        }

        let first_region = res.unwrap();
        for _ in 0..MAX_CHECK_CLUSTER_BOOTSTRAPPED_RETRY_COUNT {
            match self.pd_client.get_region(b"") {
                Ok(region) => {
                    if region.get_id() == first_region.get_id() {
                        store::check_region_epoch(&region, &first_region)?;
                        clear_prepare_bootstrap_state(db)?;
                    } else {
                        clear_prepare_bootstrap(db, first_region.get_id())?;
                    }
                    return Ok(());
                }

                Err(e) => {
                    warn!("check cluster prepare bootstrapped failed: {:?}", e);
                }
            }
            thread::sleep(Duration::from_secs(
                CHECK_CLUSTER_BOOTSTRAPPED_RETRY_SECONDS,
            ));
        }
        Err(box_err!("check cluster prepare bootstrapped failed"))
    }

    fn bootstrap_cluster(&mut self, db: &Engines, region: metapb::Region) -> Result<()> {
        let region_id = region.get_id();
        match self.pd_client.bootstrap_cluster(self.store.clone(), region) {
            Err(PdError::ClusterBootstrapped(_)) => {
                error!("cluster {} is already bootstrapped", self.cluster_id);
                clear_prepare_bootstrap(db, region_id)?;
                Ok(())
            }
            // TODO: should we clean region for other errors too?
            Err(e) => panic!("bootstrap cluster {} err: {:?}", self.cluster_id, e),
            Ok(_) => {
                clear_prepare_bootstrap_state(db)?;
                info!("bootstrap cluster {} ok", self.cluster_id);
                Ok(())
            }
        }
    }

    fn check_cluster_bootstrapped(&self) -> Result<bool> {
        for _ in 0..MAX_CHECK_CLUSTER_BOOTSTRAPPED_RETRY_COUNT {
            match self.pd_client.is_cluster_bootstrapped() {
                Ok(b) => return Ok(b),
                Err(e) => {
                    warn!("check cluster bootstrapped failed: {:?}", e);
                }
            }
            thread::sleep(Duration::from_secs(
                CHECK_CLUSTER_BOOTSTRAPPED_RETRY_SECONDS,
            ));
        }
        Err(box_err!("check cluster bootstrapped failed"))
    }

    /// Stops the Node.
    pub fn stop(&mut self) -> Result<()> {}
}

// Prepare bootstrap.
pub fn prepare_bootstrap(
    db: &DB,
    store_id: u64,
    region_id: u64,
    peer_id: u64,
) -> Result<metapb::Region> {
    let mut region = metapb::Region::new();
    region.set_id(region_id);
    region.set_start_key(keys::EMPTY_KEY.to_vec());
    region.set_end_key(keys::EMPTY_KEY.to_vec());
    region.mut_region_epoch().set_version(INIT_EPOCH_VER);
    region.mut_region_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);

    let mut peer = metapb::Peer::new();
    peer.set_store_id(store_id);
    peer.set_id(peer_id);
    region.mut_peers().push(peer);

    write_prepare_bootstrap(db, &region)?;

    Ok(region)
}

// Write first region meta and prepare state.
pub fn write_prepare_bootstrap(db: &DB, region: &metapb::Region) -> Result<()> {
    let mut state = RegionLocalState::new();
    state.set_region(region.clone());

    let wb = WriteBatch::new();
    wb.put_msg(keys::PREPARE_BOOTSTRAP_KEY, region)?;
    let handle = rocksdb_util::get_cf_handle(&db, CF_RAFT)?;
    wb.put_msg_cf(handle, &keys::region_state_key(region.get_id()), &state)?;
    write_initial_apply_state(&db, &wb, region.get_id())?;
    db.write(wb)?;
    db.sync_wal()?;

    Ok(())
}

// When we bootstrap the region or handling split new region, we must
// call this to initialize region apply state first.
pub fn write_initial_apply_state<T: Mutable>(
    db: &DB,
    wb: &T,
    region_id: u64,
) -> Result<()> {
    let mut apply_state = RaftApplyState::new();
    apply_state.set_applied_index(RAFT_INIT_LOG_INDEX);
    apply_state
        .mut_truncated_state()
        .set_index(RAFT_INIT_LOG_INDEX);
    apply_state
        .mut_truncated_state()
        .set_term(RAFT_INIT_LOG_TERM);

    let handle = rocksdb_util::get_cf_handle(db, CF_RAFT)?;
    wb.put_msg_cf(handle, &keys::apply_state_key(region_id), &apply_state)?;
    Ok(())
}

// Clear first region meta and prepare state.
pub fn clear_prepare_bootstrap(db: &DB, region_id: u64) -> Result<()> {
    let wb = WriteBatch::new();
    wb.delete(keys::PREPARE_BOOTSTRAP_KEY)?;
    // should clear raft initial state too.
    let handle = rocksdb_util::get_cf_handle(&db, CF_RAFT)?;
    wb.delete_cf(handle, &keys::region_state_key(region_id))?;
    wb.delete_cf(handle, &keys::apply_state_key(region_id))?;
    db.write(wb)?;
    db.sync_wal()?;
    Ok(())
}

// Clear prepare state
pub fn clear_prepare_bootstrap_state(db: &DB) -> Result<()> {
    db.delete(keys::PREPARE_BOOTSTRAP_KEY)?;
    db.sync_wal()?;
    Ok(())
}

// check no any data in range [start_key, end_key)
fn is_range_empty(engine: &DB, cf: &str, start_key: &[u8], end_key: &[u8]) -> Result<bool> {
    let mut count: u32 = 0;
    engine.scan_cf(cf, start_key, end_key, false, |_, _| {
        count += 1;
        Ok(false)
    })?;

    Ok(count == 0)
}

// Bootstrap the store, the DB for this store must be empty and has no data.
fn bootstrap_store(db: &DB, cluster_id: u64, store_id: u64) -> Result<()> {
    let mut ident = StoreIdent::new();

    if !is_range_empty(db, CF_DEFAULT, keys::MIN_KEY, keys::MAX_KEY)? {
        return Err(box_err!("kv store is not empty and has already had data."));
    }

    ident.set_cluster_id(cluster_id);
    ident.set_store_id(store_id);

    db.put_msg(keys::STORE_IDENT_KEY, &ident)?;
    db.sync_wal()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::check_region_epoch;
    use crate::raftstore::store::keys;
    use kvproto::metapb;

    #[test]
    fn test_check_region_epoch() {
        let mut r1 = metapb::Region::new();
        r1.set_id(1);
        r1.set_start_key(keys::EMPTY_KEY.to_vec());
        r1.set_end_key(keys::EMPTY_KEY.to_vec());
        r1.mut_region_epoch().set_version(1);
        r1.mut_region_epoch().set_conf_ver(1);

        let mut r2 = metapb::Region::new();
        r2.set_id(1);
        r2.set_start_key(keys::EMPTY_KEY.to_vec());
        r2.set_end_key(keys::EMPTY_KEY.to_vec());
        r2.mut_region_epoch().set_version(2);
        r2.mut_region_epoch().set_conf_ver(1);

        let mut r3 = metapb::Region::new();
        r3.set_id(1);
        r3.set_start_key(keys::EMPTY_KEY.to_vec());
        r3.set_end_key(keys::EMPTY_KEY.to_vec());
        r3.mut_region_epoch().set_version(1);
        r3.mut_region_epoch().set_conf_ver(2);

        assert!(check_region_epoch(&r1, &r2).is_err());
        assert!(check_region_epoch(&r1, &r3).is_err());
    }
}
