// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use std::thread::*;
use std::time::*;

use fail;

use test_raftstore::*;
use tikv_util::config::*;

#[test]
fn test_pending_peers() {
    let _guard = crate::setup();
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(100);

    let region_worker_fp = "region_apply_snap";

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let region_id = cluster.run_conf_change();
    pd_client.must_add_peer(region_id, new_peer(2, 2));

    cluster.must_put(b"k1", b"v1");

    fail::cfg(region_worker_fp, "sleep(2000)").unwrap();
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    sleep(Duration::from_secs(1));
    let pending_peers = pd_client.get_pending_peers();
    // Region worker is not started, snapshot should not be applied yet.
    assert_eq!(pending_peers[&3], new_peer(3, 3));
    // But it will be applied finally.
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    sleep(Duration::from_millis(100));
    let pending_peers = pd_client.get_pending_peers();
    assert!(pending_peers.is_empty());
}
