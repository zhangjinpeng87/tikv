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

use super::util::*;
use super::cluster::{Cluster, Simulator};
use super::transport_simulate::DropSnapshot;
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use std::time::Duration;

fn test_transfer_leader<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.run();

    // transfer leader to (2, 2)
    cluster.must_transfer_leader(1, new_peer(2, 2));
    // wait for leader transfer finish
    let (k1, v1) = (b"k1", b"v1");
    cluster.must_put(k1, v1);
    must_get_equal(&cluster.engines[&2], k1, v1);
    // check it
    assert_eq!(cluster.leader_of_region(1), Some(new_peer(2, 2)));

    // transfer leader to (3, 3)
    cluster.must_transfer_leader(1, new_peer(3, 3));
    let (k2, v2) = (b"k2", b"v2");
    cluster.must_put(k2, v2);
    must_get_equal(&cluster.engines[&3], k2, v2);
    assert_eq!(cluster.leader_of_region(1), Some(new_peer(3, 3)));

    let mut region = cluster.get_region(b"k3");
    let mut req = new_request(region.get_id(),
                              region.take_region_epoch(),
                              vec![new_put_cmd(b"k3", b"v3")]);
    req.mut_header().set_peer(new_peer(3, 3));
    // transfer leader to (4, 4)
    cluster.must_transfer_leader(1, new_peer(4, 4));
    // send request to old leader (3, 3) directly and verify it fails
    let resp = cluster.call_command(req, Duration::from_secs(3)).unwrap();
    assert!(resp.get_header().get_error().has_not_leader());
}

#[test]
fn test_server_transfer_leader() {
    let mut cluster = new_node_cluster(0, 5);
    test_transfer_leader(&mut cluster);
}

#[test]
fn test_node_transfer_leader() {
    let mut cluster = new_server_cluster(0, 5);
    test_transfer_leader(&mut cluster);
}

fn test_pd_transfer_leader<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_rule();

    cluster.run();

    cluster.must_put(b"k", b"v");

    for i in 1..4 {
        let id = i as u64;
        // select a new leader to transfer
        pd_client.set_rule(box move |_, peer| {
            if peer.get_id() == id {
                return None;
            }
            new_pd_transfer_leader(new_peer(id, id))
        });


        for _ in 0..100 {
            // reset leader and wait transfer successfully.
            cluster.reset_leader_of_region(1);

            sleep_ms(20);

            if let Some(leader) = cluster.leader_of_region(1) {
                if leader.get_id() == id {
                    break;
                }
            }
        }

        assert_eq!(cluster.leader_of_region(1), Some(new_peer(id, id)));

        // call command on this leader directly, must successfully.
        let mut region = cluster.get_region(b"");
        let mut req = new_request(region.get_id(),
                                  region.take_region_epoch(),
                                  vec![new_get_cmd(b"k")]);
        req.mut_header().set_peer(new_peer(id, id));
        let resp = cluster.call_command(req, Duration::from_secs(3)).unwrap();
        assert!(!resp.get_header().has_error());
        assert_eq!(resp.get_responses()[0].get_get().get_value(), b"v");
    }
}

#[test]
fn test_server_pd_transfer_leader() {
    let mut cluster = new_node_cluster(0, 3);
    test_pd_transfer_leader(&mut cluster);
}

#[test]
fn test_node_pd_transfer_leader() {
    let mut cluster = new_server_cluster(0, 3);
    test_pd_transfer_leader(&mut cluster);
}

fn test_transfer_leader_during_snapshot<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = cluster.pd_client.clone();
    // Disable default max peer count check.
    pd_client.disable_default_rule();
    cluster.cfg.store_cfg.raft_log_gc_tick_interval = 20;
    cluster.cfg.store_cfg.raft_log_gc_limit = 2;

    let r1 = cluster.run_conf_change();
    pd_client.must_add_peer(r1, new_peer(2, 2));

    for i in 0..1024 {
        let key = format!("{:01024}", i);
        let value = format!("{:01024}", i);
        cluster.must_put(key.as_bytes(), value.as_bytes());
    }

    // hook transport and drop all snapshot packet, so follower's status
    // will stay at snapshot.
    cluster.add_filter(DropSnapshot);
    pd_client.must_add_peer(r1, new_peer(3, 3));
    // a just added peer needs wait a couple of ticks, it'll communicate with leader
    // before getting snapshot
    sleep_ms(1000);

    cluster.transfer_leader(r1, new_peer(2, 2));
    cluster.clear_filters();

    sleep_ms(1000);
    cluster.must_put(b"k1", b"v1");
    let leader = cluster.leader_of_region(r1).unwrap();
    must_get_equal(&cluster.engines[&2], b"k1", b"v1");
    assert_eq!(leader, new_peer(1, 1));
}

#[test]
fn test_server_transfer_leader_during_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    test_transfer_leader_during_snapshot(&mut cluster);
}

#[test]
fn test_node_transfer_leader_during_snapshot() {
    let mut cluster = new_server_cluster(0, 3);
    test_transfer_leader_during_snapshot(&mut cluster);
}
