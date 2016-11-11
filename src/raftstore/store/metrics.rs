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

use prometheus::{CounterVec, GaugeVec, Histogram};

lazy_static! {
    pub static ref PEER_PROPOSAL_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_raftstore_proposal_total",
            "Total number of proposal made.",
            &["type"]
        ).unwrap();

    pub static ref PEER_ADMIN_CMD_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_raftstore_admin_cmd_total",
            "Total number of admin cmd processed.",
            &["type", "status"]
        ).unwrap();

    pub static ref PEER_APPLY_LOG_HISTOGRAM: Histogram =
        register_histogram!(
            "tikv_raftstore_apply_log_duration_seconds",
            "Bucketed histogram of peer applying log duration"
        ).unwrap();

    pub static ref STORE_RAFT_READY_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_raftstore_raft_ready_handled_total",
            "Total number of raft ready handled.",
            &["type"]
        ).unwrap();

    pub static ref STORE_RAFT_SENT_MESSAGE_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_raftstore_raft_sent_message_total",
            "Total number of raft ready sent messages.",
            &["type"]
        ).unwrap();

    pub static ref STORE_PD_HEARTBEAT_GAUGE_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_pd_heartbeat_tick_total",
            "Total number of pd heartbeat ticks.",
            &["type"]
        ).unwrap();

    pub static ref STORE_SIZE_GAUGE_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_raftstore_store_size_bytes",
            "Size of raftstore storage.",
            &["type"]
        ).unwrap();

    pub static ref STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_raftstore_snapshot_traffic_total",
            "Total number of raftstore snapshot traffic.",
            &["type"]
        ).unwrap();

    pub static ref PEER_RAFT_PROCESS_NANOS_COUNTER_VEC: CounterVec =
        register_counter_vec!(
            "tikv_raftstore_raft_process_nanos_total",
            "Total nanoseconds spent in raft processing.",
            &["type"]
        ).unwrap();

    pub static ref STORE_ENGINE_SIZE_GAUGE_VEC: GaugeVec =
        register_gauge_vec!(
            "tikv_engine_size_bytes",
            "Sizes of each column families.",
            &["type"]
        ).unwrap();

    pub static ref PEER_PROPOSE_LOG_SIZE_HISTOGRAM: Histogram =
        register_histogram!(
            histogram_opts!{
                "tikv_raftstore_propose_log_size",
                "Bucketed histogram of peer proposing log size",
                [ vec![256.0, 512.0, 1024.0, 4096.0, 65536.0, 262144.0, 524288.0, 1048576.0,
                       2097152.0, 4194304.0, 8388608.0, 16777216.0] ]
            }
        ).unwrap();
}
