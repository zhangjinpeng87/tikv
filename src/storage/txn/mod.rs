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

mod shard_mutex;
mod store;
mod scheduler;

pub use self::scheduler::Scheduler;
pub use self::store::{TxnStore, SnapshotStore};

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Engine(err: ::storage::engine::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Codec(err: ::util::codec::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Mvcc(err: ::storage::mvcc::Error) {
            from()
            cause(err)
            description(err.description())
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;
