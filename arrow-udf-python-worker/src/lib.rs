// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Out-of-process execution of Python Arrow UDFs.
//!
//! A host spawns one [`WorkerProcess`] per worker it wants and exchanges Arrow
//! IPC frames with it over a private Unix socket. Each worker loads exactly one
//! [`WorkerFunction`] (scalar or aggregate) into an embedded interpreter
//! (`arrow-udf-runtime`) and, on Linux, confines itself with Landlock and
//! seccomp before any user code runs.
//!
//! The crate provides the execution layer only: process launch, the IPC
//! protocol, lifecycle (kill and reap of the worker's process group) and the
//! OS sandbox. Pooling, admission and quotas belong to the host.
//!
//! The worker is the host's own executable (or any binary linking this crate)
//! started with host-chosen arguments; it must call [`run_worker`] before
//! starting threads or runtimes. See [`WorkerCommand`] and [`WorkerHooks`].

#![cfg(unix)]

mod child;
mod network_proxy;
mod process;
mod protocol;
mod sandbox;

pub use child::run_worker;
pub use child::WorkerHooks;
pub use network_proxy::NetworkProxy;
pub use process::allowed_env;
pub use process::WorkerCommand;
pub use process::WorkerProcess;
pub use process::PYTHON_ENV;
pub use protocol::array_batch;
pub use protocol::decode_batch;
pub use protocol::encode_aggregate_request;
pub use protocol::encode_batch;
pub use protocol::AggregateOp;
pub use protocol::WorkerFunction;
pub use protocol::MAX_FRAME_BYTES;
pub use sandbox::SANDBOX_ENFORCED;
