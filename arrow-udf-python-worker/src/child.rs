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

//! The worker side: serve one function over the IPC socket.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

use anyhow::{bail, Context, Result};
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch, UInt32Array};
use arrow_udf_runtime::python::Runtime;
use arrow_udf_runtime::CallMode;

use crate::protocol::{
    array_batch, check_length, decode_aggregate_request, decode_batch, encode_batch, AggregateOp,
    WorkerFunction, WorkerInit,
};
use crate::sandbox;

/// Host-specific steps of worker startup.
#[derive(Default)]
pub struct WorkerHooks {
    /// Runs after the interpreter is initialized and before the sandbox is
    /// entered. The sandbox requires a single-threaded process: stop helper
    /// threads the host started (e.g. allocator background threads) here.
    pub before_sandbox: Option<Box<dyn FnOnce() -> Result<()>>>,
}

/// Worker entrypoint: connect to `socket_path` (the last argument of the
/// [`WorkerCommand`](crate::WorkerCommand)), load the function the host sends
/// and serve requests until the host disconnects or a request fails.
///
/// Call it before starting threads, async runtimes or other services: the
/// sandbox must be entered while the process is single-threaded. Returns
/// `Ok` after reporting an initialization or request error to the host.
pub fn run_worker(socket_path: &Path, hooks: WorkerHooks) -> Result<()> {
    let mut socket = UnixStream::connect(socket_path).context("connect to host")?;
    let init = (|| {
        let init: WorkerInit<WorkerFunction> = serde_json::from_slice(&read_frame(&mut socket)?)?;
        let function = init.function;
        let mut builder = arrow_udf_runtime::python::Builder::default();
        if let Some(prelude) = &function.prelude {
            builder = builder.safe_codes(prelude.clone());
        }
        let mut runtime = builder.build()?;
        // The interpreter is initialized but no user code has run. Confine the
        // process now, while it is still single-threaded: the parent watcher
        // thread starts afterwards so it inherits the sandbox.
        if let Some(before_sandbox) = hooks.before_sandbox {
            before_sandbox()?;
        }
        sandbox::enter(init.allow_network).map_err(anyhow::Error::msg)?;
        watch_parent(&socket)?;
        let aggregate = function.aggregate_state.is_some();
        match function.aggregate_state {
            Some(state_field) => runtime.add_aggregate(
                &function.name,
                state_field,
                function.return_field,
                CallMode::CalledOnNullInput,
                &function.code,
            ),
            None => runtime.add_function_with_handler(
                &function.name,
                function.return_field,
                CallMode::CalledOnNullInput,
                &function.code,
                &function.handler,
            ),
        }?;
        Ok((runtime, function.name, aggregate))
    })();
    let (runtime, name, aggregate) = match init {
        Ok(value) => {
            respond(&mut socket, Ok(Vec::new()))?;
            value
        }
        Err(error) => {
            respond(&mut socket, Err(error))?;
            return Ok(());
        }
    };
    loop {
        let bytes = read_frame(&mut socket)?;
        let result = if aggregate {
            serve_aggregate(&runtime, &name, &bytes)
        } else {
            decode_batch(&bytes)
                .and_then(|input| runtime.call(&name, &input))
                .and_then(|output| encode_batch(&output))
        };
        let failed = result.is_err();
        respond(&mut socket, result)?;
        if failed {
            return Ok(());
        }
    }
}

/// Apply `apply(state, rows)` to every state that has rows, in row order.
/// States without rows are returned unchanged.
fn update_groups(
    states: &ArrayRef,
    groups: &RecordBatch,
    mut apply: impl FnMut(&dyn Array, &UInt32Array) -> Result<ArrayRef>,
) -> Result<ArrayRef> {
    let groups = groups
        .column(0)
        .as_primitive_opt::<arrow_array::types::UInt32Type>()
        .context("aggregate groups must be UInt32")?;
    let mut rows = vec![Vec::new(); states.len()];
    for (row, group) in groups.values().iter().enumerate() {
        rows.get_mut(*group as usize)
            .context("aggregate group out of range")?
            .push(row as u32);
    }
    let updated = rows
        .into_iter()
        .enumerate()
        .map(|(group, rows)| {
            let state = states.slice(group, 1);
            if rows.is_empty() {
                Ok(state)
            } else {
                apply(&state, &UInt32Array::from(rows))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    if updated.is_empty() {
        return Ok(states.slice(0, 0));
    }
    let updated = updated.iter().map(AsRef::as_ref).collect::<Vec<_>>();
    Ok(arrow_select::concat::concat(&updated)?)
}

fn serve_aggregate(runtime: &Runtime, name: &str, bytes: &[u8]) -> Result<Vec<u8>> {
    let (op, parts) = decode_aggregate_request(bytes)?;
    let output = match op {
        AggregateOp::CreateState => runtime.create_state(name)?,
        AggregateOp::Accumulate => {
            let (states, groups, input) = (parts[0].column(0), &parts[1], &parts[2]);
            if groups.num_rows() != input.num_rows() {
                bail!("aggregate groups and arguments differ in length");
            }
            update_groups(states, groups, |state, rows| {
                let input = arrow_select::take::take_record_batch(input, rows)?;
                runtime.accumulate(name, state, &input)
            })?
        }
        AggregateOp::Merge => {
            let (states, groups, others) = (parts[0].column(0), &parts[1], parts[2].column(0));
            if groups.num_rows() != others.len() {
                bail!("aggregate groups and states differ in length");
            }
            update_groups(states, groups, |state, rows| {
                let others = arrow_select::take::take(others.as_ref(), rows, None)?;
                let states = arrow_select::concat::concat(&[state, others.as_ref()])?;
                runtime.merge(name, states.as_ref())
            })?
        }
        AggregateOp::Finish => runtime.finish(name, parts[0].column(0))?,
    };
    encode_batch(&array_batch(output)?)
}

fn read_frame(socket: &mut UnixStream) -> Result<Vec<u8>> {
    let mut len = [0; 4];
    socket.read_exact(&mut len)?;
    let len = u32::from_be_bytes(len) as usize;
    check_length(len)?;
    let mut bytes = vec![0; len];
    socket.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn respond(socket: &mut UnixStream, result: Result<Vec<u8>>) -> Result<()> {
    let (status, bytes) = match result {
        Ok(bytes) => (0, bytes),
        Err(error) => (1, format!("{error:#}").into_bytes()),
    };
    check_length(bytes.len())?;
    socket.write_all(&[status])?;
    socket.write_all(&(bytes.len() as u32).to_be_bytes())?;
    socket.write_all(&bytes)?;
    Ok(())
}

// Monitor peer closure independently of Python. A SIGKILLed host cannot run
// Rust drops, and a UDF may never return to the protocol loop. Unlike Linux
// PR_SET_PDEATHSIG this does not kill a worker when just its spawning thread
// retires. This is lifecycle management, not a sandbox against hostile code.
fn watch_parent(socket: &UnixStream) -> Result<()> {
    use std::os::fd::AsRawFd;
    let socket = socket.try_clone()?;
    std::thread::Builder::new()
        .name("python-parent-watch".to_owned())
        .spawn(move || {
            let mut descriptor = libc::pollfd {
                fd: socket.as_raw_fd(),
                events: 0,
                revents: 0,
            };
            loop {
                // SAFETY: descriptor is initialized and its owning socket stays
                // alive for this loop. We observe hangup only, never consume IPC.
                let result = unsafe { libc::poll(&mut descriptor, 1, -1) };
                if result < 0
                    && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
                {
                    continue;
                }
                if result < 0
                    || descriptor.revents & (libc::POLLHUP | libc::POLLERR | libc::POLLNVAL) != 0
                {
                    // SAFETY: kill a group only when this worker is its leader.
                    // _exit terminates the entire worker even if Python holds GIL.
                    unsafe {
                        let pid = libc::getpid();
                        if libc::getpgrp() == pid {
                            libc::kill(-pid, libc::SIGKILL);
                        }
                        libc::_exit(1);
                    }
                }
            }
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::Int64Array;

    use super::*;

    #[test]
    fn test_update_groups_applies_rows_in_order() {
        let states: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let groups = array_batch(Arc::new(UInt32Array::from(vec![2, 0, 2, 0]))).unwrap();
        let mut seen = Vec::new();
        let updated = update_groups(&states, &groups, |state, rows| {
            seen.push(rows.values().to_vec());
            let state = state
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0);
            Ok(Arc::new(Int64Array::from(vec![state + rows.len() as i64])))
        })
        .unwrap();
        // Group 1 has no rows and is returned unchanged.
        assert_eq!(seen, [vec![1, 3], vec![0, 2]]);
        assert_eq!(updated.as_ref(), &Int64Array::from(vec![12, 20, 32]));

        let out_of_range = array_batch(Arc::new(UInt32Array::from(vec![3]))).unwrap();
        assert!(update_groups(&states, &out_of_range, |s, _| Ok(s.slice(0, 1))).is_err());
    }
}
