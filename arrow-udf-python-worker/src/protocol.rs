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

//! Wire protocol between a host and its workers.
//!
//! Host → worker: frames of `u32` big-endian length + bytes. The first frame is
//! the JSON [`WorkerInit`]; every later frame is one request.
//! Worker → host: `u8` status (0 = ok, 1 = error) + `u32` length + bytes, one
//! response per frame. An error response carries a UTF-8 message; the worker
//! exits after sending it.

use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Field;
use serde::{Deserialize, Serialize};

/// Upper bound of a frame, checked before allocating, including for
/// malformed peer output.
pub const MAX_FRAME_BYTES: usize = 256 * 1024 * 1024;

/// Host-controlled startup policy, independent of the UDF definition. Flattening
/// preserves the original function fields; an omitted policy denies networking.
#[derive(Serialize, Deserialize)]
pub(crate) struct WorkerInit<T> {
    #[serde(flatten)]
    pub function: T,
    #[serde(default)]
    pub allow_network: bool,
}

/// The function a worker serves for its whole life.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkerFunction {
    /// Function name, also the name of the Python module holding `code`.
    pub name: String,
    /// Scalar functions: the Python callable to invoke. Aggregates use the
    /// fixed `create_state`, `accumulate`, `merge` and `finish` names.
    pub handler: String,
    pub code: String,
    pub return_field: Field,
    /// `Some(state field)` for an aggregate function, whose requests are
    /// [`AggregateOp`] frames. `None` for a scalar function, whose requests
    /// and responses are single Arrow batches.
    pub aggregate_state: Option<Field>,
    /// Python code run before the function's module, e.g. to restrict
    /// built-ins (`arrow_udf_runtime::python::Builder::safe_codes`).
    pub prelude: Option<String>,
}

/// Aggregate requests. States stay in the host and travel with every request,
/// so workers are stateless: any worker of a pool serves any request, and a
/// killed worker loses no aggregation state.
///
/// A request frame is the op code followed by length-prefixed Arrow streams
/// (`parts`). Group-wise operations update `G` states from `N` rows: a UInt32
/// `groups` column maps each row to its state, and rows are applied to their
/// state in input order. The response is one Arrow stream with `G` rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum AggregateOp {
    /// parts: none. Response: one initial state.
    CreateState = 0,
    /// parts: states (G), groups (N), arguments (N).
    Accumulate = 1,
    /// parts: states (G), groups (N), states to merge into them (N).
    Merge = 2,
    /// parts: states (G). Response: G results.
    Finish = 3,
}

impl AggregateOp {
    fn from_u8(op: u8) -> Result<Self> {
        Ok(match op {
            0 => Self::CreateState,
            1 => Self::Accumulate,
            2 => Self::Merge,
            3 => Self::Finish,
            _ => bail!("invalid aggregate op {op}"),
        })
    }

    fn parts(self) -> usize {
        match self {
            Self::CreateState => 0,
            Self::Finish => 1,
            Self::Accumulate | Self::Merge => 3,
        }
    }
}

pub(crate) fn check_length(len: usize) -> Result<()> {
    if len > MAX_FRAME_BYTES {
        bail!("IPC frame exceeds {MAX_FRAME_BYTES} bytes; reduce batch size");
    }
    Ok(())
}

/// Encode a batch as one Arrow IPC stream.
pub fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, batch.schema().as_ref())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    check_length(bytes.len())?;
    Ok(bytes)
}

/// Decode an Arrow IPC stream holding exactly one batch.
pub fn decode_batch(bytes: &[u8]) -> Result<RecordBatch> {
    let mut reader = StreamReader::try_new(bytes, None)?;
    let batch = reader.next().context("missing Arrow batch")??;
    if reader.next().is_some() {
        bail!("unexpected extra Arrow batch");
    }
    Ok(batch)
}

/// Wrap an array as a single-column batch, the unit of aggregate requests.
pub fn array_batch(array: arrow_array::ArrayRef) -> Result<RecordBatch> {
    let schema =
        arrow_schema::Schema::new(vec![Field::new("value", array.data_type().clone(), true)]);
    Ok(RecordBatch::try_new(Arc::new(schema), vec![array])?)
}

/// Encode an aggregate request frame.
pub fn encode_aggregate_request(op: AggregateOp, parts: &[RecordBatch]) -> Result<Vec<u8>> {
    if parts.len() != op.parts() {
        bail!("{op:?} takes {} parts", op.parts());
    }
    let mut bytes = vec![op as u8];
    for part in parts {
        let encoded = encode_batch(part)?;
        bytes.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&encoded);
    }
    check_length(bytes.len())?;
    Ok(bytes)
}

pub(crate) fn decode_aggregate_request(bytes: &[u8]) -> Result<(AggregateOp, Vec<RecordBatch>)> {
    let (&op, mut rest) = bytes
        .split_first()
        .ok_or_else(|| anyhow!("empty aggregate request"))?;
    let op = AggregateOp::from_u8(op)?;
    let mut parts = Vec::with_capacity(op.parts());
    while !rest.is_empty() {
        let (len, tail) = rest
            .split_first_chunk::<4>()
            .context("truncated aggregate request")?;
        let len = u32::from_be_bytes(*len) as usize;
        if tail.len() < len {
            bail!("truncated aggregate request");
        }
        parts.push(decode_batch(&tail[..len])?);
        rest = &tail[len..];
    }
    if parts.len() != op.parts() {
        bail!("{op:?} takes {} parts", op.parts());
    }
    Ok((op, parts))
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int64Array, UInt32Array};

    use super::*;

    #[test]
    fn test_aggregate_request_round_trip() {
        let states = array_batch(Arc::new(Int64Array::from(vec![1, 2]))).unwrap();
        let groups = array_batch(Arc::new(UInt32Array::from(vec![1, 0, 1]))).unwrap();
        let rows = array_batch(Arc::new(Int64Array::from(vec![7, 8, 9]))).unwrap();
        let parts = [states, groups, rows];
        let bytes = encode_aggregate_request(AggregateOp::Accumulate, &parts).unwrap();
        let (op, decoded) = decode_aggregate_request(&bytes).unwrap();
        assert_eq!(op, AggregateOp::Accumulate);
        assert_eq!(decoded, parts);

        let bytes = encode_aggregate_request(AggregateOp::CreateState, &[]).unwrap();
        assert_eq!(decode_aggregate_request(&bytes).unwrap().1.len(), 0);
    }

    #[test]
    fn test_malformed_aggregate_requests() {
        assert!(encode_aggregate_request(AggregateOp::Finish, &[]).is_err());
        assert!(decode_aggregate_request(&[]).is_err());
        assert!(decode_aggregate_request(&[9]).is_err());
        // Finish without its part, and a part longer than the frame.
        assert!(decode_aggregate_request(&[3]).is_err());
        assert!(decode_aggregate_request(&[3, 0, 0, 0, 9, 1]).is_err());
    }

    #[test]
    fn test_worker_init_network_policy_defaults_to_denied() {
        let function = WorkerFunction {
            name: "test".into(),
            handler: "handler".into(),
            code: "def handler(x): return x".into(),
            return_field: Field::new("result", arrow_schema::DataType::Int64, true),
            aggregate_state: None,
            prelude: None,
        };
        let old = serde_json::to_vec(&function).unwrap();
        let init: WorkerInit<WorkerFunction> = serde_json::from_slice(&old).unwrap();
        assert!(!init.allow_network);
        assert_eq!(init.function, function);
        let bytes = serde_json::to_vec(&WorkerInit {
            function: &function,
            allow_network: true,
        })
        .unwrap();
        let init: WorkerInit<WorkerFunction> = serde_json::from_slice(&bytes).unwrap();
        assert!(init.allow_network);
        assert_eq!(init.function, function);
    }

    #[test]
    fn test_frame_limit() {
        assert!(check_length(MAX_FRAME_BYTES).is_ok());
        assert!(check_length(MAX_FRAME_BYTES + 1).is_err());
    }
}
