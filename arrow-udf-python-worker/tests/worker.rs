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

//! The test binary is its own worker: started with `WORKER_ARG`, it serves the
//! function; otherwise it runs the tests, spawning itself as workers.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StructArray, UInt32Array};
use arrow_schema::{DataType, Field, Fields, Schema};
use arrow_udf_python_worker::*;

const WORKER_ARG: &str = "--test-python-worker";

fn command() -> WorkerCommand {
    WorkerCommand::current_exe([WORKER_ARG]).unwrap()
}

fn scalar(name: &str, code: &str) -> WorkerFunction {
    WorkerFunction {
        name: name.into(),
        handler: "handler".into(),
        code: code.into(),
        return_field: Field::new(name, DataType::Int64, true),
        aggregate_state: None,
        prelude: None,
    }
}

fn int64_batch(values: Vec<i64>) -> RecordBatch {
    let schema = Schema::new(vec![Field::new("x", DataType::Int64, true)]);
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int64Array::from(values))]).unwrap()
}

fn int64s(array: &ArrayRef) -> Vec<i64> {
    array.as_primitive::<Int64Type>().values().to_vec()
}

/// Dropped when the worker's reaper has finished.
struct ExitFlag(Arc<AtomicBool>);

impl Drop for ExitFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

async fn spawn(function: &WorkerFunction) -> anyhow::Result<(WorkerProcess, tempfile::TempDir)> {
    let dir = tempfile::tempdir().unwrap();
    let worker = WorkerProcess::spawn(
        &command(),
        function,
        dir.path(),
        Box::new(()),
        tokio::runtime::Handle::current(),
    )
    .await?;
    Ok((worker, dir))
}

async fn call(worker: &mut WorkerProcess, values: Vec<i64>) -> anyhow::Result<Vec<i64>> {
    let response = worker.request(&encode_batch(&int64_batch(values))?).await?;
    Ok(int64s(decode_batch(&response)?.column(0)))
}

async fn scalar_tests() {
    let (mut worker, _dir) = spawn(&scalar("double", "def handler(x):\n    return x * 2\n"))
        .await
        .unwrap();
    assert_eq!(call(&mut worker, vec![1, 2, 3]).await.unwrap(), [2, 4, 6]);
    assert_eq!(call(&mut worker, vec![4]).await.unwrap(), [8]);

    // The prelude runs before the function's module.
    let mut function = scalar("prelude", "def handler(x):\n    return x + OFFSET\n");
    function.prelude = Some("import builtins\nbuiltins.OFFSET = 100\n".into());
    let (mut worker, _dir) = spawn(&function).await.unwrap();
    assert_eq!(call(&mut worker, vec![1]).await.unwrap(), [101]);

    // Initialization errors are returned by spawn; request errors by request.
    let error = spawn(&scalar("broken", "def handler(x) return x\n"))
        .await
        .err()
        .expect("invalid code must fail");
    assert!(format!("{error:#}").contains("SyntaxError"), "{error:#}");
    let (mut worker, _dir) = spawn(&scalar(
        "raises",
        "def handler(x):\n    raise ValueError('boom')\n",
    ))
    .await
    .unwrap();
    // Per-row Python exceptions come back in the runtime's `error` column.
    let response = worker
        .request(&encode_batch(&int64_batch(vec![1])).unwrap())
        .await
        .unwrap();
    let batch = decode_batch(&response).unwrap();
    let errors = batch.column_by_name("error").expect("error column");
    assert!(errors.as_string::<i32>().value(0).contains("boom"));
    // A malformed request fails the request and ends the worker.
    assert!(worker.request(b"not arrow").await.is_err());
    assert!(worker.request(b"after failure").await.is_err());
}

async fn lifecycle_tests() {
    std::env::set_var("ARROW_UDF_TEST_SECRET", "must-not-leak");
    let code = r#"
import os, resource

def handler(x):
    assert 'ARROW_UDF_TEST_SECRET' not in os.environ, 'host environment leaked'
    assert os.path.realpath(os.environ['TMPDIR']) == os.path.realpath(os.getcwd())
    assert resource.getrlimit(resource.RLIMIT_CORE) == (0, 0)
    return os.getpid()
"#;
    let dir = tempfile::tempdir().unwrap();
    let exited = Arc::new(AtomicBool::new(false));
    let mut worker = WorkerProcess::spawn(
        &command(),
        &scalar("env", code),
        dir.path(),
        Box::new(ExitFlag(exited.clone())),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let pid = call(&mut worker, vec![0]).await.unwrap()[0];
    assert_eq!(worker.id(), Some(pid as u32));
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
        assert!(status
            .lines()
            .any(|l| l.split_whitespace().eq(["NoNewPrivs:", "1"])));
        // Containers may filter every process already: the worker must have
        // installed a filter of its own on top of the host's.
        let filters = |status: &str| {
            status
                .lines()
                .find_map(|l| l.strip_prefix("Seccomp_filters:"))
                .map(|n| n.trim().parse::<u32>().unwrap())
        };
        let host = std::fs::read_to_string("/proc/self/status").unwrap();
        if let (Some(worker), Some(host)) = (filters(&status), filters(&host)) {
            assert!(worker > host, "worker installed no seccomp filter");
        }
    }

    // Dropping the handle kills the worker; keep_alive lives until it exited.
    drop(worker);
    for _ in 0..500 {
        if exited.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(exited.load(Ordering::SeqCst), "worker was not reaped");
    // SAFETY: signal 0 only checks for existence.
    assert_ne!(
        unsafe { libc::kill(pid as i32, 0) },
        0,
        "worker still alive"
    );
}

async fn aggregate_tests() {
    let state = Field::new(
        "state",
        DataType::Struct(Fields::from(vec![
            Field::new("sum", DataType::Int64, true),
            Field::new("count", DataType::Int64, true),
        ])),
        true,
    );
    let function = WorkerFunction {
        name: "agg".into(),
        handler: String::new(),
        code: r#"
class State:
    def __init__(self):
        self.sum = 0
        self.count = 0

def create_state():
    return State()

def accumulate(state, value):
    state.sum += value
    state.count += 1
    return state

def merge(state, other):
    state.sum += other.sum
    state.count += other.count
    return state

def finish(state):
    return state.sum * 1000 + state.count
"#
        .into(),
        return_field: Field::new("agg", DataType::Int64, true),
        aggregate_state: Some(state),
        prelude: None,
    };
    let (mut worker, _dir) = spawn(&function).await.unwrap();
    let initial = decode_batch(
        &worker
            .request(&encode_aggregate_request(AggregateOp::CreateState, &[]).unwrap())
            .await
            .unwrap(),
    )
    .unwrap()
    .column(0)
    .clone();
    let states = arrow_select::concat::concat(&[initial.as_ref(), initial.as_ref()]).unwrap();
    // Rows 0..6 to groups [0, 1, 0, 1, 0, 1].
    let accumulate = encode_aggregate_request(
        AggregateOp::Accumulate,
        &[
            array_batch(states).unwrap(),
            array_batch(Arc::new(UInt32Array::from(vec![0, 1, 0, 1, 0, 1]))).unwrap(),
            int64_batch(vec![1, 2, 3, 4, 5, 6]),
        ],
    )
    .unwrap();
    let updated = decode_batch(&worker.request(&accumulate).await.unwrap())
        .unwrap()
        .column(0)
        .clone();
    assert_eq!(
        updated
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .len(),
        2
    );

    // Merge both groups into the first state.
    let merge = encode_aggregate_request(
        AggregateOp::Merge,
        &[
            array_batch(updated.slice(0, 1)).unwrap(),
            array_batch(Arc::new(UInt32Array::from(vec![0]))).unwrap(),
            array_batch(updated.slice(1, 1)).unwrap(),
        ],
    )
    .unwrap();
    let merged = decode_batch(&worker.request(&merge).await.unwrap())
        .unwrap()
        .column(0)
        .clone();
    let all = arrow_select::concat::concat(&[updated.as_ref(), merged.as_ref()]).unwrap();
    let finish =
        encode_aggregate_request(AggregateOp::Finish, &[array_batch(all).unwrap()]).unwrap();
    let results = decode_batch(&worker.request(&finish).await.unwrap()).unwrap();
    assert_eq!(int64s(results.column(0)), [9_003, 12_003, 21_006]);
}

#[cfg(target_os = "linux")]
async fn sandbox_tests() {
    let secret = std::env::temp_dir().join(format!("arrow-udf-sandbox-{}", std::process::id()));
    std::fs::write(&secret, "secret").unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let code = format!(
        r#"
import os, socket, subprocess, tempfile

SECRET = {secret:?}

def attempts():
    yield 'read file', lambda: open(SECRET).read()
    yield 'read parent environ', lambda: open('/proc/%d/environ' % os.getppid()).read()
    yield 'list /', lambda: os.listdir('/')
    yield 'write outside', lambda: open(SECRET + '.new', 'w').write('x')
    yield 'chmod outside', lambda: os.chmod(SECRET, 0o777)
    yield 'tcp connect', lambda: socket.create_connection(('127.0.0.1', {port}), timeout=1)
    yield 'unix socket', lambda: socket.socket(socket.AF_UNIX)
    yield 'spawn program', lambda: subprocess.run(['/bin/true'])
    yield 'fork', lambda: os.fork() or os._exit(0)
    yield 'signal parent', lambda: os.kill(os.getppid(), 0)

def handler(x):
    for name, action in attempts():
        try:
            action()
        except PermissionError:
            continue
        raise AssertionError('sandbox allowed ' + name)
    with tempfile.NamedTemporaryFile() as f:
        f.write(b'private files still work')
    os.kill(os.getpid(), 0)
    return 1
"#,
        secret = secret.to_str().unwrap(),
    );
    let (mut worker, _dir) = spawn(&scalar("sandbox", &code)).await.unwrap();
    assert_eq!(call(&mut worker, vec![0]).await.unwrap(), [1]);
    assert_eq!(std::fs::read_to_string(&secret).unwrap(), "secret");
    std::fs::remove_file(&secret).unwrap();
    drop(listener);

    // The sandbox requires a single-threaded worker: a thread left running
    // by the host makes startup fail instead of running user code unconfined.
    let error = WorkerProcess::spawn(
        &WorkerCommand::current_exe([WORKER_ARG, "--leak-thread"]).unwrap(),
        &scalar("threads", "def handler(x):\n    return x\n"),
        tempfile::tempdir().unwrap().path(),
        Box::new(()),
        tokio::runtime::Handle::current(),
    )
    .await
    .err()
    .expect("a multi-threaded worker must not run user code");
    assert!(
        format!("{error:#}").contains("single-threaded"),
        "{error:#}"
    );
}

#[cfg(target_os = "linux")]
async fn network_policy_tests() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

    assert!(!command().allow_network);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let function = scalar(
        "network",
        &format!(
            r#"
import socket, ssl, urllib.request

def handler(x):
    ssl.create_default_context()
    with urllib.request.urlopen('http://localhost:{port}/snapshot', timeout=2) as response:
        assert response.read() == b'ok'
    try:
        socket.socket(socket.AF_UNIX)
    except PermissionError:
        return x
    raise AssertionError('Unix sockets must stay denied')
"#
        ),
    );
    // Numeric address keeps DNS failure from masking the socket permission error.
    let denied_function = scalar(
        "network_denied",
        &function.code.replace("localhost", "127.0.0.1"),
    );
    let (mut denied, _dir) = spawn(&denied_function).await.unwrap();
    let response = denied
        .request(&encode_batch(&int64_batch(vec![7])).unwrap())
        .await
        .unwrap();
    let batch = decode_batch(&response).unwrap();
    let errors = batch.column_by_name("error").unwrap().as_string::<i32>();
    assert!(
        errors.value(0).contains("Permission denied"),
        "{}",
        errors.value(0)
    );
    drop(denied);

    let server = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = tokio::io::BufReader::new(stream);
            let mut line = String::new();
            stream.read_line(&mut line).await.unwrap();
            // Local service probes must not consume the UDF's one response.
            if !line.starts_with("GET /snapshot ") {
                continue;
            }
            loop {
                line.clear();
                if stream.read_line(&mut line).await.unwrap() == 0 || line == "\r\n" {
                    break;
                }
            }
            stream
                .get_mut()
                .write_all(b"HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await
                .unwrap();
            break;
        }
    });
    let mut command = command();
    command.allow_network = true;
    let dir = tempfile::tempdir().unwrap();
    let mut worker = WorkerProcess::spawn(
        &command,
        &function,
        dir.path(),
        Box::new(()),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    assert_eq!(
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            call(&mut worker, vec![7])
        )
        .await
        .unwrap()
        .unwrap(),
        [7]
    );
    server.await.unwrap();
}

fn main() {
    let args = std::env::args_os().skip(1).collect::<Vec<_>>();
    if args.first().is_some_and(|arg| arg == WORKER_ARG) {
        let leak_thread = args.get(1).is_some_and(|arg| arg == "--leak-thread");
        let hooks = WorkerHooks {
            before_sandbox: leak_thread.then(|| {
                Box::new(|| {
                    std::thread::spawn(std::thread::park);
                    Ok(())
                }) as Box<dyn FnOnce() -> anyhow::Result<()>>
            }),
        };
        let socket = args.last().unwrap();
        let result = run_worker(std::path::Path::new(socket), hooks);
        std::process::exit(if result.is_ok() { 0 } else { 1 });
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        scalar_tests().await;
        lifecycle_tests().await;
        aggregate_tests().await;
        #[cfg(target_os = "linux")]
        sandbox_tests().await;
        #[cfg(target_os = "linux")]
        network_policy_tests().await;
        println!("python worker tests passed (sandbox enforced: {SANDBOX_ENFORCED})");
    });
}
