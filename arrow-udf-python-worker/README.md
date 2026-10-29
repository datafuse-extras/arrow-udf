# arrow-udf-python-worker

Out-of-process, sandboxed execution of Python Arrow UDFs, built on the embedded
interpreter of [`arrow-udf-runtime`](../arrow-udf-runtime).

A host starts one `WorkerProcess` per worker and exchanges Arrow IPC frames with
it over a private Unix socket. Each worker serves exactly one `WorkerFunction`,
scalar or aggregate, for its whole life. The crate is the execution layer only:
pooling, admission and quotas stay in the host.

## Launching workers

The worker is any binary that calls `run_worker` first thing in `main`, usually
the host itself started with a hidden flag:

```rust,ignore
fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    if args.get(1).is_some_and(|arg| arg == "--python-worker") {
        let hooks = WorkerHooks {
            // Host-specific setup that must happen before the sandbox, e.g.
            // stopping allocator background threads.
            before_sandbox: None,
        };
        let result = run_worker(Path::new(args.last().unwrap()), hooks);
        std::process::exit(result.is_err() as i32);
    }
    // ... the host
}

let command = WorkerCommand::current_exe(["--python-worker"])?;
let mut worker = WorkerProcess::spawn(&command, &function, private_dir, keep_alive, handle).await?;
let output = decode_batch(&worker.request(&encode_batch(&input)?).await?)?;
```

`WorkerProcess::spawn` starts the worker in its own process group with a cleared
environment (`PYTHON_ENV`, locale and BLAS thread settings only), `no_new_privs`
and no core dumps, in `working_dir`, which is also its `TMPDIR`. Dropping the
handle kills the process group and reaps it on the given runtime; `keep_alive`
(for example a quota reservation or the working directory) is dropped only after
the process has exited. A worker exits when its host disconnects, even if the
host was killed.

## Protocol

Frames are a `u32` big-endian length followed by the payload. The first frame is
the JSON `WorkerFunction` fields plus the host-controlled `allow_network` policy
(default `false` when absent); responses are a status byte, a length and the payload
(an error message on failure, after which the worker exits).

- Scalar functions: a request and its response are single Arrow IPC streams.
- Aggregate functions: requests are `AggregateOp` frames. Aggregation states stay
  in the host and travel with every request, so workers are stateless: any
  worker serves any request and a killed worker loses no state. `Accumulate` and
  `Merge` update many states per request through a `groups` column that maps
  rows to states, applying rows in input order.

## Sandbox

On Linux, a worker confines itself after the interpreter has started and before
any user code runs. Failure is fatal: the worker reports the error and never
runs user code.

- Landlock: read-only access to the Python installation (`sys.path`, stdlib,
  site-packages), system libraries and a few device/proc entries; read-write
  access only below the working directory.
- seccomp: no sockets by default, no `execve`, no new processes (`fork`, `vfork`, `clone`
  without `CLONE_THREAD`), no signals outside the worker, no ptrace, io_uring,
  bpf, namespaces, mounts or kernel modules, no path-based permission changes,
  no x32 syscalls.

A host may explicitly set `WorkerCommand::allow_network = true` for a temporary
compatibility exception. This permits outbound IPv4/IPv6 stream and datagram
sockets, plus read-only resolver and public CA data. TCP bind/connect policy is
adjusted without removing filesystem confinement; `bind`/`listen`, Unix sockets,
raw sockets and process creation stay denied. This is not a destination allowlist:
loopback and internal destinations become reachable, subject to deployment egress
policy. Do not expose this policy as user-controlled function metadata. It is
fixed at worker startup and cannot be changed by Python code.

### Host-policy HTTP(S) transport

`WorkerCommand::network_proxy` accepts an `Arc<dyn NetworkProxy>` implemented by
the host. This is a separate, fail-closed mode: it is mutually exclusive with
`allow_network = true` and requires the enforced Linux OS sandbox. It is the
transport primitive for hosts that manage endpoint permissions, for example via
Databend `NETWORK RULE` / `EXTERNAL ACCESS INTEGRATION`; it does **not** implement
those SQL policies or make a whitelist decision itself.

Before entering the sandbox, the worker connects a private Unix control stream.
A Python socket adapter sends local stream endpoints over that channel to the
host proxy and selects it through `http_proxy` / `https_proxy`. Proxy-aware
clients such as `urllib.request` and `requests` can then issue HTTP or HTTPS
CONNECT requests without an Internet socket in the worker. Native/direct
IPv4/IPv6 sockets remain denied by seccomp; restoring Python's original socket
class, ignoring proxy environment variables, or using native libraries does not
restore direct network access. Arbitrary TCP/UDP clients, async socket clients,
and libraries that require native Internet sockets are not supported.

The host MUST implement an HTTP proxy with trusted authorization context (not
context supplied by Python). It must validate every HTTP target and CONNECT
host/port, apply rule changes to new requests/connections, resolve targets on the
host, and impose parse/connect/idle/resource limits. It must not trust a client
Host header as the sole authority or allow a denied redirect target. Existing
CONNECT tunnels are opaque: hosts must explicitly define and enforce their
revocation boundary (e.g. close tunnels on policy revocation or bound their
lifetime). The transport does not inspect TLS requests inside a tunnel and does
not promise instantaneous revocation of an existing connection.

Each worker has at most 32 active host proxy tasks, with bounded control-channel
backpressure. Dropping the worker cancels the control task and its proxy tasks;
failed/cancelled worker requests still require dropping the worker as usual.
Resolver/public certificate paths are readable for client TLS support, while
all filesystem/process sandbox restrictions remain in effect. macOS remains
unconfined development-only for the legacy modes and refuses this mode rather
than claiming proxy-only network enforcement.

`cargo test -p arrow-udf-python-worker` covers local HTTP, verified HTTPS,
redirect denial, policy updates on an existing worker, concurrent streams and
kernel-level direct socket denial, without external services/packages. To also
exercise `requests`, install it into your Python environment (or a private
`PYTHONPATH`) and run
`ARROW_UDF_TEST_REQUESTS=1 cargo test -p arrow-udf-python-worker --test network_proxy`.
The flag does not install packages, and import failures fail the test.

Requirements: Linux 5.13+ with Landlock enabled, x86_64 or aarch64, and a
container seccomp profile that permits the `landlock_*` syscalls. The worker
must be single-threaded when it enters the sandbox; use
`WorkerHooks::before_sandbox` to stop host threads.

macOS has neither Landlock nor seccomp: workers run unconfined there
(`SANDBOX_ENFORCED` is `false`), which is only suitable for local development.
