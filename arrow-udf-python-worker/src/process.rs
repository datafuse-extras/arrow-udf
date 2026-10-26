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

//! The host side: start a worker process and exchange frames with it.

use std::any::Any;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{anyhow, bail, Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::process::{Child, Command};

use crate::protocol::{check_length, WorkerFunction};

/// Variables any Python process needs to locate itself and its locale.
pub const PYTHON_ENV: &[&str] = &[
    "PATH",
    "HOME",
    "LANG",
    "LANGUAGE",
    "TZ",
    "TMPDIR",
    "LD_LIBRARY_PATH",
    "DYLD_LIBRARY_PATH",
    "PYTHONHOME",
    "PYTHONPATH",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
];

/// Worker-specific variables on top of [`PYTHON_ENV`]: interpreter I/O and
/// native thread-pool tuning that operators set for numeric libraries.
const WORKER_ENV: &[&str] = &[
    "PYTHONIOENCODING",
    "PYTHONUTF8",
    "PYTHONHASHSEED",
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
];

/// Environment variables a child may inherit from the host: `names` exactly,
/// `prefixes` as families such as `LC_*`. Everything else (credentials,
/// tokens, service configuration) is withheld.
pub fn allowed_env(names: &[&str], prefixes: &[&str]) -> Vec<(OsString, OsString)> {
    std::env::vars_os()
        .filter(|(key, _)| {
            key.to_str().is_some_and(|key| {
                names.contains(&key) || prefixes.iter().any(|prefix| key.starts_with(prefix))
            })
        })
        .collect()
}

/// How to start a worker: a binary that calls [`run_worker`](crate::run_worker)
/// with the socket path, which is appended as the last argument.
#[derive(Clone, Debug)]
pub struct WorkerCommand {
    pub program: PathBuf,
    pub args: Vec<OsString>,
}

impl WorkerCommand {
    /// Re-execute the current binary with `args`, e.g. a hidden flag that the
    /// host's `main` checks before doing anything else.
    pub fn current_exe(args: impl IntoIterator<Item = impl Into<OsString>>) -> Result<Self> {
        Ok(Self {
            program: std::env::current_exe()?,
            args: args.into_iter().map(Into::into).collect(),
        })
    }
}

/// A started worker serving one [`WorkerFunction`].
///
/// The worker runs in its own process group with a cleared environment
/// ([`PYTHON_ENV`], locale and BLAS tuning only), `no_new_privs` and no core
/// dumps, in `working_dir`, which is also its `TMPDIR` and its only writable
/// location inside the sandbox. Dropping the handle kills the process group;
/// it is reaped on the `reaper` runtime, and `keep_alive` (e.g. a quota
/// reservation or the working directory) is dropped only after it exited.
///
/// A request that fails, times out or is cancelled leaves the worker in an
/// unknown state: drop it instead of sending more requests.
pub struct WorkerProcess {
    socket: UnixStream,
    reaper: Reaper,
    _ipc_dir: tempfile::TempDir,
}

/// Kills the worker's process group on drop and reaps it asynchronously,
/// dropping `keep_alive` only after the process has exited.
struct Reaper {
    child: Option<Child>,
    keep_alive: Option<Box<dyn Any + Send>>,
    reaper: tokio::runtime::Handle,
}

impl WorkerProcess {
    /// Start a worker and load `function` into it. Returns the worker's
    /// initialization error (invalid code, sandbox unavailable, ...).
    pub async fn spawn(
        command: &WorkerCommand,
        function: &WorkerFunction,
        working_dir: &Path,
        keep_alive: Box<dyn Any + Send>,
        reaper: tokio::runtime::Handle,
    ) -> Result<Self> {
        let ipc_dir = tempfile::Builder::new()
            .prefix("arrow-udf-worker-")
            .tempdir()?;
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(ipc_dir.path(), std::fs::Permissions::from_mode(0o700))?;
        }
        // A private listener is used instead of stdout: UDF print() output
        // is arbitrary.
        let socket_path = ipc_dir.path().join("ipc");
        let listener = tokio::net::UnixListener::bind(&socket_path)?;
        let mut process = Command::new(&command.program);
        process
            .args(&command.args)
            .arg(&socket_path)
            // UDF code must not see the host's credentials or service
            // configuration through os.environ.
            .env_clear()
            .envs(allowed_env(&[PYTHON_ENV, WORKER_ENV].concat(), &["LC_"]))
            .current_dir(working_dir)
            .env("TMPDIR", working_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .process_group(0);
        // SAFETY: the hook runs between fork and exec and only issues
        // async-signal-safe syscalls; it neither allocates nor takes locks.
        unsafe {
            process.pre_exec(restrict_worker_process);
        }
        let child = process.spawn().context("start worker")?;
        let pid = child.id().context("missing worker pid")?;
        log::debug!("started Python UDF worker pid={pid}");
        // Reaps the child if startup fails or is cancelled before `accept`.
        let mut startup = Reaper {
            child: Some(child),
            keep_alive: Some(keep_alive),
            reaper,
        };
        let socket = tokio::select! {
            connection = listener.accept() => connection?.0,
            status = startup.child.as_mut().unwrap().wait() => {
                bail!("worker exited during startup: {:?}", status?);
            }
        };
        let mut worker = Self {
            socket,
            reaper: Reaper {
                child: startup.child.take(),
                keep_alive: startup.keep_alive.take(),
                reaper: startup.reaper.clone(),
            },
            _ipc_dir: ipc_dir,
        };
        worker.write_frame(&serde_json::to_vec(function)?).await?;
        worker.read_response().await?;
        Ok(worker)
    }

    /// Send one request frame and wait for its response.
    pub async fn request(&mut self, request: &[u8]) -> Result<Vec<u8>> {
        self.write_frame(request).await?;
        self.read_response().await
    }

    /// The worker's process id, which is also its process group id.
    pub fn id(&self) -> Option<u32> {
        self.reaper.child.as_ref().and_then(Child::id)
    }

    async fn write_frame(&mut self, bytes: &[u8]) -> Result<()> {
        check_length(bytes.len())?;
        self.socket.write_u32(bytes.len() as u32).await?;
        self.socket.write_all(bytes).await?;
        Ok(())
    }

    async fn read_response(&mut self) -> Result<Vec<u8>> {
        let status = self.socket.read_u8().await?;
        let len = self.socket.read_u32().await? as usize;
        check_length(len)?;
        let mut bytes = vec![0; len];
        self.socket.read_exact(&mut bytes).await?;
        match status {
            0 => Ok(bytes),
            1 => Err(anyhow!("{}", String::from_utf8_lossy(&bytes))),
            _ => bail!("invalid response status"),
        }
    }
}

impl Drop for Reaper {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        let keep_alive = self.keep_alive.take();
        let pid = child.id();
        if let Some(pid) = pid {
            // SAFETY: negative pid targets the worker's dedicated process
            // group. An unreaped child's pid cannot have been reused.
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
        }
        let _ = child.start_kill();
        // Keep `keep_alive` until the process has actually exited, including
        // a cancelled startup.
        self.reaper.spawn(async move {
            if let Err(error) = child.wait().await {
                log::warn!("failed to reap Python UDF worker pid={pid:?}: {error}");
            }
            log::debug!("reaped Python UDF worker pid={pid:?}");
            drop(keep_alive);
        });
    }
}

/// Applied in the forked child before exec. This is hardening, not the
/// sandbox (see `run_worker`):
/// - no_new_privs: setuid/file-capability binaries cannot raise privileges;
/// - RLIMIT_CORE=0: a crashing worker never writes UDF memory to disk.
///
/// PR_SET_PDEATHSIG is deliberately not used: it fires when the spawning
/// thread retires, not when the host exits (the worker watches its socket).
fn restrict_worker_process() -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: prctl with integer arguments has no memory side effects.
        if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    let no_core = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: the pointer refers to a live, initialized rlimit.
    if unsafe { libc::setrlimit(libc::RLIMIT_CORE, &no_core) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}
