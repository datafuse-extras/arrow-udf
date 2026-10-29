// Copyright 2026 Datafuse Labs.
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

const WORKER_ARG: &str = "--proxy-worker";

fn main() {
    let args = std::env::args_os().skip(1).collect::<Vec<_>>();
    if args.first().is_some_and(|arg| arg == WORKER_ARG) {
        let result = arrow_udf_python_worker::run_worker(
            std::path::Path::new(args.last().unwrap()),
            arrow_udf_python_worker::WorkerHooks::default(),
        );
        if let Err(error) = &result {
            eprintln!("{error:#}");
        }
        std::process::exit(if result.is_ok() { 0 } else { 1 });
    }
    #[cfg(target_os = "linux")]
    tests::run();
}

#[cfg(target_os = "linux")]
mod tests {
    use super::WORKER_ARG;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    use anyhow::Result;
    use arrow_array::cast::AsArray;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field};
    use arrow_udf_python_worker::{NetworkProxy, WorkerCommand, WorkerFunction, WorkerProcess};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    use tokio::net::UnixStream;
    use tokio_rustls::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

    #[derive(Debug)]
    struct PolicyProxy {
        enabled: AtomicBool,
        allowed: AtomicUsize,
        denied: AtomicUsize,
        tls: Arc<tokio_rustls::rustls::ServerConfig>,
        active: AtomicUsize,
        started: tokio::sync::Notify,
        idle: tokio::sync::Notify,
    }

    struct Active<'a>(&'a PolicyProxy);

    impl Drop for Active<'_> {
        fn drop(&mut self) {
            self.0.active.fetch_sub(1, Ordering::SeqCst);
            self.0.idle.notify_one();
        }
    }

    async fn read_headers<S: tokio::io::AsyncRead + Unpin>(
        stream: &mut tokio::io::BufReader<S>,
    ) -> Result<String> {
        let mut first = String::new();
        stream.read_line(&mut first).await?;
        let mut line = String::new();
        loop {
            line.clear();
            if stream.read_line(&mut line).await? == 0 || line == "\r\n" {
                break;
            }
        }
        Ok(first)
    }

    impl NetworkProxy for PolicyProxy {
        fn serve(
            &self,
            stream: UnixStream,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
            Box::pin(async move {
                self.active.fetch_add(1, Ordering::SeqCst);
                let _active = Active(self);
                let mut stream = tokio::io::BufReader::new(stream);
                let first = read_headers(&mut stream).await?;
                if self.enabled.load(Ordering::SeqCst)
                    && first == "GET http://allowed.invalid:8080/hang HTTP/1.1\r\n"
                {
                    self.started.notify_one();
                    std::future::pending::<()>().await;
                }
                if self.enabled.load(Ordering::SeqCst)
                    && matches!(
                        first.as_str(),
                        "CONNECT allowed.invalid:443 HTTP/1.0\r\n"
                            | "CONNECT allowed.invalid:443 HTTP/1.1\r\n"
                    )
                {
                    self.allowed.fetch_add(1, Ordering::SeqCst);
                    stream
                        .get_mut()
                        .write_all(b"HTTP/1.0 200 Connection established\r\n\r\n")
                        .await?;
                    let stream = tokio_rustls::TlsAcceptor::from(self.tls.clone())
                        .accept(stream.into_inner())
                        .await?;
                    let mut stream = tokio::io::BufReader::new(stream);
                    assert_eq!(read_headers(&mut stream).await?, "GET /events HTTP/1.1\r\n");
                    stream
                        .get_mut()
                        .write_all(b"HTTP/1.0 200 OK\r\nContent-Length: 1\r\n\r\n7")
                        .await?;
                    stream.get_mut().shutdown().await?;
                } else if self.enabled.load(Ordering::SeqCst)
                    && first == "GET http://allowed.invalid:8080/redirect HTTP/1.1\r\n"
                {
                    self.allowed.fetch_add(1, Ordering::SeqCst);
                    stream.get_mut().write_all(b"HTTP/1.0 302 Found\r\nLocation: http://denied.invalid:8080/events\r\nContent-Length: 0\r\n\r\n").await?;
                } else if self.enabled.load(Ordering::SeqCst)
                    && first == "GET http://allowed.invalid:8080/events HTTP/1.1\r\n"
                {
                    self.allowed.fetch_add(1, Ordering::SeqCst);
                    stream
                        .get_mut()
                        .write_all(b"HTTP/1.0 200 OK\r\nContent-Length: 1\r\n\r\n7")
                        .await?;
                } else {
                    self.denied.fetch_add(1, Ordering::SeqCst);
                    stream
                        .get_mut()
                        .write_all(b"HTTP/1.0 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
                        .await?;
                }
                Ok(())
            })
        }
    }

    #[cfg(target_os = "linux")]
    async fn proxy_tests() {
        let certificate =
            rcgen::generate_simple_self_signed(vec!["allowed.invalid".into()]).unwrap();
        let server = tokio_rustls::rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![certificate.cert.der().clone()],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                    certificate.key_pair.serialize_der(),
                )),
            )
            .unwrap();
        let proxy = Arc::new(PolicyProxy {
            enabled: AtomicBool::new(true),
            allowed: AtomicUsize::new(0),
            denied: AtomicUsize::new(0),
            tls: Arc::new(server),
            active: AtomicUsize::new(0),
            started: tokio::sync::Notify::new(),
            idle: tokio::sync::Notify::new(),
        });
        let mut command = WorkerCommand::current_exe([WORKER_ARG]).unwrap();
        command.network_proxy = Some(proxy.clone());
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("cert.pem"), certificate.cert.pem()).unwrap();
        let mut function = WorkerFunction {
        name: "proxy".into(),
        handler: "handler".into(),
        code: r#"
import _socket, ctypes, os, socket, ssl, threading, urllib.error, urllib.request

def handler(x):
    for family in (socket.AF_INET, socket.AF_INET6, socket.AF_UNIX):
        try:
            _socket.socket(family, socket.SOCK_STREAM)
        except PermissionError:
            pass
        else:
            raise AssertionError('bypassing the adapter enabled a real socket')
    libc = ctypes.CDLL(None, use_errno=True)
    assert libc.socket(socket.AF_INET, socket.SOCK_STREAM, 0) == -1
    assert ctypes.get_errno() == 13
    with urllib.request.urlopen('http://allowed.invalid:8080/events', timeout=2) as response:
        assert response.read() == b'7'
    try:
        urllib.request.urlopen('http://denied.invalid:8080/events', timeout=2)
    except urllib.error.HTTPError as error:
        assert error.code == 403
    else:
        raise AssertionError('host policy was bypassed')
    try:
        urllib.request.urlopen('http://allowed.invalid:8080/redirect', timeout=2)
    except urllib.error.HTTPError as error:
        assert error.code == 403
    else:
        raise AssertionError('redirect escaped host policy')
    context = ssl.create_default_context(cafile='cert.pem')
    with urllib.request.urlopen('https://allowed.invalid/events', context=context, timeout=2) as response:
        assert response.read() == b'7'
    try:
        urllib.request.urlopen('https://denied.invalid/events', context=context, timeout=2)
    except urllib.error.URLError as error:
        assert '403' in str(error)
    else:
        raise AssertionError('CONNECT escaped host policy')
    # Clearing the proxy cannot restore direct access, even to an allowed host.
    previous = os.environ.pop('http_proxy')
    try:
        direct = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        direct.open('http://allowed.invalid:8080/events', timeout=2)
    except OSError:
        pass
    else:
        raise AssertionError('clearing proxy configuration bypassed the sandbox')
    finally:
        os.environ['http_proxy'] = previous
    results = []
    def request():
        with urllib.request.urlopen('http://allowed.invalid:8080/events', timeout=2) as response:
            results.append(response.read())
    threads = [threading.Thread(target=request) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert results == [b'7'] * 4
    # REQUESTS_CHECK
    return x
"#
        .into(),
        return_field: Field::new("result", DataType::Int64, true),
        aggregate_state: None,
        prelude: None,
    };
        let requests = std::env::var_os("ARROW_UDF_TEST_REQUESTS").is_some();
        if requests {
            function.code = function.code.replace(
                "# REQUESTS_CHECK",
                r#"
    import requests
    assert requests.get('http://allowed.invalid:8080/events', timeout=2).text == '7'
    assert requests.get('https://allowed.invalid/events', verify='cert.pem', timeout=2).text == '7'
    assert requests.get('http://allowed.invalid:8080/redirect', timeout=2).status_code == 403
    session = requests.Session()
    session.trust_env = False
    try:
        session.get('http://allowed.invalid:8080/events', timeout=2)
    except requests.exceptions.ConnectionError:
        pass
    else:
        raise AssertionError('requests without proxy bypassed the sandbox')
"#,
            );
        }
        let mut worker = WorkerProcess::spawn(
            &command,
            &function,
            directory.path(),
            Box::new(()),
            tokio::runtime::Handle::current(),
        )
        .await
        .unwrap();
        let input =
            arrow_udf_python_worker::array_batch(Arc::new(Int64Array::from(vec![7]))).unwrap();
        for _ in 0..2 {
            let bytes = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                worker.request(&arrow_udf_python_worker::encode_batch(&input).unwrap()),
            )
            .await
            .unwrap()
            .unwrap();
            let output: RecordBatch = arrow_udf_python_worker::decode_batch(&bytes).unwrap();
            if let Some(errors) = output.column_by_name("error") {
                panic!("{}", errors.as_string::<i32>().value(0));
            }
            assert_eq!(
                output
                    .column(0)
                    .as_primitive::<arrow_array::types::Int64Type>()
                    .value(0),
                7
            );
        }
        assert_eq!(
            proxy.allowed.load(Ordering::SeqCst),
            if requests { 20 } else { 14 }
        );
        assert_eq!(
            proxy.denied.load(Ordering::SeqCst),
            if requests { 8 } else { 6 }
        );
        // The policy belongs to the host, not a worker-startup snapshot.
        proxy.enabled.store(false, Ordering::SeqCst);
        let bytes = worker
            .request(&arrow_udf_python_worker::encode_batch(&input).unwrap())
            .await
            .unwrap();
        let output = arrow_udf_python_worker::decode_batch(&bytes).unwrap();
        assert!(output
            .column_by_name("error")
            .unwrap()
            .as_string::<i32>()
            .value(0)
            .contains("403"));
        drop(worker);

        proxy.enabled.store(true, Ordering::SeqCst);
        function.code = "import urllib.request\ndef handler(x):\n    urllib.request.urlopen('http://allowed.invalid:8080/hang', timeout=30)\n    return x\n".into();
        let mut worker = WorkerProcess::spawn(
            &command,
            &function,
            directory.path(),
            Box::new(()),
            tokio::runtime::Handle::current(),
        )
        .await
        .unwrap();
        let request = arrow_udf_python_worker::encode_batch(&input).unwrap();
        {
            let pending = worker.request(&request);
            tokio::pin!(pending);
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                tokio::select! {
                    result = &mut pending => panic!("hanging proxy returned early: {result:?}"),
                    _ = proxy.started.notified() => {}
                }
            })
            .await
            .unwrap();
        }
        drop(worker);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while proxy.active.load(Ordering::SeqCst) != 0 {
                proxy.idle.notified().await;
            }
        })
        .await
        .unwrap();

        // The compatibility exception must never silently override proxy mode.
        command.allow_network = true;
        let error = WorkerProcess::spawn(
            &command,
            &function,
            directory.path(),
            Box::new(()),
            tokio::runtime::Handle::current(),
        )
        .await
        .err()
        .unwrap();
        assert!(error.to_string().contains("mutually exclusive"));
    }

    pub fn run() {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(proxy_tests());
        println!("host proxy transport and kernel-enforced direct-network denial: passed");
    }
}
