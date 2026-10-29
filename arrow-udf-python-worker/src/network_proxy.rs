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

//! Private proxy streams from a worker that cannot create network sockets.
//!
//! The Python adapter sends one end of a local socketpair over a preconnected
//! control channel. Only the host proxy can connect to an external destination;
//! bypassing the adapter does not bypass the kernel's socket restrictions.

use std::fmt::Debug;
use std::future::Future;
use std::io;
use std::mem::{size_of, size_of_val};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use tokio::io::Interest;
use tokio::net::{UnixListener, UnixStream};
use tokio::task::{JoinHandle, JoinSet};

/// A trusted host-side HTTP proxy for one worker and its authorization context.
///
/// The stream carries ordinary HTTP proxy requests, including HTTPS CONNECT.
/// Implementations MUST authorize each target before connecting, resolve DNS on
/// the host, apply their connection/time/resource limits, and enforce policy
/// changes. Python never selects its authorization context. No networking is
/// performed by this crate on behalf of the proxy.
///
/// Direct worker networking remains denied, regardless of Python monkeypatches.
/// The adapter supports proxy-aware HTTP(S) clients, not arbitrary TCP/UDP APIs.
pub trait NetworkProxy: Debug + Send + Sync {
    fn serve(&self, stream: UnixStream) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
}

/// Owns every proxy task and stream until the worker handle is dropped.
pub(crate) struct ProxyTask(JoinHandle<()>);

impl ProxyTask {
    pub fn start(
        listener: UnixListener,
        proxy: Arc<dyn NetworkProxy>,
        runtime: &tokio::runtime::Handle,
    ) -> Self {
        Self(runtime.spawn(async move {
            if let Err(error) = serve_worker(listener, proxy).await {
                log::warn!("Python UDF proxy control channel failed: {error:#}");
            }
        }))
    }
}

impl Drop for ProxyTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn serve_worker(listener: UnixListener, proxy: Arc<dyn NetworkProxy>) -> Result<()> {
    let (control, _) = listener.accept().await.context("accept worker proxy")?;
    drop(listener);
    let mut tasks = JoinSet::new();
    loop {
        // Do not create unbounded host tasks when Python opens many streams.
        // Backpressure remains in the bounded Unix control socket buffer.
        if tasks.len() == 32 {
            let _ = tasks.join_next().await;
        }
        tokio::select! {
            result = control.async_io(Interest::READABLE, || receive_stream(&control)) => {
                let Some(stream) = result.context("receive proxy stream")? else {
                    return Ok(()); // Dropping JoinSet also cancels existing tunnels.
                };
                let proxy = proxy.clone();
                tasks.spawn(async move {
                    if let Err(error) = proxy.serve(stream).await {
                        log::debug!("Python UDF proxy request failed: {error:#}");
                    }
                });
            }
            _ = tasks.join_next(), if !tasks.is_empty() => {}
        }
    }
}

fn receive_stream(control: &UnixStream) -> io::Result<Option<UnixStream>> {
    let mut tag = [0u8; 1];
    let mut iov = libc::iovec {
        iov_base: tag.as_mut_ptr().cast(),
        iov_len: tag.len(),
    };
    // usize alignment is sufficient for cmsghdr. Extra space lets us close all
    // delivered descriptors if a malformed peer sends more than one.
    let mut ancillary = [0usize; 8];
    // SAFETY: all-zero msghdr has null pointers and lengths until set below.
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = &mut iov;
    message.msg_iovlen = 1;
    message.msg_control = ancillary.as_mut_ptr().cast();
    message.msg_controllen = size_of_val(&ancillary) as _;
    #[cfg(target_os = "linux")]
    let flags = libc::MSG_DONTWAIT | libc::MSG_CMSG_CLOEXEC;
    #[cfg(not(target_os = "linux"))]
    let flags = libc::MSG_DONTWAIT;
    // SAFETY: message points to live buffers with their actual lengths.
    let received = unsafe { libc::recvmsg(control.as_raw_fd(), &mut message, flags) };
    if received < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut descriptors = Vec::new();
    // SAFETY: ancillary headers and descriptor numbers were validated/written by
    // recvmsg. Every delivered fd is immediately owned, even on malformed input.
    unsafe {
        let mut header = libc::CMSG_FIRSTHDR(&message);
        while !header.is_null() {
            if (*header).cmsg_level == libc::SOL_SOCKET && (*header).cmsg_type == libc::SCM_RIGHTS {
                let bytes = (*header).cmsg_len as usize - libc::CMSG_LEN(0) as usize;
                for index in 0..bytes / size_of::<libc::c_int>() {
                    let fd = std::ptr::read_unaligned(
                        libc::CMSG_DATA(header).cast::<libc::c_int>().add(index),
                    );
                    descriptors.push(OwnedFd::from_raw_fd(fd));
                }
            }
            header = libc::CMSG_NXTHDR(&message, header);
        }
    }
    if received == 0 && descriptors.is_empty() {
        return Ok(None);
    }
    if tag[0] != b'P' || descriptors.len() != 1 || message.msg_flags & libc::MSG_CTRUNC != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid proxy descriptor message",
        ));
    }
    let descriptor = descriptors.pop().unwrap();
    #[cfg(not(target_os = "linux"))]
    {
        // SAFETY: descriptor is live and setting FD_CLOEXEC has no pointer args.
        if unsafe { libc::fcntl(descriptor.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    // Reject other fd kinds instead of letting an untrusted peer turn the host
    // into an arbitrary file reader or pass a listening/network socket.
    let mut peer = std::mem::MaybeUninit::<libc::sockaddr_storage>::zeroed();
    let mut length = size_of_val(&peer) as libc::socklen_t;
    // SAFETY: peer/length are writable buffers of the declared size.
    if unsafe {
        libc::getpeername(
            descriptor.as_raw_fd(),
            peer.as_mut_ptr().cast(),
            &mut length,
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the buffer was zeroed and getpeername initialized the peer family.
    let family = unsafe { (*peer.as_ptr()).ss_family };
    if family as i32 != libc::AF_UNIX {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "proxy requires a connected Unix stream",
        ));
    }
    let mut kind = 0 as libc::c_int;
    let mut length = size_of_val(&kind) as libc::socklen_t;
    // SAFETY: kind/length point to correctly sized writable buffers.
    if unsafe {
        libc::getsockopt(
            descriptor.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_TYPE,
            (&mut kind as *mut libc::c_int).cast(),
            &mut length,
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }
    if kind != libc::SOCK_STREAM {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "proxy requires SOCK_STREAM",
        ));
    }
    let stream = std::os::unix::net::UnixStream::from(descriptor);
    stream.set_nonblocking(true)?;
    UnixStream::from_std(stream).map(Some)
}

pub(crate) fn prelude(
    control: &std::os::unix::net::UnixStream,
    existing: Option<&str>,
) -> Result<String> {
    if !crate::SANDBOX_ENFORCED {
        bail!("host-proxy networking requires the Linux OS sandbox");
    }
    let adapter = include_str!("network_proxy.py");
    Ok(format!(
        "{adapter}\n_install_network_proxy({})\ndel _install_network_proxy\n{}",
        control.as_raw_fd(),
        existing.unwrap_or_default()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn send_descriptors(
        control: &std::os::unix::net::UnixStream,
        tag: u8,
        descriptors: &[libc::c_int],
    ) {
        let mut tag = [tag];
        let mut vector = libc::iovec {
            iov_base: tag.as_mut_ptr().cast(),
            iov_len: 1,
        };
        let mut ancillary = [0usize; 64];
        // SAFETY: zeroed msghdr has null pointers and lengths until set below.
        let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
        message.msg_iov = &mut vector;
        message.msg_iovlen = 1;
        if !descriptors.is_empty() {
            message.msg_control = ancillary.as_mut_ptr().cast();
            // SAFETY: ancillary is aligned/sized for these descriptors and the
            // source slice contains live fds held by the caller during sendmsg.
            unsafe {
                message.msg_controllen = libc::CMSG_SPACE(size_of_val(descriptors) as _) as _;
                assert!(message.msg_controllen as usize <= size_of_val(&ancillary));
                let header = libc::CMSG_FIRSTHDR(&message);
                (*header).cmsg_level = libc::SOL_SOCKET;
                (*header).cmsg_type = libc::SCM_RIGHTS;
                (*header).cmsg_len = libc::CMSG_LEN(size_of_val(descriptors) as _) as _;
                std::ptr::copy_nonoverlapping(
                    descriptors.as_ptr().cast::<u8>(),
                    libc::CMSG_DATA(header),
                    size_of_val(descriptors),
                );
            }
        }
        // SAFETY: all message buffers remain live through this syscall.
        assert_eq!(
            unsafe { libc::sendmsg(control.as_raw_fd(), &message, 0) },
            1
        );
    }

    #[tokio::test]
    async fn proxy_descriptor_validation_and_close_on_exec() {
        let (sender, receiver) = std::os::unix::net::UnixStream::pair().unwrap();
        receiver.set_nonblocking(true).unwrap();
        let receiver = {
            let registered = UnixStream::from_std(receiver);
            registered.unwrap()
        };
        let (first, second) = std::os::unix::net::UnixStream::pair().unwrap();
        for (tag, descriptors) in [
            (b'P', vec![]),
            (b'X', vec![first.as_raw_fd()]),
            (b'P', vec![first.as_raw_fd(), second.as_raw_fd()]),
            (b'P', vec![first.as_raw_fd(); 32]),
        ] {
            send_descriptors(&sender, tag, &descriptors);
            assert!(receive_stream(&receiver).is_err());
        }
        let file = tempfile::tempfile().unwrap();
        send_descriptors(&sender, b'P', &[file.as_raw_fd()]);
        assert!(receive_stream(&receiver).is_err());
        let (datagram, _peer) = std::os::unix::net::UnixDatagram::pair().unwrap();
        send_descriptors(&sender, b'P', &[datagram.as_raw_fd()]);
        assert!(receive_stream(&receiver).is_err());
        send_descriptors(&sender, b'P', &[first.as_raw_fd()]);
        let stream = receive_stream(&receiver).unwrap().unwrap();
        // SAFETY: querying flags on a live owned descriptor has no pointer args.
        let flags = unsafe { libc::fcntl(stream.as_raw_fd(), libc::F_GETFD) };
        assert_ne!(flags & libc::FD_CLOEXEC, 0);
        drop(sender);
        assert!(receive_stream(&receiver).unwrap().is_none());
    }
}
