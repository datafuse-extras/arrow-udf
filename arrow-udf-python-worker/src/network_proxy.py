# Copyright 2026 Datafuse Labs.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Host-owned transport adapter, installed before user code. This is NOT an
# authorization boundary: direct sockets stay denied by the OS sandbox and
# the host must authorize every request handled on the proxy streams.

def _install_network_proxy(control_fd):
    import array
    import errno
    import os
    import socket
    import threading

    original_socket = socket.socket
    original_socketpair = socket.socketpair
    control = original_socket(fileno=os.dup(control_fd))
    lock = threading.Lock()
    proxy_host = 'databend-egress.invalid'
    proxy_port = 80
    proxy_address = ('127.0.0.1', proxy_port)

    def getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        if host in (proxy_host, proxy_host.encode()) and int(port) == proxy_port:
            return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', proxy_address)]
        # DNS for actual destinations belongs to the host proxy. Never attempt
        # DNS here: even a lookup must not be a separate, ungoverned egress path.
        raise PermissionError(errno.EACCES, 'direct network access is disabled; use the host HTTP proxy')

    class ProxySocket(original_socket):
        def __init__(self, family=-1, type=-1, proto=-1, fileno=None):
            self._proxy_peer = None
            self._proxy_stream = False
            if fileno is not None:
                super().__init__(family, type, proto, fileno=fileno)
                return
            kind = socket.SOCK_STREAM if type == -1 else type & ~(socket.SOCK_CLOEXEC | socket.SOCK_NONBLOCK)
            if family in (-1, socket.AF_INET, socket.AF_INET6) and kind in (-1, socket.SOCK_STREAM):
                first, second = original_socketpair()
                super().__init__(socket.AF_UNIX, socket.SOCK_STREAM, 0, fileno=first.detach())
                self._proxy_peer = second
                self._proxy_stream = True
            else:
                # AF_UNIX and non-stream sockets still go through the kernel
                # and its deny-by-default seccomp policy.
                super().__init__(family, type, proto)

        def setsockopt(self, level, option, *args):
            if self._proxy_stream and level == socket.IPPROTO_TCP and option == socket.TCP_NODELAY:
                # This endpoint is a local Unix stream, not the remote TCP
                # socket. The proxy host owns remote socket tuning.
                return
            return super().setsockopt(level, option, *args)

        def connect(self, address):
            if self._proxy_peer is None or address != proxy_address:
                raise PermissionError(errno.EACCES, 'direct network access is disabled; use the host HTTP proxy')
            try:
                with lock:
                    sent = control.sendmsg([b'P'], [(socket.SOL_SOCKET, socket.SCM_RIGHTS,
                                                  array.array('i', [self._proxy_peer.fileno()]))])
                if sent != 1:
                    raise ConnectionError('host proxy control channel closed')
            finally:
                self._proxy_peer.close()
                self._proxy_peer = None

        def close(self):
            if self._proxy_peer is not None:
                self._proxy_peer.close()
                self._proxy_peer = None
            return super().close()

        def connect_ex(self, address):
            try:
                self.connect(address)
                return 0
            except OSError as error:
                return error.errno or errno.EIO

    socket.socket = ProxySocket
    socket.getaddrinfo = getaddrinfo
    proxy = 'http://' + proxy_host + ':' + str(proxy_port)
    # Do not inherit bypass lists or arbitrary user/operator proxies. These
    # values select the local transport only; the host checks destinations.
    for key in ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'NO_PROXY',
                'http_proxy', 'https_proxy', 'all_proxy', 'no_proxy'):
        os.environ.pop(key, None)
    os.environ['http_proxy'] = proxy
    os.environ['https_proxy'] = proxy
