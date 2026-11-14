// Copyright 2025 NetApp Inc. All Rights Reserved.
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
//
// SPDX-License-Identifier: Apache-2.0

//! Pluggable transport for ONC RPC connections.
//!
//! Every connection the client opens — portmapper, MOUNT, NFS, NFSACL, pNFS
//! data servers and reconnects after a transport failure — goes through a
//! [`Dialer`]. The default [`TcpDialer`] resolves names locally and connects
//! over TCP, binding a privileged source port unless `noresvport` is set.
//! A custom dialer can carry the RPC records over any ordered byte stream
//! (an SSH channel, a proxy, an in-memory pipe in tests) and resolve server
//! names where that stream ends.

use std::fmt;
use std::net::{IpAddr, SocketAddr};

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

/// A byte stream that carries RPC record-marked messages (RFC 5531 §11).
pub trait RpcStream: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin> RpcStream for T {}

/// A boxed [`RpcStream`], as returned by [`Dialer::dial`].
pub type BoxedRpcStream = Box<dyn RpcStream>;

/// A server endpoint: a host (an IP literal or a name) and a port.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Endpoint {
    host: String,
    port: u16,
}

impl Endpoint {
    /// An endpoint for `host`, which may be a name or an IP literal. Brackets
    /// around an IPv6 literal (as in a URL host) are removed.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        let host = host.into();
        let host = match host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
            Some(inner) => inner.to_string(),
            None => host,
        };
        Self { host, port }
    }

    /// The host as given: a name is not resolved.
    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// The socket address when the host is an IP literal.
    pub fn socket_addr(&self) -> Option<SocketAddr> {
        self.host
            .parse::<IpAddr>()
            .ok()
            .map(|ip| SocketAddr::new(ip, self.port))
    }

    /// The same host on another port.
    pub(crate) fn with_port(&self, port: u16) -> Self {
        Self {
            host: self.host.clone(),
            port,
        }
    }
}

impl From<SocketAddr> for Endpoint {
    fn from(addr: SocketAddr) -> Self {
        Self {
            host: addr.ip().to_string(),
            port: addr.port(),
        }
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

/// Opens the connections an NFS mount uses.
///
/// Implementations must be cheap to call repeatedly: the client dials again
/// after a transport failure, and once per pNFS data server.
#[async_trait]
pub trait Dialer: Send + Sync + 'static {
    /// The endpoints to try for `host:port`, in order.
    ///
    /// The default returns `host` unchanged, so [`Dialer::dial`] receives the
    /// name and resolves it where the stream ends. Override it to resolve
    /// locally and let the client try each address in turn.
    async fn resolve(&self, host: &str, port: u16) -> std::io::Result<Vec<Endpoint>> {
        Ok(vec![Endpoint::new(host, port)])
    }

    /// Opens a stream to `endpoint`. The client bounds the call with its own
    /// connect timeout.
    async fn dial(&self, endpoint: &Endpoint) -> std::io::Result<BoxedRpcStream>;
}

/// The default dialer: local name resolution and a direct TCP connection with
/// `TCP_NODELAY` and keepalives. Unless `noresvport` is set, the source port is
/// privileged (below 1024), as many NFS servers require.
#[derive(Clone, Copy, Debug, Default)]
pub struct TcpDialer {
    noresvport: bool,
}

impl TcpDialer {
    pub fn new(noresvport: bool) -> Self {
        Self { noresvport }
    }
}

#[async_trait]
impl Dialer for TcpDialer {
    async fn resolve(&self, host: &str, port: u16) -> std::io::Result<Vec<Endpoint>> {
        let endpoint = Endpoint::new(host, port);
        if endpoint.socket_addr().is_some() {
            return Ok(vec![endpoint]);
        }
        Ok(tokio::net::lookup_host((endpoint.host(), port))
            .await?
            .map(Endpoint::from)
            .collect())
    }

    async fn dial(&self, endpoint: &Endpoint) -> std::io::Result<BoxedRpcStream> {
        let addr = match endpoint.socket_addr() {
            Some(addr) => addr,
            None => tokio::net::lookup_host((endpoint.host(), endpoint.port()))
                .await?
                .next()
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("{endpoint} resolved to no address"),
                    )
                })?,
        };
        let stream = crate::connect_to_target(&addr, self.noresvport)
            .await
            .map_err(|error| match error {
                crate::NfsError::Io(error) => error,
                other => std::io::Error::other(other.to_string()),
            })?;
        Ok(Box::new(stream))
    }
}

/// A dialer for tests: records every endpoint it is asked for and hands out
/// in-memory streams queued with [`ScriptedDialer::accept`], in order. A dial
/// with nothing queued is refused.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct ScriptedDialer {
    dialed: std::sync::Mutex<Vec<Endpoint>>,
    queued: std::sync::Mutex<std::collections::VecDeque<tokio::io::DuplexStream>>,
}

#[cfg(test)]
impl ScriptedDialer {
    /// Queues a connection and returns its server end.
    pub(crate) fn accept(&self) -> tokio::io::DuplexStream {
        let (client, server) = tokio::io::duplex(1 << 20);
        self.queued.lock().unwrap().push_back(client);
        server
    }

    pub(crate) fn dialed(&self) -> Vec<Endpoint> {
        self.dialed.lock().unwrap().clone()
    }
}

#[cfg(test)]
#[async_trait]
impl Dialer for ScriptedDialer {
    async fn dial(&self, endpoint: &Endpoint) -> std::io::Result<BoxedRpcStream> {
        self.dialed.lock().unwrap().push(endpoint.clone());
        match self.queued.lock().unwrap().pop_front() {
            Some(stream) => Ok(Box::new(stream)),
            None => Err(std::io::ErrorKind::ConnectionRefused.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_keeps_names_and_strips_ipv6_brackets() {
        let named = Endpoint::new("nfs.example.internal", 2049);
        assert_eq!(named.host(), "nfs.example.internal");
        assert_eq!(named.socket_addr(), None);
        assert_eq!(named.to_string(), "nfs.example.internal:2049");

        let v6 = Endpoint::new("[fd00::1]", 111);
        assert_eq!(v6.host(), "fd00::1");
        assert_eq!(v6.socket_addr(), Some("[fd00::1]:111".parse().unwrap()));
        assert_eq!(v6.to_string(), "[fd00::1]:111");
        assert_eq!(v6.with_port(2049).port(), 2049);
    }

    #[tokio::test]
    async fn tcp_dialer_passes_ip_literals_through_without_lookup() {
        let endpoints = TcpDialer::new(true)
            .resolve("192.0.2.7", 111)
            .await
            .unwrap();
        assert_eq!(endpoints, vec![Endpoint::new("192.0.2.7", 111)]);
    }
}
