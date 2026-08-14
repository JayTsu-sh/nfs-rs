use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use tokio::net::{TcpListener, UdpSocket};
use tokio::task::JoinHandle;

use crate::error::{NfsError, Result};

pub(crate) const CB_PROGRAM: u32 = 0x4000_0000;

pub(crate) struct CallbackService {
    universal_addr: String,
    task: JoinHandle<()>,
}

impl CallbackService {
    pub(crate) async fn bind_for(server: SocketAddr) -> Result<Self> {
        let IpAddr::V4(server_ip) = server.ip() else {
            return Err(NfsError::InvalidInput(
                "NFSv4.0 delegation callbacks currently require an IPv4 endpoint".into(),
            ));
        };
        let route = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))
            .await
            .map_err(NfsError::Io)?;
        route
            .connect((server_ip, server.port()))
            .await
            .map_err(NfsError::Io)?;
        let IpAddr::V4(local_ip) = route.local_addr().map_err(NfsError::Io)?.ip() else {
            return Err(NfsError::Rpc(
                "NFSv4.0 callback route selected a non-IPv4 address".into(),
            ));
        };
        let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0))
            .await
            .map_err(NfsError::Io)?;
        let port = listener.local_addr().map_err(NfsError::Io)?.port();
        let universal_addr = format!(
            "{}.{}.{}.{}.{}.{}",
            local_ip.octets()[0],
            local_ip.octets()[1],
            local_ip.octets()[2],
            local_ip.octets()[3],
            port >> 8,
            port & 0xff
        );
        let task = tokio::spawn(async move {
            while let Ok((stream, _peer)) = listener.accept().await {
                drop(stream);
            }
        });
        Ok(Self {
            universal_addr,
            task,
        })
    }

    pub(crate) fn universal_addr(&self) -> &str {
        &self.universal_addr
    }
}

impl Drop for CallbackService {
    fn drop(&mut self) {
        self.task.abort();
    }
}
