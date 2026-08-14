use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::task::JoinHandle;

use crate::error::{NfsError, Result};

pub(crate) const CB_PROGRAM: u32 = 0x4000_0000;
const MAX_CALLBACK_RECORD: usize = 64 * 1024;

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
                tokio::spawn(async move {
                    let _ = serve_connection(stream).await;
                });
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

async fn serve_connection(mut stream: tokio::net::TcpStream) -> Result<()> {
    loop {
        let Some(call) = read_record(&mut stream).await? else {
            return Ok(());
        };
        let reply = handle_rpc_call(&call)?;
        stream
            .write_u32(0x8000_0000 | reply.len() as u32)
            .await
            .map_err(NfsError::Io)?;
        stream.write_all(&reply).await.map_err(NfsError::Io)?;
    }
}

async fn read_record(stream: &mut tokio::net::TcpStream) -> Result<Option<Vec<u8>>> {
    let mut record = Vec::new();
    loop {
        let marker = match stream.read_u32().await {
            Ok(marker) => marker,
            Err(error)
                if error.kind() == std::io::ErrorKind::UnexpectedEof && record.is_empty() =>
            {
                return Ok(None);
            }
            Err(error) => return Err(NfsError::Io(error)),
        };
        let length = (marker & 0x7fff_ffff) as usize;
        if record.len().saturating_add(length) > MAX_CALLBACK_RECORD {
            return Err(NfsError::Xdr(
                "NFSv4.0 callback RPC record exceeds the configured bound".into(),
            ));
        }
        let start = record.len();
        record.resize(start + length, 0);
        stream
            .read_exact(&mut record[start..])
            .await
            .map_err(NfsError::Io)?;
        if marker & 0x8000_0000 != 0 {
            return Ok(Some(record));
        }
    }
}

fn handle_rpc_call(call: &[u8]) -> Result<Vec<u8>> {
    if call.len() < 40 || !call.len().is_multiple_of(4) {
        return Err(NfsError::Xdr(
            "NFSv4.0 callback RPC call has an invalid length".into(),
        ));
    }
    let words: Vec<u32> = call[..40]
        .chunks_exact(4)
        .map(|word| u32::from_be_bytes([word[0], word[1], word[2], word[3]]))
        .collect();
    if words[1] != 0 {
        return Err(NfsError::Rpc(
            "NFSv4.0 callback RPC message is not a CALL".into(),
        ));
    }
    if words[2] != 2 {
        return Ok(rpc_reply(&[words[0], 1, 1, 0, 2, 2]));
    }
    if words[3] != CB_PROGRAM {
        return Ok(accepted_reply(words[0], &[1]));
    }
    if words[4] != 1 {
        return Ok(accepted_reply(words[0], &[2, 1, 1]));
    }
    if words[5] > 1 {
        return Ok(accepted_reply(words[0], &[3]));
    }
    if words[6..] != [0, 0, 0, 0] {
        return Err(NfsError::Rpc(
            "NFSv4.0 CB_NULL authentication does not match AUTH_NONE".into(),
        ));
    }
    if words[5] == 0 {
        return Ok(if call.len() == 40 {
            accepted_reply(words[0], &[0])
        } else {
            accepted_reply(words[0], &[4])
        });
    }
    let compound = match empty_compound_reply(&call[40..]) {
        Ok(compound) => compound,
        Err(_) => return Ok(accepted_reply(words[0], &[4])),
    };
    Ok(accepted_body(words[0], &compound))
}

fn empty_compound_reply(body: &[u8]) -> Result<Vec<u8>> {
    if body.len() < 16 {
        return Err(NfsError::Xdr("CB_COMPOUND envelope truncated".into()));
    }
    let tag_len = u32::from_be_bytes([body[0], body[1], body[2], body[3]]) as usize;
    let padded_tag = tag_len
        .checked_add(3)
        .ok_or_else(|| NfsError::Xdr("CB_COMPOUND tag length overflow".into()))?
        & !3;
    let envelope_len = 4usize
        .checked_add(padded_tag)
        .and_then(|length| length.checked_add(12))
        .ok_or_else(|| NfsError::Xdr("CB_COMPOUND envelope length overflow".into()))?;
    if body.len() != envelope_len || body.len() < 4 + tag_len {
        return Err(NfsError::Xdr("CB_COMPOUND envelope malformed".into()));
    }
    let minor_offset = 4 + padded_tag;
    let minor = u32::from_be_bytes(
        body[minor_offset..minor_offset + 4]
            .try_into()
            .map_err(|_| NfsError::Xdr("CB_COMPOUND minorversion truncated".into()))?,
    );
    let operation_count = u32::from_be_bytes(
        body[minor_offset + 8..minor_offset + 12]
            .try_into()
            .map_err(|_| NfsError::Xdr("CB_COMPOUND operation count truncated".into()))?,
    );
    if operation_count != 0 {
        return Err(NfsError::Xdr(
            "CB_COMPOUND operations are not implemented by this envelope decoder".into(),
        ));
    }
    let status = if minor == 0 { 0u32 } else { 10021u32 };
    let mut reply = Vec::with_capacity(12 + padded_tag);
    reply.extend_from_slice(&status.to_be_bytes());
    reply.extend_from_slice(&(tag_len as u32).to_be_bytes());
    reply.extend_from_slice(&body[4..4 + padded_tag]);
    reply.extend_from_slice(&0u32.to_be_bytes());
    Ok(reply)
}

fn accepted_reply(xid: u32, result: &[u32]) -> Vec<u8> {
    let mut words = Vec::with_capacity(5 + result.len());
    words.extend_from_slice(&[xid, 1, 0, 0, 0]);
    words.extend_from_slice(result);
    rpc_reply(&words)
}

fn accepted_body(xid: u32, body: &[u8]) -> Vec<u8> {
    let mut reply = rpc_reply(&[xid, 1, 0, 0, 0, 0]);
    reply.extend_from_slice(body);
    reply
}

fn rpc_reply(words: &[u32]) -> Vec<u8> {
    let mut reply = Vec::with_capacity(words.len() * 4);
    for word in words {
        reply.extend_from_slice(&word.to_be_bytes());
    }
    reply
}

impl Drop for CallbackService {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn socket_addr(universal: &str) -> SocketAddr {
        let fields: Vec<u16> = universal
            .split('.')
            .map(|field| field.parse().unwrap())
            .collect();
        SocketAddr::from((
            [
                fields[0] as u8,
                fields[1] as u8,
                fields[2] as u8,
                fields[3] as u8,
            ],
            fields[4] * 256 + fields[5],
        ))
    }

    fn cb_null_call(xid: u32) -> Vec<u32> {
        vec![xid, 0, 2, CB_PROGRAM, 1, 0, 0, 0, 0, 0]
    }

    async fn round_trip(stream: &mut tokio::net::TcpStream, words: &[u32]) -> Vec<u32> {
        let mut call = Vec::new();
        for word in words {
            call.extend_from_slice(&word.to_be_bytes());
        }
        stream
            .write_u32(0x8000_0000 | call.len() as u32)
            .await
            .unwrap();
        stream.write_all(&call).await.unwrap();
        let marker = stream.read_u32().await.unwrap();
        assert_eq!(marker & 0x8000_0000, 0x8000_0000);
        let mut reply = vec![0; (marker & 0x7fff_ffff) as usize];
        stream.read_exact(&mut reply).await.unwrap();
        reply
            .chunks_exact(4)
            .map(|word| u32::from_be_bytes(word.try_into().unwrap()))
            .collect()
    }

    #[tokio::test]
    async fn cb_null_round_trips_over_the_published_listener() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let mut stream = tokio::net::TcpStream::connect(socket_addr(service.universal_addr()))
            .await
            .unwrap();
        let words = round_trip(&mut stream, &cb_null_call(0x1020_3040)).await;
        assert_eq!(words, [0x1020_3040, 1, 0, 0, 0, 0]);
    }

    #[tokio::test]
    async fn cb_null_reports_precise_rpc_header_failures() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let mut stream = tokio::net::TcpStream::connect(socket_addr(service.universal_addr()))
            .await
            .unwrap();
        for (index, value, expected) in [
            (2, 3, vec![7, 1, 1, 0, 2, 2]),
            (3, CB_PROGRAM + 1, vec![7, 1, 0, 0, 0, 1]),
            (4, 2, vec![7, 1, 0, 0, 0, 2, 1, 1]),
            (5, 2, vec![7, 1, 0, 0, 0, 3]),
        ] {
            let mut call = cb_null_call(7);
            call[index] = value;
            assert_eq!(round_trip(&mut stream, &call).await, expected);
        }
    }

    #[tokio::test]
    async fn empty_cb_compound_echoes_tag_and_rejects_other_minor_versions() {
        let service = CallbackService::bind_for("127.0.0.1:2049".parse().unwrap())
            .await
            .unwrap();
        let mut stream = tokio::net::TcpStream::connect(socket_addr(service.universal_addr()))
            .await
            .unwrap();
        for (minor, expected_status) in [(0, 0), (1, 10021)] {
            let mut call = cb_null_call(11 + minor);
            call[5] = 1;
            call.extend_from_slice(&[3, u32::from_be_bytes(*b"tag\0"), minor, 1, 0]);
            let reply = round_trip(&mut stream, &call).await;
            assert_eq!(&reply[..6], &[11 + minor, 1, 0, 0, 0, 0]);
            assert_eq!(reply[6], expected_status);
            assert_eq!(reply[7], 3);
            assert_eq!(reply[8], u32::from_be_bytes(*b"tag\0"));
            assert_eq!(reply[9], 0);
        }
    }
}
