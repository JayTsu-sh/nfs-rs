use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;

const LAST_FRAGMENT: u32 = 0x8000_0000;
const LENGTH_MASK: u32 = 0x7fff_ffff;
const MAX_FRAME_SIZE: usize = 4 * 1024 * 1024 + 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndpointRole {
    Metadata,
    Data,
}

#[derive(Debug)]
pub enum ScriptStep {
    Reply(Bytes),
    ReplyFragments(Vec<Bytes>),
    DuplicateReply(Bytes),
    TruncatedReply {
        declared_len: u32,
        body: Bytes,
    },
    ReceiveNextThenReplyReverse {
        first_reply: Bytes,
        second_reply: Bytes,
    },
    Close,
    WaitThenReply {
        gate: Arc<Notify>,
        reply: Bytes,
    },
    BackchannelThenReply {
        call: Bytes,
        expected_reply: Bytes,
        reply: Bytes,
    },
}

#[derive(Debug)]
pub struct ReceivedFrame {
    pub role: EndpointRole,
    pub bytes: Bytes,
}

pub struct ScriptedRpcServer {
    role: EndpointRole,
    addr: SocketAddr,
    steps: Arc<Mutex<VecDeque<ScriptStep>>>,
    received_rx: mpsc::UnboundedReceiver<ReceivedFrame>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: JoinHandle<io::Result<()>>,
}

impl ScriptedRpcServer {
    pub async fn start(role: EndpointRole, steps: Vec<ScriptStep>) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let steps = Arc::new(Mutex::new(VecDeque::from(steps)));
        let task_steps = Arc::clone(&steps);
        let (received_tx, received_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted?;
                        tokio::select! {
                            result = serve_connection(
                                stream,
                                role,
                                Arc::clone(&task_steps),
                                received_tx.clone(),
                            ) => result?,
                            _ = &mut shutdown_rx => return Ok(()),
                        }
                    }
                    _ = &mut shutdown_rx => return Ok(()),
                }
            }
        });
        Ok(Self {
            role,
            addr,
            steps,
            received_rx,
            shutdown_tx: Some(shutdown_tx),
            task,
        })
    }

    pub fn role(&self) -> EndpointRole {
        self.role
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn next_received(&mut self) -> io::Result<ReceivedFrame> {
        self.received_rx.recv().await.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "scripted RPC receive channel closed",
            )
        })
    }

    pub fn remaining_steps(&self) -> io::Result<usize> {
        self.steps
            .lock()
            .map(|steps| steps.len())
            .map_err(|_| io::Error::other("scripted RPC step mutex poisoned"))
    }

    pub async fn shutdown(mut self) -> io::Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.task
            .await
            .map_err(|error| io::Error::other(format!("scripted RPC task failed: {error}")))?
    }
}

async fn serve_connection(
    mut stream: TcpStream,
    role: EndpointRole,
    steps: Arc<Mutex<VecDeque<ScriptStep>>>,
    received_tx: mpsc::UnboundedSender<ReceivedFrame>,
) -> io::Result<()> {
    loop {
        let request = match read_record(&mut stream).await {
            Ok(frame) => frame,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::BrokenPipe
                ) =>
            {
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        received_tx
            .send(ReceivedFrame {
                role,
                bytes: request,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "receive observer closed"))?;

        let step = steps
            .lock()
            .map_err(|_| io::Error::other("scripted RPC step mutex poisoned"))?
            .pop_front()
            .ok_or_else(|| io::Error::other("script exhausted before request arrived"))?;

        match step {
            ScriptStep::Reply(reply) => write_record(&mut stream, &reply).await?,
            ScriptStep::ReplyFragments(fragments) => {
                write_fragments(&mut stream, &fragments).await?
            }
            ScriptStep::DuplicateReply(reply) => {
                write_record(&mut stream, &reply).await?;
                write_record(&mut stream, &reply).await?;
            }
            ScriptStep::TruncatedReply { declared_len, body } => {
                if declared_len > LENGTH_MASK || declared_len as usize <= body.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid truncated reply length",
                    ));
                }
                stream.write_u32(LAST_FRAGMENT | declared_len).await?;
                stream.write_all(&body).await?;
                return Ok(());
            }
            ScriptStep::ReceiveNextThenReplyReverse {
                first_reply,
                second_reply,
            } => {
                let second_request = read_record(&mut stream).await?;
                received_tx
                    .send(ReceivedFrame {
                        role,
                        bytes: second_request,
                    })
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::BrokenPipe, "receive observer closed")
                    })?;
                write_record(&mut stream, &second_reply).await?;
                write_record(&mut stream, &first_reply).await?;
            }
            ScriptStep::Close => return Ok(()),
            ScriptStep::WaitThenReply { gate, reply } => {
                gate.notified().await;
                write_record(&mut stream, &reply).await?;
            }
            ScriptStep::BackchannelThenReply {
                call,
                expected_reply,
                reply,
            } => {
                write_record(&mut stream, &call).await?;
                let callback_reply = read_record(&mut stream).await?;
                if callback_reply != expected_reply {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "unexpected callback reply: got {} bytes, expected {}",
                            callback_reply.len(),
                            expected_reply.len()
                        ),
                    ));
                }
                write_record(&mut stream, &reply).await?;
            }
        }
    }
}

pub async fn read_record(stream: &mut TcpStream) -> io::Result<Bytes> {
    let mut output = BytesMut::new();
    loop {
        let raw = stream.read_u32().await?;
        let last = raw & LAST_FRAGMENT != 0;
        let fragment_len = (raw & LENGTH_MASK) as usize;
        let total = output
            .len()
            .checked_add(fragment_len)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "RPC size overflow"))?;
        if total > MAX_FRAME_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "RPC record exceeds scripted server limit",
            ));
        }
        let offset = output.len();
        output.resize(total, 0);
        stream.read_exact(&mut output[offset..]).await?;
        if last {
            return Ok(output.freeze());
        }
    }
}

pub async fn write_record(stream: &mut TcpStream, frame: &[u8]) -> io::Result<()> {
    write_fragments(stream, &[Bytes::copy_from_slice(frame)]).await
}

pub async fn write_fragments(stream: &mut TcpStream, fragments: &[Bytes]) -> io::Result<()> {
    if fragments.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "RPC record requires at least one fragment",
        ));
    }
    for (index, fragment) in fragments.iter().enumerate() {
        if fragment.len() > LENGTH_MASK as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RPC fragment exceeds record marker capacity",
            ));
        }
        let mut marker = fragment.len() as u32;
        if index + 1 == fragments.len() {
            marker |= LAST_FRAGMENT;
        }
        let mut header = BytesMut::with_capacity(4);
        header.put_u32(marker);
        stream.write_all(&header).await?;
        stream.write_all(fragment).await?;
    }
    Ok(())
}
