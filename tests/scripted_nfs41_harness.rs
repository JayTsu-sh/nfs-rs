mod support;

use std::io;
use std::sync::Arc;

use bytes::Bytes;
use support::scripted_rpc::{
    EndpointRole, ScriptStep, ScriptedRpcServer, read_record, write_record,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Notify;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn metadata_and_data_endpoints_remain_distinct() -> TestResult {
    let mut mds = ScriptedRpcServer::start(
        EndpointRole::Metadata,
        vec![ScriptStep::Reply(Bytes::from_static(b"mds-reply"))],
    )
    .await?;
    let mut ds = ScriptedRpcServer::start(
        EndpointRole::Data,
        vec![ScriptStep::Reply(Bytes::from_static(b"ds-reply"))],
    )
    .await?;

    let mut mds_client = TcpStream::connect(mds.addr()).await?;
    let mut ds_client = TcpStream::connect(ds.addr()).await?;
    write_record(&mut mds_client, b"metadata-call").await?;
    write_record(&mut ds_client, b"data-call").await?;

    assert_eq!(read_record(&mut mds_client).await?, b"mds-reply"[..]);
    assert_eq!(read_record(&mut ds_client).await?, b"ds-reply"[..]);
    let mds_received = mds.next_received().await?;
    let ds_received = ds.next_received().await?;
    assert_eq!(mds_received.role, EndpointRole::Metadata);
    assert_eq!(mds_received.bytes, b"metadata-call"[..]);
    assert_eq!(ds_received.role, EndpointRole::Data);
    assert_eq!(ds_received.bytes, b"data-call"[..]);
    assert_eq!(mds.role(), EndpointRole::Metadata);
    assert_eq!(ds.role(), EndpointRole::Data);
    assert_eq!(mds.remaining_steps()?, 0);
    assert_eq!(ds.remaining_steps()?, 0);

    drop(mds_client);
    drop(ds_client);
    mds.shutdown().await?;
    ds.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn fragmented_reply_is_reassembled() -> TestResult {
    let server = ScriptedRpcServer::start(
        EndpointRole::Metadata,
        vec![ScriptStep::ReplyFragments(vec![
            Bytes::from_static(b"first-"),
            Bytes::from_static(b"second"),
        ])],
    )
    .await?;
    let mut client = TcpStream::connect(server.addr()).await?;
    write_record(&mut client, b"request").await?;
    assert_eq!(read_record(&mut client).await?, b"first-second"[..]);
    drop(client);
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn deterministic_gate_controls_reply_order() -> TestResult {
    let gate = Arc::new(Notify::new());
    let mut server = ScriptedRpcServer::start(
        EndpointRole::Metadata,
        vec![ScriptStep::WaitThenReply {
            gate: Arc::clone(&gate),
            reply: Bytes::from_static(b"released"),
        }],
    )
    .await?;
    let mut client = TcpStream::connect(server.addr()).await?;
    write_record(&mut client, b"blocked-request").await?;
    assert_eq!(server.next_received().await?.bytes, b"blocked-request"[..]);
    gate.notify_one();
    assert_eq!(read_record(&mut client).await?, b"released"[..]);
    drop(client);
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn close_step_drops_connection_without_reply() -> TestResult {
    let server = ScriptedRpcServer::start(EndpointRole::Metadata, vec![ScriptStep::Close]).await?;
    let mut client = TcpStream::connect(server.addr()).await?;
    write_record(&mut client, b"request").await?;
    let error = read_record(&mut client)
        .await
        .expect_err("connection must close");
    assert!(
        matches!(
            error.kind(),
            io::ErrorKind::UnexpectedEof
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::BrokenPipe
        ),
        "unexpected close error: {error}"
    );
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn shutdown_cancels_an_active_connection() -> TestResult {
    let gate = Arc::new(Notify::new());
    let server = ScriptedRpcServer::start(
        EndpointRole::Metadata,
        vec![ScriptStep::WaitThenReply {
            gate,
            reply: Bytes::from_static(b"never-sent"),
        }],
    )
    .await?;
    let mut client = TcpStream::connect(server.addr()).await?;
    write_record(&mut client, b"request").await?;
    server.shutdown().await?;
    let error = read_record(&mut client)
        .await
        .expect_err("shutdown must close active connection");
    assert!(
        matches!(
            error.kind(),
            io::ErrorKind::UnexpectedEof
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::BrokenPipe
        ),
        "unexpected shutdown error: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn server_can_issue_backchannel_call_before_fore_reply() -> TestResult {
    let server = ScriptedRpcServer::start(
        EndpointRole::Metadata,
        vec![ScriptStep::BackchannelThenReply {
            call: Bytes::from_static(b"callback-call"),
            expected_reply: Bytes::from_static(b"callback-reply"),
            reply: Bytes::from_static(b"fore-reply"),
        }],
    )
    .await?;
    let mut client = TcpStream::connect(server.addr()).await?;
    write_record(&mut client, b"fore-call").await?;
    assert_eq!(read_record(&mut client).await?, b"callback-call"[..]);
    write_record(&mut client, b"callback-reply").await?;
    assert_eq!(read_record(&mut client).await?, b"fore-reply"[..]);
    drop(client);
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn server_can_return_two_requests_out_of_order() -> TestResult {
    let mut server = ScriptedRpcServer::start(
        EndpointRole::Metadata,
        vec![ScriptStep::ReceiveNextThenReplyReverse {
            first_reply: Bytes::from_static(b"reply-one"),
            second_reply: Bytes::from_static(b"reply-two"),
        }],
    )
    .await?;
    let mut client = TcpStream::connect(server.addr()).await?;
    write_record(&mut client, b"request-one").await?;
    write_record(&mut client, b"request-two").await?;
    assert_eq!(server.next_received().await?.bytes, b"request-one"[..]);
    assert_eq!(server.next_received().await?.bytes, b"request-two"[..]);
    assert_eq!(read_record(&mut client).await?, b"reply-two"[..]);
    assert_eq!(read_record(&mut client).await?, b"reply-one"[..]);
    drop(client);
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn duplicate_reply_is_observable() -> TestResult {
    let server = ScriptedRpcServer::start(
        EndpointRole::Metadata,
        vec![ScriptStep::DuplicateReply(Bytes::from_static(
            b"same-reply",
        ))],
    )
    .await?;
    let mut client = TcpStream::connect(server.addr()).await?;
    write_record(&mut client, b"request").await?;
    assert_eq!(read_record(&mut client).await?, b"same-reply"[..]);
    assert_eq!(read_record(&mut client).await?, b"same-reply"[..]);
    drop(client);
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn truncated_reply_is_observable() -> TestResult {
    let server = ScriptedRpcServer::start(
        EndpointRole::Metadata,
        vec![ScriptStep::TruncatedReply {
            declared_len: 32,
            body: Bytes::from_static(b"short"),
        }],
    )
    .await?;
    let mut client = TcpStream::connect(server.addr()).await?;
    write_record(&mut client, b"request").await?;
    let error = read_record(&mut client)
        .await
        .expect_err("truncated reply must fail");
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn oversized_fragment_is_rejected_before_allocation() -> TestResult {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let client_task = tokio::spawn(async move {
        let mut client = TcpStream::connect(addr).await?;
        client
            .write_u32(0x8000_0000 | (4 * 1024 * 1024 + 4097))
            .await?;
        Ok::<(), io::Error>(())
    });
    let (mut stream, _) = listener.accept().await?;
    let error = read_record(&mut stream)
        .await
        .expect_err("oversized record must fail");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    client_task.await??;
    Ok(())
}
