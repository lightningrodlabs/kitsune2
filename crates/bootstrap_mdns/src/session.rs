//! A single peer-info-exchange session over TCP.
//!
//! Both sides run the same state machine:
//!
//! 1. Send [`Hello`](crate::proto::Hello) with our nonce and space fingerprint.
//! 2. Receive peer's [`Hello`]. Check `proto_ver` and fingerprint.
//! 3. Send [`HelloAck`](crate::proto::HelloAck) with HMAC proof bound to both nonces.
//! 4. Receive peer's [`HelloAck`]. Verify their proof.
//! 5. Send our [`AgentInfoBatch`](crate::proto::AgentInfoBatch).
//! 6. Receive peer's batch. Verify signatures. Return to caller for peer_store insertion.
//!
//! Frames are length-prefixed with a u32 big-endian length header. Max
//! frame size is bounded so a malicious peer can't cause unbounded allocation.

use crate::proto::{
    self, AgentInfoBatch, FP_LEN, Hello, HelloAck, Nonce, PROTO_VER,
};
use kitsune2_api::{
    AgentInfoSigned, DynVerifier, K2Error, K2Result, SpaceId,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Hard cap on the size of any single protocol frame. 256 KiB is ample for
/// several thousand AgentInfoSigned records while bounding DoS risk.
pub const MAX_FRAME_BYTES: usize = 256 * 1024;

/// Overall session timeout. Handshake + exchange should complete well
/// within this on any healthy LAN.
pub const SESSION_TIMEOUT: Duration = Duration::from_secs(10);

/// Run one peer-info-exchange session on an already-established TCP stream.
///
/// Returns the verified [`AgentInfoSigned`] records the peer advertised.
pub async fn run(
    stream: TcpStream,
    space_id: SpaceId,
    local_infos: Vec<Arc<AgentInfoSigned>>,
    verifier: DynVerifier,
) -> K2Result<Vec<Arc<AgentInfoSigned>>> {
    tokio::time::timeout(
        SESSION_TIMEOUT,
        run_inner(stream, space_id, local_infos, verifier),
    )
    .await
    .map_err(|_| K2Error::other("mdns session timed out"))?
}

async fn run_inner(
    mut stream: TcpStream,
    space_id: SpaceId,
    local_infos: Vec<Arc<AgentInfoSigned>>,
    verifier: DynVerifier,
) -> K2Result<Vec<Arc<AgentInfoSigned>>> {
    let fp = proto::space_fingerprint(&space_id);
    let our_nonce: Nonce = proto::fresh_nonce();

    let hello = Hello {
        proto_ver: PROTO_VER,
        space_fp: fp.to_vec(),
        nonce: our_nonce.to_vec(),
    };
    write_frame(&mut stream, &proto::encode(&hello)?).await?;

    let peer_hello: Hello = proto::decode(&read_frame(&mut stream).await?)?;
    if peer_hello.proto_ver != PROTO_VER {
        return Err(K2Error::other(format!(
            "mdns peer proto_ver mismatch: got {}, want {}",
            peer_hello.proto_ver, PROTO_VER
        )));
    }
    if peer_hello.space_fp.len() != FP_LEN || peer_hello.space_fp != fp {
        return Err(K2Error::other("mdns peer space fingerprint mismatch"));
    }
    let peer_nonce: Nonce = peer_hello
        .nonce
        .clone()
        .try_into()
        .map_err(|_| K2Error::other("mdns peer nonce wrong length"))?;

    let our_proof = proto::proof(&space_id, &our_nonce, &peer_nonce);
    let ack = HelloAck {
        proof: our_proof.to_vec(),
    };
    write_frame(&mut stream, &proto::encode(&ack)?).await?;

    let peer_ack: HelloAck = proto::decode(&read_frame(&mut stream).await?)?;
    let peer_proof_arr: [u8; 32] = peer_ack
        .proof
        .clone()
        .try_into()
        .map_err(|_| K2Error::other("mdns peer proof wrong length"))?;
    if !proto::verify_proof(
        &space_id,
        &our_nonce,
        &peer_nonce,
        &peer_proof_arr,
    ) {
        return Err(K2Error::other("mdns peer failed proof of space-knowledge"));
    }

    let batch = AgentInfoBatch::from_infos(&local_infos);
    write_frame(&mut stream, &proto::encode(&batch)?).await?;

    let peer_batch: AgentInfoBatch =
        proto::decode(&read_frame(&mut stream).await?)?;
    peer_batch.decode(&verifier)
}

async fn write_frame(stream: &mut TcpStream, body: &[u8]) -> K2Result<()> {
    if body.len() > MAX_FRAME_BYTES {
        return Err(K2Error::other("mdns frame too large"));
    }
    let len = body.len() as u32;
    stream
        .write_all(&len.to_be_bytes())
        .await
        .map_err(|e| K2Error::other_src("mdns frame write len", e))?;
    stream
        .write_all(body)
        .await
        .map_err(|e| K2Error::other_src("mdns frame write body", e))?;
    stream
        .flush()
        .await
        .map_err(|e| K2Error::other_src("mdns frame flush", e))?;
    Ok(())
}

async fn read_frame(stream: &mut TcpStream) -> K2Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| K2Error::other_src("mdns frame read len", e))?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(K2Error::other("mdns frame body too large"));
    }
    let mut buf = vec![0u8; len];
    stream
        .read_exact(&mut buf)
        .await
        .map_err(|e| K2Error::other_src("mdns frame read body", e))?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kitsune2_test_utils::agent::{
        AgentBuilder, TestLocalAgent, TestVerifier,
    };
    use tokio::net::TcpListener;

    fn space(bytes: &[u8]) -> SpaceId {
        SpaceId::from(bytes::Bytes::copy_from_slice(bytes))
    }

    async fn an_agent(space_id: SpaceId) -> Arc<AgentInfoSigned> {
        AgentBuilder::default()
            .with_space(space_id)
            .build(TestLocalAgent::default())
    }

    #[tokio::test]
    async fn round_trip_exchanges_verified_infos() {
        let space_id = space(b"the-space");
        let a_info = an_agent(space_id.clone()).await;
        let b_info = an_agent(space_id.clone()).await;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_space = space_id.clone();
        let server_infos = vec![a_info.clone()];
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let verifier: DynVerifier = Arc::new(TestVerifier);
            run(stream, server_space, server_infos, verifier).await
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let verifier: DynVerifier = Arc::new(TestVerifier);
        let got_from_server =
            run(stream, space_id.clone(), vec![b_info.clone()], verifier)
                .await
                .unwrap();
        let got_from_client = server.await.unwrap().unwrap();

        assert_eq!(got_from_server.len(), 1);
        assert_eq!(got_from_server[0].agent, a_info.agent);
        assert_eq!(got_from_client.len(), 1);
        assert_eq!(got_from_client[0].agent, b_info.agent);
    }

    #[tokio::test]
    async fn wrong_space_is_rejected() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let space_a = space(b"space-a");
        let space_b = space(b"space-b");

        let server = tokio::spawn({
            let space_a = space_a.clone();
            async move {
                let (stream, _) = listener.accept().await.unwrap();
                let verifier: DynVerifier = Arc::new(TestVerifier);
                run(stream, space_a, vec![], verifier).await
            }
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let verifier: DynVerifier = Arc::new(TestVerifier);
        let res = run(stream, space_b, vec![], verifier).await;
        assert!(res.is_err(), "cross-space session must fail");
        assert!(server.await.unwrap().is_err());
    }
}
