//! The browse loop: turn matching announcements into transport dials.
//!
//! Nothing here touches the peer store. A dial makes the transport open a
//! connection and run its preflight; from there the access module takes
//! over, proves that both sides know the space's secret, and only then
//! exchanges agent infos. All discovery has to do is make the connection
//! happen.

use crate::dial_policy::DialPolicy;
use crate::discovery;
use crate::fingerprint::SpaceFingerprint;
use kitsune2_api::{DynTransport, SpaceId, Url};
use mdns_sd::ServiceEvent;
use std::sync::{Arc, Mutex};
use tracing::{debug, trace};

/// What this node looks like on the LAN, so the browse loop can recognise
/// its own announcements. The instance name is fixed for the life of the
/// service; the URL follows whatever is currently advertised.
#[derive(Debug)]
pub struct LocalIdentity {
    fullname: String,
    url: Mutex<Option<Url>>,
}

impl LocalIdentity {
    /// An identity with the given mDNS instance fullname and no URL yet.
    pub fn new(fullname: String) -> Self {
        Self {
            fullname,
            url: Mutex::new(None),
        }
    }

    /// Record the URL we are currently announcing.
    pub fn set_url(&self, url: Url) {
        *self.url.lock().expect("identity poisoned") = Some(url);
    }

    fn url(&self) -> Option<Url> {
        self.url.lock().expect("identity poisoned").clone()
    }
}

/// Consume browse events until the source closes, dialling every peer that
/// passes the announcement filter and the dial policy.
///
/// Dials run in their own tasks so that a slow connect never holds up the
/// event stream; the policy's in-flight cap bounds how many run at once.
pub async fn browse_loop(
    rx: flume::Receiver<ServiceEvent>,
    space_id: SpaceId,
    fp: SpaceFingerprint,
    identity: Arc<LocalIdentity>,
    tx: DynTransport,
    policy: Arc<DialPolicy>,
) {
    while let Ok(event) = rx.recv_async().await {
        let self_url = identity.url();
        let Some(peer) = discovery::resolved_to_peer(
            &event,
            &fp,
            &identity.fullname,
            self_url.as_ref(),
        ) else {
            continue;
        };
        if !policy.claim(&peer.url) {
            trace!(url = %peer.url, "mdns: peer within dial cooldown, skipping");
            continue;
        }
        debug!(url = %peer.url, fullname = %peer.fullname, "mdns: discovered peer, dialling");

        let tx = tx.clone();
        let policy = policy.clone();
        let space_id = space_id.clone();
        tokio::spawn(async move {
            let _slot = policy.acquire_slot().await;
            match tx.dial(space_id, peer.url.clone()).await {
                Ok(()) => debug!(url = %peer.url, "mdns: dial succeeded"),
                Err(err) => debug!(?err, url = %peer.url, "mdns: dial failed"),
            }
        });
    }
    trace!("mdns: browse event source closed, browse loop ending");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::test_support::*;
    use crate::fingerprint::space_fingerprint;
    use kitsune2_api::{MockTransport, SpaceId};
    use std::time::Duration;

    const SELF: &str = "self-instance";
    const SELF_URL: &str = "ws://self.test:80/selfpeer";
    const PEER_A: &str = "ws://a.test:80/peera";
    const PEER_B: &str = "ws://b.test:80/peerb";

    fn space() -> SpaceId {
        SpaceId::from(bytes::Bytes::from_static(b"space"))
    }

    /// A transport that only records which URLs it was asked to dial.
    fn recording_transport() -> (DynTransport, Arc<Mutex<Vec<Url>>>) {
        let dials: Arc<Mutex<Vec<Url>>> = Arc::new(Mutex::new(Vec::new()));
        let mut mock = MockTransport::new();
        let record = dials.clone();
        mock.expect_dial().returning(move |_space_id, url| {
            record.lock().unwrap().push(url);
            Box::pin(async { Ok(()) })
        });
        (Arc::new(mock), dials)
    }

    async fn settle() {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn dials_each_discovered_peer_once_within_cooldown() {
        let (tx, dials) = recording_transport();
        let (event_tx, event_rx) = flume::unbounded();
        let identity = Arc::new(LocalIdentity::new(fullname(SELF)));
        identity.set_url(Url::from_str(SELF_URL).unwrap());
        let policy = Arc::new(DialPolicy::new(Duration::from_secs(60), 4));
        let fp = space_fingerprint(&space());
        let fp_hex = hex::encode(fp);

        let loop_task = tokio::spawn(browse_loop(
            event_rx,
            space(),
            fp,
            identity,
            tx,
            policy,
        ));

        // The same peer resolved three times, a second peer once, our own
        // record, a record for another space and a resolve without a URL.
        for _ in 0..3 {
            event_tx
                .send(resolved(
                    "peer-a",
                    &[("spacefp", &fp_hex), ("url", PEER_A)],
                ))
                .unwrap();
        }
        event_tx
            .send(resolved("peer-b", &[("spacefp", &fp_hex), ("url", PEER_B)]))
            .unwrap();
        event_tx
            .send(resolved(SELF, &[("spacefp", &fp_hex), ("url", SELF_URL)]))
            .unwrap();
        event_tx
            .send(resolved(
                "peer-c",
                &[("spacefp", &hex::encode([9u8; 32])), ("url", PEER_A)],
            ))
            .unwrap();
        event_tx
            .send(resolved("peer-d", &[("spacefp", &fp_hex)]))
            .unwrap();
        settle().await;

        let mut dialled = dials.lock().unwrap().clone();
        dialled.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        assert_eq!(
            dialled,
            vec![
                Url::from_str(PEER_A).unwrap(),
                Url::from_str(PEER_B).unwrap()
            ]
        );

        // Closing the event source ends the loop.
        drop(event_tx);
        tokio::time::timeout(Duration::from_secs(1), loop_task)
            .await
            .expect("browse loop should end when its source closes")
            .unwrap();
    }

    #[tokio::test]
    async fn a_peer_announced_under_our_own_url_is_not_dialled() {
        let (tx, dials) = recording_transport();
        let (event_tx, event_rx) = flume::unbounded();
        let identity = Arc::new(LocalIdentity::new(fullname(SELF)));
        let policy = Arc::new(DialPolicy::new(Duration::from_secs(60), 4));
        let fp = space_fingerprint(&space());
        let fp_hex = hex::encode(fp);
        let _loop_task = tokio::spawn(browse_loop(
            event_rx,
            space(),
            fp,
            identity.clone(),
            tx,
            policy,
        ));

        // Before we know our URL an announcement naming it is dialled, as
        // it would be for any other peer; once we know it, it is ours.
        identity.set_url(Url::from_str(SELF_URL).unwrap());
        event_tx
            .send(resolved("echo", &[("spacefp", &fp_hex), ("url", SELF_URL)]))
            .unwrap();
        settle().await;
        assert!(dials.lock().unwrap().is_empty());
    }
}
