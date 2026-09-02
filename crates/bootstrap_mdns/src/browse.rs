//! The browse loop: route each announcement heard on the LAN to the space
//! it belongs to.
//!
//! One loop serves every space on the shared daemon. A resolved record is
//! matched by its fingerprint against the registry of spaces this node is
//! in, and handed to that space's entry, which decides whether to dial.
//! Nothing here touches the peer store: a dial makes the transport open a
//! connection and run its preflight, and from there the access module
//! proves that both sides know the space's secret before any agent info
//! changes hands.

use crate::discovery;
use crate::fingerprint::SpaceFingerprint;
use crate::space::SpaceEntry;
use kitsune2_api::Url;
use mdns_sd::ServiceEvent;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::trace;

/// The spaces currently sharing the daemon, by the fingerprint they
/// announce and match on.
pub type Registry = Mutex<HashMap<SpaceFingerprint, Arc<SpaceEntry>>>;

/// Consume browse events until the source closes.
pub async fn browse_loop(
    rx: flume::Receiver<ServiceEvent>,
    registry: Arc<Registry>,
) {
    // Withdrawal events carry only the record's name, so what each name
    // last announced is kept here to know what to forget.
    let mut announced: HashMap<String, (SpaceFingerprint, Url)> =
        HashMap::new();
    while let Ok(event) = rx.recv_async().await {
        match event {
            ServiceEvent::ServiceResolved(svc) => {
                let Some((fp, url)) = discovery::parse_record(&svc) else {
                    trace!(fullname = %svc.fullname, "mdns: ignoring record without a usable spacefp and url");
                    continue;
                };
                let Some(entry) = lookup(&registry, &fp) else {
                    trace!(fullname = %svc.fullname, "mdns: record for a space this node is not in");
                    continue;
                };
                if entry.is_own_record(&svc.fullname, &url) {
                    continue;
                }
                if let Some((_, previous)) =
                    announced.insert(svc.fullname.clone(), (fp, url.clone()))
                    && previous != url
                {
                    entry.record_removed(&previous);
                }
                trace!(fullname = %svc.fullname, %url, "mdns: resolved record");
                entry.record_resolved(url);
            }
            ServiceEvent::ServiceRemoved(_, fullname) => {
                if let Some((fp, url)) = announced.remove(&fullname)
                    && let Some(entry) = lookup(&registry, &fp)
                {
                    entry.record_removed(&url);
                }
            }
            _ => {}
        }
    }
    trace!("mdns: browse event source closed, browse loop ending");
}

fn lookup(
    registry: &Registry,
    fp: &SpaceFingerprint,
) -> Option<Arc<SpaceEntry>> {
    registry.lock().expect("poison").get(fp).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::Daemon as _;
    use crate::discovery::test_support::*;
    use crate::fingerprint::space_fingerprint;
    use kitsune2_api::{DynTransport, MockTransport, SpaceId};
    use std::time::Duration;

    const PEER_A: &str = "ws://a.test:80/peera";
    const PEER_B: &str = "ws://b.test:80/peerb";
    const SELF_URL: &str = "ws://self.test:80/selfpeer";

    fn url(s: &str) -> Url {
        Url::from_str(s).unwrap()
    }

    /// A transport that records its dials and never reports a connection.
    fn recording_transport() -> (DynTransport, Arc<Mutex<Vec<Url>>>) {
        let dials: Arc<Mutex<Vec<Url>>> = Arc::new(Mutex::new(Vec::new()));
        let record = dials.clone();
        let mut mock = MockTransport::new();
        mock.expect_dial().returning(move |_space, url| {
            record.lock().unwrap().push(url);
            Box::pin(async { Ok(()) })
        });
        mock.expect_get_connected_peers()
            .returning(|| Box::pin(async { Ok(Vec::new()) }));
        (Arc::new(mock), dials)
    }

    struct Harness {
        daemon: Arc<FakeDaemon>,
        registry: Arc<Registry>,
        _loop_task: tokio::task::JoinHandle<()>,
    }

    fn harness() -> Harness {
        let daemon = FakeDaemon::new();
        let registry: Arc<Registry> = Arc::new(Mutex::new(HashMap::new()));
        let rx = daemon.browse().unwrap();
        let loop_task = tokio::spawn(browse_loop(rx, registry.clone()));
        Harness {
            daemon,
            registry,
            _loop_task: loop_task,
        }
    }

    fn join(
        h: &Harness,
        space: &[u8],
    ) -> (Arc<SpaceEntry>, Arc<Mutex<Vec<Url>>>) {
        let space_id = SpaceId::from(bytes::Bytes::copy_from_slice(space));
        let (tx, dials) = recording_transport();
        let entry = SpaceEntry::new(
            space_id.clone(),
            space_fingerprint(&space_id),
            h.daemon.clone(),
            tx,
            4,
        );
        h.registry
            .lock()
            .unwrap()
            .insert(*entry.fingerprint(), entry.clone());
        (entry, dials)
    }

    async fn settle() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn records_are_routed_to_the_space_they_commit_to() {
        let h = harness();
        let (a, dials_a) = join(&h, b"space-a");
        let (b, dials_b) = join(&h, b"space-b");

        h.daemon
            .deliver(resolved_peer("peer-1", a.fingerprint(), PEER_A));
        h.daemon
            .deliver(resolved_peer("peer-2", b.fingerprint(), PEER_B));
        h.daemon
            .deliver(resolved_peer("peer-3", &[9u8; 32], PEER_A));
        h.daemon.deliver(resolved("peer-4", &[("url", PEER_A)]));
        settle().await;

        assert_eq!(*dials_a.lock().unwrap(), vec![url(PEER_A)]);
        assert_eq!(*dials_b.lock().unwrap(), vec![url(PEER_B)]);
    }

    #[tokio::test]
    async fn our_own_record_is_not_dialled() {
        let h = harness();
        let (a, dials) = join(&h, b"space-a");
        a.advertise(&url(SELF_URL)).unwrap();

        // Our record heard back by name, and an echo of our URL under
        // another name.
        let own_instance = a.fullname().replace(SERVICE_TYPE, "");
        h.daemon.deliver(resolved_peer(
            own_instance.trim_end_matches('.'),
            a.fingerprint(),
            PEER_A,
        ));
        h.daemon
            .deliver(resolved_peer("echo", a.fingerprint(), SELF_URL));
        settle().await;
        assert!(dials.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_withdrawn_record_is_forgotten() {
        let h = harness();
        let (a, dials) = join(&h, b"space-a");

        h.daemon
            .deliver(resolved_peer("peer-1", a.fingerprint(), PEER_A));
        settle().await;
        assert_eq!(*dials.lock().unwrap(), vec![url(PEER_A)]);

        h.daemon.deliver(removed("peer-1"));
        settle().await;
        a.redial_unconnected().await;
        settle().await;
        assert_eq!(dials.lock().unwrap().len(), 1, "nothing left to redial");

        // Heard again, it is new again.
        h.daemon
            .deliver(resolved_peer("peer-1", a.fingerprint(), PEER_A));
        settle().await;
        assert_eq!(dials.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_record_that_changes_its_url_forgets_the_old_one() {
        let h = harness();
        let (a, dials) = join(&h, b"space-a");

        h.daemon
            .deliver(resolved_peer("peer-1", a.fingerprint(), PEER_A));
        h.daemon
            .deliver(resolved_peer("peer-1", a.fingerprint(), PEER_B));
        settle().await;
        dials.lock().unwrap().clear();

        a.redial_unconnected().await;
        settle().await;
        assert_eq!(*dials.lock().unwrap(), vec![url(PEER_B)]);
    }

    #[tokio::test]
    async fn a_space_that_left_no_longer_receives_records() {
        let h = harness();
        let (a, dials_a) = join(&h, b"space-a");
        let (b, dials_b) = join(&h, b"space-b");
        h.registry.lock().unwrap().remove(a.fingerprint());

        h.daemon
            .deliver(resolved_peer("peer-1", a.fingerprint(), PEER_A));
        h.daemon
            .deliver(resolved_peer("peer-2", b.fingerprint(), PEER_B));
        settle().await;

        assert!(dials_a.lock().unwrap().is_empty());
        assert_eq!(*dials_b.lock().unwrap(), vec![url(PEER_B)]);
    }
}
