//! The browse loop: route each announcement heard on the LAN to the space
//! it belongs to.
//!
//! One loop serves every space on the shared daemon. A resolved record is
//! matched by the fingerprint string it carries against the registry of
//! spaces this node is in, and handed to that space's entry, which decides
//! whether to dial. Nothing here touches the peer store: a dial makes the
//! transport open a connection and run its preflight, and from there the
//! access module proves that both sides know the space's secret before any
//! agent info changes hands.
//!
//! mDNS delivers a record once and stays silent while it is unchanged, so
//! a space that joins after a LAN peer's record was resolved would never
//! hear it. The loop therefore keeps the latest record of every name it
//! has heard, for every fingerprint, and a joining space is replayed the
//! ones carrying its own.

use crate::discovery;
use crate::space::SpaceEntry;
use mdns_sd::ServiceEvent;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::trace;

/// The most records remembered for replay. Past this, the record heard
/// longest ago makes room.
pub const MAX_CACHED_RECORDS: usize = 1024;

/// The latest resolution of one record name.
#[derive(Debug)]
struct CachedRecord {
    /// The `spacefp` TXT value as announced.
    fp: String,
    /// The `url` TXT value as announced.
    url: String,
    /// Position in the order of hearing, for eviction.
    seq: u64,
}

/// The spaces sharing the daemon, keyed by the fingerprint string they
/// announce and match on, and the records heard for every fingerprint.
#[derive(Debug, Default)]
pub struct BrowseState {
    spaces: HashMap<String, Arc<SpaceEntry>>,
    cache: HashMap<String, CachedRecord>,
    next_seq: u64,
}

/// [`BrowseState`] as shared between the browse loop and the registry.
pub type SharedBrowseState = Arc<Mutex<BrowseState>>;

impl BrowseState {
    /// Register `entry` for its fingerprint, replaying every record heard
    /// so far that carries it. Returns the entry previously registered for
    /// the same fingerprint, if any.
    pub fn register(
        &mut self,
        entry: Arc<SpaceEntry>,
    ) -> Option<Arc<SpaceEntry>> {
        let key = entry.fingerprint().encode();
        for (fullname, record) in &self.cache {
            if record.fp != key {
                continue;
            }
            if let Some(url) = discovery::parse_peer_url(&record.url) {
                trace!(fullname, %url, "mdns: replaying cached record to a joining space");
                entry.record_resolved(fullname, url);
            }
        }
        self.spaces.insert(key, entry)
    }

    /// Stop routing to `entry`. A slot taken over by a newer entry for the
    /// same fingerprint is left alone.
    pub fn unregister(&mut self, entry: &Arc<SpaceEntry>) {
        let key = entry.fingerprint().encode();
        if self
            .spaces
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, entry))
        {
            self.spaces.remove(&key);
        }
    }

    /// How many spaces are registered.
    pub fn space_count(&self) -> usize {
        self.spaces.len()
    }

    fn space(&self, fp: &str) -> Option<Arc<SpaceEntry>> {
        self.spaces.get(fp).cloned()
    }

    /// Remember the latest resolution of `fullname`. Returns the space
    /// that previously owned the name, when the record moved to another
    /// fingerprint and that space must forget it.
    fn resolved(
        &mut self,
        fullname: &str,
        fp: &str,
        url: &str,
    ) -> Option<Arc<SpaceEntry>> {
        let seq = self.next_seq;
        self.next_seq += 1;
        let previous = self.cache.insert(
            fullname.to_string(),
            CachedRecord {
                fp: fp.to_string(),
                url: url.to_string(),
                seq,
            },
        );
        self.evict_past_cap();
        previous
            .filter(|previous| previous.fp != fp)
            .and_then(|previous| self.space(&previous.fp))
    }

    /// Forget `fullname`, returning the space it was routed to.
    fn removed(&mut self, fullname: &str) -> Option<Arc<SpaceEntry>> {
        let record = self.cache.remove(fullname)?;
        self.space(&record.fp)
    }

    fn evict_past_cap(&mut self) {
        while self.cache.len() > MAX_CACHED_RECORDS {
            let Some(oldest) = self
                .cache
                .iter()
                .min_by_key(|(_, record)| record.seq)
                .map(|(fullname, _)| fullname.clone())
            else {
                break;
            };
            self.cache.remove(&oldest);
        }
    }

    /// How many records are cached.
    #[cfg(test)]
    pub fn cached_count(&self) -> usize {
        self.cache.len()
    }
}

/// Consume browse events until the source closes.
pub async fn browse_loop(
    rx: flume::Receiver<ServiceEvent>,
    state: SharedBrowseState,
) {
    while let Ok(event) = rx.recv_async().await {
        match event {
            ServiceEvent::ServiceResolved(svc) => {
                let Some((fp, url)) = discovery::record_fields(&svc) else {
                    trace!(fullname = %svc.fullname, "mdns: ignoring record without spacefp and url");
                    continue;
                };
                let (lost_by, entry) = {
                    let mut state = state.lock().expect("poison");
                    (state.resolved(&svc.fullname, fp, url), state.space(fp))
                };
                if let Some(lost_by) = lost_by {
                    lost_by.record_removed(&svc.fullname);
                }
                let Some(entry) = entry else {
                    trace!(fullname = %svc.fullname, "mdns: record for a space this node is not in");
                    continue;
                };
                let Some(url) = discovery::parse_peer_url(url) else {
                    trace!(fullname = %svc.fullname, "mdns: record without a peer url to dial");
                    continue;
                };
                trace!(fullname = %svc.fullname, %url, "mdns: resolved record");
                entry.record_resolved(&svc.fullname, url);
            }
            ServiceEvent::ServiceRemoved(_, fullname) => {
                let entry = state.lock().expect("poison").removed(&fullname);
                if let Some(entry) = entry {
                    entry.record_removed(&fullname);
                }
            }
            _ => {}
        }
    }
    trace!("mdns: browse event source closed, browse loop ending");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::Daemon as _;
    use crate::test_support::*;
    use kitsune2_api::Url;

    const PEER_A: &str = "ws://a.test:80/peera";
    const PEER_B: &str = "ws://b.test:80/peerb";
    const SELF_URL: &str = "ws://self.test:80/selfpeer";

    struct Harness {
        daemon: Arc<FakeDaemon>,
        state: SharedBrowseState,
        _loop_task: tokio::task::JoinHandle<()>,
    }

    fn harness() -> Harness {
        let daemon = FakeDaemon::new();
        let state: SharedBrowseState = Default::default();
        let rx = daemon.browse().unwrap();
        let loop_task = tokio::spawn(browse_loop(rx, state.clone()));
        Harness {
            daemon,
            state,
            _loop_task: loop_task,
        }
    }

    /// Register a space that already has a URL of its own, so that it
    /// dials what it hears.
    fn join(h: &Harness, space: &[u8]) -> (Arc<SpaceEntry>, Dials) {
        let (tx, dials) = recording_transport(vec![]);
        let entry = SpaceEntry::new(
            space_id(space),
            test_fp(space),
            h.daemon.clone(),
            tx,
            4,
        );
        entry
            .advertise(&url(&format!("ws://{}.test:80/self", space.len())))
            .unwrap();
        h.state.lock().unwrap().register(entry.clone());
        (entry, dials)
    }

    #[tokio::test]
    async fn records_are_routed_to_the_space_they_commit_to() {
        let h = harness();
        let (a, dials_a) = join(&h, b"space-a");
        let (b, dials_b) = join(&h, b"space-bb");

        h.daemon
            .deliver(resolved_peer("peer-1", a.fingerprint(), PEER_A));
        h.daemon
            .deliver(resolved_peer("peer-2", b.fingerprint(), PEER_B));
        h.daemon
            .deliver(resolved_peer("peer-3", &test_fp(&[9u8; 32]), PEER_A));
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
        a.reconcile_from_transport().await;
        settle().await;
        assert_eq!(dials.lock().unwrap().len(), 1, "nothing left to redial");
        assert_eq!(h.state.lock().unwrap().cached_count(), 0);

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

        a.reconcile_from_transport().await;
        settle().await;
        assert_eq!(*dials.lock().unwrap(), vec![url(PEER_B)]);
    }

    /// A record name that re-resolves under another fingerprint has left
    /// the space that recorded it, whatever the new fingerprint says.
    #[tokio::test]
    async fn a_record_that_changes_fingerprint_moves_between_spaces() {
        let h = harness();
        let (a, dials_a) = join(&h, b"space-a");
        let (b, dials_b) = join(&h, b"space-bb");

        h.daemon
            .deliver(resolved_peer("peer-1", a.fingerprint(), PEER_A));
        settle().await;
        assert_eq!(*dials_a.lock().unwrap(), vec![url(PEER_A)]);

        h.daemon
            .deliver(resolved_peer("peer-1", b.fingerprint(), PEER_A));
        settle().await;
        assert_eq!(*dials_b.lock().unwrap(), vec![url(PEER_A)]);
        dials_a.lock().unwrap().clear();

        // A has nothing left to redial; the removal reaches B only.
        a.reconcile_from_transport().await;
        settle().await;
        assert!(dials_a.lock().unwrap().is_empty());

        h.daemon.deliver(removed("peer-1"));
        settle().await;
        b.reconcile_from_transport().await;
        settle().await;
        assert_eq!(dials_b.lock().unwrap().len(), 1, "B forgot the record");
    }

    #[tokio::test]
    async fn a_space_that_left_no_longer_receives_records() {
        let h = harness();
        let (a, dials_a) = join(&h, b"space-a");
        let (b, dials_b) = join(&h, b"space-bb");
        h.state.lock().unwrap().unregister(&a);

        h.daemon
            .deliver(resolved_peer("peer-1", a.fingerprint(), PEER_A));
        h.daemon
            .deliver(resolved_peer("peer-2", b.fingerprint(), PEER_B));
        settle().await;

        assert!(dials_a.lock().unwrap().is_empty());
        assert_eq!(*dials_b.lock().unwrap(), vec![url(PEER_B)]);
    }

    /// A space created after a LAN peer's record was resolved is replayed
    /// that record on joining, since mDNS will not deliver it again.
    #[tokio::test]
    async fn a_space_joining_later_is_replayed_the_records_for_it() {
        let h = harness();
        let fp = test_fp(b"space-a");
        h.daemon.deliver(resolved_peer("peer-1", &fp, PEER_A));
        h.daemon
            .deliver(resolved_peer("peer-2", &test_fp(b"other"), PEER_B));
        settle().await;
        assert_eq!(h.state.lock().unwrap().cached_count(), 2);

        let (_a, dials_a) = join(&h, b"space-a");
        settle().await;
        assert_eq!(*dials_a.lock().unwrap(), vec![url(PEER_A)]);
    }

    #[test]
    fn the_record_heard_longest_ago_makes_room_past_the_cap() {
        let mut state = BrowseState::default();
        for i in 0..MAX_CACHED_RECORDS {
            state.resolved(&format!("r{i}"), "fp", "ws://x.test:80/x");
        }
        state.resolved("r0", "fp", "ws://x.test:80/x");
        state.resolved("extra", "fp", "ws://x.test:80/x");
        assert_eq!(state.cached_count(), MAX_CACHED_RECORDS);
        assert!(state.cache.contains_key("extra"));
        assert!(state.cache.contains_key("r0"), "re-heard");
        assert!(!state.cache.contains_key("r1"), "the oldest made room");
        let _: Option<Url> = None;
    }
}
