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
//! ones carrying its own. That cache serves replay only: which space
//! holds a record — and must forget it when the LAN withdraws it — is a
//! question for the spaces themselves, so that a record the cache made
//! room for is still forgotten by its owner.

use crate::cap::evict_past_cap;
use crate::discovery;
use crate::space::SpaceEntry;
use mdns_sd::ServiceEvent;
use std::cmp::Reverse;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::trace;

/// The most records remembered for replay per fingerprint. Past this, the
/// record heard longest ago for that fingerprint makes room, so what the
/// LAN says about other spaces never costs a space its own records.
pub const MAX_RECORDS_PER_FINGERPRINT: usize = 64;

/// The most fingerprints remembered for replay. Past this, the
/// fingerprint heard from longest ago makes room, unless a space of this
/// node is registered for it.
pub const MAX_CACHED_FINGERPRINTS: usize = 256;

/// The latest resolution of one record name.
#[derive(Debug)]
struct CachedRecord {
    /// The `url` TXT value as announced.
    url: String,
    /// Position in the order of hearing, for eviction.
    seq: u64,
}

/// The records heard for one fingerprint, by name.
type Bucket = HashMap<String, CachedRecord>;

/// The spaces sharing the daemon, keyed by the fingerprint string they
/// announce and match on, and the records heard for every fingerprint.
#[derive(Debug, Default)]
pub struct BrowseState {
    spaces: HashMap<String, Arc<SpaceEntry>>,
    cache: HashMap<String, Bucket>,
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
        for (fullname, record) in self.cache.get(&key).into_iter().flatten() {
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
    #[cfg(test)]
    pub fn space_count(&self) -> usize {
        self.spaces.len()
    }

    /// Every registered space.
    pub fn entries(&self) -> Vec<Arc<SpaceEntry>> {
        self.spaces.values().cloned().collect()
    }

    fn space(&self, fp: &str) -> Option<Arc<SpaceEntry>> {
        self.spaces.get(fp).cloned()
    }

    /// Remember the latest resolution of `fullname` under `fp`.
    fn resolved(&mut self, fullname: &str, fp: &str, url: &str) {
        let seq = self.next_seq;
        self.next_seq += 1;
        // A name that moved to another fingerprint is one record, not two.
        self.forget(fullname, Some(fp));
        let bucket = self.cache.entry(fp.to_string()).or_default();
        bucket.insert(
            fullname.to_string(),
            CachedRecord {
                url: url.to_string(),
                seq,
            },
        );
        evict_past_cap(bucket, MAX_RECORDS_PER_FINGERPRINT, |_, record| {
            Some(Reverse(record.seq))
        });
        let registered = &self.spaces;
        evict_past_cap(
            &mut self.cache,
            MAX_CACHED_FINGERPRINTS,
            |fp, bucket| {
                if registered.contains_key(fp) {
                    return None;
                }
                let last_heard = bucket.values().map(|r| r.seq).max();
                Some(Reverse(last_heard))
            },
        );
    }

    /// Forget `fullname` wherever it is cached.
    fn removed(&mut self, fullname: &str) {
        self.forget(fullname, None);
    }

    /// Drop `fullname` from every bucket but `except`, and the buckets it
    /// leaves empty.
    fn forget(&mut self, fullname: &str, except: Option<&str>) {
        self.cache.retain(|fp, bucket| {
            if except != Some(fp.as_str()) {
                bucket.remove(fullname);
            }
            !bucket.is_empty()
        });
    }

    /// How many records are cached, over every fingerprint.
    #[cfg(test)]
    pub fn cached_count(&self) -> usize {
        self.cache.values().map(Bucket::len).sum()
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
                let (entry, others) = {
                    let mut state = state.lock().expect("poison");
                    state.resolved(&svc.fullname, fp, url);
                    (state.space(fp), state.entries())
                };
                // A name that re-resolves under another fingerprint has
                // left whichever space held it, whatever the new
                // fingerprint says.
                for other in others {
                    let is_owner = entry
                        .as_ref()
                        .is_some_and(|entry| Arc::ptr_eq(entry, &other));
                    if !is_owner && other.has_record(&svc.fullname) {
                        other.record_removed(&svc.fullname);
                    }
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
                let entries = {
                    let mut state = state.lock().expect("poison");
                    state.removed(&fullname);
                    state.entries()
                };
                for entry in entries {
                    if entry.has_record(&fullname) {
                        entry.record_removed(&fullname);
                    }
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

    fn fill(state: &mut BrowseState, fp: &str, prefix: &str, n: usize) {
        for i in 0..n {
            state.resolved(&format!("{prefix}{i}"), fp, "ws://x.test:80/x");
        }
    }

    fn cached(state: &BrowseState, fp: &str, fullname: &str) -> bool {
        state
            .cache
            .get(fp)
            .is_some_and(|bucket| bucket.contains_key(fullname))
    }

    #[test]
    fn each_fingerprint_keeps_its_own_records_past_its_cap() {
        let mut state = BrowseState::default();
        state.resolved("mine", "fp-a", "ws://x.test:80/x");
        fill(&mut state, "fp-b", "r", MAX_RECORDS_PER_FINGERPRINT);
        state.resolved("r0", "fp-b", "ws://x.test:80/x");
        state.resolved("extra", "fp-b", "ws://x.test:80/x");

        assert!(
            cached(&state, "fp-a", "mine"),
            "another fingerprint's flood"
        );
        assert_eq!(state.cache["fp-b"].len(), MAX_RECORDS_PER_FINGERPRINT);
        assert!(cached(&state, "fp-b", "extra"));
        assert!(cached(&state, "fp-b", "r0"), "re-heard");
        assert!(!cached(&state, "fp-b", "r1"), "the oldest made room");
    }

    /// A flood of fingerprints this node is not in must not evict the
    /// records of a space it is in; among the rest the fingerprint heard
    /// from longest ago goes first.
    #[tokio::test]
    async fn a_registered_fingerprint_survives_a_flood_of_others() {
        let h = harness();
        let (a, _) = join(&h, b"space-a");
        let key = a.fingerprint().encode();
        let mut state = h.state.lock().unwrap();
        state.resolved("mine", &key, "ws://x.test:80/x");
        state.resolved("early", "fp-early", "ws://x.test:80/x");
        for i in 0..MAX_CACHED_FINGERPRINTS {
            state.resolved(
                &format!("flood-{i}"),
                &format!("fp-{i}"),
                "ws://x.test:80/x",
            );
        }

        assert_eq!(state.cache.len(), MAX_CACHED_FINGERPRINTS);
        assert!(cached(&state, &key, "mine"));
        assert!(!state.cache.contains_key("fp-early"));
    }

    /// The cache serves replay; which space must forget a withdrawn
    /// record is asked of the spaces, so a record the cache made room for
    /// is still forgotten by the space that holds it.
    #[tokio::test]
    async fn a_record_evicted_from_the_cache_is_still_forgotten_on_removal() {
        let h = harness();
        let (a, dials) = join(&h, b"space-a");
        for i in 0..=MAX_RECORDS_PER_FINGERPRINT {
            h.daemon.deliver(resolved_peer(
                &format!("peer-{i}"),
                a.fingerprint(),
                &format!("ws://p{i}.test:80/peer{i}"),
            ));
        }
        let last = discovery::fullname(
            SERVICE_TYPE,
            &format!("peer-{MAX_RECORDS_PER_FINGERPRINT}"),
        );
        wait_until(|| a.has_record(&last)).await;
        assert!(!dials.lock().unwrap().is_empty());
        let first = discovery::fullname(SERVICE_TYPE, "peer-0");
        assert!(a.has_record(&first));
        assert!(
            !cached(
                &h.state.lock().unwrap(),
                &a.fingerprint().encode(),
                &first
            ),
            "the cache made room for the newest record"
        );

        h.daemon.deliver(removed("peer-0"));
        wait_until(|| !a.has_record(&first)).await;
    }
}
