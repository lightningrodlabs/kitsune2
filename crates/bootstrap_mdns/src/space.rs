//! One space's share of the shared mDNS presence: its record on the
//! daemon, and the peers the LAN announces for it.

use crate::dials::Announcements;
use crate::discovery::{self, DynDaemon};
use crate::fingerprint::SpaceFingerprint;
use kitsune2_api::{DynTransport, K2Result, SpaceId, Url};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tracing::{debug, trace};

/// A space registered with the shared daemon.
#[derive(Debug)]
pub struct SpaceEntry {
    space_id: SpaceId,
    fp: SpaceFingerprint,
    instance: String,
    daemon: DynDaemon,
    tx: DynTransport,
    /// The peer URL currently announced for this space, if any.
    advertised: Mutex<Option<Url>>,
    announced: Announcements,
}

impl SpaceEntry {
    /// An entry for `space_id` announcing under a fresh random instance
    /// name on `daemon`, with at most `max_concurrent_dials` in flight.
    pub fn new(
        space_id: SpaceId,
        fp: SpaceFingerprint,
        daemon: DynDaemon,
        tx: DynTransport,
        max_concurrent_dials: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            space_id,
            fp,
            instance: discovery::random_name(),
            daemon,
            tx,
            advertised: Mutex::new(None),
            announced: Announcements::new(max_concurrent_dials),
        })
    }

    /// The space this entry belongs to.
    pub fn space_id(&self) -> &SpaceId {
        &self.space_id
    }

    /// The commitment this space announces and matches on.
    pub fn fingerprint(&self) -> &SpaceFingerprint {
        &self.fp
    }

    /// Full mDNS name of this space's own record.
    pub fn fullname(&self) -> String {
        discovery::fullname(self.daemon.service_type(), &self.instance)
    }

    /// The peer URL currently announced for this space.
    pub fn advertised(&self) -> Option<Url> {
        self.advertised.lock().expect("poison").clone()
    }

    /// Announce `url` as this node's peer URL for the space, replacing any
    /// earlier announcement. Returns `true` when this is the first URL
    /// the space has had, which is the moment it becomes worth dialling
    /// from. Announcing the URL already advertised is a no-op.
    pub fn advertise(&self, url: &Url) -> K2Result<bool> {
        let mut advertised = self.advertised.lock().expect("poison");
        if advertised.as_ref() == Some(url) {
            return Ok(false);
        }
        let first = advertised.is_none();
        if !first {
            // Commands are handled by the daemon in order, so the fresh
            // record is registered only after the stale one is gone.
            self.daemon.unregister(&self.instance)?;
            *advertised = None;
        }
        let txt = discovery::record_txt(&self.fp, url);
        let txt: Vec<(&str, &str)> =
            txt.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        self.daemon.register(&self.instance, &txt)?;
        *advertised = Some(url.clone());
        drop(advertised);
        // An echo of our own URL under another name may have been
        // recorded before we knew it was ours.
        self.announced.forget_url(url);
        Ok(first)
    }

    /// Withdraw this space's record, if one is announced.
    pub fn withdraw(&self) {
        let mut advertised = self.advertised.lock().expect("poison");
        if advertised.take().is_some()
            && let Err(err) = self.daemon.unregister(&self.instance)
        {
            debug!(?err, fullname = %self.fullname(), "mdns: failed to withdraw record");
        }
    }

    /// Whether a record with this name or URL is this node's own
    /// announcement for the space.
    pub fn is_own_record(&self, fullname: &str, url: &Url) -> bool {
        fullname == self.fullname() || self.advertised().as_ref() == Some(url)
    }

    /// Whether this node can be dialled back yet. Until a local agent has
    /// a URL the space has no handler registered and no agents to
    /// preflight with, so a dial would fail on our own side.
    fn is_dialable(&self) -> bool {
        self.advertised.lock().expect("poison").is_some()
    }

    /// The LAN resolved the record `fullname` naming `url` for this space.
    /// A URL heard for the first time is dialled right away; one heard
    /// before waits for the next reconciliation, which only dials it if it
    /// is still unconnected.
    pub fn record_resolved(&self, fullname: &str, url: Url) {
        if self.is_own_record(fullname, &url) {
            return;
        }
        if !self.announced.record_resolved(fullname, url.clone()) {
            trace!(%url, fullname, "mdns: known peer re-announced");
            return;
        }
        if !self.is_dialable() {
            debug!(%url, space = ?self.space_id, "mdns: discovered peer, dialling once we have a url");
            return;
        }
        debug!(%url, space = ?self.space_id, "mdns: discovered peer, dialling");
        self.announced.try_dial(&self.tx, &self.space_id, url);
    }

    /// The LAN withdrew the record `fullname`.
    pub fn record_removed(&self, fullname: &str) {
        trace!(fullname, space = ?self.space_id, "mdns: peer announcement withdrawn");
        self.announced.record_removed(fullname);
    }

    /// Dial every announced peer that `connected` does not list.
    pub fn reconcile(&self, connected: &HashSet<Url>) {
        if !self.is_dialable() {
            return;
        }
        let own = self.advertised();
        for url in self.announced.urls() {
            if connected.contains(&url) || own.as_ref() == Some(&url) {
                continue;
            }
            trace!(%url, space = ?self.space_id, "mdns: announced peer not connected, redialling");
            if !self.announced.try_dial(&self.tx, &self.space_id, url) {
                break;
            }
        }
    }

    /// Reconcile against what the transport reports as connected.
    pub async fn reconcile_from_transport(&self) {
        match self.tx.get_connected_peers().await {
            Ok(peers) => self.reconcile(&peers.into_iter().collect()),
            Err(err) => debug!(
                ?err,
                "mdns: could not list connected peers, skipping redial round"
            ),
        }
    }

    /// Reconcile in a task of its own, for callers that cannot await.
    pub fn reconcile_soon(self: &Arc<Self>) {
        let entry = self.clone();
        tokio::spawn(async move { entry.reconcile_from_transport().await });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;

    const PEER_A: &str = "ws://a.test:80/peera";
    const PEER_B: &str = "ws://b.test:80/peerb";
    const SELF_URL: &str = "ws://self.test:80/selfpeer";

    fn space() -> SpaceId {
        space_id(b"space")
    }

    fn entry(tx: DynTransport, cap: usize) -> Arc<SpaceEntry> {
        SpaceEntry::new(space(), test_fp(b"space"), FakeDaemon::new(), tx, cap)
    }

    /// An entry that has a URL of its own and can therefore dial.
    fn dialable_entry(tx: DynTransport, cap: usize) -> Arc<SpaceEntry> {
        let entry = entry(tx, cap);
        entry.advertise(&url(SELF_URL)).unwrap();
        entry
    }

    #[tokio::test]
    async fn a_new_url_is_dialled_once_until_reconciled() {
        let (tx, dials) = recording_transport(vec![]);
        let entry = dialable_entry(tx, 4);

        entry.record_resolved("r1", url(PEER_A));
        entry.record_resolved("r1", url(PEER_A));
        entry.record_resolved("r2", url(PEER_A));
        settle().await;
        assert_eq!(*dials.lock().unwrap(), vec![url(PEER_A)]);

        entry.reconcile_from_transport().await;
        settle().await;
        assert_eq!(*dials.lock().unwrap(), vec![url(PEER_A), url(PEER_A)]);
    }

    /// Before the space has a URL of its own it is not dialable back and
    /// cannot even preflight, so what the LAN announces is only recorded;
    /// the first URL triggers the dials.
    #[tokio::test]
    async fn nothing_is_dialled_before_the_space_has_a_url() {
        let (tx, dials) = recording_transport(vec![]);
        let entry = entry(tx, 4);

        entry.record_resolved("r1", url(PEER_A));
        entry.reconcile_from_transport().await;
        settle().await;
        assert!(dials.lock().unwrap().is_empty());

        assert!(entry.advertise(&url(SELF_URL)).unwrap(), "first url");
        entry.reconcile_soon();
        settle().await;
        assert_eq!(*dials.lock().unwrap(), vec![url(PEER_A)]);

        assert!(!entry.advertise(&url(PEER_B)).unwrap(), "a later url");
    }

    #[tokio::test]
    async fn reconciliation_skips_connected_and_withdrawn_peers() {
        let (tx, dials) = recording_transport(vec![url(PEER_A)]);
        let entry = dialable_entry(tx, 4);

        entry.record_resolved("r1", url(PEER_A));
        entry.record_resolved("r2", url(PEER_B));
        settle().await;
        dials.lock().unwrap().clear();

        // A is connected, B is not: only B is redialled.
        entry.reconcile_from_transport().await;
        settle().await;
        assert_eq!(*dials.lock().unwrap(), vec![url(PEER_B)]);
        dials.lock().unwrap().clear();

        // Once the LAN withdraws B, nothing is left to redial.
        entry.record_removed("r2");
        entry.reconcile_from_transport().await;
        settle().await;
        assert!(dials.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_url_skipped_at_the_cap_is_picked_up_by_the_next_round() {
        let release = Arc::new(tokio::sync::Notify::new());
        // A counts as connected once dialled, so every round has only B
        // left to dial.
        let (tx, dials) =
            blocking_transport(release.clone(), vec![url(PEER_A)]);
        let entry = dialable_entry(tx, 1);

        entry.record_resolved("r1", url(PEER_A));
        entry.record_resolved("r2", url(PEER_B));
        settle().await;
        assert_eq!(*dials.lock().unwrap(), vec![url(PEER_A)]);

        // The round finds the slot still taken by A's dial.
        entry.reconcile_from_transport().await;
        settle().await;
        assert_eq!(dials.lock().unwrap().len(), 1);

        release.notify_one();
        settle().await;
        entry.reconcile_from_transport().await;
        settle().await;
        assert_eq!(dials.lock().unwrap().len(), 2);
        assert!(dials.lock().unwrap().contains(&url(PEER_B)));
    }

    #[tokio::test]
    async fn our_own_record_and_url_are_never_dialled() {
        let (tx, dials) = recording_transport(vec![]);
        let entry = entry(tx, 4);

        // An echo of our URL recorded before we knew it was ours.
        entry.record_resolved("echo", url(SELF_URL));
        entry.advertise(&url(SELF_URL)).unwrap();
        entry.record_resolved(&entry.fullname(), url(PEER_A));
        entry.record_resolved("echo-again", url(SELF_URL));
        entry.reconcile_from_transport().await;
        settle().await;
        assert!(dials.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn advertise_registers_and_replaces_the_record() {
        let daemon = FakeDaemon::new();
        let (tx, _) = recording_transport(vec![]);
        let entry =
            SpaceEntry::new(space(), test_fp(b"space"), daemon.clone(), tx, 1);
        assert!(entry.advertised().is_none());

        entry.advertise(&url(PEER_A)).unwrap();
        entry.advertise(&url(PEER_A)).unwrap();
        assert_eq!(entry.advertised(), Some(url(PEER_A)));
        assert_eq!(daemon.registered.lock().unwrap().len(), 1);
        assert!(daemon.unregistered.lock().unwrap().is_empty());
        assert!(entry.is_own_record("someone-else", &url(PEER_A)));
        assert!(entry.is_own_record(&entry.fullname(), &url(PEER_B)));
        assert!(!entry.is_own_record("someone-else", &url(PEER_B)));

        entry.advertise(&url(PEER_B)).unwrap();
        assert_eq!(daemon.registered.lock().unwrap().len(), 2);
        assert_eq!(daemon.unregistered.lock().unwrap().len(), 1);

        entry.withdraw();
        assert!(entry.advertised().is_none());
        assert_eq!(daemon.unregistered.lock().unwrap().len(), 2);
    }
}
