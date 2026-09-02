//! One space's share of the process-wide mDNS presence: its record on the
//! shared daemon, and the peers the LAN announces for it.

use crate::dials::DialState;
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
    fullname: String,
    daemon: DynDaemon,
    tx: DynTransport,
    /// The peer URL currently announced for this space, if any.
    advertised: Mutex<Option<Url>>,
    dials: DialState,
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
        let instance = discovery::random_name();
        let fullname = discovery::fullname(daemon.as_ref(), &instance);
        Arc::new(Self {
            space_id,
            fp,
            instance,
            fullname,
            daemon,
            tx,
            advertised: Mutex::new(None),
            dials: DialState::new(max_concurrent_dials),
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
    pub fn fullname(&self) -> &str {
        &self.fullname
    }

    /// The peer URL currently announced for this space.
    pub fn advertised(&self) -> Option<Url> {
        self.advertised.lock().expect("poison").clone()
    }

    /// Announce `url` as this node's peer URL for the space, replacing any
    /// earlier announcement. Announcing the URL already advertised is a
    /// no-op.
    pub fn advertise(&self, url: &Url) -> K2Result<()> {
        let mut advertised = self.advertised.lock().expect("poison");
        if advertised.as_ref() == Some(url) {
            return Ok(());
        }
        if advertised.is_some() {
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
        Ok(())
    }

    /// Withdraw this space's record, if one is announced.
    pub fn withdraw(&self) {
        let mut advertised = self.advertised.lock().expect("poison");
        if advertised.take().is_some()
            && let Err(err) = self.daemon.unregister(&self.instance)
        {
            debug!(?err, fullname = %self.fullname, "mdns: failed to withdraw record");
        }
    }

    /// Whether a record with this name or URL is this node's own
    /// announcement for the space.
    pub fn is_own_record(&self, fullname: &str, url: &Url) -> bool {
        fullname == self.fullname || self.advertised().as_ref() == Some(url)
    }

    /// The LAN announced `url` for this space. A URL heard for the first
    /// time is dialled right away; one heard before waits for the next
    /// reconciliation, which only dials it if it is still unconnected.
    pub fn record_resolved(&self, url: Url) {
        if !self.dials.discovered(&url) {
            trace!(%url, "mdns: known peer re-announced");
            return;
        }
        debug!(%url, space = ?self.space_id, "mdns: discovered peer, dialling");
        self.dials.try_dial(&self.tx, &self.space_id, url);
    }

    /// The LAN withdrew its announcement of `url` for this space.
    pub fn record_removed(&self, url: &Url) {
        trace!(%url, space = ?self.space_id, "mdns: peer announcement withdrawn");
        self.dials.forget(url);
    }

    /// Dial every announced peer the transport is not connected to.
    pub async fn redial_unconnected(&self) {
        let connected: HashSet<Url> = match self.tx.get_connected_peers().await
        {
            Ok(peers) => peers.into_iter().collect(),
            Err(err) => {
                debug!(
                    ?err,
                    "mdns: could not list connected peers, skipping redial round"
                );
                return;
            }
        };
        for url in self.dials.discovered_urls() {
            if connected.contains(&url) {
                continue;
            }
            trace!(%url, space = ?self.space_id, "mdns: announced peer not connected, redialling");
            if !self.dials.try_dial(&self.tx, &self.space_id, url) {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::test_support::FakeDaemon;
    use crate::fingerprint::space_fingerprint;
    use kitsune2_api::MockTransport;
    use std::time::Duration;

    const PEER_A: &str = "ws://a.test:80/peera";
    const PEER_B: &str = "ws://b.test:80/peerb";

    fn url(s: &str) -> Url {
        Url::from_str(s).unwrap()
    }

    fn space() -> SpaceId {
        SpaceId::from(bytes::Bytes::from_static(b"space"))
    }

    /// A transport recording its dials and reporting `connected` as its
    /// open connections.
    fn transport(connected: Vec<Url>) -> (DynTransport, Arc<Mutex<Vec<Url>>>) {
        let dials: Arc<Mutex<Vec<Url>>> = Arc::new(Mutex::new(Vec::new()));
        let record = dials.clone();
        let mut mock = MockTransport::new();
        mock.expect_dial().returning(move |_space, url| {
            record.lock().unwrap().push(url);
            Box::pin(async { Ok(()) })
        });
        mock.expect_get_connected_peers().returning(move || {
            let connected = connected.clone();
            Box::pin(async move { Ok(connected) })
        });
        (Arc::new(mock), dials)
    }

    fn entry(tx: DynTransport, cap: usize) -> Arc<SpaceEntry> {
        SpaceEntry::new(
            space(),
            space_fingerprint(&space()),
            FakeDaemon::new(),
            tx,
            cap,
        )
    }

    async fn settle() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn a_new_url_is_dialled_once_until_reconciled() {
        let (tx, dials) = transport(vec![]);
        let entry = entry(tx, 4);

        entry.record_resolved(url(PEER_A));
        entry.record_resolved(url(PEER_A));
        entry.record_resolved(url(PEER_A));
        settle().await;
        assert_eq!(*dials.lock().unwrap(), vec![url(PEER_A)]);

        entry.redial_unconnected().await;
        settle().await;
        assert_eq!(*dials.lock().unwrap(), vec![url(PEER_A), url(PEER_A)]);
    }

    #[tokio::test]
    async fn reconciliation_skips_connected_and_withdrawn_peers() {
        let (tx, dials) = transport(vec![url(PEER_A)]);
        let entry = entry(tx, 4);

        entry.record_resolved(url(PEER_A));
        entry.record_resolved(url(PEER_B));
        settle().await;
        dials.lock().unwrap().clear();

        // A is connected, B is not: only B is redialled.
        entry.redial_unconnected().await;
        settle().await;
        assert_eq!(*dials.lock().unwrap(), vec![url(PEER_B)]);
        dials.lock().unwrap().clear();

        // Once the LAN withdraws B, nothing is left to redial.
        entry.record_removed(&url(PEER_B));
        entry.redial_unconnected().await;
        settle().await;
        assert!(dials.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_url_skipped_at_the_cap_is_picked_up_by_the_next_round() {
        let release = Arc::new(tokio::sync::Notify::new());
        let dials: Arc<Mutex<Vec<Url>>> = Arc::new(Mutex::new(Vec::new()));
        let mut mock = MockTransport::new();
        {
            let record = dials.clone();
            let release = release.clone();
            mock.expect_dial().returning(move |_space, url| {
                record.lock().unwrap().push(url);
                let release = release.clone();
                Box::pin(async move {
                    release.notified().await;
                    Ok(())
                })
            });
        }
        // A counts as connected once dialled, so every round has only B
        // left to dial.
        mock.expect_get_connected_peers()
            .returning(|| Box::pin(async { Ok(vec![url(PEER_A)]) }));
        let entry = entry(Arc::new(mock), 1);

        entry.record_resolved(url(PEER_A));
        entry.record_resolved(url(PEER_B));
        settle().await;
        assert_eq!(*dials.lock().unwrap(), vec![url(PEER_A)]);

        // The round finds the slot still taken by A's dial.
        entry.redial_unconnected().await;
        settle().await;
        assert_eq!(dials.lock().unwrap().len(), 1);

        release.notify_one();
        settle().await;
        entry.redial_unconnected().await;
        settle().await;
        assert_eq!(dials.lock().unwrap().len(), 2);
        assert!(dials.lock().unwrap().contains(&url(PEER_B)));
    }

    #[tokio::test]
    async fn advertise_registers_and_replaces_the_record() {
        let daemon = FakeDaemon::new();
        let (tx, _) = transport(vec![]);
        let entry = SpaceEntry::new(
            space(),
            space_fingerprint(&space()),
            daemon.clone(),
            tx,
            1,
        );
        assert!(entry.advertised().is_none());

        entry.advertise(&url(PEER_A)).unwrap();
        entry.advertise(&url(PEER_A)).unwrap();
        assert_eq!(entry.advertised(), Some(url(PEER_A)));
        assert_eq!(daemon.registered.lock().unwrap().len(), 1);
        assert!(daemon.unregistered.lock().unwrap().is_empty());
        assert!(entry.is_own_record("someone-else", &url(PEER_A)));
        assert!(entry.is_own_record(entry.fullname(), &url(PEER_B)));
        assert!(!entry.is_own_record("someone-else", &url(PEER_B)));

        entry.advertise(&url(PEER_B)).unwrap();
        assert_eq!(daemon.registered.lock().unwrap().len(), 2);
        assert_eq!(daemon.unregistered.lock().unwrap().len(), 1);

        entry.withdraw();
        assert!(entry.advertised().is_none());
        assert_eq!(daemon.unregistered.lock().unwrap().len(), 2);
    }
}
