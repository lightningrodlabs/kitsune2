//! The dial state of one space: which peer URLs the LAN currently
//! announces for it, and the dials in flight toward them.
//!
//! A first dial toward a freshly announced peer commonly fails — the
//! peer's transport record may not have reached this node's lookup cache
//! yet — and mDNS does not re-resolve a record that has not changed, so a
//! dial fired once per announcement could leave a LAN peer undialled for
//! good. The state therefore remembers every announced URL until the LAN
//! withdraws it, and the owner reconciles that set against the transport's
//! connections on a timer.
//!
//! Dials are bounded, not queued: a dial that finds no free slot is
//! skipped, and the next reconciliation picks the peer up if it is still
//! announced. What the LAN says can grow without bound; what this node
//! does about it cannot.

use kitsune2_api::{DialOutcome, DynTransport, SpaceId, Url};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, trace};

/// Every URL the LAN currently announces for a space, with when each was
/// last heard.
type Discovered = Arc<Mutex<HashMap<Url, Instant>>>;

/// Announced peers and in-flight dials for one space.
///
/// Dropping the state aborts every dial still in flight, so a space that
/// leaves does not keep its transport busy for the connect timeout.
#[derive(Debug)]
pub struct DialState {
    discovered: Discovered,
    in_flight: Arc<Semaphore>,
    dials: Mutex<JoinSet<()>>,
}

impl DialState {
    /// State allowing at most `max_concurrent` dials in flight.
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            discovered: Arc::new(Mutex::new(HashMap::new())),
            in_flight: Arc::new(Semaphore::new(max_concurrent)),
            dials: Mutex::new(JoinSet::new()),
        }
    }

    /// Record that `url` was just announced. Returns `true` when it was not
    /// known before.
    pub fn discovered(&self, url: &Url) -> bool {
        self.discovered
            .lock()
            .expect("poison")
            .insert(url.clone(), Instant::now())
            .is_none()
    }

    /// The LAN no longer announces `url`.
    pub fn forget(&self, url: &Url) {
        self.discovered.lock().expect("poison").remove(url);
    }

    /// Every URL currently announced.
    pub fn discovered_urls(&self) -> Vec<Url> {
        self.discovered
            .lock()
            .expect("poison")
            .keys()
            .cloned()
            .collect()
    }

    /// Start a dial toward `url` in its own task if a slot is free.
    /// Returns `false` when every slot is taken; the peer is not queued.
    pub fn try_dial(
        &self,
        tx: &DynTransport,
        space_id: &SpaceId,
        url: Url,
    ) -> bool {
        let Ok(permit) = self.in_flight.clone().try_acquire_owned() else {
            trace!(%url, "mdns: no free dial slot, skipping until the next round");
            return false;
        };
        let tx = tx.clone();
        let space_id = space_id.clone();
        let discovered = self.discovered.clone();
        let mut dials = self.dials.lock().expect("poison");
        // Finished dials leave their result behind until collected.
        while dials.try_join_next().is_some() {}
        dials.spawn(async move {
            let _permit = permit;
            match tx.dial(space_id, url.clone()).await {
                Ok(DialOutcome::Connected) => {
                    debug!(%url, "mdns: dial succeeded")
                }
                Ok(DialOutcome::Blocked) => {
                    // The space refuses this peer, so no round should
                    // dial it again; only a fresh announcement puts it
                    // back, where the block is checked anew.
                    debug!(%url, "mdns: peer is blocked in this space, forgetting it until re-announced");
                    discovered.lock().expect("poison").remove(&url);
                }
                Ok(outcome) => {
                    debug!(?outcome, %url, "mdns: dial ended with an outcome this crate does not know")
                }
                Err(err) => debug!(?err, %url, "mdns: dial failed"),
            }
        });
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kitsune2_api::MockTransport;
    use std::time::Duration;

    fn url(s: &str) -> Url {
        Url::from_str(s).unwrap()
    }

    fn space() -> SpaceId {
        SpaceId::from(bytes::Bytes::from_static(b"space"))
    }

    /// A transport whose dials block until `release` is notified.
    fn blocking_transport(
        release: Arc<tokio::sync::Notify>,
    ) -> (DynTransport, Arc<Mutex<Vec<Url>>>) {
        let dials: Arc<Mutex<Vec<Url>>> = Arc::new(Mutex::new(Vec::new()));
        let record = dials.clone();
        let mut mock = MockTransport::new();
        mock.expect_dial().returning(move |_space, url| {
            record.lock().unwrap().push(url);
            let release = release.clone();
            Box::pin(async move {
                release.notified().await;
                Ok(DialOutcome::Connected)
            })
        });
        (Arc::new(mock), dials)
    }

    #[test]
    fn discovered_reports_only_the_first_sighting() {
        let state = DialState::new(4);
        let a = url("ws://a.test:80/peerA");
        assert!(state.discovered(&a));
        assert!(!state.discovered(&a));
        state.forget(&a);
        assert!(state.discovered(&a));
        assert_eq!(state.discovered_urls(), vec![a]);
    }

    #[tokio::test]
    async fn dials_beyond_the_cap_are_skipped_not_queued() {
        let release = Arc::new(tokio::sync::Notify::new());
        let (tx, dials) = blocking_transport(release.clone());
        let state = DialState::new(1);

        assert!(state.try_dial(&tx, &space(), url("ws://a.test:80/peerA")));
        assert!(!state.try_dial(&tx, &space(), url("ws://b.test:80/peerB")));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(dials.lock().unwrap().len(), 1);

        release.notify_one();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(state.try_dial(&tx, &space(), url("ws://b.test:80/peerB")));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(dials.lock().unwrap().len(), 2);
    }

    /// A peer the space blocks must not be dialled round after round: the
    /// blocked outcome drops it until the LAN announces it afresh.
    #[tokio::test]
    async fn a_blocked_dial_forgets_the_url_until_re_announced() {
        let dials: Arc<Mutex<Vec<Url>>> = Arc::new(Mutex::new(Vec::new()));
        let mut mock = MockTransport::new();
        {
            let record = dials.clone();
            mock.expect_dial().returning(move |_space, url| {
                record.lock().unwrap().push(url);
                Box::pin(async { Ok(DialOutcome::Blocked) })
            });
        }
        let tx: DynTransport = Arc::new(mock);
        let state = DialState::new(4);
        let a = url("ws://a.test:80/peerA");

        assert!(state.discovered(&a));
        assert!(state.try_dial(&tx, &space(), a.clone()));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(dials.lock().unwrap().len(), 1);
        assert!(
            state.discovered_urls().is_empty(),
            "a blocked peer is forgotten"
        );

        // Announced again, it counts as new and is checked again.
        assert!(state.discovered(&a));
        assert_eq!(state.discovered_urls(), vec![a]);
    }

    #[tokio::test]
    async fn dropping_the_state_aborts_dials_in_flight() {
        let release = Arc::new(tokio::sync::Notify::new());
        let (tx, _dials) = blocking_transport(release);
        let state = DialState::new(1);
        assert!(state.try_dial(&tx, &space(), url("ws://a.test:80/peerA")));
        let slots = state.in_flight.clone();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(slots.available_permits(), 0);

        drop(state);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            slots.available_permits(),
            1,
            "an aborted dial must release its slot"
        );
    }
}
