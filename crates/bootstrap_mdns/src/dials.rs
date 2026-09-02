//! What the LAN currently announces for one space, and the dials in
//! flight toward it.
//!
//! mDNS speaks in records: a record has a name, and it is the name that
//! is announced, updated and withdrawn. A peer that restarts announces a
//! new record name for the same URL while its old record has not expired
//! yet, so a URL must stay known until the last record naming it is gone.
//! Records are the unit of bookkeeping here; the URLs worth dialling are
//! derived from them.
//!
//! A first dial toward a freshly announced peer commonly fails — the
//! peer's transport record may not have reached this node's lookup cache
//! yet — and mDNS does not re-deliver a record that has not changed, so a
//! dial fired once per announcement could leave a LAN peer undialled for
//! good. Announced URLs are therefore remembered until the LAN withdraws
//! every record naming them, and the owner reconciles that set against the
//! transport's connections on a timer.
//!
//! Dials are bounded, not queued: a dial that finds no free slot is
//! skipped, and the next reconciliation picks the peer up if it is still
//! announced. What the LAN says can grow without bound; what this node
//! does about it cannot.

use kitsune2_api::{DialOutcome, DynTransport, SpaceId, Url};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, trace};

/// The most URLs remembered for one space. Past this, the URL heard
/// longest ago makes room, so a LAN full of announcements — or a flood of
/// forged ones — costs a fixed amount of memory per space.
pub const MAX_URLS: usize = 256;

/// The records announced for a space, by name, and the URLs they name.
#[derive(Debug, Default)]
struct Records {
    by_name: HashMap<String, Url>,
    urls: HashMap<Url, UrlState>,
}

#[derive(Debug)]
struct UrlState {
    last_heard: Instant,
}

impl Records {
    /// Record that `fullname` now names `url`. Returns `true` when no
    /// record named `url` before.
    fn resolved(&mut self, fullname: &str, url: Url) -> bool {
        if let Some(previous) =
            self.by_name.insert(fullname.to_string(), url.clone())
            && previous != url
        {
            self.release(&previous);
        }
        let new = match self.urls.entry(url) {
            Entry::Occupied(mut state) => {
                state.get_mut().last_heard = Instant::now();
                false
            }
            Entry::Vacant(slot) => {
                slot.insert(UrlState {
                    last_heard: Instant::now(),
                });
                true
            }
        };
        self.enforce_cap();
        new
    }

    /// The record `fullname` is gone.
    fn removed(&mut self, fullname: &str) {
        if let Some(url) = self.by_name.remove(fullname) {
            self.release(&url);
        }
    }

    /// Drop every record naming `url`, and the URL with them.
    fn forget_url(&mut self, url: &Url) {
        self.by_name.retain(|_, named| named != url);
        self.urls.remove(url);
    }

    /// Drop `url` once no record names it any more.
    fn release(&mut self, url: &Url) {
        if !self.by_name.values().any(|named| named == url) {
            self.urls.remove(url);
        }
    }

    /// Make room by forgetting the URLs heard longest ago. The URL just
    /// heard is the newest, so it is never the one to go.
    fn enforce_cap(&mut self) {
        while self.urls.len() > MAX_URLS {
            let Some(oldest) = self
                .urls
                .iter()
                .min_by_key(|(_, state)| state.last_heard)
                .map(|(url, _)| url.clone())
            else {
                break;
            };
            self.forget_url(&oldest);
        }
    }
}

/// Announced peers and in-flight dials for one space.
///
/// Dropping the state aborts every dial still in flight, so a space that
/// leaves does not keep its transport busy for the connect timeout.
#[derive(Debug)]
pub struct Announcements {
    records: Arc<Mutex<Records>>,
    in_flight: Arc<Semaphore>,
    dials: Mutex<JoinSet<()>>,
}

impl Announcements {
    /// State allowing at most `max_concurrent` dials in flight. A cap of
    /// zero would never dial, so it is read as one.
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            records: Arc::new(Mutex::new(Records::default())),
            in_flight: Arc::new(Semaphore::new(max_concurrent.max(1))),
            dials: Mutex::new(JoinSet::new()),
        }
    }

    /// The record `fullname` was resolved naming `url`. Returns `true`
    /// when no record named `url` before.
    pub fn record_resolved(&self, fullname: &str, url: Url) -> bool {
        self.records.lock().expect("poison").resolved(fullname, url)
    }

    /// The record `fullname` was withdrawn.
    pub fn record_removed(&self, fullname: &str) {
        self.records.lock().expect("poison").removed(fullname)
    }

    /// Drop every record naming `url` until the LAN announces it afresh.
    pub fn forget_url(&self, url: &Url) {
        self.records.lock().expect("poison").forget_url(url)
    }

    /// Every URL some record currently names.
    pub fn urls(&self) -> Vec<Url> {
        self.records
            .lock()
            .expect("poison")
            .urls
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
        let records = self.records.clone();
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
                    records.lock().expect("poison").forget_url(&url);
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
    use crate::test_support::*;
    use kitsune2_api::MockTransport;
    use std::time::Duration;

    const A: &str = "ws://a.test:80/peerA";
    const B: &str = "ws://b.test:80/peerB";

    fn space() -> SpaceId {
        space_id(b"space")
    }

    fn sorted(mut urls: Vec<Url>) -> Vec<Url> {
        urls.sort_by_key(|u| u.to_string());
        urls
    }

    #[test]
    fn a_url_is_new_only_the_first_time_a_record_names_it() {
        let state = Announcements::new(4);
        assert!(state.record_resolved("r1", url(A)));
        assert!(!state.record_resolved("r1", url(A)));
        assert!(!state.record_resolved("r2", url(A)));
        assert_eq!(state.urls(), vec![url(A)]);
    }

    /// A restarted peer announces a fresh record for its URL while the
    /// old one lingers; losing the old record must not lose the URL.
    #[test]
    fn a_url_named_by_two_records_survives_losing_one() {
        let state = Announcements::new(4);
        state.record_resolved("old", url(A));
        state.record_resolved("new", url(A));

        state.record_removed("old");
        assert_eq!(state.urls(), vec![url(A)]);

        state.record_removed("new");
        assert!(state.urls().is_empty());
        assert!(state.record_resolved("new", url(A)), "new again");
    }

    #[test]
    fn a_record_that_changes_url_releases_the_old_one() {
        let state = Announcements::new(4);
        state.record_resolved("r1", url(A));
        state.record_resolved("r2", url(A));

        // Another record still names A, so A stays.
        state.record_resolved("r1", url(B));
        assert_eq!(sorted(state.urls()), sorted(vec![url(A), url(B)]));

        // The last record naming A moves away: A goes.
        state.record_resolved("r2", url(B));
        assert_eq!(state.urls(), vec![url(B)]);
    }

    #[test]
    fn forgetting_a_url_drops_every_record_naming_it() {
        let state = Announcements::new(4);
        state.record_resolved("r1", url(A));
        state.record_resolved("r2", url(A));
        state.record_resolved("r3", url(B));

        state.forget_url(&url(A));
        assert_eq!(state.urls(), vec![url(B)]);
        assert!(state.record_resolved("r1", url(A)), "new again");
    }

    #[test]
    fn the_url_heard_longest_ago_makes_room_past_the_cap() {
        let state = Announcements::new(4);
        for i in 0..MAX_URLS {
            state.record_resolved(
                &format!("r{i}"),
                url(&format!("ws://p{i}.test:80/peer{i}")),
            );
        }
        assert_eq!(state.urls().len(), MAX_URLS);

        // Hearing the very first URL again makes it the newest.
        state.record_resolved("r0", url("ws://p0.test:80/peer0"));
        state.record_resolved("extra", url(A));

        let urls = state.urls();
        assert_eq!(urls.len(), MAX_URLS);
        assert!(urls.contains(&url(A)), "the newcomer is kept");
        assert!(urls.contains(&url("ws://p0.test:80/peer0")), "re-heard");
        assert!(
            !urls.contains(&url("ws://p1.test:80/peer1")),
            "the oldest made room"
        );
    }

    #[tokio::test]
    async fn dials_beyond_the_cap_are_skipped_not_queued() {
        let release = Arc::new(tokio::sync::Notify::new());
        let (tx, dials) = blocking_transport(release.clone(), vec![]);
        let state = Announcements::new(1);

        assert!(state.try_dial(&tx, &space(), url(A)));
        assert!(!state.try_dial(&tx, &space(), url(B)));
        settle().await;
        assert_eq!(dials.lock().unwrap().len(), 1);

        release.notify_one();
        settle().await;
        assert!(state.try_dial(&tx, &space(), url(B)));
        settle().await;
        assert_eq!(dials.lock().unwrap().len(), 2);
    }

    /// A peer the space blocks must not be dialled round after round: the
    /// blocked outcome drops it until the LAN announces it afresh.
    #[tokio::test]
    async fn a_blocked_dial_forgets_the_url_until_re_announced() {
        let dials: Dials = Arc::new(Mutex::new(Vec::new()));
        let mut mock = MockTransport::new();
        {
            let record = dials.clone();
            mock.expect_dial().returning(move |_space, url| {
                record.lock().unwrap().push(url);
                Box::pin(async { Ok(DialOutcome::Blocked) })
            });
        }
        let tx: DynTransport = Arc::new(mock);
        let state = Announcements::new(4);

        assert!(state.record_resolved("r1", url(A)));
        assert!(state.try_dial(&tx, &space(), url(A)));
        settle().await;
        assert_eq!(dials.lock().unwrap().len(), 1);
        assert!(state.urls().is_empty(), "a blocked peer is forgotten");

        // Announced again, it counts as new and is checked again.
        assert!(state.record_resolved("r1", url(A)));
        assert_eq!(state.urls(), vec![url(A)]);
    }

    #[tokio::test]
    async fn dropping_the_state_aborts_dials_in_flight() {
        let release = Arc::new(tokio::sync::Notify::new());
        let (tx, _dials) = blocking_transport(release, vec![]);
        let state = Announcements::new(1);
        assert!(state.try_dial(&tx, &space(), url(A)));
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
