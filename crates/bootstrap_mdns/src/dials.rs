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

use crate::cap::evict_past_cap;
use kitsune2_api::{DialOutcome, DynTransport, SpaceId, Url};
use std::cmp::Reverse;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, trace};

/// The most URLs remembered for one space, so that a LAN full of
/// announcements — or a flood of forged ones — costs a fixed amount of
/// memory per space. Past this, the URLs that keep failing make room
/// first, the oldest-heard among equals, and a URL that connected last.
pub const MAX_URLS: usize = 256;

/// The most reconciliation rounds a URL waits between dials. A peer that
/// keeps failing is still tried, just rarely; a fresh announcement puts it
/// back on the short schedule.
pub const MAX_BACKOFF_ROUNDS: u64 = 16;

/// The records announced for a space, by name, and the URLs they name.
#[derive(Debug, Default)]
struct Records {
    by_name: HashMap<String, Url>,
    urls: HashMap<Url, UrlState>,
    /// The reconciliation rounds run so far; schedules count in rounds
    /// rather than time so that they need no clock of their own.
    round: u64,
    /// The URL this node announces for the space. An echo of our own
    /// record, under whatever name, names nobody to dial, and the
    /// decision is taken here, under the same lock that starts dials, so
    /// that an announcement racing an echo cannot slip one through.
    own: Option<Url>,
}

/// One URL's standing: when it was last heard, and when it may be dialled
/// again after failing.
#[derive(Debug)]
struct UrlState {
    last_heard: Instant,
    /// The first round the URL may be dialled in again.
    next_round: u64,
    /// How many rounds the next failure will push `next_round` out by.
    backoff_rounds: u64,
    /// Whether the last dial connected. A peer this node has reached is
    /// worth more than any number of announcements when room is short.
    connected: bool,
    /// Whether a dial toward the URL is running. A URL being dialled is
    /// not due for another one.
    in_flight: bool,
}

impl UrlState {
    fn fresh() -> Self {
        Self {
            last_heard: Instant::now(),
            next_round: 0,
            backoff_rounds: 1,
            connected: false,
            in_flight: false,
        }
    }

    /// How readily this URL makes room when the cap is hit: the ones that
    /// never connected go before the ones that did, the ones that keep
    /// failing before the ones that do not, the oldest-heard among equals.
    fn eviction_rank(&self) -> (bool, u64, Reverse<Instant>) {
        (
            !self.connected,
            self.backoff_rounds,
            Reverse(self.last_heard),
        )
    }

    /// Heard again, or connected: back on the short schedule.
    fn reset(&mut self) {
        self.next_round = 0;
        self.backoff_rounds = 1;
    }
}

impl Records {
    /// Record that `fullname` now names `url`. Returns `true` when no
    /// record named `url` before. Our own URL is never recorded.
    fn resolved(&mut self, fullname: &str, url: Url) -> bool {
        if self.own.as_ref() == Some(&url) {
            return false;
        }
        if let Some(previous) =
            self.by_name.insert(fullname.to_string(), url.clone())
            && previous != url
        {
            self.release(&previous);
        }
        let new = match self.urls.entry(url.clone()) {
            Entry::Occupied(mut state) => {
                state.get_mut().last_heard = Instant::now();
                state.get_mut().reset();
                false
            }
            Entry::Vacant(slot) => {
                slot.insert(UrlState::fresh());
                true
            }
        };
        self.enforce_cap(&url);
        new
    }

    /// The record `fullname` is gone. A URL other records still name is
    /// put back on the short schedule: something about the peer changed.
    fn removed(&mut self, fullname: &str) {
        if let Some(url) = self.by_name.remove(fullname) {
            self.release(&url);
            if let Some(state) = self.urls.get_mut(&url) {
                state.reset();
            }
        }
    }

    /// Start a reconciliation round and return the URLs due for a dial in
    /// it: those whose schedule allows one and that are not being dialled
    /// right now.
    fn due(&mut self) -> Vec<Url> {
        self.round += 1;
        let round = self.round;
        self.urls
            .iter()
            .filter(|(_, state)| !state.in_flight && state.next_round <= round)
            .map(|(url, _)| url.clone())
            .collect()
    }

    /// Claim `url` for a dial. Returns `false` when there is nothing to
    /// dial: the URL is not announced (any more), is our own, or is being
    /// dialled already.
    fn start_dial(&mut self, url: &Url) -> bool {
        if self.own.as_ref() == Some(url) {
            return false;
        }
        match self.urls.get_mut(url) {
            Some(state) if !state.in_flight => {
                state.in_flight = true;
                true
            }
            _ => false,
        }
    }

    /// The dial toward `url` is over, however it ended.
    fn end_dial(&mut self, url: &Url) {
        if let Some(state) = self.urls.get_mut(url) {
            state.in_flight = false;
        }
    }

    /// This node now announces `url` for the space: forget whatever the
    /// LAN said under that URL, and record none of it from now on.
    fn set_own(&mut self, url: &Url) {
        self.forget_url(url);
        self.own = Some(url.clone());
    }

    /// A dial toward `url` failed: wait longer before the next one, up to
    /// [`MAX_BACKOFF_ROUNDS`].
    fn failed(&mut self, url: &Url) {
        if let Some(state) = self.urls.get_mut(url) {
            state.connected = false;
            state.next_round = self.round + state.backoff_rounds;
            state.backoff_rounds =
                (state.backoff_rounds * 2).min(MAX_BACKOFF_ROUNDS);
        }
    }

    /// The space refuses `url`. A block can be lifted while the record
    /// stands unchanged, so the URL stays known and is tried again on the
    /// longest schedule, where the block is checked anew.
    fn blocked(&mut self, url: &Url) {
        if let Some(state) = self.urls.get_mut(url) {
            state.connected = false;
            state.next_round = self.round + MAX_BACKOFF_ROUNDS;
            state.backoff_rounds = MAX_BACKOFF_ROUNDS;
        }
    }

    /// A dial toward `url` connected.
    fn connected(&mut self, url: &Url) {
        if let Some(state) = self.urls.get_mut(url) {
            state.reset();
            state.connected = true;
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

    /// Make room past [`MAX_URLS`] by forgetting the URLs that rank
    /// highest for eviction. `just_heard` is what the LAN said last and is
    /// never the one to go.
    fn enforce_cap(&mut self, just_heard: &Url) {
        let evicted = evict_past_cap(&mut self.urls, MAX_URLS, |url, state| {
            (url != just_heard).then(|| state.eviction_rank())
        });
        for url in evicted {
            self.by_name.retain(|_, named| named != &url);
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

    /// Whether a record of that name is known.
    pub fn has_record(&self, fullname: &str) -> bool {
        self.records
            .lock()
            .expect("poison")
            .by_name
            .contains_key(fullname)
    }

    /// This node now announces `url` for the space, so nothing the LAN
    /// says under that URL is worth a dial.
    pub fn set_own(&self, url: &Url) {
        self.records.lock().expect("poison").set_own(url)
    }

    /// Every URL some record currently names.
    #[cfg(test)]
    pub fn urls(&self) -> Vec<Url> {
        self.records
            .lock()
            .expect("poison")
            .urls
            .keys()
            .cloned()
            .collect()
    }

    /// How many URLs have a dial in flight.
    #[cfg(test)]
    pub fn in_flight_count(&self) -> usize {
        self.records
            .lock()
            .expect("poison")
            .urls
            .values()
            .filter(|state| state.in_flight)
            .count()
    }

    /// Whether any record is known at all.
    pub fn is_empty(&self) -> bool {
        self.records.lock().expect("poison").urls.is_empty()
    }

    /// Start a reconciliation round: the URLs whose schedule allows a
    /// dial now. Each call is one round.
    pub fn due_urls(&self) -> Vec<Url> {
        self.records.lock().expect("poison").due()
    }

    /// Start a dial toward `url` in its own task, if `url` is still worth
    /// one and a slot is free. Returns whether a dial was started; a peer
    /// that found every slot taken is not queued.
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
        // Claiming the URL and spawning happen under one lock, so that
        // whatever changes the URL's standing meanwhile — a withdrawal,
        // our own announcement of it — sees the dial in flight or the
        // dial sees the change; never neither.
        let mut records = self.records.lock().expect("poison");
        if !records.start_dial(&url) {
            trace!(%url, "mdns: nothing to dial for this url");
            return false;
        }
        let tx = tx.clone();
        let space_id = space_id.clone();
        let records_for_task = self.records.clone();
        let mut dials = self.dials.lock().expect("poison");
        // Finished dials leave their result behind until collected.
        while dials.try_join_next().is_some() {}
        dials.spawn(async move {
            let _permit = permit;
            let outcome = tx.dial(space_id, url.clone()).await;
            let mut records = records_for_task.lock().expect("poison");
            records.end_dial(&url);
            match outcome {
                Ok(DialOutcome::Connected) => {
                    debug!(%url, "mdns: dial succeeded");
                    records.connected(&url);
                }
                Ok(DialOutcome::Blocked) => {
                    debug!(%url, "mdns: peer is blocked in this space, retrying on the longest schedule");
                    records.blocked(&url);
                }
                Err(err) => {
                    debug!(?err, %url, "mdns: dial failed");
                    records.failed(&url);
                }
            }
        });
        drop(records);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;
    use kitsune2_api::MockTransport;

    const A: &str = "ws://a.test:80/peerA";
    const B: &str = "ws://b.test:80/peerB";
    const C: &str = "ws://c.test:80/peerC";

    fn url_of(s: &str) -> Url {
        url(s)
    }

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
    fn announcing_a_url_as_our_own_drops_every_record_naming_it() {
        let state = Announcements::new(4);
        state.record_resolved("r1", url(A));
        state.record_resolved("r2", url(A));
        state.record_resolved("r3", url(B));

        state.set_own(&url(A));
        assert_eq!(state.urls(), vec![url(B)]);
        assert!(!state.has_record("r1"));
        assert!(!state.record_resolved("r1", url(A)), "never again");
        assert_eq!(state.urls(), vec![url(B)]);
    }

    fn forged(i: usize) -> Url {
        url(&format!("ws://p{i}.test:80/peer{i}"))
    }

    /// Fill the state with `n` forged records, named `f0..`.
    fn flood(state: &Announcements, n: usize) {
        for i in 0..n {
            state.record_resolved(&format!("f{i}"), forged(i));
        }
    }

    #[test]
    fn the_url_heard_longest_ago_makes_room_past_the_cap() {
        let state = Announcements::new(4);
        flood(&state, MAX_URLS);
        assert_eq!(state.urls().len(), MAX_URLS);

        // Hearing the very first URL again makes it the newest.
        state.record_resolved("f0", forged(0));
        state.record_resolved("extra", url(A));

        let urls = state.urls();
        assert_eq!(urls.len(), MAX_URLS);
        assert!(urls.contains(&url(A)), "the newcomer is kept");
        assert!(urls.contains(&forged(0)), "re-heard");
        assert!(!urls.contains(&forged(1)), "the oldest made room");
    }

    /// A flood of announcements must not push out the peers this node
    /// actually talks to: a URL that connected is the last to go, and
    /// among the rest the ones that keep failing go first.
    #[tokio::test]
    async fn a_connected_url_survives_a_flood_and_failing_urls_go_first() {
        let (tx, _) = failing_transport();
        let state = Announcements::new(4);
        state.record_resolved("good", url(A));
        state.record_resolved("bad", url(B));
        state.records.lock().unwrap().connected(&url(A));
        // B fails twice, so it carries the highest backoff around.
        for _ in 0..2 {
            for u in state.due_urls() {
                if u == url(B) {
                    state.try_dial(&tx, &space(), u);
                }
            }
            wait_until(|| state.in_flight_count() == 0).await;
        }

        flood(&state, MAX_URLS + 1);

        let urls = state.urls();
        assert_eq!(urls.len(), MAX_URLS);
        assert!(urls.contains(&url(A)), "the connected peer is kept");
        assert!(!urls.contains(&url(B)), "the failing peer went first");
        assert!(!urls.contains(&forged(0)), "then the oldest forged one");
    }

    /// A transport whose every dial fails.
    fn failing_transport() -> (DynTransport, Dials) {
        let dials: Dials = Arc::new(Mutex::new(Vec::new()));
        let record = dials.clone();
        let mut mock = MockTransport::new();
        mock.expect_dial().returning(move |_space, url| {
            record.lock().unwrap().push(url);
            Box::pin(async { Err(kitsune2_api::K2Error::other("no route")) })
        });
        (Arc::new(mock), dials)
    }

    /// Dial everything due this round.
    fn dial_due(state: &Announcements, tx: &DynTransport) {
        for url in state.due_urls() {
            state.try_dial(tx, &space(), url);
        }
    }

    /// Run one reconciliation round: dial everything due, and let the
    /// dials finish so their outcome is recorded before the next round.
    async fn round(state: &Announcements, tx: &DynTransport) {
        dial_due(state, tx);
        wait_until(|| state.in_flight_count() == 0).await;
    }

    /// A failing URL is dialled at rounds 1, 2, 4, 8, 16, then every 16.
    #[tokio::test]
    async fn a_failing_url_backs_off_exponentially_up_to_the_cap() {
        let (tx, dials) = failing_transport();
        let state = Announcements::new(4);
        state.record_resolved("r1", url(A));

        let mut dialled_in = Vec::new();
        for r in 1..=(MAX_BACKOFF_ROUNDS * 3 + 1) {
            let before = dials.lock().unwrap().len();
            round(&state, &tx).await;
            if dials.lock().unwrap().len() > before {
                dialled_in.push(r);
            }
        }
        assert_eq!(dialled_in, vec![1, 2, 4, 8, 16, 32, 48]);
    }

    /// A re-announcement says something changed on the peer's side, so
    /// the wait is over: it is dialled in the very next round.
    #[tokio::test]
    async fn a_re_announcement_resets_the_backoff() {
        let (tx, dials) = failing_transport();
        let state = Announcements::new(4);
        state.record_resolved("r1", url(A));
        for _ in 0..3 {
            round(&state, &tx).await;
        }
        // Rounds 1 and 2 dialled; the URL now waits until round 4.
        assert_eq!(dials.lock().unwrap().len(), 2);

        state.record_resolved("r2", url(A));
        round(&state, &tx).await;
        assert_eq!(dials.lock().unwrap().len(), 3, "dialled at round 4");
        // No wait: the reset put the backoff back to one round.
        round(&state, &tx).await;
        assert_eq!(dials.lock().unwrap().len(), 4, "and at round 5");
    }

    /// A URL heard once and never again — mDNS repeats nothing that has
    /// not changed — is still dialled for as long as it is announced.
    #[tokio::test]
    async fn a_stable_record_is_never_expired_by_time() {
        let (tx, dials) = failing_transport();
        let state = Announcements::new(4);
        state.record_resolved("r1", url(A));
        for _ in 0..(MAX_BACKOFF_ROUNDS * 4) {
            round(&state, &tx).await;
        }
        assert_eq!(state.urls(), vec![url(A)]);
        assert!(dials.lock().unwrap().len() >= 6);
    }

    #[tokio::test]
    async fn dials_beyond_the_cap_are_skipped_not_queued() {
        let release = Arc::new(tokio::sync::Notify::new());
        let (tx, dials) = blocking_transport(release.clone(), vec![]);
        let state = Announcements::new(1);
        state.record_resolved("a", url(A));
        state.record_resolved("b", url(B));

        assert!(state.try_dial(&tx, &space(), url(A)));
        assert!(!state.try_dial(&tx, &space(), url(B)));
        assert_eq!(wait_for_dials(&dials, 1).await, vec![url(A)]);

        release.notify_one();
        wait_until(|| state.in_flight.available_permits() == 1).await;
        assert!(state.try_dial(&tx, &space(), url(B)));
        assert_eq!(wait_for_dials(&dials, 2).await, vec![url(A), url(B)]);
    }

    /// A peer the space blocks must not be dialled round after round, but
    /// a block can be lifted while the record stands unchanged, so the
    /// blocked outcome puts the URL on the longest schedule rather than
    /// forgetting it.
    #[tokio::test]
    async fn a_blocked_dial_is_retried_on_the_longest_schedule() {
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
        state.record_resolved("r1", url(A));

        let mut dialled_in = Vec::new();
        for r in 1..=(MAX_BACKOFF_ROUNDS * 2 + 1) {
            let before = dials.lock().unwrap().len();
            round(&state, &tx).await;
            if dials.lock().unwrap().len() > before {
                dialled_in.push(r);
            }
        }
        assert_eq!(dialled_in, vec![1, 17, 33]);
        assert_eq!(state.urls(), vec![url(A)], "still known");
    }

    /// A URL being dialled is not due for another dial, and its slot is
    /// the only one it holds: the rest keep flowing.
    #[tokio::test]
    async fn a_url_with_a_dial_in_flight_is_not_due() {
        let (tx, dials) = transport_with(vec![], |url| async move {
            if url == url_of(A) {
                std::future::pending::<()>().await;
            }
            Ok(DialOutcome::Connected)
        });
        let state = Announcements::new(4);
        state.record_resolved("a", url(A));
        state.record_resolved("b", url(B));
        state.record_resolved("c", url(C));

        // A never completes, so a round cannot wait for it: B and C are
        // dialled in both rounds, A in the first only.
        dial_due(&state, &tx);
        wait_for_dials(&dials, 3).await;
        dial_due(&state, &tx);
        let dialled = wait_for_dials(&dials, 5).await;
        assert_eq!(state.in_flight.available_permits(), 3, "A holds one");
        assert_eq!(
            dialled.iter().filter(|u| **u == url(A)).count(),
            1,
            "one dial for the url in flight: {dialled:?}"
        );
        assert_eq!(dialled.iter().filter(|u| **u == url(B)).count(), 2);
        assert_eq!(dialled.iter().filter(|u| **u == url(C)).count(), 2);
    }

    /// Announcing a URL as our own closes the door on a dial toward it
    /// even when the echo was recorded first and a dial is about to
    /// start.
    #[tokio::test]
    async fn our_own_url_is_neither_recorded_nor_dialled() {
        let (tx, dials) = recording_transport(vec![]);
        let state = Announcements::new(4);

        assert!(state.record_resolved("echo", url(A)));
        state.set_own(&url(A));
        assert!(state.urls().is_empty(), "the echo is forgotten");
        assert!(!state.try_dial(&tx, &space(), url(A)));
        assert!(!state.record_resolved("echo-again", url(A)));
        assert!(state.urls().is_empty());
        assert_eq!(state.in_flight_count(), 0, "nothing was spawned");
        assert!(dials.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn dropping_the_state_aborts_dials_in_flight() {
        let release = Arc::new(tokio::sync::Notify::new());
        let (tx, _dials) = blocking_transport(release, vec![]);
        let state = Announcements::new(1);
        state.record_resolved("a", url(A));
        assert!(state.try_dial(&tx, &space(), url(A)));
        let slots = state.in_flight.clone();
        wait_for_dials(&_dials, 1).await;
        assert_eq!(slots.available_permits(), 0);

        drop(state);
        // An aborted dial must release its slot.
        wait_until(|| slots.available_permits() == 1).await;
    }
}
