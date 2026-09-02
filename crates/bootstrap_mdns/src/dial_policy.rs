//! Rate limiting for the dials that discovery triggers.
//!
//! Every resolved announcement is a request to dial someone, and mDNS is
//! both chatty and unauthenticated, so two independent limits apply before
//! a dial reaches the transport: a per-URL cooldown, so that one peer is
//! tried at most once per window however often its record is resolved, and
//! a cap on dials in flight, so that a burst of announcements cannot flood
//! the transport with connection attempts.

use kitsune2_api::Url;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// The dial limits for one bootstrap instance.
#[derive(Debug)]
pub struct DialPolicy {
    cooldown: Duration,
    last_attempt: Mutex<HashMap<Url, Instant>>,
    in_flight: Arc<Semaphore>,
}

impl DialPolicy {
    /// Create a policy allowing one attempt per `cooldown` per URL and at
    /// most `max_concurrent` dials in flight.
    pub fn new(cooldown: Duration, max_concurrent: usize) -> Self {
        Self {
            cooldown,
            last_attempt: Mutex::new(HashMap::new()),
            in_flight: Arc::new(Semaphore::new(max_concurrent.max(1))),
        }
    }

    /// Claim the right to dial `url` now. Returns `false` when an attempt
    /// toward this URL was already claimed within the cooldown window.
    ///
    /// The claim is recorded whether or not the dial succeeds: a failed
    /// dial is the case the cooldown exists for.
    pub fn claim(&self, url: &Url) -> bool {
        let now = Instant::now();
        let mut last = self.last_attempt.lock().expect("dial policy poisoned");
        // Forget URLs whose window has passed so the map cannot grow with
        // every record ever heard.
        last.retain(|_, at| now.duration_since(*at) < self.cooldown);
        if last.contains_key(url) {
            return false;
        }
        last.insert(url.clone(), now);
        true
    }

    /// Wait for a slot in the in-flight cap. The slot is released when the
    /// returned permit is dropped.
    pub async fn acquire_slot(&self) -> OwnedSemaphorePermit {
        self.in_flight
            .clone()
            .acquire_owned()
            .await
            .expect("dial semaphore is never closed")
    }

    /// Number of dials that could start right now without waiting.
    pub fn free_slots(&self) -> usize {
        self.in_flight.available_permits()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(s: &str) -> Url {
        Url::from_str(s).unwrap()
    }

    #[test]
    fn same_url_is_claimed_once_per_cooldown() {
        let policy = DialPolicy::new(Duration::from_secs(60), 4);
        let a = url("ws://a.test:80/peerA");
        assert!(policy.claim(&a));
        assert!(!policy.claim(&a));
        assert!(!policy.claim(&a));
    }

    #[test]
    fn distinct_urls_do_not_share_a_cooldown() {
        let policy = DialPolicy::new(Duration::from_secs(60), 4);
        assert!(policy.claim(&url("ws://a.test:80/peerA")));
        assert!(policy.claim(&url("ws://b.test:80/peerB")));
    }

    #[test]
    fn a_url_can_be_claimed_again_after_the_cooldown() {
        let policy = DialPolicy::new(Duration::from_millis(20), 4);
        let a = url("ws://a.test:80/peerA");
        assert!(policy.claim(&a));
        assert!(!policy.claim(&a));
        std::thread::sleep(Duration::from_millis(40));
        assert!(policy.claim(&a));
    }

    #[tokio::test]
    async fn in_flight_dials_are_capped() {
        let policy = DialPolicy::new(Duration::from_secs(60), 2);
        assert_eq!(policy.free_slots(), 2);
        let p1 = policy.acquire_slot().await;
        let p2 = policy.acquire_slot().await;
        assert_eq!(policy.free_slots(), 0);

        let third = tokio::time::timeout(
            Duration::from_millis(50),
            policy.acquire_slot(),
        )
        .await;
        assert!(third.is_err(), "a third dial must wait for a free slot");

        drop(p1);
        assert_eq!(policy.free_slots(), 1);
        let _p3 = policy.acquire_slot().await;
        drop(p2);
        assert_eq!(policy.free_slots(), 1);
    }

    #[test]
    fn a_zero_cap_still_allows_one_dial() {
        let policy = DialPolicy::new(Duration::from_secs(60), 0);
        assert_eq!(policy.free_slots(), 1);
    }
}
