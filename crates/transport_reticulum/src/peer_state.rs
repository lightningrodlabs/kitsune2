//! Two-level connection map: per-peer refcount + per-space LinkContext.
//!
//! This module isolates the complexity of mapping kitsune2's per-peer
//! `TxImp` model onto Reticulum's per-(peer, space) Link model.

use crate::destination::DynLink;
use kitsune2_api::SpaceId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// State for a single per-space link to a peer.
#[derive(Debug)]
pub(crate) struct LinkContext {
    /// The Reticulum link for this (peer, space) pair.
    pub link: DynLink,
    /// Space ID this link is associated with.
    pub space_id: SpaceId,
}

/// Preflight state for a peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PreflightState {
    /// Preflight exchange has not started.
    None,
    /// We have sent our preflight, waiting for remote's.
    Sent,
    /// Preflight exchange is complete.
    Ready,
}

/// Per-peer state, containing preflight status and per-space links.
#[derive(Debug)]
pub(crate) struct PeerState {
    /// Preflight state -- per-peer, not per-link.
    pub preflight_state: Mutex<PreflightState>,
    /// Per-space links. Key is the SpaceId.
    pub links: Mutex<HashMap<SpaceId, LinkContext>>,
}

impl PeerState {
    /// Create a new PeerState with no links and no preflight.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            preflight_state: Mutex::new(PreflightState::None),
            links: Mutex::new(HashMap::new()),
        })
    }

    /// Insert a link for a given space. Returns true if this is the first
    /// link for this peer (i.e., `peer_connect` should be triggered).
    pub fn insert_link(&self, space_id: SpaceId, link: DynLink) -> bool {
        let mut links = self.links.lock().expect("poisoned");
        let was_empty = links.is_empty();
        links.insert(space_id.clone(), LinkContext { link, space_id });
        was_empty
    }

    /// Remove the link for a given space. Returns true if this was the
    /// last link for this peer (i.e., `peer_disconnect` should be triggered).
    pub fn remove_link(&self, space_id: &SpaceId) -> bool {
        let mut links = self.links.lock().expect("poisoned");
        links.remove(space_id);
        links.is_empty()
    }

    /// Get the link for a specific space, if any.
    pub fn get_link(&self, space_id: &SpaceId) -> Option<DynLink> {
        self.links
            .lock()
            .expect("poisoned")
            .get(space_id)
            .map(|ctx| ctx.link.clone())
    }

    /// Get any link (used for preflight routing when no specific space is targeted).
    pub fn any_link(&self) -> Option<DynLink> {
        self.links
            .lock()
            .expect("poisoned")
            .values()
            .next()
            .map(|ctx| ctx.link.clone())
    }

    /// Number of active per-space links.
    pub fn link_count(&self) -> usize {
        self.links.lock().expect("poisoned").len()
    }

    /// Tear down all links.
    pub fn teardown_all_links(&self) {
        let links = self.links.lock().expect("poisoned");
        for ctx in links.values() {
            let _ = ctx.link.teardown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::destination::{LinkId, LinkStatus};
    use rns_transport::hash::AddressHash;

    /// A fake link for testing peer_state logic.
    #[derive(Debug)]
    struct FakeLink {
        id: LinkId,
    }

    impl FakeLink {
        fn new(id: u8) -> Arc<Self> {
            Arc::new(Self {
                id: AddressHash::new([id; 16]),
            })
        }
    }

    impl crate::destination::Link for FakeLink {
        fn id(&self) -> LinkId {
            self.id
        }
        fn peer_identity_hash(&self) -> AddressHash {
            AddressHash::new([0u8; 16])
        }
        fn status(&self) -> LinkStatus {
            LinkStatus::Active
        }
        fn data_packet(&self, _data: &[u8]) -> kitsune2_api::K2Result<Vec<u8>> {
            Ok(Vec::new())
        }
        fn teardown(&self) -> Option<Vec<u8>> {
            None
        }
    }

    fn space(s: &str) -> SpaceId {
        SpaceId::from(bytes::Bytes::from(s.to_string()))
    }

    #[test]
    fn first_link_triggers_connect() {
        let state = PeerState::new();
        let is_first = state.insert_link(space("s1"), FakeLink::new(1));
        assert!(is_first, "first link should trigger peer_connect");
    }

    #[test]
    fn second_link_does_not_trigger_connect() {
        let state = PeerState::new();
        state.insert_link(space("s1"), FakeLink::new(1));
        let is_first = state.insert_link(space("s2"), FakeLink::new(2));
        assert!(!is_first, "second link should not re-trigger peer_connect");
    }

    #[test]
    fn last_link_removal_triggers_disconnect() {
        let state = PeerState::new();
        state.insert_link(space("s1"), FakeLink::new(1));
        state.insert_link(space("s2"), FakeLink::new(2));

        let is_last = state.remove_link(&space("s1"));
        assert!(
            !is_last,
            "penultimate removal should not trigger disconnect"
        );

        let is_last = state.remove_link(&space("s2"));
        assert!(is_last, "last removal should trigger peer_disconnect");
    }

    #[test]
    fn preflight_state_transitions() {
        let state = PeerState::new();
        assert_eq!(
            *state.preflight_state.lock().unwrap(),
            PreflightState::None
        );

        *state.preflight_state.lock().unwrap() = PreflightState::Sent;
        assert_eq!(
            *state.preflight_state.lock().unwrap(),
            PreflightState::Sent
        );

        *state.preflight_state.lock().unwrap() = PreflightState::Ready;
        assert_eq!(
            *state.preflight_state.lock().unwrap(),
            PreflightState::Ready
        );
    }

    #[test]
    fn get_link_by_space() {
        let state = PeerState::new();
        state.insert_link(space("s1"), FakeLink::new(1));
        assert!(state.get_link(&space("s1")).is_some());
        assert!(state.get_link(&space("s2")).is_none());
    }

    #[test]
    fn any_link_returns_something() {
        let state = PeerState::new();
        assert!(state.any_link().is_none());
        state.insert_link(space("s1"), FakeLink::new(1));
        assert!(state.any_link().is_some());
    }
}
