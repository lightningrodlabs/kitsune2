//! The one mDNS presence a factory keeps, shared by every space it
//! creates.
//!
//! A node that is in many spaces would otherwise start a daemon per space
//! — two multicast sockets, a thread and a probed hostname each — and
//! browse the same service type once per space, so that every node on the
//! LAN answered every space's query for every record. Here one daemon
//! browses once and the browse loop routes what it hears by fingerprint.
//! Spaces come and go against the registry; the daemon itself lives as
//! long as the factory that started it.

use crate::browse::{SharedBrowseState, browse_loop};
use crate::discovery::DynDaemon;
use crate::fingerprint::SpaceFingerprint;
use crate::space::SpaceEntry;
use kitsune2_api::{DynTransport, K2Result, SpaceId};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::debug;

/// A running daemon, the spaces registered on it, the browse loop that
/// feeds them and the ticker that reconciles them.
#[derive(Debug)]
pub struct SharedMdns {
    daemon: DynDaemon,
    state: SharedBrowseState,
    browse_task: JoinHandle<()>,
    reconcile_task: JoinHandle<()>,
}

impl Drop for SharedMdns {
    fn drop(&mut self) {
        self.browse_task.abort();
        self.reconcile_task.abort();
    }
}

impl SharedMdns {
    /// Start browsing on `daemon` with no spaces registered yet, and
    /// reconciling every `interval` — the interval of whichever space
    /// starts the daemon; spaces joining later share it.
    pub fn start(daemon: DynDaemon, interval: Duration) -> K2Result<Arc<Self>> {
        let rx = daemon.browse()?;
        let state: SharedBrowseState = Default::default();
        let browse_task = tokio::spawn(browse_loop(rx, state.clone()));
        let reconcile_task =
            tokio::spawn(reconcile_loop(state.clone(), interval));
        debug!(service_type = daemon.service_type(), "mdns: browsing");
        Ok(Arc::new(Self {
            daemon,
            state,
            browse_task,
            reconcile_task,
        }))
    }

    /// Register a space, so that records committing to `fp` reach it —
    /// those heard before it joined included.
    pub fn join(
        &self,
        space_id: SpaceId,
        fp: SpaceFingerprint,
        tx: DynTransport,
        max_concurrent_dials: usize,
    ) -> Arc<SpaceEntry> {
        let entry = SpaceEntry::new(
            space_id,
            fp,
            self.daemon.clone(),
            tx,
            max_concurrent_dials,
        );
        let previous =
            self.state.lock().expect("poison").register(entry.clone());
        if let Some(previous) = previous {
            previous.withdraw();
        }
        entry
    }

    /// Withdraw a space's record and stop routing to it.
    pub fn leave(&self, entry: &Arc<SpaceEntry>) {
        self.state.lock().expect("poison").unregister(entry);
        entry.withdraw();
    }

    /// How many spaces are registered.
    #[cfg(test)]
    pub fn space_count(&self) -> usize {
        self.state.lock().expect("poison").space_count()
    }
}

/// Every `interval`, let every registered space that has heard anything
/// reconcile against its own transport's connections. One ticker drives
/// them all; the connection lists are the spaces' own, since a space is
/// created with its transport and nothing says two spaces share one.
async fn reconcile_loop(state: SharedBrowseState, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first tick completes immediately; announcements dial themselves
    // on arrival, so the first round of reconciliation waits an interval.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let entries = state.lock().expect("poison").entries();
        for entry in entries {
            if entry.has_announcements() {
                entry.reconcile_from_transport().await;
            }
        }
    }
}
