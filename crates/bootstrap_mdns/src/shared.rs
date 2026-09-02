//! The one mDNS presence a process keeps, shared by every space.
//!
//! A node that is in many spaces would otherwise start a daemon per space
//! — two multicast sockets, a thread and a probed hostname each — and
//! browse the same service type once per space, so that every node on the
//! LAN answered every space's query for every record. Here one daemon
//! browses once and the browse loop routes what it hears by fingerprint.
//! Spaces come and go against the registry; the daemon itself lives as
//! long as the factory that started it.

use crate::browse::{Registry, browse_loop};
use crate::discovery::DynDaemon;
use crate::fingerprint::SpaceFingerprint;
use crate::space::SpaceEntry;
use kitsune2_api::{DynTransport, K2Result, SpaceId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;
use tracing::debug;

/// A running daemon, the spaces registered on it and the browse loop that
/// feeds them.
#[derive(Debug)]
pub struct SharedMdns {
    daemon: DynDaemon,
    registry: Arc<Registry>,
    browse_task: JoinHandle<()>,
}

impl Drop for SharedMdns {
    fn drop(&mut self) {
        self.browse_task.abort();
    }
}

impl SharedMdns {
    /// Start browsing on `daemon` with no spaces registered yet.
    pub fn start(daemon: DynDaemon) -> K2Result<Arc<Self>> {
        let rx = daemon.browse()?;
        let registry: Arc<Registry> = Arc::new(Mutex::new(HashMap::new()));
        let browse_task = tokio::spawn(browse_loop(rx, registry.clone()));
        debug!(service_type = daemon.service_type(), "mdns: browsing");
        Ok(Arc::new(Self {
            daemon,
            registry,
            browse_task,
        }))
    }

    /// Register a space, so that records committing to `fp` reach it.
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
        if let Some(previous) = self
            .registry
            .lock()
            .expect("poison")
            .insert(fp, entry.clone())
        {
            previous.withdraw();
        }
        entry
    }

    /// Withdraw a space's record and stop routing to it. A registry slot
    /// taken over by a newer entry for the same space is left alone.
    pub fn leave(&self, entry: &Arc<SpaceEntry>) {
        let mut registry = self.registry.lock().expect("poison");
        if registry
            .get(entry.fingerprint())
            .is_some_and(|current| Arc::ptr_eq(current, entry))
        {
            registry.remove(entry.fingerprint());
        }
        drop(registry);
        entry.withdraw();
    }

    /// How many spaces are registered.
    #[cfg(test)]
    pub fn space_count(&self) -> usize {
        self.registry.lock().expect("poison").len()
    }
}
