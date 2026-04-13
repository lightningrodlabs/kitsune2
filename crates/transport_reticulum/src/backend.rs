//! Real `rns_transport` backend implementation of the `destination` traits.
//!
//! Thin wrappers over `rns_transport::transport::Transport`,
//! `SingleInputDestination`, and `Link`, used by [`ReticulumNode::from_config`].

use crate::destination::{
    AnnounceInfo, Destination, DynDestination, DynLink, Endpoint, Link,
    LinkId, LinkStatus,
};
use bytes::Bytes;
use kitsune2_api::{BoxFut, K2Error, K2Result};
use rand_core::OsRng;
use rns_transport::destination::DestinationName;
use rns_transport::hash::AddressHash;
use rns_transport::identity::{Identity, PrivateIdentity};
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;
use tracing::{debug, warn};

/// Shared handle to a live `rns_transport::Transport`.
pub(crate) type SharedTransport =
    Arc<TokioMutex<rns_transport::transport::Transport>>;

/// Real `Endpoint` implementation backed by `rns_transport::Transport`.
pub(crate) struct RealEndpoint {
    transport: SharedTransport,
    identity: PrivateIdentity,
    announce_bridge_tx:
        tokio::sync::broadcast::Sender<AnnounceInfo>,
}

impl std::fmt::Debug for RealEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealEndpoint")
            .field("identity_hash", &self.identity.as_identity().address_hash)
            .finish()
    }
}

impl RealEndpoint {
    /// Create a new `RealEndpoint`, spawning the announce bridge task.
    pub(crate) async fn new(
        transport: SharedTransport,
        identity: PrivateIdentity,
    ) -> Self {
        let (announce_bridge_tx, _) =
            tokio::sync::broadcast::channel::<AnnounceInfo>(256);

        // Subscribe once to the underlying announce stream and re-broadcast
        // a lightweight `AnnounceInfo` so consumers don't need `rns_transport`
        // types.
        let announce_rx = {
            let t = transport.lock().await;
            t.recv_announces().await
        };
        spawn_announce_bridge(announce_rx, announce_bridge_tx.clone());

        Self {
            transport,
            identity,
            announce_bridge_tx,
        }
    }

    pub(crate) fn identity(&self) -> &PrivateIdentity {
        &self.identity
    }

    pub(crate) fn transport(&self) -> &SharedTransport {
        &self.transport
    }
}

fn spawn_announce_bridge(
    mut announce_rx: tokio::sync::broadcast::Receiver<
        rns_transport::transport::AnnounceEvent,
    >,
    tx: tokio::sync::broadcast::Sender<AnnounceInfo>,
) {
    tokio::spawn(async move {
        loop {
            match announce_rx.recv().await {
                Ok(ev) => {
                    // Validate the announce to reconstruct the full Identity.
                    // The announce packet lives on the destination; read its
                    // identity via the lock and synthesize an `AnnounceInfo`.
                    let identity = {
                        let dest = ev.destination.lock().await;
                        dest.identity
                    };
                    let app_data =
                        Bytes::copy_from_slice(ev.app_data.as_slice());
                    let info = AnnounceInfo {
                        identity,
                        app_data,
                        name_hash: ev.name_hash,
                        hops: ev.hops,
                    };
                    // Best effort: if no subscribers, drop.
                    let _ = tx.send(info);
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    warn!(n, "announce bridge lagged");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    debug!("announce bridge closed");
                    break;
                }
            }
        }
    });
}

impl Endpoint for RealEndpoint {
    fn add_destination(
        &self,
        name: DestinationName,
    ) -> BoxFut<'_, K2Result<DynDestination>> {
        Box::pin(async move {
            let identity = self.identity.clone();
            let dest_arc = {
                let mut t = self.transport.lock().await;
                t.add_destination(identity, name).await
            };
            let dest = RealDestination::new(dest_arc, name).await;
            Ok(Arc::new(dest) as DynDestination)
        })
    }

    fn link_to(
        &self,
        _identity: Identity,
        _app_name: String,
        _aspect: String,
    ) -> BoxFut<'_, K2Result<DynLink>> {
        Box::pin(async move {
            Err(K2Error::other(
                "RealEndpoint::link_to not yet implemented",
            ))
        })
    }

    fn send_packet(&self, _packet: &[u8]) -> BoxFut<'_, K2Result<()>> {
        Box::pin(async move {
            Err(K2Error::other(
                "RealEndpoint::send_packet not yet implemented",
            ))
        })
    }

    fn send_resource(
        &self,
        _link_id: &LinkId,
        _data: &[u8],
    ) -> BoxFut<'_, K2Result<()>> {
        Box::pin(async move {
            Err(K2Error::other(
                "RealEndpoint::send_resource not yet implemented",
            ))
        })
    }

    fn packet_mdu(&self) -> usize {
        rns_transport::packet::PACKET_MDU
    }

    fn recv_announces(
        &self,
    ) -> BoxFut<
        '_,
        K2Result<tokio::sync::broadcast::Receiver<AnnounceInfo>>,
    > {
        Box::pin(async move { Ok(self.announce_bridge_tx.subscribe()) })
    }

    fn recv_resource_data(
        &self,
    ) -> BoxFut<
        '_,
        K2Result<tokio::sync::mpsc::Receiver<(LinkId, Bytes)>>,
    > {
        Box::pin(async move {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            // TODO: bridge from Transport::received_data_events().
            Ok(rx)
        })
    }

    fn recv_links(
        &self,
    ) -> BoxFut<
        '_,
        K2Result<tokio::sync::mpsc::Receiver<DynLink>>,
    > {
        Box::pin(async move {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            // TODO: bridge from Transport::in_link_events().
            Ok(rx)
        })
    }
}

/// Real `Destination` implementation.
struct RealDestination {
    inner: Arc<
        TokioMutex<rns_transport::destination::SingleInputDestination>,
    >,
    name: DestinationName,
    address_hash: AddressHash,
}

impl std::fmt::Debug for RealDestination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealDestination")
            .field("address_hash", &self.address_hash)
            .finish()
    }
}

impl RealDestination {
    async fn new(
        inner: Arc<
            TokioMutex<rns_transport::destination::SingleInputDestination>,
        >,
        name: DestinationName,
    ) -> Self {
        let address_hash = {
            let d = inner.lock().await;
            d.desc.address_hash
        };
        Self {
            inner,
            name,
            address_hash,
        }
    }
}

impl Destination for RealDestination {
    fn address_hash(&self) -> AddressHash {
        self.address_hash
    }

    fn name(&self) -> DestinationName {
        self.name
    }

    fn announce<'a>(
        &'a self,
        app_data: Option<&'a [u8]>,
    ) -> BoxFut<'a, K2Result<Vec<u8>>> {
        Box::pin(async move {
            let mut guard = self.inner.lock().await;
            let packet =
                guard.announce(OsRng, app_data).map_err(|e| {
                    K2Error::other(format!("rns announce failed: {e:?}"))
                })?;
            Ok(packet.data.as_slice().to_vec())
        })
    }
}

/// Real `Link` implementation (scaffolded).
pub(crate) struct RealLink {
    inner: Arc<TokioMutex<rns_transport::destination::link::Link>>,
    id: LinkId,
    peer_hash: AddressHash,
}

impl std::fmt::Debug for RealLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealLink")
            .field("id", &self.id)
            .field("peer_hash", &self.peer_hash)
            .finish()
    }
}

impl Link for RealLink {
    fn id(&self) -> LinkId {
        self.id
    }

    fn peer_identity_hash(&self) -> AddressHash {
        self.peer_hash
    }

    fn status(&self) -> LinkStatus {
        // Map rns_transport::LinkStatus -> ours without blocking: use try_lock.
        match self.inner.try_lock() {
            Ok(g) => match g.status() {
                rns_transport::destination::link::LinkStatus::Pending => {
                    LinkStatus::Pending
                }
                rns_transport::destination::link::LinkStatus::Handshake => {
                    LinkStatus::Handshake
                }
                rns_transport::destination::link::LinkStatus::Active => {
                    LinkStatus::Active
                }
                rns_transport::destination::link::LinkStatus::Stale => {
                    LinkStatus::Stale
                }
                rns_transport::destination::link::LinkStatus::Closed => {
                    LinkStatus::Closed
                }
            },
            Err(_) => LinkStatus::Active,
        }
    }

    fn data_packet(&self, _data: &[u8]) -> K2Result<Vec<u8>> {
        Err(K2Error::other(
            "RealLink::data_packet not yet implemented",
        ))
    }

    fn teardown(&self) -> Option<Vec<u8>> {
        None
    }
}
