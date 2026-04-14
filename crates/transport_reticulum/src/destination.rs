//! Abstraction traits for Reticulum transport operations.
//!
//! These traits mirror the pattern from `transport_iroh/src/endpoint.rs`,
//! wrapping `rns_transport` types so unit tests can swap in fakes.

use bytes::Bytes;
use kitsune2_api::{BoxFut, K2Result};
use rns_transport::destination::DestinationName;
use rns_transport::hash::AddressHash;
use rns_transport::identity::Identity;
use std::sync::Arc;

/// LinkId alias -- `rns_transport` uses `AddressHash` directly.
pub(crate) type LinkId = AddressHash;

/// Status of a Reticulum Link.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LinkStatus {
    /// Link request sent, waiting for proof.
    Pending,
    /// Handshake in progress.
    Handshake,
    /// Link is active and ready for data.
    Active,
    /// Link has gone stale (no recent traffic).
    Stale,
    /// Link has been closed.
    Closed,
}

/// Trait abstracting a Reticulum Link for testability.
pub(crate) trait Link: Send + Sync + std::fmt::Debug {
    /// Get this link's ID.
    fn id(&self) -> LinkId;

    /// Get the peer's public Identity address hash.
    fn peer_identity_hash(&self) -> AddressHash;

    /// Get the address hash of the **local** destination this link
    /// terminates at.
    ///
    /// For inbound links, this tells us *which of our per-space
    /// destinations* the peer linked to, so the links router can map
    /// the link to a SpaceId. For outbound links we already know the
    /// destination, but exposing it here uniformly keeps the router
    /// logic symmetric.
    fn local_destination_hash(&self) -> AddressHash;

    /// Get the current link status.
    fn status(&self) -> LinkStatus;

    /// Send a small payload as a single rns Packet (≤ PACKET_MDU).
    ///
    /// This bypasses the Resource advertise/request/fragments/proof
    /// handshake. Used for preflight frames and any other payload
    /// that fits in the MDU. The data router on the receiver side
    /// gets it via `Endpoint::recv_resource_data` (which the
    /// `RealEndpoint` bridges from `received_data_events` for
    /// `data_packet` traffic).
    fn send_small<'a>(&'a self, data: &'a [u8]) -> BoxFut<'a, K2Result<()>>;

    /// Tear down the link, returning a teardown packet if applicable.
    fn teardown(&self) -> Option<Vec<u8>>;
}

/// Trait abstracting a Reticulum Destination (per-space, inbound).
pub(crate) trait Destination: Send + Sync + std::fmt::Debug {
    /// The destination's address hash.
    fn address_hash(&self) -> AddressHash;

    /// The destination name (app + aspect).
    fn name(&self) -> DestinationName;

    /// Create and return an announce packet.
    ///
    /// Async because the real impl needs to acquire a `tokio::Mutex` on
    /// the underlying `SingleInputDestination`.
    fn announce<'a>(
        &'a self,
        app_data: Option<&'a [u8]>,
    ) -> BoxFut<'a, K2Result<Vec<u8>>>;
}

/// Announce event received from the network.
///
/// `Identity` is `Copy` but does not implement `Debug`, so we provide
/// a manual `Debug` impl that elides the key material.
#[derive(Clone)]
pub(crate) struct AnnounceInfo {
    /// The full peer Identity extracted from the announce.
    pub identity: Identity,
    /// Application data attached to the announce.
    pub app_data: Bytes,
    /// The name hash for aspect filtering.
    pub name_hash: [u8; 10],
    /// Hop count.
    pub hops: u8,
}

impl std::fmt::Debug for AnnounceInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnnounceInfo")
            .field("identity_hash", &self.identity.address_hash)
            .field("app_data_len", &self.app_data.len())
            .field("name_hash", &self.name_hash)
            .field("hops", &self.hops)
            .finish()
    }
}

/// Trait abstracting the Reticulum transport endpoint.
pub(crate) trait Endpoint:
    'static + Send + Sync + std::fmt::Debug
{
    /// Add a new destination (per-space) to the transport.
    fn add_destination(
        &self,
        name: DestinationName,
    ) -> BoxFut<'_, K2Result<Arc<dyn Destination>>>;

    /// Initiate a link to a peer by their Identity and target aspect.
    fn link_to(
        &self,
        identity: Identity,
        app_name: String,
        aspect: String,
    ) -> BoxFut<'_, K2Result<Arc<dyn Link>>>;

    /// Send a raw packet (e.g. from `Link::data_packet`).
    fn send_packet(&self, packet: &[u8]) -> BoxFut<'_, K2Result<()>>;

    /// Send a large payload via the Resource abstraction.
    fn send_resource(
        &self,
        link_id: &LinkId,
        data: &[u8],
    ) -> BoxFut<'_, K2Result<()>>;

    /// Get the packet MDU (max data unit for a single packet).
    fn packet_mdu(&self) -> usize;

    /// Subscribe to announce events.
    fn recv_announces(
        &self,
    ) -> BoxFut<'_, K2Result<tokio::sync::broadcast::Receiver<AnnounceInfo>>>;

    /// Subscribe to incoming resource data events.
    fn recv_resource_data(
        &self,
    ) -> BoxFut<'_, K2Result<tokio::sync::mpsc::Receiver<(LinkId, Bytes)>>>;

    /// Subscribe to incoming link events (new inbound links).
    fn recv_links(
        &self,
    ) -> BoxFut<'_, K2Result<tokio::sync::mpsc::Receiver<Arc<dyn Link>>>>;

    /// Subscribe to link-close events. Each yielded `LinkId` identifies
    /// a link that has transitioned to `LinkStatus::Closed` (either
    /// because the remote tore it down or because rns timed it out).
    ///
    /// A single close receiver is consumed by the transport's
    /// link-close router, which decrements the per-peer refcount and
    /// fires `TxImpHnd::peer_disconnect` on the last-link close.
    fn recv_link_closures(
        &self,
    ) -> BoxFut<'_, K2Result<tokio::sync::mpsc::Receiver<LinkId>>>;
}

pub(crate) type DynEndpoint = Arc<dyn Endpoint>;
pub(crate) type DynLink = Arc<dyn Link>;
pub(crate) type DynDestination = Arc<dyn Destination>;
