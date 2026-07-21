//! The physical-layer abstraction all broadcast backends implement.
//!
//! A [`BroadcastMedium`] models a shared "air": frames are transmitted
//! into it with no addressing, no delivery guarantee and no notion of a
//! connection, and every node in range hears every frame. Everything
//! kitsune2-specific (peers, spaces, virtual connections, chunking)
//! lives above this trait, so a backend only has to answer "how do I
//! put bytes into the air and hear bytes from it".

use bytes::Bytes;
use futures::stream::BoxStream;
use kitsune2_api::{BoxFut, K2Result};

/// A physical broadcast medium.
///
/// Implementations must be cheap to share (`Arc`) and must tolerate
/// concurrent `transmit` calls. Frames may be lost, duplicated or
/// reordered by the medium; the layers above are designed for that.
pub trait BroadcastMedium: 'static + Send + Sync + std::fmt::Debug {
    /// Short stable name of the medium kind, e.g. `"mem"` or `"udpm"`.
    ///
    /// Used as the host label in peer urls
    /// (`ws://<kind>.bcast:1/<node-id>`), so it must be a valid DNS
    /// label: lowercase alphanumerics only.
    fn kind(&self) -> &'static str;

    /// The largest frame this medium can carry in one transmission.
    fn mtu(&self) -> usize;

    /// Rough sustained throughput in bytes per second.
    ///
    /// Not a limit — used by the layers above to scale timers (idle
    /// timeouts, reassembly eviction) to the speed of the medium.
    fn est_bytes_per_sec(&self) -> u32;

    /// True if the medium cannot listen while transmitting
    /// (e.g. sound, screen). Reserved for phase-2 scheduling; the
    /// phase-1 media are all full duplex.
    fn half_duplex(&self) -> bool;

    /// Fire a frame into the air. Best effort: an `Ok` result means the
    /// frame was handed to the medium, not that anyone heard it.
    ///
    /// Callers must respect [`Self::mtu`]; implementations should error
    /// on oversized frames rather than truncate.
    fn transmit(&self, frame: Bytes) -> BoxFut<'_, K2Result<()>>;

    /// The stream of every frame heard on the medium.
    ///
    /// Depending on the backend this may include our own transmissions;
    /// the frame layer filters those by sender id. Called once per
    /// transport instance.
    fn frames(&self) -> BoxStream<'static, Bytes>;
}

/// Trait-object [`BroadcastMedium`].
pub type DynBroadcastMedium = std::sync::Arc<dyn BroadcastMedium>;
