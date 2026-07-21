//! An in-process "shared air" medium for deterministic tests.
//!
//! All transports holding the same [`MemAir`] instance hear each
//! other's frames (including their own — the transport filters those by
//! sender id, exactly as it must on real media). Configurable loss and
//! latency let tests exercise the lossy-medium behavior of the layers
//! above without any real I/O.
//!
//! Transports configured with `medium = "mem"` through the factory all
//! share one process-global instance, mirroring the process-global
//! registry of core's `MemTransport`. Tests that need isolation from
//! other tests in the same process should construct their own instance
//! via [`MemAir::create`] and use
//! [`BroadcastTransportFactory::create_with_medium`](crate::BroadcastTransportFactory::create_with_medium).

use crate::medium::{BroadcastMedium, DynBroadcastMedium};
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use kitsune2_api::{BoxFut, K2Error, K2Result};
use std::sync::{Arc, OnceLock};

/// Configuration for a [`MemAir`] instance.
#[derive(Debug, Clone)]
pub struct MemAirConfig {
    /// Largest frame the air will carry. Default 1400, matching the
    /// udp multicast medium so tests exercise the same chunking
    /// boundaries.
    pub mtu: usize,

    /// Probability in `[0.0, 1.0]` that any receiver independently
    /// fails to hear a frame. Default 0.0.
    pub loss: f64,

    /// Artificial delay applied to every transmission. Default zero.
    pub latency: std::time::Duration,
}

impl Default for MemAirConfig {
    fn default() -> Self {
        Self {
            mtu: 1400,
            loss: 0.0,
            latency: std::time::Duration::ZERO,
        }
    }
}

/// An in-process broadcast medium.
pub struct MemAir {
    config: MemAirConfig,
    channel: tokio::sync::broadcast::Sender<Bytes>,
}

impl std::fmt::Debug for MemAir {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemAir")
            .field("config", &self.config)
            .finish()
    }
}

impl MemAir {
    /// Create a new, isolated patch of air.
    pub fn create(config: MemAirConfig) -> Arc<Self> {
        // Deep capacity: the air should drop frames because tests asked
        // it to (loss), not because a receiver lagged.
        let (channel, _) = tokio::sync::broadcast::channel(4096);
        Arc::new(Self { config, channel })
    }

    /// The process-global instance used by `medium = "mem"` config.
    pub fn global() -> Arc<Self> {
        static GLOBAL: OnceLock<Arc<MemAir>> = OnceLock::new();
        GLOBAL
            .get_or_init(|| MemAir::create(MemAirConfig::default()))
            .clone()
    }
}

impl BroadcastMedium for MemAir {
    fn kind(&self) -> &'static str {
        "mem"
    }

    fn mtu(&self) -> usize {
        self.config.mtu
    }

    fn est_bytes_per_sec(&self) -> u32 {
        // In-process: effectively instant.
        u32::MAX
    }

    fn half_duplex(&self) -> bool {
        false
    }

    fn transmit(&self, frame: Bytes) -> BoxFut<'_, K2Result<()>> {
        Box::pin(async move {
            if frame.len() > self.config.mtu {
                return Err(K2Error::other(format!(
                    "frame of {} bytes exceeds mem air mtu {}",
                    frame.len(),
                    self.config.mtu
                )));
            }
            if !self.config.latency.is_zero() {
                tokio::time::sleep(self.config.latency).await;
            }
            // An air with no listeners swallows the frame; that is
            // normal broadcast behavior, not an error.
            let _ = self.channel.send(frame);
            Ok(())
        })
    }

    fn frames(&self) -> BoxStream<'static, Bytes> {
        let receiver = self.channel.subscribe();
        let loss = self.config.loss;
        futures::stream::unfold(receiver, move |mut receiver| async move {
            loop {
                match receiver.recv().await {
                    Ok(frame) => {
                        if loss > 0.0
                            && rand::Rng::r#gen::<f64>(&mut rand::thread_rng())
                                < loss
                        {
                            continue;
                        }
                        return Some((frame, receiver));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(
                        n,
                    )) => {
                        tracing::warn!(dropped = n, "mem air receiver lagged");
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        return None;
                    }
                }
            }
        })
        .boxed()
    }
}

/// Convenience: an isolated air as a [`DynBroadcastMedium`].
pub fn mem_medium(config: MemAirConfig) -> DynBroadcastMedium {
    MemAir::create(config)
}
