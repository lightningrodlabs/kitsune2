#![deny(missing_docs)]
//! A kitsune2 transport that multiplexes several backend transports and
//! selects — and can switch — the active one at runtime.
//!
//! A single binary compiles in every transport backend it supports
//! (iroh, broadcast media, later reticulum, ...); cargo features
//! control what is *compiled in*, while module config controls what is
//! *active*. This is the same single-binary gating pattern the
//! mdns-bootstrap work established for bootstrap modules.
//!
//! # Selection
//!
//! The `switchTransport.active` config key names the active backend.
//! Each backend also contributes its own module config (e.g.
//! `irohTransport`, `broadcastTransport`) exactly as it would when used
//! directly.
//!
//! # Runtime switching
//!
//! The factory registers a config update callback on
//! `switchTransport.active`. Setting that key at runtime — e.g. from a
//! conductor admin call plumbed through to
//! [`kitsune2_api::Config::set_module_config`] — tears up a fresh
//! backend, replays all space and module handler registrations onto
//! it, swaps it in, and announces the new listening address so spaces
//! re-sign and re-publish their agent infos. From the network's
//! perspective the node simply moved, which is exactly what happened;
//! peers reachable only on the old transport age out through the
//! normal unresponsive path.
//!
//! Switching is deliberately modeled as single-active. Running several
//! backends concurrently (hear on all, send on best) needs multi-url
//! agent infos and is out of scope here; see
//! `docs/design/broadcast-transport.md` §4.

use kitsune2_api::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

/// SwitchTransport configuration types.
pub mod config {
    /// Configuration for the
    /// [`SwitchableTransportFactory`](super::SwitchableTransportFactory).
    #[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
    #[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
    #[serde(rename_all = "camelCase")]
    pub struct SwitchTransportConfig {
        /// The name of the active backend.
        ///
        /// Defaults to the first registered backend.
        pub active: String,
    }

    /// Module-level config wrapper.
    #[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
    #[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
    #[serde(rename_all = "camelCase")]
    pub struct SwitchTransportModConfig {
        /// The actual config for the switch.
        pub switch_transport: SwitchTransportConfig,
    }
}

pub use config::*;

/// Factory wrapping a set of named backend [`TransportFactory`]
/// instances.
#[derive(Debug)]
pub struct SwitchableTransportFactory {
    backends: Vec<(String, DynTransportFactory)>,
}

impl SwitchableTransportFactory {
    /// Construct over the given `(name, factory)` backends.
    ///
    /// Names must be unique and non-empty; the first backend is the
    /// default. Panics on an empty list — a switch with nothing to
    /// switch between is a programmer error.
    pub fn create(
        backends: Vec<(String, DynTransportFactory)>,
    ) -> DynTransportFactory {
        if backends.is_empty() {
            panic!("SwitchableTransportFactory needs at least one backend");
        }
        Arc::new(Self { backends })
    }

    fn backend(&self, name: &str) -> Option<DynTransportFactory> {
        self.backends
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, f)| f.clone())
    }
}

impl TransportFactory for SwitchableTransportFactory {
    fn default_config(&self, config: &mut Config) -> K2Result<()> {
        for (_, factory) in &self.backends {
            factory.default_config(config)?;
        }
        config.set_module_config(&SwitchTransportModConfig {
            switch_transport: SwitchTransportConfig {
                active: self.backends[0].0.clone(),
            },
        })
    }

    fn validate_config(&self, config: &Config) -> K2Result<()> {
        let mod_config: SwitchTransportModConfig =
            config.get_module_config()?;
        let active = &mod_config.switch_transport.active;
        if self.backend(active).is_none() {
            return Err(K2Error::other(format!(
                "switchTransport.active names unknown backend {active:?} \
                 (available: {:?})",
                self.backends.iter().map(|(n, _)| n).collect::<Vec<_>>()
            )));
        }
        // Validate every backend, not just the active one: the switch
        // can move to any of them at runtime.
        for (_, factory) in &self.backends {
            factory.validate_config(config)?;
        }
        Ok(())
    }

    fn create(
        &self,
        builder: Arc<Builder>,
        handler: DynTxHandler,
    ) -> BoxFut<'static, K2Result<DynTransport>> {
        let backends = self.backends.clone();
        Box::pin(async move {
            let mod_config: SwitchTransportModConfig =
                builder.config.get_module_config()?;
            let active = mod_config.switch_transport.active;
            let factory = backends
                .iter()
                .find(|(n, _)| n == &active)
                .map(|(_, f)| f.clone())
                .ok_or_else(|| {
                    K2Error::other(format!(
                        "switchTransport.active names unknown backend \
                         {active:?}"
                    ))
                })?;

            let inner =
                factory.create(builder.clone(), handler.clone()).await?;

            let out = Arc::new(SwitchTransport {
                backends,
                builder: builder.clone(),
                handler,
                current: Mutex::new(CurrentBackend {
                    name: active,
                    transport: inner,
                }),
                registrations: Mutex::new(Registrations::default()),
                switch_serial: tokio::sync::Mutex::new(()),
            });

            // Runtime switching: react to switchTransport.active
            // updates. Weak, so dropping the transport also retires
            // the callback's grip on it.
            let weak: Weak<SwitchTransport> = Arc::downgrade(&out);
            builder.config.register_entry_update_cb(
                &["switchTransport", "active"],
                Arc::new(move |value| {
                    let Some(this) = weak.upgrade() else {
                        return;
                    };
                    let Some(name) = value.as_str().map(str::to_string) else {
                        tracing::warn!(
                            ?value,
                            "ignoring non-string switchTransport.active"
                        );
                        return;
                    };
                    tokio::task::spawn(async move {
                        if let Err(err) = this.switch_to(&name).await {
                            tracing::error!(
                                ?err,
                                backend = %name,
                                "transport switch failed; keeping current \
                                 backend"
                            );
                        }
                    });
                }),
            )?;

            Ok(out as DynTransport)
        })
    }
}

#[derive(Debug)]
struct CurrentBackend {
    name: String,
    transport: DynTransport,
}

/// Handler registrations to replay onto a newly activated backend.
#[derive(Debug, Default)]
struct Registrations {
    spaces: HashMap<SpaceId, DynTxSpaceHandler>,
    modules: HashMap<(SpaceId, String), DynTxModuleHandler>,
}

/// The switching transport. See the crate docs.
#[derive(Debug)]
struct SwitchTransport {
    backends: Vec<(String, DynTransportFactory)>,
    builder: Arc<Builder>,
    handler: DynTxHandler,
    current: Mutex<CurrentBackend>,
    registrations: Mutex<Registrations>,
    /// Serializes switches; a switch that races a switch is resolved
    /// in arrival order.
    switch_serial: tokio::sync::Mutex<()>,
}

impl SwitchTransport {
    fn inner(&self) -> DynTransport {
        self.current.lock().unwrap().transport.clone()
    }

    /// Activate the named backend, replaying all registrations.
    async fn switch_to(&self, name: &str) -> K2Result<()> {
        let _serial = self.switch_serial.lock().await;

        if self.current.lock().unwrap().name == name {
            return Ok(());
        }
        let factory = self
            .backends
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, f)| f.clone())
            .ok_or_else(|| {
                K2Error::other(format!(
                    "cannot switch to unknown transport backend {name:?}"
                ))
            })?;

        tracing::info!(backend = %name, "switching transport backend");

        let fresh = factory
            .create(self.builder.clone(), self.handler.clone())
            .await?;

        // Replay registrations onto the fresh backend before exposing
        // it, so no incoming event can find a missing handler.
        let (spaces, modules) = {
            let registrations = self.registrations.lock().unwrap();
            (registrations.spaces.clone(), registrations.modules.clone())
        };
        let mut new_url = None;
        for (space_id, handler) in &spaces {
            new_url =
                fresh.register_space_handler(space_id.clone(), handler.clone());
        }
        for ((space_id, module), handler) in &modules {
            fresh.register_module_handler(
                space_id.clone(),
                module.clone(),
                handler.clone(),
            );
        }

        // Swap. Dropping the old transport tears down its tasks and
        // connections.
        {
            let mut current = self.current.lock().unwrap();
            current.name = name.to_string();
            current.transport = fresh;
        }

        // The node moved: announce the new address so spaces re-sign
        // and re-publish agent infos. (The backend already announced
        // to the top-level handler during its own create, before the
        // space handlers were replayed.)
        if let Some(url) = new_url {
            for handler in spaces.values() {
                handler.new_listening_address(url.clone()).await;
            }
        }

        Ok(())
    }
}

impl Transport for SwitchTransport {
    fn register_space_handler(
        &self,
        space_id: SpaceId,
        handler: DynTxSpaceHandler,
    ) -> Option<Url> {
        self.registrations
            .lock()
            .unwrap()
            .spaces
            .insert(space_id.clone(), handler.clone());
        self.inner().register_space_handler(space_id, handler)
    }

    fn register_module_handler(
        &self,
        space_id: SpaceId,
        module: String,
        handler: DynTxModuleHandler,
    ) {
        self.registrations
            .lock()
            .unwrap()
            .modules
            .insert((space_id.clone(), module.clone()), handler.clone());
        self.inner()
            .register_module_handler(space_id, module, handler)
    }

    fn disconnect(&self, peer: Url, reason: Option<String>) -> BoxFut<'_, ()> {
        let inner = self.inner();
        Box::pin(async move { inner.disconnect(peer, reason).await })
    }

    fn send_space_notify(
        &self,
        peer: Url,
        space_id: SpaceId,
        data: bytes::Bytes,
    ) -> BoxFut<'_, K2Result<()>> {
        let inner = self.inner();
        Box::pin(
            async move { inner.send_space_notify(peer, space_id, data).await },
        )
    }

    fn send_module(
        &self,
        peer: Url,
        space_id: SpaceId,
        module: String,
        data: bytes::Bytes,
    ) -> BoxFut<'_, K2Result<()>> {
        let inner = self.inner();
        Box::pin(async move {
            inner.send_module(peer, space_id, module, data).await
        })
    }

    fn get_connected_peers(&self) -> BoxFut<'_, K2Result<Vec<Url>>> {
        let inner = self.inner();
        Box::pin(async move { inner.get_connected_peers().await })
    }

    fn unregister_space(&self, space_id: SpaceId) -> BoxFut<'_, ()> {
        {
            let mut registrations = self.registrations.lock().unwrap();
            registrations.spaces.remove(&space_id);
            registrations
                .modules
                .retain(|(space, _), _| space != &space_id);
        }
        let inner = self.inner();
        Box::pin(async move { inner.unregister_space(space_id).await })
    }

    fn dump_network_stats(&self) -> BoxFut<'_, K2Result<ApiTransportStats>> {
        let inner = self.inner();
        Box::pin(async move { inner.dump_network_stats().await })
    }
}

#[cfg(test)]
mod test;
