//! In-memory fakes for unit-testing. See [`harness`].

pub(crate) mod harness;

pub(crate) use harness::{fake_announce, fake_identity, FakeEndpoint};
