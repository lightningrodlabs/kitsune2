//! In-memory fakes for unit-testing. See [`harness`].

pub(crate) mod harness;

pub(crate) use harness::{FakeEndpoint, fake_announce, fake_identity};
