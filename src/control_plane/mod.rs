pub(crate) mod admin;
mod auth;
mod client;
mod contract_api;
pub(crate) mod contracts;
pub(crate) use client::LeaseFence;
mod event_sink;
mod inspection;
#[cfg(test)]
#[path = "../../tests/unit/control_plane/inspection_tests.rs"]
mod inspection_tests;
mod invocation;
mod issuer;
mod local;
mod local_build;
mod process;
mod protocol;
mod public_api;
mod regions;
mod replication;
mod service;
pub(crate) mod session;
pub(crate) mod socket_ticket;

use std::time::Duration;

pub(crate) const SUPPORTED_CONTROL_PLANE_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_CONTROL_PLANE_MESSAGE_BYTES: usize = SUPPORTED_CONTROL_PLANE_PAYLOAD_BYTES;
pub(crate) const CONTROL_PLANE_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub use local::{DevOptions, serve_local};

#[cfg(test)]
pub(crate) use self::auth::ActorInvocationCapability;
pub use self::process::{
    ControlPlaneProcessConfig, ControlPlaneStorageConfig, serve_control_plane,
};
pub(crate) use self::{
    admin::PostgresAdminRegistry,
    auth::{ActorJwtVerifier, ActorPrincipal, ActorTokenPurpose},
    client::ControlPlaneClient,
    issuer::ActorJwtIssuer,
    service::ControlPlaneService,
};
