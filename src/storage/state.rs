use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
};

use chrono::{DateTime, Utc};
use semver::Version;
use serde::{Deserialize, Serialize};

use super::StateKind;
use crate::events::{
    ActorStarted, ActorsStarted, HostHeartbeat, HostStarted, ProviderInfo, ProviderStarted,
};

/// A wasmCloud Capability provider
// NOTE: We probably aren't going to use this _right now_ so we've kept it pretty minimal. But it is
// possible that we could query wadm for more general data about the lattice in the future, so we do
// want to store this
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Provider {
    /// ID of the provider, normally a public nkey
    pub id: String,

    /// Name of the provider
    pub name: String,

    /// Issuer of the (signed) provider
    pub issuer: String,

    /// Contract ID
    pub contract_id: String,

    /// The reference used to start the provider. Can be empty if it was started from a file
    pub reference: String,

    /// The linkname the provider was started with
    pub link_name: String,

    /// The hosts this provider is running on
    pub hosts: HashMap<String, ProviderStatus>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ProviderStatus {
    /// The provider is starting and hasn't returned a heartbeat yet
    Pending,
    /// The provider is running
    Running,
    /// The provider failed to start
    // TODO(thomastaylor312): In the future, we'll probably want to decay out a provider from state
    // if it hasn't had a heartbeat
    Failed,
}

impl Default for ProviderStatus {
    fn default() -> Self {
        Self::Pending
    }
}

impl ToString for ProviderStatus {
    fn to_string(&self) -> String {
        match self {
            Self::Pending => "pending".to_string(),
            Self::Running => "running".to_string(),
            Self::Failed => "failed".to_string(),
        }
    }
}

impl StateKind for Provider {
    const KIND: &'static str = "provider";
}

impl From<ProviderStarted> for Provider {
    fn from(value: ProviderStarted) -> Self {
        Provider {
            id: value.public_key,
            name: value.claims.name,
            issuer: value.claims.issuer,
            contract_id: value.contract_id,
            reference: value.image_ref,
            link_name: value.link_name,
            ..Default::default()
        }
    }
}

impl From<&ProviderStarted> for Provider {
    fn from(value: &ProviderStarted) -> Self {
        Provider {
            id: value.public_key.clone(),
            name: value.claims.name.clone(),
            issuer: value.claims.issuer.clone(),
            contract_id: value.contract_id.clone(),
            reference: value.image_ref.clone(),
            link_name: value.link_name.clone(),
            ..Default::default()
        }
    }
}

/// An individual ActorInstance, named to differentiate from [ActorInstance](wasmcloud_control_interface::ActorInstance)
#[derive(Debug, Serialize, Deserialize, Clone, Default, Eq)]
pub struct WadmActorInstance {
    /// The GUID for this actor instance
    pub instance_id: String,

    /// Annotations attached to this acotr instance
    pub annotations: HashMap<String, String>,
}

impl WadmActorInstance {
    /// Convenience function to construct an instance from only the instance ID, mostly
    /// used for hash lookup purposes
    pub fn from_id(instance_id: String) -> WadmActorInstance {
        Self {
            instance_id,
            annotations: HashMap::new(),
        }
    }
}

// Custom Hash impl's due to nested HashMaps and HashSets
impl Hash for WadmActorInstance {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.instance_id.hash(state);
    }
}

/// NOTE(brooksmtownsend): This is compared only by the instance ID
/// as it's a GUID and lookups don't require annotations
impl PartialEq for WadmActorInstance {
    fn eq(&self, other: &Self) -> bool {
        self.instance_id == other.instance_id
    }
}

/// A wasmCloud Actor
// NOTE: We probably aren't going to use this _right now_ so we've kept it pretty minimal. But it is
// possible that we could query wadm for more general data about the lattice in the future, so we do
// want to store this
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Actor {
    /// ID of the actor, normally a public (n)key
    pub id: String,

    /// Name of the provider
    pub name: String,

    /// Capabilities that the actor requires
    pub capabilities: Vec<String>,

    /// Issuer of the (signed) actor
    pub issuer: String,

    /// Call alias to use for the actor
    pub call_alias: Option<String>,

    /// All instances of this actor running in the lattice, keyed by the host ID
    /// and contains a list of all [ActorInstances](ActorInstance) on that host
    pub instances: HashMap<String, HashSet<WadmActorInstance>>,

    /// The reference used to start the actor. Can be empty if it was started from a file
    pub reference: String,
}

impl Actor {
    /// A helper method that returns the total count of running copies of this actor, regardless of
    /// which host they are running on
    pub fn count(&self) -> usize {
        self.instances
            .values()
            .map(|instances| instances.len())
            .sum()
    }
}

impl StateKind for Actor {
    const KIND: &'static str = "actor";
}

impl From<ActorStarted> for Actor {
    fn from(value: ActorStarted) -> Self {
        Actor {
            id: value.public_key,
            name: value.claims.name,
            capabilities: value.claims.capabilites,
            issuer: value.claims.issuer,
            call_alias: value.claims.call_alias,
            reference: value.image_ref,
            ..Default::default()
        }
    }
}

impl From<&ActorStarted> for Actor {
    fn from(value: &ActorStarted) -> Self {
        Actor {
            id: value.public_key.clone(),
            name: value.claims.name.clone(),
            capabilities: value.claims.capabilites.clone(),
            issuer: value.claims.issuer.clone(),
            call_alias: value.claims.call_alias.clone(),
            reference: value.image_ref.clone(),
            ..Default::default()
        }
    }
}

impl From<ActorsStarted> for Actor {
    fn from(value: ActorsStarted) -> Self {
        Actor {
            id: value.public_key,
            name: value.claims.name,
            capabilities: value.claims.capabilites,
            issuer: value.claims.issuer,
            call_alias: value.claims.call_alias,
            reference: value.image_ref,
            ..Default::default()
        }
    }
}

impl From<&ActorsStarted> for Actor {
    fn from(value: &ActorsStarted) -> Self {
        Actor {
            id: value.public_key.clone(),
            name: value.claims.name.clone(),
            capabilities: value.claims.capabilites.clone(),
            issuer: value.claims.issuer.clone(),
            call_alias: value.claims.call_alias.clone(),
            reference: value.image_ref.clone(),
            ..Default::default()
        }
    }
}

/// A wasmCloud host
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Host {
    /// A map of actor IDs to the number of instances of the actor running on the host
    // NOTE(thomastaylor312): If we ever start storing a _ton_ of actors and it gets slow, we might
    // want to consider switching out the hash algorithm to something like `ahash` to speed up
    // lookups and deserialization
    pub actors: HashMap<String, usize>,

    /// The randomly generated friendly name of the host
    pub friendly_name: String,

    /// An arbitrary hashmap of string labels attached to the host
    pub labels: HashMap<String, String>,

    /// Additional annotations that have been added to the host
    pub annotations: HashMap<String, String>,

    /// A set of running providers on the host
    pub providers: HashSet<ProviderInfo>,

    /// The current uptime of the host in seconds
    pub uptime_seconds: usize,

    /// The host version that is running
    // NOTE(thomastaylor312): Right now a host started event doesn't emit the version, so a newly
    // started host can't be registered with one. We should probably add that to the host started
    // event and then modify it here
    pub version: Option<Version>,

    /// The ID of this host, in the form of its nkey encoded public key
    pub id: String,

    /// The time when this host was last seen, as a RFC3339 timestamp
    pub last_seen: DateTime<Utc>,
}

impl StateKind for Host {
    const KIND: &'static str = "host";
}

impl From<HostStarted> for Host {
    fn from(value: HostStarted) -> Self {
        Host {
            friendly_name: value.friendly_name,
            id: value.id,
            labels: value.labels,
            last_seen: Utc::now(),
            ..Default::default()
        }
    }
}

impl From<&HostStarted> for Host {
    fn from(value: &HostStarted) -> Self {
        Host {
            friendly_name: value.friendly_name.clone(),
            id: value.id.clone(),
            labels: value.labels.clone(),
            last_seen: Utc::now(),
            ..Default::default()
        }
    }
}

impl From<HostHeartbeat> for Host {
    fn from(value: HostHeartbeat) -> Self {
        Host {
            actors: value.actors,
            friendly_name: value.friendly_name,
            labels: value.labels,
            annotations: value.annotations,
            providers: value.providers.into_iter().collect(),
            uptime_seconds: value.uptime_seconds,
            version: Some(value.version),
            id: value.id,
            last_seen: Utc::now(),
        }
    }
}

impl From<&HostHeartbeat> for Host {
    fn from(value: &HostHeartbeat) -> Self {
        Host {
            actors: value.actors.clone(),
            friendly_name: value.friendly_name.clone(),
            labels: value.labels.clone(),
            annotations: value.annotations.clone(),
            providers: value.providers.iter().cloned().collect(),
            uptime_seconds: value.uptime_seconds,
            version: Some(value.version.clone()),
            id: value.id.clone(),
            last_seen: Utc::now(),
        }
    }
}
