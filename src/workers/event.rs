use std::collections::{hash_map::Entry, HashMap, HashSet};

use anyhow::Result;
use tracing::{debug, instrument, trace, warn};
use wasmcloud_control_interface::{ActorDescription, ProviderDescription};

use crate::commands::Command;
use crate::consumers::{
    manager::{WorkError, WorkResult, Worker},
    ScopedMessage,
};
use crate::events::*;
use crate::publisher::Publisher;
use crate::scaler::manager::{ScalerList, ScalerManager};
use crate::server::StatusInfo;
use crate::storage::{Actor, Host, Provider, ProviderStatus, Store, WadmActorInstance};
use crate::APP_SPEC_ANNOTATION;

use super::event_helpers::*;

pub struct EventWorker<StateStore, C: Clone, P: Clone> {
    store: StateStore,
    ctl_client: C,
    command_publisher: CommandPublisher<P>,
    status_publisher: StatusPublisher<P>,
    scalers: ScalerManager<StateStore, P, C>,
}

impl<StateStore, C, P> EventWorker<StateStore, C, P>
where
    StateStore: Store + Send + Sync + Clone + 'static,
    C: ClaimsSource + InventorySource + LinkSource + Clone + Send + Sync + 'static,
    P: Publisher + Clone + Send + Sync + 'static,
{
    /// Creates a new event worker configured to use the given store and control interface client for fetching state
    pub fn new(
        store: StateStore,
        ctl_client: C,
        command_publisher: CommandPublisher<P>,
        status_publisher: StatusPublisher<P>,
        manager: ScalerManager<StateStore, P, C>,
    ) -> EventWorker<StateStore, C, P> {
        EventWorker {
            store,
            ctl_client,
            command_publisher,
            status_publisher,
            scalers: manager,
        }
    }

    // BEGIN HANDLERS
    // NOTE(thomastaylor312): These use anyhow errors because in the _single_ case where we have to
    // call the lattice controller, we no longer just have error types from the store. To handle the
    // multiple error cases, it was just easier to catch it into an anyhow Error and then convert at
    // the end

    #[instrument(level = "debug", skip(self, actor), fields(actor_id = %actor.public_key, host_id = %actor.host_id))]
    async fn handle_actor_started(
        &self,
        lattice_id: &str,
        actor: &ActorStarted,
    ) -> anyhow::Result<()> {
        trace!("Adding newly started actor to store");
        debug!("Fetching current data for actor");
        // Because we could have created an actor from the host heartbeat, we just overwrite
        // everything except counts here
        let mut actor_data = Actor::from(actor);
        if let Some(current) = self
            .store
            .get::<Actor>(lattice_id, &actor.public_key)
            .await?
        {
            trace!(actor = ?current, "Found existing actor data");
            // Merge in current counts
            actor_data.instances = current.instances;
        }
        // Update actor count in the host
        if let Some(mut host) = self.store.get::<Host>(lattice_id, &actor.host_id).await? {
            trace!(host = ?host, "Found existing host data");

            host.actors
                .entry(actor.public_key.clone())
                .and_modify(|count| *count += 1)
                .or_insert(1);

            self.store
                .store(lattice_id, host.id.to_owned(), host)
                .await?
        }

        // Update count of the data
        actor_data
            .instances
            .entry(actor.host_id.clone())
            .and_modify(|val| {
                val.insert(WadmActorInstance {
                    instance_id: actor.instance_id.to_owned(),
                    annotations: actor.annotations.to_owned(),
                });
            })
            .or_insert_with(|| {
                HashSet::from_iter([WadmActorInstance {
                    instance_id: actor.instance_id.to_owned(),
                    annotations: actor.annotations.to_owned(),
                }])
            });

        self.store
            .store(lattice_id, actor.public_key.clone(), actor_data)
            .await
            .map_err(anyhow::Error::from)
    }

    #[instrument(level = "debug", skip(self, actor), fields(actor_id = %actor.public_key, host_id = %actor.host_id))]
    async fn handle_actor_stopped(
        &self,
        lattice_id: &str,
        actor: &ActorStopped,
    ) -> anyhow::Result<()> {
        trace!("Removing stopped actor from store");
        debug!("Fetching current data for actor");
        if let Some(mut current) = self
            .store
            .get::<Actor>(lattice_id, &actor.public_key)
            .await?
        {
            trace!(actor = ?current, "Found existing actor data");

            // Remove here to take ownership, then insert back into the map
            if let Some(mut current_instances) = current.instances.remove(&actor.host_id) {
                if current_instances
                    .remove(&WadmActorInstance::from_id(actor.instance_id.to_owned()))
                    && current_instances.is_empty()
                {
                    trace!(host_id = %actor.host_id, "Stopped last actor on host");
                } else {
                    trace!(host_id = %actor.host_id, "Stopped actor instance on host");

                    current
                        .instances
                        .insert(actor.host_id.clone(), current_instances);
                }
            }

            if current.instances.is_empty() {
                trace!("Last actor instance was removed, removing actor from storage");
                self.store
                    .delete::<Actor>(lattice_id, &actor.public_key)
                    .await
            } else {
                self.store
                    .store(lattice_id, actor.public_key.clone(), current)
                    .await
            }?;
        }

        // Update actor count in the host
        if let Some(mut host) = self.store.get::<Host>(lattice_id, &actor.host_id).await? {
            trace!(host = ?host, "Found existing host data");
            match host.actors.get(&actor.public_key) {
                Some(existing_count) if *existing_count <= 1 => {
                    host.actors.remove(&actor.public_key);
                }
                Some(existing_count) => {
                    host.actors
                        .insert(actor.public_key.to_owned(), *existing_count - 1);
                }
                // you cannot delete what doesn't exist
                None => (),
            }

            self.store
                .store(lattice_id, host.id.to_owned(), host)
                .await?
        }

        Ok(())
    }

    #[instrument(level = "debug", skip(self, host), fields(host_id = %host.id))]
    async fn handle_host_heartbeat(
        &self,
        lattice_id: &str,
        host: &HostHeartbeat,
    ) -> anyhow::Result<()> {
        debug!("Updating store with current host heartbeat information");
        // TODO(thomastaylor312) We update some annotation data (namely providers) in the Host data
        // for now. This is not ideal, but for actual consumption, we should probably rewrite the
        // `Provider` info to handle annotations better
        let mut host_data = Host::from(host);
        if let Some(mut current_host_data) = self.store.get::<Host>(lattice_id, &host.id).await? {
            host_data.providers = host_data
                .providers
                .into_iter()
                .map(|mut info| {
                    // Taking to avoid clone
                    if let Some(current_info) = current_host_data.providers.take(&info) {
                        info.annotations = current_info.annotations;
                    }
                    info
                })
                .collect();
        }
        self.store
            .store(lattice_id, host.id.clone(), host_data)
            .await?;

        // NOTE(brooksmtownsend): Currently, the heartbeat does not tell us the instance IDs or annotations
        // of actors, or the annotations of providers. We need to make an inventory request to get this
        // information so we can properly update state.
        let host_inventory = self.ctl_client.get_inventory(&host.id).await?;

        // NOTE: We can return an error here and then nack because we'll just reupdate the host data
        // with the exact same host heartbeat entry. There is no possibility of a duplicate
        self.heartbeat_provider_update(lattice_id, host, host_inventory.providers)
            .await?;

        // NOTE: We can return an error here and then nack because we'll just reupdate the host data
        // with the exact same host heartbeat entry. There is no possibility of a duplicate
        self.heartbeat_actor_update(lattice_id, host, host_inventory.actors)
            .await?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self, host), fields(host_id = %host.id))]
    async fn handle_host_started(
        &self,
        lattice_id: &str,
        host: &HostStarted,
    ) -> anyhow::Result<()> {
        debug!("Updating store with new host");
        // New hosts have nothing running on them yet, so just drop it in the store
        self.store
            .store(lattice_id, host.id.clone(), Host::from(host))
            .await
            .map_err(anyhow::Error::from)
    }

    #[instrument(level = "debug", skip(self, host), fields(host_id = %host.id))]
    async fn handle_host_stopped(
        &self,
        lattice_id: &str,
        host: &HostStopped,
    ) -> anyhow::Result<()> {
        debug!("Handling host stopped event");
        // NOTE(thomastaylor312): Generally to get a host stopped event, the host should have
        // already sent a bunch of stop actor/provider events, but for correctness sake, we fetch
        // the current host and make sure all the actors and providers are removed
        trace!("Fetching current host data");
        let current: Host = match self.store.get(lattice_id, &host.id).await? {
            Some(h) => h,
            None => {
                debug!("Got host stopped event for a host we didn't have in the store");
                return Ok(());
            }
        };

        trace!("Fetching actors from store to remove stopped instances");
        let all_actors = self.store.list::<Actor>(lattice_id).await?;

        #[allow(clippy::type_complexity)]
        let (actors_to_update, actors_to_delete): (
            Vec<(String, Actor)>,
            Vec<(String, Actor)>,
        ) = all_actors
            .into_iter()
            .filter_map(|(id, mut actor)| {
                if current.actors.contains_key(&id) {
                    actor.instances.remove(&current.id);
                    Some((id, actor))
                } else {
                    None
                }
            })
            .partition(|(_, actor)| !actor.instances.is_empty());
        trace!("Storing updated actors in store");
        self.store.store_many(lattice_id, actors_to_update).await?;

        trace!("Removing actors with no more running instances");
        self.store
            .delete_many::<Actor, _, _>(lattice_id, actors_to_delete.into_iter().map(|(id, _)| id))
            .await?;

        trace!("Fetching providers from store to remove stopped instances");
        let all_providers = self.store.list::<Provider>(lattice_id).await?;

        #[allow(clippy::type_complexity)]
        let (providers_to_update, providers_to_delete): (Vec<(String, Provider)>, Vec<(String, Provider)>) = current
            .providers
            .into_iter()
            .filter_map(|info| {
                let key = crate::storage::provider_id(&info.public_key, &info.link_name);
                // NOTE: We can do this without cloning, but it led to some confusing code involving
                // `remove` from the owned `all_providers` map. This is more readable at the expense of
                // a clone for few providers
                match all_providers.get(&key).cloned() {
                    // If we successfully remove the host, map it to the right type, otherwise we can
                    // continue onward
                    Some(mut prov) => prov.hosts.remove(&host.id).map(|_| (key, prov)),
                    None => {
                        warn!(key = %key, "Didn't find provider in storage even though host said it existed");
                        None
                    }
                }
            })
            .partition(|(_, provider)| !provider.hosts.is_empty());
        trace!("Storing updated providers in store");
        self.store
            .store_many(lattice_id, providers_to_update)
            .await?;

        trace!("Removing providers with no more running instances");
        self.store
            .delete_many::<Provider, _, _>(
                lattice_id,
                providers_to_delete.into_iter().map(|(id, _)| id),
            )
            .await?;

        // Order matters here: Now that we've cleaned stuff up, remove the host. We do this last
        // because if any of the above fails after we remove the host, we won't be able to fetch the
        // data to remove the actors and providers on a retry.
        debug!("Deleting host from store");
        self.store
            .delete::<Host>(lattice_id, &host.id)
            .await
            .map_err(anyhow::Error::from)
    }

    #[instrument(
        level = "debug",
        skip(self, provider),
        fields(
            public_key = %provider.public_key,
            link_name = %provider.link_name,
            contract_id = %provider.contract_id
        )
    )]
    async fn handle_provider_started(
        &self,
        lattice_id: &str,
        provider: &ProviderStarted,
    ) -> anyhow::Result<()> {
        debug!("Handling provider started event");
        let id = crate::storage::provider_id(&provider.public_key, &provider.link_name);
        trace!("Fetching current data from store");
        let mut needs_host_update = false;
        let provider_data = if let Some(mut current) =
            self.store.get::<Provider>(lattice_id, &id).await?
        {
            // Using the entry api is a bit more efficient because we do a single key lookup
            let mut prov = match current.hosts.entry(provider.host_id.clone()) {
                Entry::Occupied(_) => {
                    trace!("Found host entry for the provider already in store. Will not update");
                    current
                }
                Entry::Vacant(entry) => {
                    entry.insert(ProviderStatus::default());
                    needs_host_update = true;
                    current
                }
            };
            // Update missing fields if they exist. Right now if we just discover a provider from
            // health check, these will be empty
            if prov.issuer.is_empty() || prov.reference.is_empty() {
                let new_prov = Provider::from(provider);
                prov.issuer = new_prov.issuer;
                prov.reference = new_prov.reference;
            }
            prov
        } else {
            trace!("No current provider found in store");
            let mut prov = Provider::from(provider);
            prov.hosts = HashMap::from([(provider.host_id.clone(), ProviderStatus::default())]);
            needs_host_update = true;
            prov
        };

        // Insert provider into host map
        if let (Some(mut host), true) = (
            self.store
                .get::<Host>(lattice_id, &provider.host_id)
                .await?,
            needs_host_update,
        ) {
            trace!(host = ?host, "Found existing host data");

            host.providers.replace(ProviderInfo {
                contract_id: provider.contract_id.to_owned(),
                link_name: provider.link_name.to_owned(),
                public_key: provider.public_key.to_owned(),
                annotations: provider.annotations.to_owned(),
            });

            self.store
                .store(lattice_id, host.id.to_owned(), host)
                .await?
        }

        debug!("Storing updated provider in store");
        self.store
            .store(lattice_id, id, provider_data)
            .await
            .map_err(anyhow::Error::from)
    }

    #[instrument(
        level = "debug",
        skip(self, provider),
        fields(
            public_key = %provider.public_key,
            link_name = %provider.link_name,
            contract_id = %provider.contract_id
        )
    )]
    async fn handle_provider_stopped(
        &self,
        lattice_id: &str,
        provider: &ProviderStopped,
    ) -> anyhow::Result<()> {
        debug!("Handling provider stopped event");
        let id = crate::storage::provider_id(&provider.public_key, &provider.link_name);
        trace!("Fetching current data from store");

        // Remove provider from host map
        if let Some(mut host) = self
            .store
            .get::<Host>(lattice_id, &provider.host_id)
            .await?
        {
            trace!(host = ?host, "Found existing host data");

            host.providers.remove(&ProviderInfo {
                contract_id: provider.contract_id.to_owned(),
                link_name: provider.link_name.to_owned(),
                public_key: provider.public_key.to_owned(),
                // We don't have this information, nor do we need it since we don't hash based
                // on annotations
                annotations: HashMap::new(),
            });

            self.store
                .store(lattice_id, host.id.to_owned(), host)
                .await?
        }

        if let Some(mut current) = self.store.get::<Provider>(lattice_id, &id).await? {
            if current.hosts.remove(&provider.host_id).is_none() {
                trace!(host_id = %provider.host_id, "Did not find host entry in provider");
                return Ok(());
            }
            if current.hosts.is_empty() {
                debug!("Provider is no longer running on any hosts. Removing from store");
                self.store
                    .delete::<Provider>(lattice_id, &id)
                    .await
                    .map_err(anyhow::Error::from)
            } else {
                debug!("Storing updated provider");
                self.store
                    .store(lattice_id, id, current)
                    .await
                    .map_err(anyhow::Error::from)
            }
        } else {
            trace!("No current provider found in store");
            Ok(())
        }
    }

    #[instrument(
        level = "debug",
        skip(self, provider),
        fields(
            public_key = %provider.public_key,
            link_name = %provider.link_name,
        )
    )]
    async fn handle_provider_health_check(
        &self,
        lattice_id: &str,
        host_id: &str,
        provider: &ProviderHealthCheckInfo,
        failed: bool,
    ) -> anyhow::Result<()> {
        debug!("Handling provider health check event");
        trace!("Getting current provider");
        let id = crate::storage::provider_id(&provider.public_key, &provider.link_name);
        let mut current: Provider = match self.store.get(lattice_id, &id).await? {
            Some(p) => p,
            None => {
                trace!("Didn't find provider in store. Creating");
                Provider {
                    id: provider.public_key.clone(),
                    link_name: provider.link_name.clone(),
                    ..Default::default()
                }
            }
        };
        debug!("Updating store with current status");
        let status = if failed {
            ProviderStatus::Failed
        } else {
            ProviderStatus::Running
        };
        current.hosts.insert(host_id.to_owned(), status);

        // TODO(thomastaylor312): Once we are able to fetch refmaps from the ctl client, we should
        // make it update any empty references with the data from the refmap

        self.store
            .store(lattice_id, id, current)
            .await
            .map_err(anyhow::Error::from)
    }

    // END HANDLER FUNCTIONS
    async fn populate_actor_info(
        &self,
        actors: &HashMap<String, Actor>,
        host_id: &str,
        instance_map: HashMap<String, HashSet<WadmActorInstance>>,
    ) -> anyhow::Result<Vec<(String, Actor)>> {
        let claims = self.ctl_client.get_claims().await?;

        Ok(instance_map
            .into_iter()
            .map(|(actor_id, instances)| {
                if let Some(actor) = actors.get(&actor_id) {
                    // Construct modified Actor with new instances included
                    let mut new_instances = actor.instances.clone();
                    new_instances.insert(host_id.to_owned(), instances);
                    let actor = Actor {
                        instances: new_instances,
                        ..actor.clone()
                    };

                    (actor_id, actor)
                } else if let Some(claim) = claims.get(&actor_id) {
                    (
                        actor_id.clone(),
                        Actor {
                            id: actor_id,
                            name: claim.name.to_owned(),
                            capabilities: claim.capabilities.to_owned(),
                            issuer: claim.issuer.to_owned(),
                            instances: HashMap::from_iter([(host_id.to_owned(), instances)]),
                            ..Default::default()
                        },
                    )
                } else {
                    warn!("Claims not found for actor on host, information is missing");

                    (
                        actor_id.clone(),
                        Actor {
                            id: actor_id,
                            name: "".to_owned(),
                            capabilities: Vec::new(),
                            issuer: "".to_owned(),
                            instances: HashMap::from_iter([(host_id.to_owned(), instances)]),
                            ..Default::default()
                        },
                    )
                }
            })
            .collect::<Vec<(String, Actor)>>())
    }

    #[instrument(level = "debug", skip(self, host), fields(host_id = %host.id))]
    async fn heartbeat_actor_update(
        &self,
        lattice_id: &str,
        host: &HostHeartbeat,
        inventory_actors: Vec<ActorDescription>,
    ) -> anyhow::Result<()> {
        debug!("Fetching current actor state");
        let actors = self.store.list::<Actor>(lattice_id).await?;

        let host_instances = inventory_actors
            .iter()
            .map(|actor_description| {
                (
                    actor_description.id.to_owned(),
                    actor_description
                        .instances
                        .iter()
                        .map(|instance| WadmActorInstance {
                            instance_id: instance.instance_id.to_owned(),
                            annotations: instance.annotations.clone().unwrap_or_default(),
                        })
                        .collect::<HashSet<WadmActorInstance>>(),
                )
            })
            .collect::<HashMap<String, HashSet<WadmActorInstance>>>();

        // Compare stored Actors to the "true" list on this host, updating stored
        // Actors when they differ from the authoratative heartbeat
        let actors_to_update = host_instances
            .into_iter()
            .filter_map(|(actor_id, instances)| {
                if actors
                    .get(&actor_id)
                    .map(|actor| {
                        actor
                            .instances
                            .get(&host.id)
                            .map(|store_instances| store_instances == &instances)
                            .unwrap_or(false)
                    })
                    .unwrap_or(false)
                {
                    None
                } else {
                    Some((actor_id, instances))
                }
            })
            // actor ID to all instances on this host
            .collect::<HashMap<String, HashSet<WadmActorInstance>>>();

        let actors_to_store = self
            .populate_actor_info(&actors, &host.id, actors_to_update)
            .await?;

        trace!("Updating actors with new status from host");

        self.store.store_many(lattice_id, actors_to_store).await?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self, host), fields(host_id = %host.id))]
    async fn heartbeat_provider_update(
        &self,
        lattice_id: &str,
        host: &HostHeartbeat,
        inventory_providers: Vec<ProviderDescription>,
    ) -> anyhow::Result<()> {
        debug!("Fetching current provider state");
        let providers = self.store.list::<Provider>(lattice_id).await?;
        let providers_to_update = inventory_providers.iter().filter_map(|info| {
            let provider_id = crate::storage::provider_id(&info.id, &info.link_name);
            // NOTE: We can do this without cloning, but it led to some confusing code involving
            // `remove` from the owned `providers` map. This is more readable at the expense of
            // a clone for few providers
            match providers.get(&provider_id).cloned() {
                Some(mut prov) => {
                    let mut has_changes = false;
                    // A health check from a provider we hadn't registered doesn't have a contract
                    // id, so check if that needs to be set
                    if prov.contract_id.is_empty() {
                        prov.contract_id = info.contract_id.clone();
                        has_changes = true;
                    }
                    if let Entry::Vacant(entry) = prov.hosts.entry(host.id.clone()) {
                        entry.insert(ProviderStatus::default());
                        has_changes = true;
                    }
                    if has_changes {
                        Some((provider_id, prov))
                    } else {
                        None
                    }
                }
                None => {
                    // If we don't already have the provider, create a basic one so we know it
                    // exists at least. The next provider heartbeat will fix it for us
                    Some((
                        provider_id,
                        Provider {
                            id: info.id.clone(),
                            contract_id: info.contract_id.clone(),
                            link_name: info.link_name.clone(),
                            hosts: [(host.id.clone(), ProviderStatus::default())].into(),
                            ..Default::default()
                        },
                    ))
                }
            }
        });

        trace!("Updating providers with new status from host");
        self.store
            .store_many(lattice_id, providers_to_update)
            .await?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self, data), fields(name = %data.manifest.metadata.name))]
    async fn handle_manifest_published(
        &self,
        lattice_id: &str,
        data: &ManifestPublished,
    ) -> anyhow::Result<()> {
        debug!(name = %data.manifest.metadata.name, "Handling published manifest");

        let old_scalers = self
            .scalers
            .remove_raw_scalers(&data.manifest.metadata.name)
            .await;
        let scalers = self.scalers.scalers_for_manifest(&data.manifest);

        if let Some(old_scalers) = old_scalers {
            // This relies on the idea that an ID is a unique identifier for a scaler, and any
            // change in the ID is indicative of the fact that the scaler is outdated and should be cleaned up.
            let (_updated_component, outdated_component): (ScalerList, ScalerList) = old_scalers
                .into_iter()
                .partition(|old| scalers.iter().any(|new| new.id() == old.id()));

            // Clean up any resources from scalers that no longer exist
            let futs = outdated_component
                .iter()
                .map(|s| async { s.cleanup().await });
            let commands = futures::future::join_all(futs)
                .await
                .into_iter()
                .filter_map(|res: Result<Vec<Command>>| res.ok())
                .flatten()
                .collect::<Vec<Command>>();

            // Now handle the result from cleaning up old scalers
            if let Err(e) = self.command_publisher.publish_commands(commands).await {
                warn!(error = ?e, "Failed to publish cleanup commands from old application, some resources may be left behind");
            };
        }

        // Get the results of the first reconcilation pass before we store the scalers. Publish the
        // commands for the ones that succeeded (as those scalers will have entered backoff mode if
        // they are backoff wrapped), and then return an error indicating failure so the event is
        // redelivered. When it is, the ones that succeeded will be in backoff mode and the ones
        // that failed will be retried.

        // TODO: inefficient to compute scalers twice, consider refactoring
        let scalers = self.scalers.add_scalers(&data.manifest).await?;

        let (commands, res) = get_commands_and_result(
            scalers.iter().map(|s| s.reconcile()),
            "Errors occurred during initial reconciliation",
        )
        .await;

        let status = if commands.is_empty() {
            scaler_status(&scalers).await
        } else {
            StatusInfo::compensating("Model deployed, running initial compensating commands")
        };

        trace!(?status, "Setting status");
        if let Err(e) = self
            .status_publisher
            .publish_status(data.manifest.metadata.name.as_ref(), status)
            .await
        {
            warn!("Failed to set manifest status: {e:}");
        };

        // Now handle the result from reconciliation
        trace!(?commands, "Publishing commands");
        self.command_publisher.publish_commands(commands).await?;

        res
    }

    #[instrument(level = "debug", skip(self))]
    async fn run_scalers_with_hint(&self, event: &Event, name: &str) -> anyhow::Result<()> {
        let scalers = match self.scalers.get_scalers(name).await {
            Some(scalers) => scalers,
            None => {
                debug!("No scalers currently exist for model");
                return Ok(());
            }
        };
        let (commands, res) = get_commands_and_result(
            scalers.iter().map(|s| s.handle_event(event)),
            "Errors occurred while handling event",
        )
        .await;

        let status = if commands.is_empty() {
            scaler_status(&scalers).await
        } else {
            StatusInfo::compensating(&format!(
                "Event modified scaler \"{}\" state, running compensating commands",
                name.to_owned(),
            ))
        };
        trace!(?status, "Setting status");
        if let Err(e) = self.status_publisher.publish_status(name, status).await {
            warn!(error = ?e, "Failed to set status for scaler");
        };

        trace!(?commands, "Publishing commands");
        self.command_publisher.publish_commands(commands).await?;

        res
    }

    #[instrument(level = "debug", skip(self))]
    async fn run_all_scalers(&self, event: &Event) -> anyhow::Result<()> {
        let scalers = self.scalers.get_all_scalers().await;

        let futs = scalers.iter().map(|(name, scalers)| async {
            let (commands, res) = get_commands_and_result(
                scalers.iter().map(|scaler| scaler.handle_event(event)),
                "Errors occurred while handling event with all scalers",
            )
            .await;

            let status = if commands.is_empty() {
                scaler_status(scalers).await
            } else {
                StatusInfo::compensating(&format!(
                    "Event modified scaler \"{}\" state, running compensating commands",
                    name.to_owned(),
                ))
            };

            trace!(?status, "Setting status");
            if let Err(e) = self.status_publisher.publish_status(name, status).await {
                warn!(error = ?e, "Failed to set status for scaler");
            };

            (commands, res)
        });

        // Resolve futures, computing commands for scalers, publishing statuses, and combining any errors
        let (commands, res) = futures::future::join_all(futs).await.into_iter().fold(
            (vec![], Ok(())),
            |(mut cmds, res), (mut new_cmds, new_res)| {
                cmds.append(&mut new_cmds);
                let res = match (res, new_res) {
                    (Ok(_), Ok(_)) => Ok(()),
                    (Ok(_), Err(e)) | (Err(e), Ok(_)) => Err(e),
                    (Err(e), Err(e2)) => Err(e.context(e2)),
                };
                (cmds, res)
            },
        );

        trace!(?commands, "Publishing commands");
        self.command_publisher.publish_commands(commands).await?;

        res
    }
}

#[async_trait::async_trait]
impl<StateStore, C, P> Worker for EventWorker<StateStore, C, P>
where
    StateStore: Store + Send + Sync + Clone + 'static,
    C: ClaimsSource + InventorySource + LinkSource + Clone + Send + Sync + 'static,
    P: Publisher + Clone + Send + Sync + 'static,
{
    type Message = Event;

    #[instrument(level = "debug", skip(self))]
    async fn do_work(&self, mut message: ScopedMessage<Self::Message>) -> WorkResult<()> {
        // Everything in this block returns a name hint for the success case and an error otherwise
        let res = match message.as_ref() {
            // NOTE(brooksmtownsend): For now, the plural events trigger scaler runs but do
            // not modify state. Ideally we'd use this to update the state of the lattice instead of the
            // individual events, but for now we're missing instance_id information. A separate issue should
            // be opened to track this and treating actors as cattle not pets (ignoring instance IDs).
            Event::ActorsStarted(actor) => Ok(actor
                .annotations
                .get(APP_SPEC_ANNOTATION)
                .map(|s| s.as_str())),
            Event::ActorsStopped(actor) => Ok(actor
                .annotations
                .get(APP_SPEC_ANNOTATION)
                .map(|s| s.as_str())),
            Event::ActorStarted(actor) => self
                .handle_actor_started(&message.lattice_id, actor)
                .await
                .map(|_| {
                    actor
                        .annotations
                        .get(APP_SPEC_ANNOTATION)
                        .map(|s| s.as_str())
                }),
            Event::ActorStopped(actor) => self
                .handle_actor_stopped(&message.lattice_id, actor)
                .await
                .map(|_| {
                    actor
                        .annotations
                        .get(APP_SPEC_ANNOTATION)
                        .map(|s| s.as_str())
                }),
            Event::HostHeartbeat(host) => self
                .handle_host_heartbeat(&message.lattice_id, host)
                .await
                .map(|_| None),
            Event::HostStarted(host) => self
                .handle_host_started(&message.lattice_id, host)
                .await
                .map(|_| None),
            Event::HostStopped(host) => self
                .handle_host_stopped(&message.lattice_id, host)
                .await
                .map(|_| None),
            Event::LinkdefDeleted(_ld) => Ok(None),
            Event::ProviderStarted(provider) => self
                .handle_provider_started(&message.lattice_id, provider)
                .await
                .map(|_| {
                    provider
                        .annotations
                        .get(APP_SPEC_ANNOTATION)
                        .map(|s| s.as_str())
                }),
            // NOTE(thomastaylor312): Provider stopped events need to be handled by all scalers as
            // it they could need to adjust their provider count based on the number of providers
            // available throughout the whole lattice (e.g. if a provider managed by another
            // manifest is removed, but this manifest still needs one). Ideally we should have a way
            // to have a "global" list of required providers in a lattice so we never shut one down
            // just to spin it back up, but for now we'll just deal with this as is
            Event::ProviderStopped(provider) => self
                .handle_provider_stopped(&message.lattice_id, provider)
                .await
                .map(|_| None),
            Event::ProviderHealthCheckPassed(ProviderHealthCheckPassed { data, host_id }) => self
                .handle_provider_health_check(&message.lattice_id, host_id, data, false)
                .await
                .map(|_| None),
            Event::ProviderHealthCheckFailed(ProviderHealthCheckFailed { data, host_id }) => self
                .handle_provider_health_check(&message.lattice_id, host_id, data, true)
                .await
                .map(|_| None),
            Event::ManifestPublished(data) => self
                .handle_manifest_published(&message.lattice_id, data)
                .await
                .map(|_| None),
            Event::ManifestUnpublished(data) => {
                debug!("Handling unpublished manifest");

                match self.scalers.remove_scalers(&data.name).await {
                    Some(Ok(_)) => {
                        if let Err(e) = self
                            .status_publisher
                            .publish_status(&data.name, StatusInfo::undeployed(""))
                            .await
                        {
                            warn!(error = ?e, "Failed to set status to undeployed");
                        }
                        return message.ack().await.map_err(WorkError::from);
                    }
                    Some(Err(e)) => {
                        message.nack().await;
                        return Err(WorkError::Other(e.into()));
                    }
                    None => Ok(None),
                }
            }
            // These events won't ever update state, but it's important to let the scalers know
            // that a start failed because they may be waiting for it to start
            Event::ActorsStartFailed(actor) => Ok(actor
                .annotations
                .get(APP_SPEC_ANNOTATION)
                .map(|s| s.as_str())),
            Event::ProviderStartFailed(_provider) => Ok(None),
            // All other events we don't care about for state.
            _ => {
                trace!("Got event we don't care about. Skipping");
                Ok(None)
            }
        };

        let res = match res {
            Ok(Some(name)) => self.run_scalers_with_hint(&message, name).await,
            Ok(None) => self.run_all_scalers(&message).await,
            Err(e) => Err(e),
        }
        .map_err(Box::<dyn std::error::Error + Send + 'static>::from);

        if let Err(e) = res {
            message.nack().await;
            return Err(WorkError::Other(e));
        }

        message.ack().await.map_err(WorkError::from)
    }
}

/// Helper that runs any iterable of futures and returns a list of commands and the proper result to
/// use as a response (Ok if there were no errors, all of the errors combined otherwise)
async fn get_commands_and_result<Fut, I>(futs: I, error_message: &str) -> (Vec<Command>, Result<()>)
where
    Fut: futures::Future<Output = Result<Vec<Command>>>,
    I: IntoIterator<Item = Fut>,
{
    let (commands, errors): (Vec<_>, Vec<_>) = futures::future::join_all(futs)
        .await
        .into_iter()
        .partition(Result::is_ok);
    // SAFETY: We partitioned by Ok and Err, so the unwraps are fine
    (
        commands.into_iter().flat_map(Result::unwrap).collect(),
        map_to_result(
            errors.into_iter().map(Result::unwrap_err).collect(),
            error_message,
        ),
    )
}

/// Helper function to get the status of all scalers
async fn scaler_status(scalers: &ScalerList) -> StatusInfo {
    let futs = scalers.iter().map(|s| s.status());
    let status = futures::future::join_all(futs).await;
    StatusInfo {
        status_type: status.iter().map(|s| s.status_type).sum(),
        message: status
            .into_iter()
            .filter_map(|s| {
                let message = s.message.trim();
                if message.is_empty() {
                    None
                } else {
                    Some(message.to_owned())
                }
            })
            .collect::<Vec<_>>()
            .join(", "),
    }
}

fn map_to_result(errors: Vec<anyhow::Error>, error_message: &str) -> Result<()> {
    if errors.is_empty() {
        Ok(())
    } else {
        let mut error = anyhow::anyhow!("{error_message}");
        for e in errors {
            error = error.context(e);
        }
        Err(error)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use tokio::sync::RwLock;
    use wasmcloud_control_interface::{
        ActorDescription, ActorInstance, HostInventory, ProviderDescription,
    };

    use super::*;

    use crate::{
        storage::ReadStore,
        test_util::{NoopPublisher, TestLatticeSource, TestStore},
    };

    // NOTE: This test is rather long because we want to run through what an actual state generation
    // loop would look like. This mostly covers happy path, while the other tests cover more of the
    // edge cases
    #[tokio::test]
    async fn test_all_state() {
        let store = Arc::new(TestStore::default());
        let inventory = Arc::new(RwLock::new(HashMap::default()));
        let lattice_source = TestLatticeSource {
            inventory: inventory.clone(),
            ..Default::default()
        };

        let lattice_id = "all_state";

        let command_publisher = CommandPublisher::new(NoopPublisher, "doesntmatter");
        let status_publisher = StatusPublisher::new(NoopPublisher, "doesntmatter");
        let worker = EventWorker::new(
            store.clone(),
            lattice_source.clone(),
            command_publisher.clone(),
            status_publisher.clone(),
            ScalerManager::test_new(
                NoopPublisher,
                lattice_id,
                store.clone(),
                command_publisher,
                lattice_source,
            )
            .await,
        );

        let host1_id = "DS1".to_string();
        let host2_id = "starkiller".to_string();

        /***********************************************************/
        /******************** Host Start Tests *********************/
        /***********************************************************/
        let labels = HashMap::from([("superweapon".to_string(), "true".to_string())]);
        worker
            .handle_host_started(
                lattice_id,
                &HostStarted {
                    friendly_name: "death-star-42".to_string(),
                    id: host1_id.clone(),
                    labels: labels.clone(),
                },
            )
            .await
            .expect("Should be able to handle event");

        let current_state = store.list::<Host>(lattice_id).await.unwrap();
        assert_eq!(current_state.len(), 1, "Only one host should be in store");
        let host = current_state
            .get("DS1")
            .expect("Host should exist in state");
        assert_eq!(
            host.friendly_name, "death-star-42",
            "Host should have the proper name in state"
        );
        assert_eq!(host.labels, labels, "Host should have the correct labels");

        let labels2 = HashMap::from([
            ("superweapon".to_string(), "true".to_string()),
            ("lazy_writing".to_string(), "true".to_string()),
        ]);
        worker
            .handle_host_started(
                lattice_id,
                &HostStarted {
                    friendly_name: "starkiller-base-2015".to_string(),
                    id: host2_id.clone(),
                    labels: labels2.clone(),
                },
            )
            .await
            .expect("Should be able to handle event");

        let current_state = store.list::<Host>(lattice_id).await.unwrap();
        assert_eq!(current_state.len(), 2, "Both hosts should be in the store");
        let host = current_state
            .get("starkiller")
            .expect("Host should exist in state");
        assert_eq!(
            host.friendly_name, "starkiller-base-2015",
            "Host should have the proper name in state"
        );
        assert_eq!(host.labels, labels2, "Host should have the correct labels");

        // Now just double check that the other host didn't change in response to the new one
        let host = current_state
            .get("DS1")
            .expect("Host should exist in state");
        assert_eq!(
            host.friendly_name, "death-star-42",
            "Host should have the proper name in state"
        );
        assert_eq!(host.labels, labels, "Host should have the correct labels");

        /***********************************************************/
        /******************** Actor Start Tests ********************/
        /***********************************************************/

        let actor1 = ActorStarted {
            claims: ActorClaims {
                call_alias: Some("Grand Moff".into()),
                capabilites: vec!["empire:command".into()],
                issuer: "Sheev Palpatine".into(),
                name: "Grand Moff Tarkin".into(),
                version: "0.1.0".into(),
                ..Default::default()
            },
            image_ref: "coruscant.galactic.empire/tarkin:0.1.0".into(),
            public_key: "TARKIN".into(),
            host_id: host1_id.clone(),
            annotations: HashMap::default(),
            instance_id: "haskdhjkas-123jkh123-asdads".to_string(),
        };

        let actor2 = ActorStarted {
            claims: ActorClaims {
                call_alias: Some("Darth".into()),
                capabilites: vec!["empire:command".into(), "force_user:sith".into()],
                issuer: "Sheev Palpatine".into(),
                name: "Darth Vader".into(),
                version: "0.1.0".into(),
                ..Default::default()
            },
            image_ref: "coruscant.galactic.empire/vader:0.1.0".into(),
            public_key: "DARTHVADER".into(),
            host_id: host1_id.clone(),
            annotations: HashMap::default(),
            instance_id: "2-haskdhjkas-123jkh123-asdads".to_string(),
        };

        // Start a single actor first just to make sure that works properly, then start all of them
        // across the two hosts
        worker
            .handle_actor_started(lattice_id, &actor1)
            .await
            .expect("Should be able to handle actor event");

        let actors = store.list::<Actor>(lattice_id).await.unwrap();
        assert_eq!(actors.len(), 1, "Should only be 1 actor in state");
        assert_actor(&actors, &actor1, &[(&host1_id, 1)]);

        // The stored host should also now have this actor in its map
        let host = store
            .get::<Host>(lattice_id, &host1_id)
            .await
            .expect("Should be able to access store")
            .expect("Should have the host in the store");
        assert_eq!(*host.actors.get(&actor1.public_key).unwrap_or(&0), 1_usize);

        worker
            .handle_actor_started(
                lattice_id,
                &ActorStarted {
                    instance_id: "unique-instance-id".to_string(),
                    ..actor1.clone()
                },
            )
            .await
            .expect("Should be able to handle actor event");

        for n in 0..2 {
            // Create unique instance ID based on loop iteration
            let evt2 = ActorStarted {
                instance_id: format!("{n}-{}", &actor2.instance_id),
                ..actor2.clone()
            };
            worker
                .handle_actor_started(lattice_id, &evt2)
                .await
                .expect("Should be able to handle actor event");

            // Start the actors on the other host as well
            worker
                .handle_actor_started(
                    lattice_id,
                    &ActorStarted {
                        host_id: host2_id.clone(),
                        instance_id: format!("{n}-host2-{}", &actor1.instance_id),
                        ..actor1.clone()
                    },
                )
                .await
                .expect("Should be able to handle actor event");

            worker
                .handle_actor_started(
                    lattice_id,
                    &ActorStarted {
                        host_id: host2_id.clone(),
                        instance_id: format!("{n}-host2-v2-{}", &actor2.instance_id),
                        ..actor2.clone()
                    },
                )
                .await
                .expect("Should be able to handle actor event");
        }

        let actors = store.list::<Actor>(lattice_id).await.unwrap();
        assert_eq!(
            actors.len(),
            2,
            "Should have the correct number of actors in state"
        );

        // Check the first actor
        assert_actor(&actors, &actor1, &[(&host1_id, 2), (&host2_id, 2)]);
        // Check the second actor
        assert_actor(&actors, &actor2, &[(&host1_id, 2), (&host2_id, 2)]);

        /***********************************************************/
        /****************** Provider Start Tests *******************/
        /***********************************************************/

        let provider1 = ProviderStarted {
            claims: ProviderClaims {
                issuer: "Sheev Palpatine".into(),
                name: "Force Choke".into(),
                version: "0.1.0".into(),
                ..Default::default()
            },
            image_ref: "coruscant.galactic.empire/force_choke:0.1.0".into(),
            public_key: "CHOKE".into(),
            host_id: host1_id.clone(),
            annotations: HashMap::default(),
            instance_id: "1".to_string(),
            contract_id: "force_user:sith".into(),
            link_name: "default".into(),
        };

        let provider2 = ProviderStarted {
            claims: ProviderClaims {
                issuer: "Sheev Palpatine".into(),
                name: "Death Star Laser".into(),
                version: "0.1.0".into(),
                ..Default::default()
            },
            image_ref: "coruscant.galactic.empire/laser:0.1.0".into(),
            public_key: "BYEBYEALDERAAN".into(),
            host_id: host2_id.clone(),
            annotations: HashMap::default(),
            instance_id: "2".to_string(),
            contract_id: "empire:command".into(),
            link_name: "default".into(),
        };

        worker
            .handle_provider_started(lattice_id, &provider1)
            .await
            .expect("Should be able to handle provider event");
        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 1, "Should only be 1 provider in state");
        assert_provider(&providers, &provider1, &[&host1_id]);

        // Now start the second provider on both hosts (so we can test some things in the next test)
        worker
            .handle_provider_started(lattice_id, &provider2)
            .await
            .expect("Should be able to handle provider event");
        worker
            .handle_provider_started(
                lattice_id,
                &ProviderStarted {
                    host_id: host1_id.clone(),
                    instance_id: "3".to_string(),
                    ..provider2.clone()
                },
            )
            .await
            .expect("Should be able to handle provider event");
        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 2, "Should only be 2 providers in state");
        assert_provider(&providers, &provider2, &[&host1_id, &host2_id]);

        // Check that hosts got updated properly
        let hosts = store.list::<Host>(lattice_id).await.unwrap();
        assert_eq!(hosts.len(), 2, "Should only have 2 hosts");
        let host = hosts.get(&host1_id).expect("Host should still exist");
        assert_eq!(
            host.actors.len(),
            2,
            "Should have two different actors running"
        );
        assert_eq!(
            host.providers.len(),
            2,
            "Should have two different providers running"
        );
        let host = hosts.get(&host2_id).expect("Host should still exist");
        assert_eq!(
            host.actors.len(),
            2,
            "Should have two different actors running"
        );
        assert_eq!(
            host.providers.len(),
            1,
            "Should have a single provider running"
        );

        /***********************************************************/
        /******************* Host Heartbeat Test *******************/
        /***********************************************************/

        // NOTE(brooksmtownsend): Painful manual manipulation of host inventory
        // to satisfy the way we currently query the inventory when handling heartbeats.
        *inventory.write().await = HashMap::from_iter([
            (
                host1_id.to_string(),
                HostInventory {
                    friendly_name: "my-host-1".to_string(),
                    issuer: "my-issuer-1".to_string(),
                    actors: vec![
                        ActorDescription {
                            id: actor1.public_key.to_string(),
                            image_ref: None,
                            /// The individual instances of this actor that are running
                            instances: vec![
                                ActorInstance {
                                    annotations: None,
                                    instance_id: "1".to_string(),
                                    revision: 0,
                                },
                                ActorInstance {
                                    annotations: None,
                                    instance_id: "2".to_string(),
                                    revision: 0,
                                },
                            ],
                            name: None,
                        },
                        ActorDescription {
                            id: actor2.public_key.to_string(),
                            image_ref: None,
                            /// The individual instances of this actor that are running
                            instances: vec![
                                ActorInstance {
                                    annotations: None,
                                    instance_id: "3".to_string(),
                                    revision: 0,
                                },
                                ActorInstance {
                                    annotations: None,
                                    instance_id: "4".to_string(),
                                    revision: 0,
                                },
                            ],
                            name: None,
                        },
                    ],
                    host_id: host1_id.to_string(),
                    labels: HashMap::new(),
                    providers: vec![
                        ProviderDescription {
                            contract_id: provider1.contract_id.clone(),
                            link_name: provider1.link_name.clone(),
                            id: provider1.public_key.clone(),
                            annotations: Some(HashMap::new()),
                            image_ref: Some(provider1.image_ref.clone()),
                            name: Some("One".to_string()),
                            revision: 0,
                        },
                        ProviderDescription {
                            contract_id: provider2.contract_id.clone(),
                            link_name: provider2.link_name.clone(),
                            id: provider2.public_key.clone(),
                            annotations: Some(HashMap::new()),
                            image_ref: Some(provider2.image_ref.clone()),
                            name: Some("Two".to_string()),
                            revision: 0,
                        },
                    ],
                },
            ),
            (
                host2_id.to_string(),
                HostInventory {
                    friendly_name: "my-host-2".to_string(),
                    issuer: "my-issuer-1".to_string(),
                    actors: vec![
                        ActorDescription {
                            id: actor1.public_key.to_string(),
                            image_ref: None,
                            /// The individual instances of this actor that are running
                            instances: vec![
                                ActorInstance {
                                    annotations: None,
                                    instance_id: "5".to_string(),
                                    revision: 0,
                                },
                                ActorInstance {
                                    annotations: None,
                                    instance_id: "6".to_string(),
                                    revision: 0,
                                },
                            ],
                            name: None,
                        },
                        ActorDescription {
                            id: actor2.public_key.to_string(),
                            image_ref: None,
                            /// The individual instances of this actor that are running
                            instances: vec![
                                ActorInstance {
                                    annotations: None,
                                    instance_id: "7".to_string(),
                                    revision: 0,
                                },
                                ActorInstance {
                                    annotations: None,
                                    instance_id: "8".to_string(),
                                    revision: 0,
                                },
                            ],
                            name: None,
                        },
                    ],
                    host_id: host2_id.to_string(),
                    labels: HashMap::new(),
                    providers: vec![ProviderDescription {
                        contract_id: provider2.contract_id.clone(),
                        link_name: provider2.link_name.clone(),
                        id: provider2.public_key.clone(),
                        annotations: Some(HashMap::new()),
                        image_ref: Some(provider2.image_ref.clone()),
                        name: Some("Two".to_string()),
                        revision: 0,
                    }],
                },
            ),
        ]);

        worker
            .handle_host_heartbeat(
                lattice_id,
                &HostHeartbeat {
                    actors: HashMap::from([
                        (actor1.public_key.clone(), 2),
                        (actor2.public_key.clone(), 2),
                    ]),
                    friendly_name: "death-star-42".to_string(),
                    labels: labels.clone(),
                    providers: vec![
                        ProviderInfo {
                            contract_id: provider1.contract_id.clone(),
                            link_name: provider1.link_name.clone(),
                            public_key: provider1.public_key.clone(),
                            annotations: HashMap::new(),
                        },
                        ProviderInfo {
                            contract_id: provider2.contract_id.clone(),
                            link_name: provider2.link_name.clone(),
                            public_key: provider2.public_key.clone(),
                            annotations: HashMap::new(),
                        },
                    ],
                    uptime_human: "30s".into(),
                    uptime_seconds: 30,
                    version: semver::Version::parse("0.61.0").unwrap(),
                    id: host1_id.clone(),
                    annotations: HashMap::default(),
                },
            )
            .await
            .expect("Should be able to handle host heartbeat");

        worker
            .handle_host_heartbeat(
                lattice_id,
                &HostHeartbeat {
                    actors: HashMap::from([
                        (actor1.public_key.clone(), 2),
                        (actor2.public_key.clone(), 2),
                    ]),
                    friendly_name: "starkiller-base-2015".to_string(),
                    labels: labels2.clone(),
                    providers: vec![ProviderInfo {
                        contract_id: provider2.contract_id.clone(),
                        link_name: provider2.link_name.clone(),
                        public_key: provider2.public_key.clone(),
                        annotations: HashMap::new(),
                    }],
                    uptime_human: "30s".into(),
                    uptime_seconds: 30,
                    version: semver::Version::parse("0.61.0").unwrap(),
                    id: host2_id.clone(),
                    annotations: HashMap::default(),
                },
            )
            .await
            .expect("Should be able to handle host heartbeat");

        // Check that our actor and provider data is still correct.
        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 2, "Should still have 2 providers in state");
        assert_provider(&providers, &provider1, &[&host1_id]);
        assert_provider(&providers, &provider2, &[&host1_id, &host2_id]);

        let actors = store.list::<Actor>(lattice_id).await.unwrap();
        assert_eq!(actors.len(), 2, "Should still have 2 actors in state");
        assert_actor(&actors, &actor1, &[(&host1_id, 2), (&host2_id, 2)]);
        assert_actor(&actors, &actor2, &[(&host1_id, 2), (&host2_id, 2)]);

        /***********************************************************/
        /******************** Actor Stop Tests *********************/
        /***********************************************************/

        // Stop them on one host first
        let stopped_one = ActorStopped {
            annotations: HashMap::default(),
            instance_id: "1".to_string(),
            public_key: actor1.public_key.clone(),
            host_id: host1_id.clone(),
        };
        let stopped_two = ActorStopped {
            annotations: HashMap::default(),
            instance_id: "2".to_string(),
            public_key: actor1.public_key.clone(),
            host_id: host1_id.clone(),
        };

        worker
            .handle_actor_stopped(lattice_id, &stopped_one)
            .await
            .expect("Should be able to handle actor stop event");
        worker
            .handle_actor_stopped(lattice_id, &stopped_two)
            .await
            .expect("Should be able to handle actor stop event");

        let actors = store.list::<Actor>(lattice_id).await.unwrap();
        assert_eq!(actors.len(), 2, "Should still have 2 actors in state");
        assert_actor(&actors, &actor1, &[(&host2_id, 2)]);
        assert_actor(&actors, &actor2, &[(&host1_id, 2), (&host2_id, 2)]);

        let host = store
            .get::<Host>(lattice_id, &host2_id)
            .await
            .expect("Should be able to access store")
            .expect("Should have the host in the store");
        assert_eq!(*host.actors.get(&actor1.public_key).unwrap_or(&0), 2_usize);
        assert_eq!(*host.actors.get(&actor2.public_key).unwrap_or(&0), 2_usize);

        // Now stop on the other
        let stopped_one_host_two = ActorStopped {
            host_id: host2_id.clone(),
            instance_id: "5".to_string(),
            ..stopped_one
        };
        let stopped_two_host_two = ActorStopped {
            host_id: host2_id.clone(),
            instance_id: "6".to_string(),
            ..stopped_two
        };

        worker
            .handle_actor_stopped(lattice_id, &stopped_one_host_two)
            .await
            .expect("Should be able to handle actor stop event");
        worker
            .handle_actor_stopped(lattice_id, &stopped_two_host_two)
            .await
            .expect("Should be able to handle actor stop event");

        let actors = store.list::<Actor>(lattice_id).await.unwrap();
        assert_eq!(actors.len(), 1, "Should only have 1 actor in state");
        // Double check the the old one is still ok
        assert_actor(&actors, &actor2, &[(&host1_id, 2), (&host2_id, 2)]);

        /***********************************************************/
        /******************* Provider Stop Tests *******************/
        /***********************************************************/

        worker
            .handle_provider_stopped(
                lattice_id,
                &ProviderStopped {
                    annotations: HashMap::default(),
                    contract_id: provider2.contract_id.clone(),
                    instance_id: provider2.instance_id.clone(),
                    link_name: provider2.link_name.clone(),
                    public_key: provider2.public_key.clone(),
                    reason: String::new(),
                    host_id: host1_id.clone(),
                },
            )
            .await
            .expect("Should be able to handle provider stop event");

        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 2, "Should still have 2 providers in state");
        assert_provider(&providers, &provider1, &[&host1_id]);
        assert_provider(&providers, &provider2, &[&host2_id]);

        // Check that hosts got updated properly
        let hosts = store.list::<Host>(lattice_id).await.unwrap();
        assert_eq!(hosts.len(), 2, "Should only have 2 hosts");
        let host = hosts.get(&host1_id).expect("Host should still exist");
        assert_eq!(host.actors.len(), 1, "Should have 1 actor running");
        assert_eq!(host.providers.len(), 1, "Should have 1 provider running");
        let host = hosts.get(&host2_id).expect("Host should still exist");
        assert_eq!(host.actors.len(), 1, "Should have 1 actor running");
        assert_eq!(
            host.providers.len(),
            1,
            "Should have a single provider running"
        );

        /***********************************************************/
        /***************** Heartbeat Tests Part 2 ******************/
        /***********************************************************/

        // NOTE(brooksmtownsend): Painful manual manipulation of host inventory
        // to satisfy the way we currently query the inventory when handling heartbeats.
        *inventory.write().await = HashMap::from_iter([
            (
                host1_id.to_string(),
                HostInventory {
                    friendly_name: "my-host-3".to_string(),
                    issuer: "my-issuer-2".to_string(),
                    actors: vec![ActorDescription {
                        id: actor2.public_key.to_string(),
                        image_ref: None,
                        /// The individual instances of this actor that are running
                        instances: vec![
                            ActorInstance {
                                annotations: None,
                                instance_id: "3".to_string(),
                                revision: 0,
                            },
                            ActorInstance {
                                annotations: None,
                                instance_id: "4".to_string(),
                                revision: 0,
                            },
                        ],
                        name: None,
                    }],
                    host_id: host1_id.to_string(),
                    labels: HashMap::new(),
                    // Leaving incomplete purposefully, we don't need this info
                    providers: vec![],
                },
            ),
            (
                host2_id.to_string(),
                HostInventory {
                    friendly_name: "my-host-4".to_string(),
                    issuer: "my-issuer-2".to_string(),
                    actors: vec![ActorDescription {
                        id: actor2.public_key.to_string(),
                        image_ref: None,
                        /// The individual instances of this actor that are running
                        instances: vec![
                            ActorInstance {
                                annotations: None,
                                instance_id: "7".to_string(),
                                revision: 0,
                            },
                            ActorInstance {
                                annotations: None,
                                instance_id: "8".to_string(),
                                revision: 0,
                            },
                        ],
                        name: None,
                    }],
                    host_id: host2_id.to_string(),
                    labels: HashMap::new(),
                    // Leaving incomplete purposefully, we don't need this info
                    providers: vec![],
                },
            ),
        ]);

        // Heartbeat the first host and make sure nothing has changed
        worker
            .handle_host_heartbeat(
                lattice_id,
                &HostHeartbeat {
                    actors: HashMap::from([(actor2.public_key.clone(), 2)]),
                    friendly_name: "death-star-42".to_string(),
                    labels,
                    providers: vec![ProviderInfo {
                        contract_id: provider1.contract_id.clone(),
                        link_name: provider1.link_name.clone(),
                        public_key: provider1.public_key.clone(),
                        annotations: HashMap::new(),
                    }],
                    uptime_human: "60s".into(),
                    uptime_seconds: 60,
                    version: semver::Version::parse("0.61.0").unwrap(),
                    id: host1_id.clone(),
                    annotations: HashMap::default(),
                },
            )
            .await
            .expect("Should be able to handle host heartbeat");

        worker
            .handle_host_heartbeat(
                lattice_id,
                &HostHeartbeat {
                    actors: HashMap::from([(actor2.public_key.clone(), 2)]),
                    friendly_name: "starkiller-base-2015".to_string(),
                    labels: labels2,
                    providers: vec![ProviderInfo {
                        contract_id: provider2.contract_id.clone(),
                        link_name: provider2.link_name.clone(),
                        public_key: provider2.public_key.clone(),
                        annotations: HashMap::new(),
                    }],
                    uptime_human: "60s".into(),
                    uptime_seconds: 60,
                    version: semver::Version::parse("0.61.0").unwrap(),
                    id: host2_id.clone(),
                    annotations: HashMap::default(),
                },
            )
            .await
            .expect("Should be able to handle host heartbeat");

        // Check that the heartbeat kept state consistent
        let hosts = store.list::<Host>(lattice_id).await.unwrap();
        assert_eq!(hosts.len(), 2, "Should only have 2 hosts");
        let host = hosts.get(&host1_id).expect("Host should still exist");
        assert_eq!(host.actors.len(), 1, "Should have 1 actor running");
        assert_eq!(host.providers.len(), 1, "Should have 1 provider running");
        let host = hosts.get(&host2_id).expect("Host should still exist");
        assert_eq!(host.actors.len(), 1, "Should have 1 actor running");
        assert_eq!(
            host.providers.len(),
            1,
            "Should have a single provider running"
        );

        // Double check providers and actors are the same
        let actors = store.list::<Actor>(lattice_id).await.unwrap();
        assert_eq!(actors.len(), 1, "Should only have 1 actor in state");
        assert_actor(&actors, &actor2, &[(&host1_id, 2), (&host2_id, 2)]);

        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 2, "Should still have 2 providers in state");
        assert_provider(&providers, &provider1, &[&host1_id]);
        assert_provider(&providers, &provider2, &[&host2_id]);

        /***********************************************************/
        /********************* Host Stop Tests *********************/
        /***********************************************************/

        worker
            .handle_host_stopped(
                lattice_id,
                &HostStopped {
                    labels: HashMap::default(),
                    id: host1_id.clone(),
                },
            )
            .await
            .expect("Should be able to handle host stopped event");

        let hosts = store.list::<Host>(lattice_id).await.unwrap();
        assert_eq!(hosts.len(), 1, "Should only have 1 host");
        let host = hosts.get(&host2_id).expect("Host should still exist");
        assert_eq!(host.actors.len(), 1, "Should have 1 actor running");
        assert_eq!(
            host.providers.len(),
            1,
            "Should have a single provider running"
        );

        // Double check providers and actors are the same
        let actors = store.list::<Actor>(lattice_id).await.unwrap();
        assert_eq!(actors.len(), 1, "Should only have 1 actor in state");
        assert_actor(&actors, &actor2, &[(&host2_id, 2)]);

        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 1, "Should now have 1 provider in state");
        assert_provider(&providers, &provider2, &[&host2_id]);
    }

    #[tokio::test]
    async fn test_discover_running_host() {
        let actor1_id = "SKYWALKER".to_string();
        let actor2_id = "ORGANA".to_string();
        let lattice_id = "discover_running_host";
        let claims = HashMap::from([
            (
                actor1_id.clone(),
                Claims {
                    name: "tosche_station".to_string(),
                    capabilities: vec!["wasmcloud:httpserver".to_string()],
                    issuer: "GEORGELUCAS".to_string(),
                },
            ),
            (
                actor2_id.clone(),
                Claims {
                    name: "alderaan".to_string(),
                    capabilities: vec!["wasmcloud:keyvalue".to_string()],
                    issuer: "GEORGELUCAS".to_string(),
                },
            ),
        ]);
        let store = Arc::new(TestStore::default());
        let inventory = Arc::new(RwLock::new(HashMap::default()));
        let lattice_source = TestLatticeSource {
            claims: claims.clone(),
            inventory: inventory.clone(),
            ..Default::default()
        };
        let command_publisher = CommandPublisher::new(NoopPublisher, "doesntmatter");
        let status_publisher = StatusPublisher::new(NoopPublisher, "doesntmatter");
        let worker = EventWorker::new(
            store.clone(),
            lattice_source.clone(),
            command_publisher.clone(),
            status_publisher.clone(),
            ScalerManager::test_new(
                NoopPublisher,
                lattice_id,
                store.clone(),
                command_publisher,
                lattice_source,
            )
            .await,
        );

        let provider_id = "HYPERDRIVE".to_string();
        let link_name = "default".to_string();
        let host_id = "WHATAPIECEOFJUNK".to_string();
        // NOTE(brooksmtownsend): Painful manual manipulation of host inventory
        // to satisfy the way we currently query the inventory when handling heartbeats.
        *inventory.write().await = HashMap::from_iter([(
            host_id.to_string(),
            HostInventory {
                friendly_name: "my-host-5".to_string(),
                issuer: "my-issuer-3".to_string(),
                actors: vec![
                    ActorDescription {
                        id: actor1_id.to_string(),
                        image_ref: None,
                        /// The individual instances of this actor that are running
                        instances: vec![
                            ActorInstance {
                                annotations: None,
                                instance_id: "1".to_string(),
                                revision: 0,
                            },
                            ActorInstance {
                                annotations: None,
                                instance_id: "2".to_string(),
                                revision: 0,
                            },
                        ],
                        name: None,
                    },
                    ActorDescription {
                        id: actor2_id.to_string(),
                        image_ref: None,
                        /// The individual instances of this actor that are running
                        instances: vec![ActorInstance {
                            annotations: None,
                            instance_id: "3".to_string(),
                            revision: 0,
                        }],
                        name: None,
                    },
                ],
                host_id: host_id.to_string(),
                labels: HashMap::new(),
                providers: vec![ProviderDescription {
                    contract_id: "lightspeed".into(),
                    link_name: link_name.clone(),
                    id: provider_id.clone(),
                    annotations: Some(HashMap::new()),
                    image_ref: Some("".to_string()),
                    name: Some("".to_string()),
                    revision: 0,
                }],
            },
        )]);

        // Heartbeat with actors and providers that don't exist in the store yet
        worker
            .handle_host_heartbeat(
                lattice_id,
                &HostHeartbeat {
                    actors: HashMap::from([(actor1_id.clone(), 2), (actor2_id.clone(), 1)]),
                    friendly_name: "millenium_falcon-1977".to_string(),
                    labels: HashMap::default(),
                    providers: vec![ProviderInfo {
                        contract_id: "lightspeed".into(),
                        link_name: link_name.clone(),
                        public_key: provider_id.clone(),
                        annotations: HashMap::new(),
                    }],
                    uptime_human: "60s".into(),
                    uptime_seconds: 60,
                    version: semver::Version::parse("0.61.0").unwrap(),
                    id: host_id.clone(),
                    annotations: HashMap::default(),
                },
            )
            .await
            .expect("Should be able to handle host heartbeat");

        // We test that the host is created in other tests, so just check that the actors and
        // providers were created properly
        let actors = store.list::<Actor>(lattice_id).await.unwrap();
        assert_eq!(actors.len(), 2, "Store should now have two actors");
        let actor = actors.get(&actor1_id).expect("Actor should exist");
        let expected = claims.get(&actor1_id).unwrap();
        assert_eq!(actor.name, expected.name, "Data should match");
        assert_eq!(
            actor.capabilities, expected.capabilities,
            "Data should match"
        );
        assert_eq!(actor.issuer, expected.issuer, "Data should match");
        assert_eq!(
            actor
                .instances
                .get(&host_id)
                .expect("Host should exist in count")
                .len(),
            2,
            "Should have the right number of actors running"
        );

        let actor = actors.get(&actor2_id).expect("Actor should exist");
        let expected = claims.get(&actor2_id).unwrap();
        assert_eq!(actor.name, expected.name, "Data should match");
        assert_eq!(
            actor.capabilities, expected.capabilities,
            "Data should match"
        );
        assert_eq!(actor.issuer, expected.issuer, "Data should match");
        assert_eq!(
            actor
                .instances
                .get(&host_id)
                .expect("Host should exist in count")
                .len(),
            1,
            "Should have the right number of actors running"
        );

        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 1, "Should have 1 provider in the store");
        let provider = providers
            .get(&crate::storage::provider_id(&provider_id, &link_name))
            .expect("Provider should exist");
        assert_eq!(provider.id, provider_id, "Data should match");
        assert_eq!(provider.link_name, link_name, "Data should match");
        assert!(
            provider.hosts.contains_key(&host_id),
            "Should have found host in provider store"
        );
    }

    #[tokio::test]
    async fn test_provider_status_update() {
        let store = Arc::new(TestStore::default());
        let lattice_source = TestLatticeSource::default();
        let lattice_id = "provider_status";
        let command_publisher = CommandPublisher::new(NoopPublisher, "doesntmatter");
        let status_publisher = StatusPublisher::new(NoopPublisher, "doesntmatter");
        let worker = EventWorker::new(
            store.clone(),
            lattice_source.clone(),
            command_publisher.clone(),
            status_publisher.clone(),
            ScalerManager::test_new(
                NoopPublisher,
                lattice_id,
                store.clone(),
                command_publisher,
                lattice_source,
            )
            .await,
        );

        let host_id = "CLOUDCITY".to_string();

        // Trigger a provider started and then a health check
        let provider = ProviderStarted {
            claims: ProviderClaims {
                issuer: "Lando Calrissian".into(),
                name: "Tibanna Gas Mining".into(),
                version: "0.1.0".into(),
                ..Default::default()
            },
            image_ref: "bespin.lando.inc/tibanna:0.1.0".into(),
            public_key: "GAS".into(),
            host_id: host_id.clone(),
            annotations: HashMap::default(),
            instance_id: String::new(),
            contract_id: "mining".into(),
            link_name: "default".into(),
        };

        worker
            .handle_provider_started(lattice_id, &provider)
            .await
            .expect("Should be able to handle provider started event");
        worker
            .handle_provider_health_check(
                lattice_id,
                &host_id,
                &ProviderHealthCheckInfo {
                    link_name: provider.link_name.clone(),
                    public_key: provider.public_key.clone(),
                    contract_id: provider.contract_id.clone(),
                },
                false,
            )
            .await
            .expect("Should be able to handle a provider health check event");

        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 1, "Only 1 provider should exist");
        let prov = providers
            .get(&crate::storage::provider_id(
                &provider.public_key,
                &provider.link_name,
            ))
            .expect("Provider should exist");
        assert!(
            matches!(
                prov.hosts
                    .get(&host_id)
                    .expect("Should find status for host"),
                ProviderStatus::Running
            ),
            "Provider should have a running status"
        );

        // Now try a failed status
        worker
            .handle_provider_health_check(
                lattice_id,
                &host_id,
                &ProviderHealthCheckInfo {
                    link_name: provider.link_name.clone(),
                    public_key: provider.public_key.clone(),
                    contract_id: provider.contract_id.clone(),
                },
                true,
            )
            .await
            .expect("Should be able to handle a provider health check event");

        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 1, "Only 1 provider should exist");
        let prov = providers
            .get(&crate::storage::provider_id(
                &provider.public_key,
                &provider.link_name,
            ))
            .expect("Provider should exist");
        assert!(
            matches!(
                prov.hosts
                    .get(&host_id)
                    .expect("Should find status for host"),
                ProviderStatus::Failed
            ),
            "Provider should have a running status"
        );
    }

    #[tokio::test]
    async fn test_provider_contract_id_from_heartbeat() {
        let store = Arc::new(TestStore::default());
        let inventory = Arc::new(RwLock::new(HashMap::default()));
        let lattice_source = TestLatticeSource {
            inventory: inventory.clone(),
            ..Default::default()
        };
        let lattice_id = "provider_contract_id";

        let command_publisher = CommandPublisher::new(NoopPublisher, "doesntmatter");
        let status_publisher = StatusPublisher::new(NoopPublisher, "doesntmatter");
        let worker = EventWorker::new(
            store.clone(),
            lattice_source.clone(),
            command_publisher.clone(),
            status_publisher.clone(),
            ScalerManager::test_new(
                NoopPublisher,
                lattice_id,
                store.clone(),
                command_publisher,
                lattice_source,
            )
            .await,
        );

        let host_id = "BEGGARSCANYON";
        let link_name = "default";
        let public_key = "SKYHOPPER";
        let contract_id = "blasting:womprats";

        *inventory.write().await = HashMap::from_iter([(
            host_id.to_string(),
            HostInventory {
                friendly_name: "my-host-6".to_string(),
                issuer: "my-issuer-4".to_string(),
                actors: vec![],
                labels: HashMap::new(),
                host_id: host_id.to_string(),
                providers: vec![ProviderDescription {
                    contract_id: contract_id.to_string(),
                    link_name: link_name.to_string(),
                    id: public_key.to_string(),
                    annotations: Some(HashMap::new()),
                    image_ref: Some("".to_string()),
                    name: Some("".to_string()),
                    revision: 0,
                }],
            },
        )]);

        // Health check a provider that we don't have in the store yet
        worker
            .handle_provider_health_check(
                lattice_id,
                host_id,
                &ProviderHealthCheckInfo {
                    link_name: link_name.to_string(),
                    public_key: public_key.to_string(),
                    contract_id: contract_id.to_string(),
                },
                false,
            )
            .await
            .expect("Should be able to handle a provider health check event");

        // Now heartbeat a host
        worker
            .handle_host_heartbeat(
                lattice_id,
                &HostHeartbeat {
                    actors: HashMap::default(),
                    friendly_name: "tatooine-1977".to_string(),
                    labels: HashMap::default(),
                    providers: vec![ProviderInfo {
                        contract_id: contract_id.to_string(),
                        link_name: link_name.to_string(),
                        public_key: public_key.to_string(),
                        annotations: HashMap::new(),
                    }],
                    uptime_human: "60s".into(),
                    uptime_seconds: 60,
                    version: semver::Version::parse("0.61.0").unwrap(),
                    id: host_id.to_string(),
                    annotations: HashMap::default(),
                },
            )
            .await
            .expect("Should be able to handle host heartbeat");

        // Now check that our provider exists and has the contract id set
        let providers = store.list::<Provider>(lattice_id).await.unwrap();
        assert_eq!(providers.len(), 1, "Only 1 provider should exist");
        let prov = providers
            .get(&crate::storage::provider_id(public_key, link_name))
            .expect("Provider should exist");
        assert_eq!(
            prov.contract_id, contract_id,
            "Provider should have contract id set"
        );
    }

    #[tokio::test]
    async fn test_heartbeat_updates_stale_data() {
        let store = Arc::new(TestStore::default());
        let inventory = Arc::new(RwLock::new(HashMap::default()));
        let lattice_source = TestLatticeSource {
            inventory: inventory.clone(),
            ..Default::default()
        };
        let lattice_id = "update_data";

        let command_publisher = CommandPublisher::new(NoopPublisher, "doesntmatter");
        let status_publisher = StatusPublisher::new(NoopPublisher, "doesntmatter");
        let worker = EventWorker::new(
            store.clone(),
            lattice_source.clone(),
            command_publisher.clone(),
            status_publisher.clone(),
            ScalerManager::test_new(
                NoopPublisher,
                lattice_id,
                store.clone(),
                command_publisher,
                lattice_source,
            )
            .await,
        );

        let actor_instance = "asdhjkahsd-123123-fasda-feeee";
        let host_id = "jabbaspalace";

        // Store some existing stuff
        store
            .store(
                lattice_id,
                "jabba".to_string(),
                Actor {
                    id: "jabba".to_string(),
                    instances: HashMap::from([(
                        host_id.to_string(),
                        HashSet::from_iter([WadmActorInstance::from_id(
                            actor_instance.to_string(),
                        )]),
                    )]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        *inventory.write().await = HashMap::from_iter([(
            host_id.to_string(),
            HostInventory {
                friendly_name: "my-host-7".to_string(),
                issuer: "my-issuer-5".to_string(),
                actors: vec![ActorDescription {
                    id: "jabba".to_string(),
                    image_ref: None,
                    name: None,
                    instances: vec![
                        ActorInstance {
                            instance_id: "1".to_string(),
                            annotations: None,
                            revision: 0,
                        },
                        ActorInstance {
                            instance_id: "2".to_string(),
                            annotations: None,
                            revision: 0,
                        },
                    ],
                }],
                labels: HashMap::new(),
                host_id: host_id.to_string(),
                providers: vec![],
            },
        )]);

        // Now heartbeat and make sure stuff that isn't running is removed
        worker
            .handle_host_heartbeat(
                lattice_id,
                &HostHeartbeat {
                    actors: HashMap::from([("jabba".to_string(), 2)]),
                    friendly_name: "palace-1983".to_string(),
                    labels: HashMap::default(),
                    providers: vec![],
                    uptime_human: "60s".into(),
                    uptime_seconds: 60,
                    version: semver::Version::parse("0.61.0").unwrap(),
                    id: host_id.to_string(),
                    annotations: HashMap::default(),
                },
            )
            .await
            .expect("Should be able to handle host heartbeat");

        let actors = store.list::<Actor>(lattice_id).await.unwrap();
        assert_eq!(actors.len(), 1, "Should have 1 actor in the store");
        let actor = actors.get("jabba").expect("Actor should exist");
        assert_eq!(actor.count(), 2, "Should now have 2 actors");
    }

    fn assert_actor(
        actors: &HashMap<String, Actor>,
        event: &ActorStarted,
        expected_counts: &[(&str, usize)],
    ) {
        let actor = actors
            .get(&event.public_key)
            .expect("Actor should exist in store");
        assert_eq!(
            actor.id, event.public_key,
            "Actor ID stored should be correct"
        );
        assert_eq!(
            actor.call_alias, event.claims.call_alias,
            "Other data in actor should be correct"
        );
        assert_eq!(
            expected_counts.len(),
            actor.instances.len(),
            "Should have the proper number of hosts the actor is running on"
        );
        for (expected_host, expected_count) in expected_counts.iter() {
            assert_eq!(
                actor
                    .instances
                    .get(*expected_host)
                    .cloned()
                    .expect("Actor should have a count for the host")
                    .len(),
                *expected_count,
                "Actor count on host should be correct"
            );
        }
    }

    fn assert_provider(
        providers: &HashMap<String, Provider>,
        event: &ProviderStarted,
        running_on_hosts: &[&str],
    ) {
        let provider = providers
            .get(&crate::storage::provider_id(
                &event.public_key,
                &event.link_name,
            ))
            .expect("Correct provider should exist in store");
        assert_eq!(
            provider.name, event.claims.name,
            "Provider should have the correct data in state"
        );
        assert!(
            provider.hosts.len() == running_on_hosts.len()
                && running_on_hosts
                    .iter()
                    .all(|host_id| provider.hosts.contains_key(*host_id)),
            "Provider should be set to the correct hosts"
        );
    }
}
