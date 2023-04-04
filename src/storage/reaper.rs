//! Contains helpers for reaping Hosts that haven't received a heartbeat within a configured amount
//! of time and actors and providers on hosts that no longer exist

use std::collections::HashMap;

use chrono::{Duration, Utc};
use tokio::{task::JoinHandle, time};
use tracing::{debug, error, info, instrument, trace};

use super::{Actor, Host, Provider, Store};

/// A struct that can reap various pieces of data from the given store
pub struct Reaper<S> {
    store: S,
    interval: Duration,
    handles: HashMap<String, JoinHandle<()>>,
}

impl<S: Store + Clone + Send + Sync + 'static> Reaper<S> {
    /// Creates a new reaper using the given store configured to check for reaping every
    /// `check_interval` for all passed lattice IDs. This reaper will immediately begin executing
    /// spawned tasks. When the reaper is dropped, it will stop polling all tasks. This function
    /// will panic if you pass it a duration that is larger than the maximum value accepted by the
    /// `chrono` library. As this is a rare case, we don't actually return an error and panic
    /// instead
    ///
    /// The reaper will wait for 2 * `check_interval` before removing anything. For example, if
    /// `check_interval` is set to 30s, then after 30s, the item is considered to be in a "warning"
    /// state. This isn't actually reflected in state right now, but it will be logged. When the
    /// next tick fires (around 60s total), then the item will be removed from the store
    pub fn new(
        store: S,
        check_interval: std::time::Duration,
        lattices_to_observe: impl IntoIterator<Item = String>,
    ) -> Reaper<S> {
        let interval = Duration::from_std(check_interval)
            .expect("The given duration is out of bounds for a max duration value");
        let cloned_store = store.clone();
        let handles = lattices_to_observe.into_iter().map(move |id| {
            (
                id.clone(),
                tokio::spawn(
                    Undertaker {
                        store: cloned_store.clone(),
                        lattice_id: id,
                        interval,
                    }
                    .reap(),
                ),
            )
        });
        Reaper {
            store,
            interval,
            handles: handles.collect(),
        }
    }

    /// Adds a new lattice to be reaped
    pub fn observe(&mut self, lattice_id: String) {
        self.handles.insert(
            lattice_id.clone(),
            tokio::spawn(
                Undertaker {
                    store: self.store.clone(),
                    lattice_id,
                    interval: self.interval,
                }
                .reap(),
            ),
        );
    }

    /// Stops observing the given lattice
    pub fn remove(&mut self, lattice_id: &str) {
        if let Some(handle) = self.handles.remove(lattice_id) {
            handle.abort();
        }
    }
}

struct Undertaker<S> {
    store: S,
    lattice_id: String,
    interval: Duration,
}

impl<S: Store + Clone + Send + Sync + 'static> Undertaker<S> {
    #[instrument(level = "debug", skip(self), fields(lattice_id = %self.lattice_id, check_interval = %self.interval))]
    async fn reap(self) {
        debug!("Starting reaper");
        // SAFETY: We created this Duration from a std Duration, so it should unwrap back just fine
        let mut ticker = time::interval(self.interval.to_std().unwrap());
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        loop {
            ticker.tick().await;
            trace!("Tick fired, running reap tasks");
            // We want to reap hosts first so that the state is up to date for reaping actors and providers
            self.reap_hosts().await;
            // Now get the current list of hosts
            let hosts = match self.store.list::<Host>(&self.lattice_id).await {
                Ok(n) => n,
                Err(e) => {
                    error!(error = %e, "Error when fetching hosts from store. Will retry on next tick");
                    continue;
                }
            };
            // Reap actors and providers simultaneously
            futures::join!(self.reap_actors(&hosts), self.reap_providers(&hosts));
            trace!("Completed reap tasks");
        }
    }

    #[instrument(level = "debug", skip(self), fields(lattice_id = %self.lattice_id))]
    async fn reap_hosts(&self) {
        let hosts = match self.store.list::<Host>(&self.lattice_id).await {
            Ok(n) => n,
            Err(e) => {
                error!(error = %e, "Error when fetching hosts from store. Will retry on next tick");
                return;
            }
        };

        let hosts_to_remove = hosts.into_iter().filter_map(|(id, host)| {
            let elapsed = Utc::now() - host.last_seen;
            if elapsed > (self.interval * 2) {
                info!(%id, friendly_name = %host.friendly_name, "Host has not been seen for 2 intervals. Will reap node");
                Some(id)
            } else if elapsed > self.interval {
                info!(%id, friendly_name = %host.friendly_name, "Host has not been seen for 1 interval. Next check will reap node from store");
                None
            } else {
                None
            }
        });

        if let Err(e) = self
            .store
            .delete_many::<Host, _, _>(&self.lattice_id, hosts_to_remove)
            .await
        {
            error!(error = %e, "Error when deleting hosts from store. Will retry on next tick")
        }
    }

    #[instrument(level = "debug", skip(self, hosts), fields(lattice_id = %self.lattice_id))]
    async fn reap_actors(&self, hosts: &HashMap<String, Host>) {
        let actors = match self.store.list::<Actor>(&self.lattice_id).await {
            Ok(n) => n,
            Err(e) => {
                error!(error = %e, "Error when fetching actors from store. Will retry on next tick");
                return;
            }
        };

        let (actors_to_remove, actors_to_update): (HashMap<String, Actor>, HashMap<String, Actor>) =
            actors
                .into_iter()
                .filter_map(|(id, mut actor)| {
                    let current_num_hosts = actor.count.len();
                    // Only keep the instances where the host exists
                    actor.count.retain(|host_id, _| hosts.contains_key(host_id));
                    // If we got rid of something, that means this needs to update
                    (current_num_hosts != actor.count.len()).then_some((id, actor))
                })
                .partition(|(_, actor)| actor.count.is_empty());

        debug!(to_remove = %actors_to_remove.len(), to_update = %actors_to_update.len(), "Filtered out list of actors to update and reap");

        if let Err(e) = self
            .store
            .store_many(&self.lattice_id, actors_to_update)
            .await
        {
            error!(error = %e, "Error when storing updated actors. Will retry on next tick");
            return;
        }

        if let Err(e) = self
            .store
            .delete_many::<Actor, _, _>(&self.lattice_id, actors_to_remove.keys())
            .await
        {
            error!(error = %e, "Error when deleting actors from store. Will retry on next tick")
        }
    }

    #[instrument(level = "debug", skip(self, hosts), fields(lattice_id = %self.lattice_id))]
    async fn reap_providers(&self, hosts: &HashMap<String, Host>) {
        let providers = match self.store.list::<Provider>(&self.lattice_id).await {
            Ok(n) => n,
            Err(e) => {
                error!(error = %e, "Error when fetching actors from store. Will retry on next tick");
                return;
            }
        };

        let (providers_to_remove, providers_to_update): (
            HashMap<String, Provider>,
            HashMap<String, Provider>,
        ) = providers
            .into_iter()
            .filter_map(|(id, mut provider)| {
                let current_num_hosts = provider.hosts.len();
                // Only keep the instances where the host exists
                provider
                    .hosts
                    .retain(|host_id, _| hosts.contains_key(host_id));
                // If we got rid of something, that means this needs to update
                (current_num_hosts != provider.hosts.len()).then_some((id, provider))
            })
            .partition(|(_, provider)| provider.hosts.is_empty());

        debug!(to_remove = %providers_to_remove.len(), to_update = %providers_to_update.len(), "Filtered out list of providers to update and reap");

        if let Err(e) = self
            .store
            .store_many(&self.lattice_id, providers_to_update)
            .await
        {
            error!(error = %e, "Error when storing updated providers. Will retry on next tick");
            return;
        }

        if let Err(e) = self
            .store
            .delete_many::<Provider, _, _>(&self.lattice_id, providers_to_remove.keys())
            .await
        {
            error!(error = %e, "Error when deleting providers from store. Will retry on next tick")
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{collections::HashSet, sync::Arc};

    use crate::{
        storage::{ProviderStatus, ReadStore},
        test_util::TestStore,
    };

    #[tokio::test]
    async fn test_reaping() {
        let store = Arc::new(TestStore::default());

        let lattice_id = "reaper";
        let actor_id = "testactor";
        let host1_id = "host1";
        let host2_id = "host2";

        // Prepopulate the store
        store
            .store_many(
                lattice_id,
                [
                    (
                        actor_id.to_string(),
                        Actor {
                            id: actor_id.to_string(),
                            count: HashMap::from([
                                (host1_id.to_string(), 1),
                                (host2_id.to_string(), 1),
                            ]),
                            ..Default::default()
                        },
                    ),
                    (
                        "idontexist".to_string(),
                        Actor {
                            id: "idontexist".to_string(),
                            count: HashMap::from([(host1_id.to_string(), 1)]),
                            ..Default::default()
                        },
                    ),
                ],
            )
            .await
            .unwrap();

        store
            .store(
                lattice_id,
                "fakeprovider".to_string(),
                Provider {
                    id: "fakeprovider".to_string(),
                    hosts: HashMap::from([(host1_id.to_string(), ProviderStatus::Running)]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        store
            .store_many(
                lattice_id,
                [
                    (
                        host1_id.to_string(),
                        Host {
                            actors: HashMap::from([(actor_id.to_string(), 1)]),
                            providers: HashSet::default(),
                            id: host1_id.to_string(),
                            last_seen: Utc::now(),
                            ..Default::default()
                        },
                    ),
                    (
                        host2_id.to_string(),
                        Host {
                            actors: HashMap::default(),
                            providers: HashSet::default(),
                            id: host2_id.to_string(),
                            // Make this host stick around for longer
                            last_seen: Utc::now() + Duration::milliseconds(600),
                            ..Default::default()
                        },
                    ),
                ],
            )
            .await
            .unwrap();

        let reap_interval = std::time::Duration::from_millis(500);
        // Interval + wiggle
        let wait = reap_interval + std::time::Duration::from_millis(100);
        let _reaper = Reaper::new(store.clone(), reap_interval, [lattice_id.to_owned()]);

        // Wait for first node to be reaped (two ticks)
        tokio::time::sleep(wait * 2).await;

        // Now check that the providers, actors, and hosts were reaped
        let hosts = store.list::<Host>(lattice_id).await.unwrap();
        assert_eq!(hosts.len(), 1, "Only one host should be left");
        let actors = store.list::<Actor>(lattice_id).await.unwrap();
        assert_eq!(actors.len(), 1, "Only one actor should remain in the store");
        actors
            .get(actor_id)
            .expect("Should have the correct actor in the store");

        assert!(
            store.list::<Provider>(lattice_id).await.unwrap().is_empty(),
            "No providers should exist"
        );
    }
}
