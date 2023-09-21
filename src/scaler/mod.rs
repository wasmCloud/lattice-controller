use std::{sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use tokio::{
    sync::{Mutex, RwLock},
    task::JoinHandle,
};
use tracing::{instrument, trace, Instrument};

use crate::{
    commands::Command,
    events::{
        ActorsStartFailed, ActorsStarted, ActorsStopped, Event, Linkdef, LinkdefSet,
        ProviderStartFailed, ProviderStarted,
    },
    model::TraitProperty,
    publisher::Publisher,
    server::StatusInfo,
};

pub mod daemonscaler;
pub mod manager;
pub mod spreadscaler;

use manager::Notifications;

const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// A trait describing a struct that can be configured to compute the difference between
/// desired state and configured state, returning a set of commands to approach desired state.
///
/// Implementers of this trait can choose how to access state, but it's generally recommended to
/// use a [ReadStore](crate::storage::ReadStore) so that it can retrieve current information about
/// state using a common trait that only allows store access and not modification
///
/// Typically a Scaler should be configured with `update_config`, then use the `reconcile` method
/// for an inital set of commands. As events change the state, they should also be given to the Scaler
/// to determine if actions need to be taken in response to an event
#[async_trait]
pub trait Scaler {
    /// A unique identifier for this scaler type. This is used for logging and for selecting
    /// specific scalers as needed. Generally this should be something like
    /// `$NAME_OF_SCALER_TYPE-$MODEL_NAME-$OCI_REF`. However, the only requirement is that it can
    /// uniquely identify a scaler
    fn id(&self) -> &str;

    /// Determine the status of this scaler according to reconciliation logic. This is the opportunity
    /// for scalers to indicate that they are unhealthy with a message as to what's missing.
    async fn status(&self) -> StatusInfo;

    /// Provide a scaler with configuration to use internally when computing commands This should
    /// trigger a reconcile with the new configuration.
    ///
    /// This config can be anything that can be turned into a
    /// [`TraitProperty`](crate::model::TraitProperty). Additional configuration outside of what is
    /// available in a `TraitProperty` can be passed when constructing the scaler
    async fn update_config(&mut self, config: TraitProperty) -> Result<Vec<Command>>;

    /// Compute commands that must be taken given an event that changes the lattice state
    async fn handle_event(&self, event: &Event) -> Result<Vec<Command>>;

    /// Compute commands that must be taken to achieve desired state as specified in config
    async fn reconcile(&self) -> Result<Vec<Command>>;

    /// Returns the list of commands needed to cleanup for a scaler
    ///
    /// This purposefully does not consume the scaler so that if there is a failure it can be kept
    /// around
    async fn cleanup(&self) -> Result<Vec<Command>>;
}

/// The BackoffAwareScaler is a wrapper around a scaler that is responsible for
/// computing a proper backoff in terms of `expected_events` for the scaler based
/// on its commands. When the BackoffAwareScaler handles events that it's expecting,
/// it does not compute new commands and instead removes them from the list.
///
/// This effectively allows the inner Scaler to only worry about the logic around
/// reconciling and handling events, rather than be concerned about whether or not
/// it should handle a specific event, if it's causing jitter, overshoot, etc.
///
/// The `notifier` is used to publish notifications to add, remove, or recompute
/// expected events with scalers on other wadm instances, as only one wadm instance
/// at a time will handle a specific event.
pub(crate) struct BackoffAwareScaler<T, P> {
    scaler: T,
    notifier: P,
    notify_subject: String,
    model_name: String,
    /// A list of (success, Option<failure>) events that the scaler is expecting
    #[allow(clippy::type_complexity)]
    expected_events: Arc<RwLock<Vec<(Event, Option<Event>)>>>,
    /// Responsible for clearing up the expected events list after a certain amount of time
    event_cleaner: Mutex<Option<JoinHandle<()>>>,
    /// The amount of time to wait before cleaning up the expected events list
    cleanup_timeout: std::time::Duration,
}

impl<T, P> BackoffAwareScaler<T, P>
where
    T: Scaler + Send + Sync,
    P: Publisher + Send + Sync + 'static,
{
    /// Wraps the given scaler in a new backoff aware scaler. `cleanup_timeout` can be set to a
    /// desired waiting time, otherwise it will default to 30s
    pub fn new(
        scaler: T,
        notifier: P,
        notify_subject: &str,
        model_name: &str,
        cleanup_timeout: Option<Duration>,
    ) -> Self {
        Self {
            scaler,
            notifier,
            notify_subject: notify_subject.to_owned(),
            model_name: model_name.to_string(),
            expected_events: Arc::new(RwLock::new(Vec::new())),
            event_cleaner: Mutex::new(None),
            cleanup_timeout: cleanup_timeout.unwrap_or(DEFAULT_WAIT_TIMEOUT),
        }
    }

    pub async fn event_count(&self) -> usize {
        self.expected_events.read().await.len()
    }

    /// Adds events to the expected events list
    ///
    /// # Arguments
    /// `events` - A list of (success, failure) events to add to the expected events list
    /// `clear_previous` - If true, clears the previous expected events list before adding the new events
    async fn add_events<I>(&self, events: I, clear_previous: bool)
    where
        I: IntoIterator<Item = (Event, Option<Event>)>,
    {
        let mut expected_events = self.expected_events.write().await;
        if clear_previous {
            expected_events.clear();
        }
        expected_events.extend(events);
        self.set_timed_cleanup().await;
    }

    /// Removes an event pair from the expected events list if one matches the given event
    /// Returns true if the event was removed, false otherwise
    async fn remove_event(&self, event: &Event) -> Result<bool> {
        let mut expected_events = self.expected_events.write().await;
        let before_count = expected_events.len();
        expected_events.retain(|(success, fail)| {
            // Retain the event if it doesn't match either the success or optional failure event.
            // Most events have a possibility of seeing a failure and either one means we saw the
            // event we were expecting
            !evt_matches_expected(success, event)
                && !fail
                    .as_ref()
                    .map(|f| evt_matches_expected(f, event))
                    .unwrap_or(false)
        });
        Ok(expected_events.len() != before_count)
    }

    /// Handles an incoming event for the given scaler.
    ///
    /// This function processes the event and returns a vector of commands to be executed.
    /// It also manages the expected events list, removing successfully handled events
    /// and adding new expected events based on the executed commands, and using the notifier
    /// to send notifications to other scalers running on different wadm instances.
    ///
    /// # Arguments
    ///
    /// * `scaler`: A reference to the `ScalerWithEvents` struct which represents the scaler with events.
    /// * `event`: A reference to the `Event` struct which represents the incoming event to be handled.
    ///
    /// # Returns
    ///
    /// * `Result<Vec<Command>>`: A `Result` containing a vector of `Command` structs if successful,
    ///   or an error of type `anyhow::Error` if any error occurs while processing the event.
    #[instrument(level = "trace", skip_all, fields(scaler_id = %self.id()))]
    async fn handle_event_internal(&self, event: &Event) -> anyhow::Result<Vec<Command>> {
        let model_name = &self.model_name;
        let commands: Vec<Command> = if self.remove_event(event).await? {
            trace!("Scaler received event that it was expecting");
            let data = serde_json::to_vec(&Notifications::RemoveExpectedEvent {
                name: model_name.to_owned(),
                scaler_id: self.scaler.id().to_owned(),
                event: event.to_owned().try_into()?,
            })?;
            self.notifier
                .publish(data, Some(&self.notify_subject))
                .await?;

            // The scaler was expecting this event and it shouldn't respond with commands
            Vec::with_capacity(0)
        } else if self.event_count().await > 0 {
            trace!("Scaler received event but is still expecting events, ignoring");
            // If a scaler is expecting events still, don't have it handle events. This is effectively
            // the backoff mechanism within wadm
            Vec::with_capacity(0)
        } else {
            trace!("Scaler is not backing off, handling event");
            let commands = self.scaler.handle_event(event).await?;

            // Based on the commands, compute the events that we expect to see for this scaler. The scaler
            // will then ignore incoming events until all of the expected events have been received.
            let expected_events = commands
                .iter()
                .filter_map(|cmd| cmd.corresponding_event(model_name));

            // Only let other scalers know if we generated commands to take
            if !commands.is_empty() {
                trace!("Scaler generated commands, notifying other scalers to register expected events");
                let data = serde_json::to_vec(&Notifications::RegisterExpectedEvents {
                    name: model_name.to_owned(),
                    scaler_id: self.scaler.id().to_owned(),
                    triggering_event: Some(event.to_owned().try_into()?),
                })?;

                self.notifier
                    .publish(data, Some(&self.notify_subject))
                    .await?;
            }

            self.add_events(expected_events, false).await;
            commands
        };

        Ok(commands)
    }

    #[instrument(level = "trace", skip_all, fields(scaler_id = %self.id()))]
    async fn reconcile_internal(&self) -> Result<Vec<Command>> {
        // If we're already in backoff, return an empty list
        let current_event_count = self.event_count().await;
        if current_event_count > 0 {
            trace!(%current_event_count, "Scaler is backing off, not reconciling");
            return Ok(Vec::with_capacity(0));
        }
        match self.scaler.reconcile().await {
            // "Back off" scaler with expected corresponding events if the scaler generated commands
            Ok(commands) if !commands.is_empty() => {
                trace!("Reconcile generated commands, notifying other scalers to register expected events");
                let data = serde_json::to_vec(&Notifications::RegisterExpectedEvents {
                    name: self.model_name.to_owned(),
                    scaler_id: self.scaler.id().to_owned(),
                    triggering_event: None,
                })?;
                self.notifier
                    .publish(data, Some(&self.notify_subject))
                    .await?;
                self.add_events(
                    commands
                        .iter()
                        .filter_map(|command| command.corresponding_event(&self.model_name)),
                    true,
                )
                .await;
                Ok(commands)
            }
            Ok(commands) => {
                trace!("Reconcile generated no commands, no need to register expected events");
                Ok(commands)
            }
            Err(e) => Err(e),
        }
    }

    /// Sets a timed cleanup task to clear the expected events list after a timeout
    async fn set_timed_cleanup(&self) {
        let mut event_cleaner = self.event_cleaner.lock().await;
        // Clear any existing handle
        if let Some(handle) = event_cleaner.take() {
            handle.abort();
        }
        let expected_events = self.expected_events.clone();
        let timeout = self.cleanup_timeout;

        *event_cleaner = Some(tokio::spawn(
            async move {
                tokio::time::sleep(timeout).await;
                trace!("Reached event cleanup timeout, clearing expected events");
                expected_events.write().await.clear();
            }
            .instrument(tracing::trace_span!("event_cleaner", scaler_id = %self.id())),
        ));
    }
}

#[async_trait]
/// The [Scaler](Scaler) trait implementation for the [BackoffAwareScaler](BackoffAwareScaler)
/// is mostly a simple wrapper, with two exceptions, which allow scalers to sync expected
/// events between different wadm instances.
///
/// * `handle_event` calls an internal method that uses a notifier to publish notifications to
///   all Scalers, even running on different wadm instances, to handle that event. The resulting
///   commands from those scalers are ignored as this instance is already handling the event.
/// * `reconcile` calls an internal method that uses a notifier to ensure all Scalers, even
///   running on different wadm instances, compute their expected events in response to the
///   reconciliation commands in order to "back off".
impl<T, P> Scaler for BackoffAwareScaler<T, P>
where
    T: Scaler + Send + Sync,
    P: Publisher + Send + Sync + 'static,
{
    fn id(&self) -> &str {
        // Pass through the ID of the wrapped scaler
        self.scaler.id()
    }

    async fn status(&self) -> StatusInfo {
        self.scaler.status().await
    }

    async fn update_config(&mut self, config: TraitProperty) -> Result<Vec<Command>> {
        self.scaler.update_config(config).await
    }

    async fn handle_event(&self, event: &Event) -> Result<Vec<Command>> {
        self.handle_event_internal(event).await
    }

    async fn reconcile(&self) -> Result<Vec<Command>> {
        self.reconcile_internal().await
    }

    async fn cleanup(&self) -> Result<Vec<Command>> {
        self.scaler.cleanup().await
    }
}

/// A specialized function that compares an incoming lattice event to an "expected" event
/// stored alongside a [Scaler](Scaler).
///
/// This is not a PartialEq or Eq implementation because there are strict assumptions that do not always hold.
/// For example, an incoming and expected event are equal even if their claims are not equal, because we cannot
/// compute that information from a [Command](Command). However, this is not a valid comparison if actually
/// comparing two events for equality.
fn evt_matches_expected(incoming: &Event, expected: &Event) -> bool {
    match (incoming, expected) {
        (
            // NOTE(brooksmtownsend): It may be worth it to simply use the count here as
            // extra information. If we receive the exact event but the count is different, that
            // may mean some instances failed to start on that host. The cause for this isn't
            // well known but if we find ourselves missing expected events we should revisit
            Event::ActorsStarted(ActorsStarted {
                annotations: a1,
                image_ref: i1,
                count: c1,
                host_id: h1,
                ..
            }),
            Event::ActorsStarted(ActorsStarted {
                annotations: a2,
                image_ref: i2,
                count: c2,
                host_id: h2,
                ..
            }),
        ) => a1 == a2 && i1 == i2 && c1 == c2 && h1 == h2,
        (
            Event::ActorsStartFailed(ActorsStartFailed {
                annotations: a1,
                image_ref: i1,
                host_id: h1,
                ..
            }),
            Event::ActorsStartFailed(ActorsStartFailed {
                annotations: a2,
                image_ref: i2,
                host_id: h2,
                ..
            }),
        ) => a1 == a2 && i1 == i2 && h1 == h2,
        (
            Event::ActorsStopped(ActorsStopped {
                annotations: a1,
                public_key: p1,
                count: c1,
                host_id: h1,
                ..
            }),
            Event::ActorsStopped(ActorsStopped {
                annotations: a2,
                public_key: p2,
                count: c2,
                host_id: h2,
                ..
            }),
        ) => a1 == a2 && p1 == p2 && c1 == c2 && h1 == h2,
        (
            Event::ProviderStarted(ProviderStarted {
                annotations: a1,
                image_ref: i1,
                link_name: l1,
                host_id: h1,
                ..
            }),
            Event::ProviderStarted(ProviderStarted {
                annotations: a2,
                image_ref: i2,
                link_name: l2,
                host_id: h2,
                ..
            }),
        ) => a1 == a2 && i1 == i2 && l1 == l2 && h1 == h2,
        // NOTE(brooksmtownsend): This is a little less information than we really need here.
        // Image reference + annotations would be nice
        (
            Event::ProviderStartFailed(ProviderStartFailed {
                link_name: l1,
                host_id: h1,
                ..
            }),
            Event::ProviderStartFailed(ProviderStartFailed {
                link_name: l2,
                host_id: h2,
                ..
            }),
        ) => l1 == l2 && h1 == h2,
        (
            Event::LinkdefSet(LinkdefSet {
                linkdef:
                    Linkdef {
                        actor_id: a1,
                        contract_id: c1,
                        link_name: l1,
                        provider_id: p1,
                        values: v1,
                        ..
                    },
            }),
            Event::LinkdefSet(LinkdefSet {
                linkdef:
                    Linkdef {
                        actor_id: a2,
                        contract_id: c2,
                        link_name: l2,
                        provider_id: p2,
                        values: v2,
                        ..
                    },
            }),
        ) => a1 == a2 && c1 == c2 && l1 == l2 && p1 == p2 && v1 == v2,
        _ => false,
    }
}
