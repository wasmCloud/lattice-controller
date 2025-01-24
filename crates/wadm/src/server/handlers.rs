use std::collections::HashMap;

use anyhow::anyhow;
use async_nats::{jetstream::stream::Stream, Client, Message, Subject};
use serde_json::json;
use tracing::{debug, error, instrument, trace};
use wadm_types::api::{ModelSummary, StatusInfo, StatusType};
use wadm_types::validation::{is_valid_manifest_name, validate_manifest_version, ValidationOutput};
use wadm_types::{
    api::{
        DeleteModelRequest, DeleteModelResponse, DeleteResult, DeployModelRequest,
        DeployModelResponse, DeployResult, GetModelRequest, GetModelResponse, GetResult,
        ListModelsResponse, PutModelResponse, PutResult, PutResultSuccess, Status, StatusResponse,
        StatusResult, UndeployModelRequest, VersionInfo, VersionResponse,
    },
    CapabilityProperties, Manifest, Properties,
};
use wadm_types::{ComponentProperties, LATEST_VERSION};

use crate::{model::StoredManifest, publisher::Publisher};

use super::{parser::parse_manifest, storage::ModelStorage, ManifestNotifier};

pub(crate) struct Handler<P> {
    pub(crate) store: ModelStorage,
    pub(crate) client: Client,
    pub(crate) notifier: ManifestNotifier<P>,
    pub(crate) status_stream: Stream,
}

impl<P: Publisher> Handler<P> {
    #[instrument(level = "debug", skip(self, msg))]
    pub async fn put_model(&self, msg: Message, account_id: Option<&str>, lattice_id: &str) {
        trace!("Parsing incoming manifest");
        let manifest = match parse_manifest(msg.payload.into(), msg.headers.as_ref()) {
            Ok(m) => m,
            Err(e) => {
                self.send_error(msg.reply, format!("Unable to parse manifest: {e:?}"))
                    .await;
                return;
            }
        };

        trace!(
            ?manifest,
            "Manifest is valid. Fetching current manifests from store"
        );

        let manifest_validation_output = validate_manifest_version(manifest.version());
        let manifest_validation_errors = manifest_validation_output.errors();
        if !manifest_validation_errors.is_empty() {
            self.send_error(
                msg.reply,
                format!(
                    "invalid manifest version, errors: {:#?}",
                    manifest_validation_errors
                        .iter()
                        .map(|e| e.msg.clone())
                        .collect::<Vec<String>>()
                        .join("\n")
                ),
            )
            .await;
            return;
        }

        let manifest_name = manifest.metadata.name.trim().to_string();
        if !is_valid_manifest_name(&manifest_name) {
            self.send_error(
                msg.reply,
                format!(
                    "Manifest name {manifest_name} contains invalid characters. Manifest names can only contain alphanumeric characters, dashes, and underscores.",
                ),
            )
            .await;
            return;
        }

        let (mut current_manifests, current_revision) =
            match self.store.get(account_id, lattice_id, &manifest_name).await {
                Ok(Some(data)) => data,
                Ok(None) => (StoredManifest::default(), 0),
                Err(e) => {
                    error!(error = %e, "Unable to fetch data from store");
                    self.send_error(msg.reply, "Internal storage error".to_string())
                        .await;
                    return;
                }
            };

        if let Some(error_message) = validate_manifest(&manifest).await.err() {
            self.send_error(msg.reply, error_message.to_string()).await;
            return;
        }

        let all_stored_manifests = self
            .store
            .list(account_id, lattice_id)
            .await
            .unwrap_or_default();
        let deployed_shared_apps: Vec<&Manifest> = all_stored_manifests
            .iter()
            // Only keep deployed, shared applications
            .filter(|manifest| {
                manifest.deployed_version().is_some() && manifest.get_current().shared()
            })
            .map(|manifest| manifest.get_current())
            .collect();

        // NOTE(brooksmtownsend): You can put an application with missing shared components, because
        // the time where you truly need them is when you deploy the application. This can cause a bit
        // of friction when it comes to deploy, but it avoids the frustrating race condition where you
        // - Put the application looking for a deployed shared component
        // - Undeploy the application with the shared component
        // - Deploy the new application looking for the shared component (error)
        let missing_shared_components = manifest.missing_shared_components(&deployed_shared_apps);
        let message = if missing_shared_components.is_empty() {
            format!(
                "Successfully put manifest {} {}",
                manifest_name,
                current_manifests.current_version().to_owned()
            )
        } else {
            format!(
                "Successfully put manifest {} {}, but some shared components are not deployed: {:?}",
                manifest_name,
                current_manifests.current_version().to_owned(),
                missing_shared_components
            )
        };

        let incoming_version = manifest.version().to_owned();
        if !current_manifests.add_version(manifest) {
            self.send_error(
                msg.reply,
                format!("Manifest version {} already exists", incoming_version),
            )
            .await;
            return;
        }

        let resp: PutModelResponse = PutModelResponse {
            // If we successfully insert, the given manifest version will be the new current version
            current_version: current_manifests.current_version().to_string(),
            result: if current_manifests.count() == 1 {
                PutResult::Success(PutResultSuccess::Created)
            } else {
                PutResult::Success(PutResultSuccess::NewVersion(
                    current_manifests.current_version().to_string(),
                ))
            },
            name: manifest_name.clone(),
            total_versions: current_manifests.count(),
            message,
        };

        trace!(total_manifests = %resp.total_versions, "Storing manifests");
        if let Err(e) = self
            .store
            .set(
                account_id,
                lattice_id,
                current_manifests,
                Some(current_revision),
            )
            .await
        {
            error!(error = %e, "Unable to store updated data");
            self.send_error(msg.reply, "Internal storage error".to_string())
                .await;
            return;
        }

        trace!("Storage complete, sending reply");
        self.send_reply(
            msg.reply,
            // NOTE: We are constructing all data here, so this shouldn't fail, but just in case we
            // unwrap to nothing
            serde_json::to_vec(&resp).unwrap_or_default(),
        )
        .await
    }

    #[instrument(level = "debug", skip(self, msg))]
    pub async fn get_model(
        &self,
        msg: Message,
        account_id: Option<&str>,
        lattice_id: &str,
        name: &str,
    ) {
        // For empty payloads, just fetch the latest version
        let req: GetModelRequest = if msg.payload.is_empty() {
            GetModelRequest { version: None }
        } else {
            match serde_json::from_reader(std::io::Cursor::new(msg.payload)) {
                Ok(r) => r,
                Err(e) => {
                    self.send_error(
                        msg.reply,
                        format!("Unable to parse get application request: {e:?}"),
                    )
                    .await;
                    return;
                }
            }
        };

        let (manifests, _) = match self.store.get(account_id, lattice_id, name).await {
            Ok(Some(m)) => m,
            Ok(None) => {
                self.send_reply(
                    msg.reply,
                    // NOTE: We are constructing all data here, so this shouldn't fail, but just in
                    // case we unwrap to nothing
                    serde_json::to_vec(&GetModelResponse {
                        result: GetResult::NotFound,
                        message: format!("Application with the name {name} not found"),
                        manifest: None,
                    })
                    .unwrap_or_default(),
                )
                .await;
                return;
            }
            Err(e) => {
                error!(error = %e, "Unable to fetch data");
                self.send_error(msg.reply, "Internal storage error".to_string())
                    .await;
                return;
            }
        };
        let reply = match req.version {
            Some(version) => {
                if let Some(current) = manifests.get_version(&version) {
                    GetModelResponse {
                        manifest: Some(current.to_owned()),
                        result: GetResult::Success,
                        message: format!("Successfully fetched application {name} {version}"),
                    }
                } else {
                    self.send_reply(
                        msg.reply,
                        // NOTE: We are constructing all data here, so this shouldn't fail, but just
                        // in case we unwrap to nothing
                        serde_json::to_vec(&GetModelResponse {
                            result: GetResult::NotFound,
                            message: format!(
                                "Application {name} with version {version} doesn't exist"
                            ),
                            manifest: None,
                        })
                        .unwrap_or_default(),
                    )
                    .await;
                    return;
                }
            }
            None => GetModelResponse {
                manifest: Some(manifests.get_current().to_owned()),
                result: GetResult::Success,
                message: format!("Successfully fetched application {name}"),
            },
        };
        // NOTE: We _just_ deserialized this from the store above, so we should be just fine. but
        // just in case we unwrap to the default
        self.send_reply(msg.reply, serde_json::to_vec(&reply).unwrap_or_default())
            .await
    }

    // NOTE: This is the same as list_models but responds with just the list of models instead of using
    // the new wrapper type. This can be removed in 0.15.0 once clients all query the new subject
    #[instrument(level = "debug", skip(self, msg))]
    pub(crate) async fn list_models_deprecated(
        &self,
        msg: Message,
        account_id: Option<&str>,
        lattice_id: &str,
    ) {
        let stored_manifests = match self.store.list(account_id, lattice_id).await {
            Ok(d) => d,
            Err(e) => {
                error!(error = %e, "Unable to fetch data");
                self.send_error(msg.reply, "Internal storage error".to_string())
                    .await;
                return;
            }
        };

        let application_summaries = stored_manifests.into_iter().map(|manifest| async {
            let status = self
                .get_manifest_status(lattice_id, manifest.name())
                .await
                .unwrap_or_else(|| {
                    Status::new(StatusInfo::waiting(
                "Waiting for status: Lattice contains no hosts, deployment not started.",
            ), vec![])
                });
            summary_from_manifest_status(manifest, status)
        });

        let models: Vec<ModelSummary> = futures::future::join_all(application_summaries).await;

        // NOTE: We _just_ deserialized this from the store above and then manually constructed it,
        // so we should be just fine. Just in case though, we unwrap to default
        self.send_reply(msg.reply, serde_json::to_vec(&models).unwrap_or_default())
            .await
    }

    #[instrument(level = "debug", skip(self, msg))]
    pub async fn list_models(&self, msg: Message, account_id: Option<&str>, lattice_id: &str) {
        let stored_manifests = match self.store.list(account_id, lattice_id).await {
            Ok(d) => d,
            Err(e) => {
                error!(error = %e, "Unable to fetch data");
                self.send_error(msg.reply, "Internal storage error".to_string())
                    .await;
                return;
            }
        };

        let application_summaries = stored_manifests.into_iter().map(|manifest| async {
            let status = self
                .get_manifest_status(lattice_id, manifest.name())
                .await
                .unwrap_or_else(|| {
                    Status::new(StatusInfo::waiting(
                "Waiting for status: Lattice contains no hosts, deployment not started.",
            ), vec![])
                });
            summary_from_manifest_status(manifest, status)
        });

        let models: Vec<ModelSummary> = futures::future::join_all(application_summaries).await;

        let reply = ListModelsResponse {
            result: GetResult::Success,
            message: "Successfully fetched list of applications".to_string(),
            models,
        };

        // NOTE: We _just_ deserialized this from the store above and then manually constructed it,
        // so we should be just fine. Just in case though, we unwrap to default
        self.send_reply(msg.reply, serde_json::to_vec(&reply).unwrap_or_default())
            .await
    }

    // NOTE(thomastaylor312): This method differs from the wadm 0.3 docs as it doesn't include
    // timestamp (at least for now). However, this is guaranteed to return the list of versions
    // ordered by time of creation. When we document, we should change this to reflect that
    #[instrument(level = "debug", skip(self, msg))]
    pub async fn list_versions(
        &self,
        msg: Message,
        account_id: Option<&str>,
        lattice_id: &str,
        name: &str,
    ) {
        let data: VersionResponse = match self.store.get(account_id, lattice_id, name).await {
            Ok(Some((manifest, _))) => VersionResponse {
                result: GetResult::Success,
                message: format!("Successfully fetched versions for application {name}"),
                versions: manifest
                    .all_versions()
                    .into_iter()
                    .cloned()
                    .map(|v| {
                        let deployed = manifest.is_deployed(&v);
                        VersionInfo {
                            version: v,
                            deployed,
                        }
                    })
                    .collect(),
            },
            Ok(None) => VersionResponse {
                result: GetResult::NotFound,
                message: format!("Application with the name {name} not found"),
                versions: Vec::with_capacity(0),
            },
            Err(e) => {
                error!(error = %e, "Unable to fetch data");
                self.send_error(msg.reply, "Internal storage error".to_string())
                    .await;
                return;
            }
        };
        // NOTE: We _just_ deserialized this from the store above and then manually constructed it,
        // so we should be just fine. Just in case though, we unwrap to default
        self.send_reply(msg.reply, serde_json::to_vec(&data).unwrap_or_default())
            .await
    }

    #[instrument(level = "debug", skip(self, msg))]
    pub async fn delete_model(
        &self,
        msg: Message,
        account_id: Option<&str>,
        lattice_id: &str,
        name: &str,
    ) {
        let req: DeleteModelRequest = if msg.payload.is_empty() {
            DeleteModelRequest { version: None }
        } else {
            match serde_json::from_reader(std::io::Cursor::new(msg.payload)) {
                Ok(r) => r,
                Err(e) => {
                    self.send_error(
                        msg.reply,
                        format!("Unable to parse delete application request: {e:?}"),
                    )
                    .await;
                    return;
                }
            }
        };

        // TODO(#451): if shared and deployed, make sure that no other shared apps are using it
        let reply_data = {
            match self.store.get(account_id, lattice_id, name).await {
                Ok(Some((mut current, current_revision))) => {
                    if let Some(version) = req.version {
                        let deleted = current.delete_version(&version);
                        if deleted && !current.is_empty() {
                            // If the version we deleted was the deployed one, undeploy it
                            let deployed_version = current.deployed_version();
                            let undeploy = if deployed_version
                                .map(|v| v == version)
                                .unwrap_or(false)
                            {
                                trace!(?deployed_version, deleted_version = %version, "Deployed version matches deleted. Will undeploy");
                                current.undeploy();
                                true
                            } else {
                                trace!(?deployed_version, deleted_version = %version, "Deployed version does not match deleted version. Will not undeploy");
                                false
                            };
                            self.store
                                .set(account_id, lattice_id, current, Some(current_revision))
                                .await
                                .map(|_| DeleteModelResponse {
                                    result: DeleteResult::Deleted,
                                    message: format!(
                                        "Successfully deleted version {version} of application {name}"
                                    ),
                                    undeploy,
                                })
                                .unwrap_or_else(|e| {
                                    error!(error = %e, "Unable to delete data");
                                    DeleteModelResponse {
                                        result: DeleteResult::Error,
                                        message: format!(
                                            "Internal storage error when deleting {version} of application {name}"
                                        ),
                                        undeploy: false,
                                    }
                                })
                        } else if deleted && current.is_empty() {
                            // If we deleted the last one, delete the model from the store
                            self.store
                                .delete(account_id, lattice_id, name)
                                .await
                                .map(|_| DeleteModelResponse {
                                    result: DeleteResult::Deleted,
                                    message: format!(
                                        "Successfully deleted last version of application {name}"
                                    ),
                                    // By default if it is all gone, we definitely undeployed things
                                    undeploy: true,
                                })
                                .unwrap_or_else(|e| {
                                    error!(error = %e, "Unable to delete data");
                                    DeleteModelResponse {
                                        result: DeleteResult::Deleted,
                                        message: format!(
                                            "Internal storage error when deleting {version} of application {name}"
                                        ),
                                        undeploy: false,
                                    }
                                })
                        } else {
                            DeleteModelResponse {
                                result: DeleteResult::Noop,
                                message: format!("Application version {version} doesn't exist"),
                                undeploy: false,
                            }
                        }
                    } else {
                        match self.store.delete(account_id, lattice_id, name).await {
                            Ok(_) => {
                                DeleteModelResponse {
                                    result: DeleteResult::Deleted,
                                    message: format!("Successfully deleted application {name}"),
                                    // By default if it is all gone, we definitely undeployed things
                                    undeploy: true,
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "Unable to delete data");
                                DeleteModelResponse {
                                    result: DeleteResult::Error,
                                    message: format!(
                                        "Internal storage error when deleting application {name}"
                                    ),
                                    undeploy: false,
                                }
                            }
                        }
                    }
                }
                Ok(None) => DeleteModelResponse {
                    result: DeleteResult::Noop,
                    message: format!("Application {name} doesn't exist or was already deleted"),
                    undeploy: false,
                },
                Err(e) => {
                    error!(error = %e, "Unable to fetch current manifest data for application {name}");
                    DeleteModelResponse {
                        result: DeleteResult::Error,
                        message: format!("Internal storage error while fetching manifest data for application {name}"),
                        undeploy: false,
                    }
                }
            }
        };

        // On a noop, we should still send an undeploy in case of notification failure
        // TODO(thomastaylor312): We might want to come back and revisit how we handle a failure
        // like this in the delete case. If the data gets deleted, but we can't send it, we get into
        // an odd state. So I'd rather err on the side of caution and send a notification that gets
        // ignored
        if reply_data.undeploy || matches!(reply_data.result, DeleteResult::Noop) {
            trace!("Sending undeploy notification");
            if let Err(e) = self.notifier.undeployed(lattice_id, name).await {
                error!(error = ?e, "Error when attempting to send undeploy notification during delete");
                self.send_reply(
                    msg.reply,
                    // NOTE: We are constructing all data here, so this shouldn't fail, but just in
                    // case we unwrap to nothing
                    serde_json::to_vec(&DeleteModelResponse {
                        result: DeleteResult::Error,
                        message: "Error notifying processors of newly undeployed manifest on delete. This is likely a transient error, so please retry the request. Please note that the response will say it is a noop, but will notify the processors".to_string(),
                        undeploy: false,
                    })
                    .unwrap_or_default(),
                )
                .await;
                return;
            }
        }

        // NOTE: We control all the data getting sent in here, but we unwrap to default just in case
        self.send_reply(
            msg.reply,
            serde_json::to_vec(&reply_data).unwrap_or_default(),
        )
        .await
    }

    #[instrument(level = "debug", skip(self, msg))]
    pub async fn deploy_model(
        &self,
        msg: Message,
        account_id: Option<&str>,
        lattice_id: &str,
        name: &str,
    ) {
        let req: DeployModelRequest = if msg.payload.is_empty() {
            DeployModelRequest { version: None }
        } else {
            match serde_json::from_reader(std::io::Cursor::new(msg.payload)) {
                Ok(r) => r,
                Err(e) => {
                    self.send_error(
                        msg.reply,
                        format!("Unable to parse deploy application request: {e:?}"),
                    )
                    .await;
                    return;
                }
            }
        };
        trace!(?req, "Got request");

        trace!("Fetching current data from store");
        let (mut manifests, current_revision) =
            match self.store.get(account_id, lattice_id, name).await {
                Ok(Some(m)) => m,
                Ok(None) => {
                    self.send_reply(
                        msg.reply,
                        // NOTE: We are constructing all data here, so this shouldn't fail, but just in
                        // case we unwrap to nothing
                        serde_json::to_vec(&DeployModelResponse {
                            result: DeployResult::NotFound,
                            message: format!("Application with the name {name} not found"),
                            name: name.to_string(),
                            version: req.version.clone(),
                        })
                        .unwrap_or_default(),
                    )
                    .await;
                    return;
                }
                Err(e) => {
                    error!(error = %e, "Unable to fetch data");
                    self.send_error(msg.reply, "Internal storage error".to_string())
                        .await;
                    return;
                }
            };

        // Retrieve all stored models in the lattice
        let stored_models = match self.store.list(account_id, lattice_id).await {
            Ok(d) => d,
            Err(e) => {
                error!(error = %e, "Unable to fetch data");
                self.send_error(msg.reply, "Internal storage error".to_string())
                    .await;
                return;
            }
        };

        // Fetch the model that's being staged for deployment for validation
        let staged_model = match req.version.clone() {
            Some(v) if v == LATEST_VERSION => manifests.get_current(),
            Some(v) => {
                if let Some(model) = manifests.get_version(&v) {
                    model
                } else {
                    trace!("Requested version does not exist");
                    self.send_reply(
                        msg.reply,
                        // NOTE: We are constructing all data here, so this shouldn't fail, but just in
                        // case we unwrap to nothing
                        serde_json::to_vec(&DeployModelResponse {
                            result: DeployResult::Error,
                            message: format!("Application with the name '{name}' does not have a version '{v}' to deploy"),
                            name: name.to_string(),
                            version: Some(v.to_string()),
                        })
                        .unwrap_or_default(),
                    )
                    .await;
                    return;
                }
            }
            // Get the current version if payload version is None, since deploy() does the same
            None => manifests.get_current(),
        };

        // Retrieve all the existing identifiers of deployed components and providers, and check if the staged model has any duplicates
        let mut existing_ids: HashMap<String, String> = HashMap::new();
        for model_summary in stored_models.iter() {
            // Excluding models that do not have a deployed version at present
            if model_summary.deployed_version().is_some() {
                let (stored_manifest, _) = match self
                    .store
                    .get(account_id, lattice_id, &model_summary.name())
                    .await
                {
                    Ok(Some(m)) => m,
                    Ok(None) => (StoredManifest::default(), 0),
                    Err(e) => {
                        error!(error = %e, "Unable to fetch data");
                        self.send_error(msg.reply, "Internal storage error".to_string())
                            .await;
                        return;
                    }
                };

                // Performing checks against all other manifests except previous versions of the current manifest
                // Because upgrading versions is a valid case for carrying over the same identifiers
                if stored_manifest.name() != name {
                    if let Some(deployed_manifest) = stored_manifest.get_deployed() {
                        for component in deployed_manifest.spec.components.iter() {
                            let (Properties::Capability {
                                properties: CapabilityProperties { id, .. },
                            }
                            | Properties::Component {
                                properties: ComponentProperties { id, .. },
                            }) = &component.properties;

                            if let Some(id) = id.as_ref() {
                                existing_ids
                                    .insert(id.to_string(), stored_manifest.name().to_string());
                            }
                        }
                    };
                }
            }
        }

        // Compare if any of the identifiers in the staged model are duplicates
        for component in staged_model.spec.components.iter() {
            let (Properties::Capability {
                properties: CapabilityProperties { id, .. },
            }
            | Properties::Component {
                properties: ComponentProperties { id, .. },
            }) = &component.properties;

            if let Some(id) = id.as_ref() {
                if let Some(conflicting_manifest_name) = existing_ids.get(id) {
                    error!(
                        id,
                        conflicting_manifest_name,
                        "Component identifier is already deployed in a different application.",
                    );
                    self.send_error(
                                msg.reply,
                                format!(
                                    "Component identifier '{id}' is already deployed in a different application '{conflicting_manifest_name}'."
                            ),
                            )
                            .await;
                    return;
                }
            }
        }

        // TODO(#451): If this app is shared, or the previous version was, make sure that shared
        // components that have dependent applications are still present

        let deployed_apps: Vec<&Manifest> = stored_models
            .iter()
            .filter(|a| a.deployed_version().is_some() && a.get_current().shared())
            .map(|a| a.get_current())
            .collect();
        let missing_shared_components = staged_model.missing_shared_components(&deployed_apps);

        // Ensure all shared components point to a valid component that is deployed in another application
        if !missing_shared_components.is_empty() {
            self.send_error(
                msg.reply,
                format!("Application contains shared components that are not deployed in other applications: {:?}", missing_shared_components.iter().map(|c| &c.name).collect::<Vec<_>>())
            ).await;
            return;
        }

        if !manifests.deploy(req.version.clone()) {
            trace!("Requested version does not exist");
            self.send_reply(
                msg.reply,
                // NOTE: We are constructing all data here, so this shouldn't fail, but just in
                // case we unwrap to nothing
                serde_json::to_vec(&DeployModelResponse {
                    result: DeployResult::Error,
                    message: format!(
                        "Application with the name {name} does not have the specified version to deploy"
                    ),
                    name: name.to_string(),
                    version: req.version,
                })
                .unwrap_or_default(),
            )
            .await;
            return;
        }
        // SAFETY: We can unwrap here because we know we _just_ successfully deployed the manifest so they should all exist
        let manifest = manifests
            .get_version(manifests.deployed_version().unwrap())
            .unwrap()
            .to_owned();

        let manifest_version = manifest.version().to_string();
        let reply = self
            .store
            .set(account_id, lattice_id, manifests, Some(current_revision))
            .await
            .map(|_| DeployModelResponse {
                result: DeployResult::Acknowledged,
                message: format!(
                    "Successfully deployed application {name} {}",
                    manifest.version()
                ),
                name: name.to_string(),
                version: Some(manifest_version.clone()),
            })
            .unwrap_or_else(|e| {
                error!(error = %e, "Unable to store updated data");
                DeployModelResponse {
                    result: DeployResult::Error,
                    message: "Internal storage error".to_string(),
                    name: name.to_string(),
                    version: Some(manifest_version.clone()),
                }
            });
        trace!("Manifest saved in store, sending notification");
        if let Err(e) = self.notifier.deployed(lattice_id, manifest).await {
            error!(error = ?e, "Error when attempting to send deployed notification");
            self.send_reply(
                msg.reply,
                // NOTE: We are constructing all data here, so this shouldn't fail, but just in
                // case we unwrap to nothing
                serde_json::to_vec(&DeployModelResponse {
                    result: DeployResult::Error,
                    message: "Error notifying processors of newly deployed manifest. This is likely a transient error, so please retry the request".to_string(),
                    name: name.to_string(),
                    version: Some(manifest_version),
                })
                .unwrap_or_default(),
            )
            .await;
            return;
        }
        trace!(resp = ?reply, "Sending response");
        self.send_reply(
            msg.reply,
            // NOTE: We are constructing all data here, so this shouldn't fail, but just in
            // case we unwrap to nothing
            serde_json::to_vec(&reply).unwrap_or_default(),
        )
        .await;
    }

    #[instrument(level = "debug", skip(self, msg))]
    pub async fn undeploy_model(
        &self,
        msg: Message,
        account_id: Option<&str>,
        lattice_id: &str,
        name: &str,
    ) {
        let req: UndeployModelRequest = if msg.payload.is_empty() {
            UndeployModelRequest {}
        } else {
            match serde_json::from_reader(std::io::Cursor::new(msg.payload)) {
                Ok(r) => r,
                Err(e) => {
                    self.send_error(
                        msg.reply,
                        format!("Unable to parse undeploy application request: {e:?}"),
                    )
                    .await;
                    return;
                }
            }
        };
        trace!(?req, "Got request");

        trace!("Fetching current data from store");
        let (mut manifests, current_revision) =
            match self.store.get(account_id, lattice_id, name).await {
                Ok(Some(m)) => m,
                Ok(None) => {
                    self.send_reply(
                        msg.reply,
                        // NOTE: We are constructing all data here, so this shouldn't fail, but just in
                        // case we unwrap to nothing
                        serde_json::to_vec(&DeployModelResponse {
                            result: DeployResult::NotFound,
                            message: format!("Application with the name {name} not found"),
                            name: name.to_string(),
                            version: None,
                        })
                        .unwrap_or_default(),
                    )
                    .await;
                    return;
                }
                Err(e) => {
                    error!(error = %e, "Unable to fetch data");
                    self.send_error(msg.reply, "Internal storage error".to_string())
                        .await;
                    return;
                }
            };
        // TODO(#451): if shared, make sure that no other shared apps are using it

        let reply = if manifests.undeploy() {
            trace!("Manifest undeployed. Storing updated manifest");

            self.store
                .set(account_id, lattice_id, manifests, Some(current_revision))
                .await
                .map(|_| DeployModelResponse {
                    result: DeployResult::Acknowledged,
                    message: format!("Successfully undeployed application {name}"),
                    name: name.to_string(),
                    version: None,
                })
                .unwrap_or_else(|e| {
                    error!(error = %e, "Unable to store updated data");
                    DeployModelResponse {
                        result: DeployResult::Error,
                        message: "Internal storage error".to_string(),
                        name: name.to_string(),
                        version: None,
                    }
                })
        } else {
            trace!("Manifest was already undeployed");
            DeployModelResponse {
                result: DeployResult::Acknowledged,
                message: format!("Application {name} was already undeployed"),
                name: name.to_string(),
                version: None,
            }
        };
        // We always want to resend in an undeploy in case things failed last time
        if matches!(reply.result, DeployResult::Acknowledged) {
            trace!("Sending undeploy notification");
            if let Err(e) = self.notifier.undeployed(lattice_id, name).await {
                error!(error = ?e, "Error when attempting to send undeploy notification");
                self.send_reply(
                    msg.reply,
                    // NOTE: We are constructing all data here, so this shouldn't fail, but just in
                    // case we unwrap to nothing
                    serde_json::to_vec(&DeployModelResponse {
                        result: DeployResult::Error,
                        message: "Error notifying processors of undeployed manifest. This is likely a transient error, so please retry the request".to_string(),
                        name: name.to_string(),
                        version: None,
                    })
                    .unwrap_or_default(),
                )
                .await;
                return;
            }
        }
        trace!(resp = ?reply, "Sending response");
        self.send_reply(
            msg.reply,
            // NOTE: We are constructing all data here, so this shouldn't fail, but just in
            // case we unwrap to nothing
            serde_json::to_vec(&reply).unwrap_or_default(),
        )
        .await;
    }

    #[instrument(level = "debug", skip(self, msg))]
    pub async fn model_status(
        &self,
        msg: Message,
        account_id: Option<&str>,
        lattice_id: &str,
        name: &str,
    ) {
        trace!("Fetching current manifest status");
        let status = self
            .get_manifest_status(lattice_id, name)
            .await
            .unwrap_or_default();

        self.send_reply(
            msg.reply,
            // NOTE: We are constructing all data here, so this shouldn't fail, but just in
            // case we unwrap to nothing
            serde_json::to_vec(&StatusResponse {
                result: StatusResult::Ok,
                message: format!("Successfully fetched status for application {name}"),
                status: Some(status),
            })
            .unwrap_or_default(),
        )
        .await;
    }

    /// Sends a reply to the topic with the given data, logging an error if one occurs when
    /// sending the reply
    #[instrument(level = "debug", skip(self, data))]
    pub async fn send_reply(&self, reply: Option<Subject>, data: Vec<u8>) {
        let reply_topic = match reply {
            Some(t) => t,
            None => {
                debug!("No reply topic was sent. Skipping reply");
                return;
            }
        };

        if let Err(e) = self.client.publish(reply_topic, data.into()).await {
            error!(error = %e, "Unable to send reply");
        }
    }

    /// Sends an error reply
    #[instrument(level = "error", skip(self, error_message))]
    pub async fn send_error(&self, reply: Option<Subject>, error_message: String) {
        // SAFETY: We control the construction of the JSON here and all data going in, so this
        // shouldn't fail except in some sort of really odd case. In those cases, we just unwrap to
        // a default
        let response = serde_json::to_vec(&json!({
            // NOTE: This is a cheating response. Basically all of our API methods have an error
            // variant in their result enum that serializes to this, so we just make it easy on
            // ourselves rather than taking concrete types
            "result": "error",
            "message": error_message,
        }))
        .unwrap_or_default();
        self.send_reply(reply, response).await;
    }

    async fn get_manifest_status(&self, lattice_id: &str, name: &str) -> Option<Status> {
        // NOTE(brooksmtownsend): We're getting the last raw message instead of direct get here
        // to ensure we fetch the latest message from the cluster leader.
        match self
            .status_stream
            .get_last_raw_message_by_subject(&format!("wadm.status.{lattice_id}.{name}",))
            .await
            .map(|status_msg| serde_json::from_slice::<Status>(&status_msg.payload))
        {
            Ok(Ok(status)) => Some(status),
            // Application status doesn't exist or is invalid, assuming undeployed
            _ => None,
        }
    }
}

/// Helper function to create a [`ModelSummary`] from a [`StoredManifest`] and [`Status`]
fn summary_from_manifest_status(manifest: StoredManifest, status: Status) -> ModelSummary {
    // TODO: Remove in 0.14.0. This is to ensure that older clients that don't
    // understand the `Waiting` status type can still deserialize the ModelSummary
    let status_type = if status.info.status_type == StatusType::Waiting {
        StatusType::Undeployed
    } else {
        status.info.status_type
    };
    #[allow(deprecated)]
    ModelSummary {
        name: manifest.name().to_owned(),
        version: manifest.current_version().to_owned(),
        description: manifest.get_current().description().map(|s| s.to_owned()),
        deployed_version: manifest.get_deployed().map(|m| m.version().to_owned()),
        status: status_type,
        status_message: Some(status.info.message.to_owned()),
        detailed_status: status,
    }
}

// Manifest validation
pub(crate) async fn validate_manifest(manifest: &Manifest) -> anyhow::Result<()> {
    let failures = wadm_types::validation::validate_manifest(manifest).await?;
    for failure in failures {
        if matches!(
            failure.level,
            wadm_types::validation::ValidationFailureLevel::Error
        ) {
            return Err(anyhow!(failure.msg));
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use std::io::BufReader;
    use std::path::Path;

    use super::*;
    use anyhow::{Context, Result};
    use serde_yaml;
    use wadm_types::validation::valid_oam_label;

    pub(crate) fn deserialize_yaml(filepath: impl AsRef<Path>) -> Result<Manifest> {
        let file = std::fs::File::open(filepath)?;
        let reader = BufReader::new(file);
        let yaml_string: Manifest = serde_yaml::from_reader(reader)?;
        Ok(yaml_string)
    }

    #[tokio::test]
    async fn test_manifest_validation() {
        let correct_manifest = deserialize_yaml("../../tests/fixtures/manifests/simple.yaml")
            .expect("Should be able to parse");

        assert!(validate_manifest(&correct_manifest).await.is_ok());

        let manifest = deserialize_yaml("../../tests/fixtures/manifests/incorrect_component.yaml")
            .expect("Should be able to parse");

        match validate_manifest(&manifest).await {
            Ok(()) => panic!("Should have detected incorrect component"),
            Err(e) => {
                assert!(e
                    .to_string()
                    .contains("Duplicate component name in manifest: ui"))
            }
        }

        let manifest = deserialize_yaml("../../tests/fixtures/manifests/duplicate_component.yaml")
            .expect("Should be able to parse");

        match validate_manifest(&manifest).await {
            Ok(()) => panic!("Should have detected duplicate component"),
            Err(e) => assert!(e
                .to_string()
                .contains("Duplicate component name in manifest")),
        }

        let manifest = deserialize_yaml("../../tests/fixtures/manifests/duplicate_id1.yaml")
            .expect("Should be able to parse");

        match validate_manifest(&manifest).await {
            Ok(()) => {
                panic!("Should have detected duplicate component ID in provider properties")
            }
            Err(e) => assert!(e
                .to_string()
                .contains("Duplicate component identifier in manifest")),
        }

        let manifest = deserialize_yaml("../../tests/fixtures/manifests/duplicate_id2.yaml")
            .expect("Should be able to parse");

        match validate_manifest(&manifest).await {
            Ok(()) => panic!("Should have detected duplicate component ID in component properties"),
            Err(e) => assert!(e
                .to_string()
                .contains("Duplicate component identifier in manifest")),
        }

        let manifest =
            deserialize_yaml("../../tests/fixtures/manifests/missing_capability_component.yaml")
                .expect("Should be able to parse");

        match validate_manifest(&manifest).await {
            Ok(()) => panic!("Should have detected missing capability component"),
            Err(e) => assert!(e
                .to_string()
                .contains("The following capability component(s) are missing from the manifest: ")),
        }

        let manifest = deserialize_yaml("../../tests/fixtures/manifests/duplicate_links.yaml")
            .expect("Should be able to parse");

        match validate_manifest(&manifest).await {
            Ok(()) => panic!("Should have detected duplicate links"),
            Err(e) => assert!(e
                .to_string()
                .contains("Duplicate link found inside component")),
        }

        let manifest =
            deserialize_yaml("../../tests/fixtures/manifests/correct_unique_interface_links.yaml")
                .expect("Should be able to parse");
        assert!(validate_manifest(&manifest).await.is_ok());

        let manifest = deserialize_yaml(
            "../../tests/fixtures/manifests/incorrect_unique_interface_links.yaml",
        )
        .expect("Should be able to parse");
        match validate_manifest(&manifest).await {
            Ok(()) => panic!("Should have detected duplicate interface links"),
            Err(e) => assert!(
                e.to_string()
                    .contains("Duplicate link found inside component")
                    && e.to_string().contains("atomics"),
                "Error should mention duplicate interface"
            ),
        }
    }

    /// Ensure that a long image ref in a manifest works,
    /// for both providers and components
    #[tokio::test]
    async fn manifest_name_long_image_ref() -> Result<()> {
        validate_manifest(
            &deserialize_yaml("../../tests/fixtures/manifests/long_image_refs.yaml")
                .context("failed to deserialize YAML")?,
        )
        .await
        .context("failed to validate long image ref")?;
        Ok(())
    }

    #[tokio::test]
    async fn validate_oam_label_rules() {
        // Valid labels
        assert!(valid_oam_label((&"foo".to_string(), &"bar".to_string())));
        assert!(valid_oam_label((
            &"app.oam.io/name".to_string(),
            &"wasmcloud".to_string()
        )));
        assert!(valid_oam_label((
            &"justaregularstring".to_string(),
            &"wasmcloud".to_string()
        )));
        assert!(valid_oam_label((
            &"dash-period.numb3r/any_v4lue".to_string(),
            &"this can be any string".to_string()
        )));

        // Invalid labels
        assert!(!valid_oam_label((
            &"my_prefix/app-name".to_string(),
            &"wasmcloud".to_string()
        )));
        assert!(!valid_oam_label((
            &"1my_prefix/app-name".to_string(),
            &"wasmcloud".to_string()
        )));
        assert!(!valid_oam_label((
            &"my_prefix---/app-name".to_string(),
            &"wasmcloud".to_string()
        )));
        assert!(!valid_oam_label((
            &"my_prefix/app-name...".to_string(),
            &"wasmcloud".to_string()
        )));
        assert!(!valid_oam_label((
            &"a".repeat(255).to_string(),
            &"toolong".to_string()
        )));
    }
}
