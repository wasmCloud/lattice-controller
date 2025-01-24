use serde::{Deserialize, Serialize};

use crate::Manifest;

/// The default topic prefix for the wadm API;
pub const DEFAULT_WADM_TOPIC_PREFIX: &str = "wadm.api";
pub const WADM_STATUS_API_PREFIX: &str = "wadm.status";

/// The request body for getting a manifest
#[derive(Debug, Serialize, Deserialize)]
pub struct GetModelRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

/// The response from a get request
#[derive(Debug, Serialize, Deserialize)]
pub struct GetModelResponse {
    pub result: GetResult,
    #[serde(default)]
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest: Option<Manifest>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListModelsResponse {
    pub result: GetResult,
    #[serde(default)]
    pub message: String,
    pub models: Vec<ModelSummary>,
}

/// Possible outcomes of a get request
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum GetResult {
    Error,
    Success,
    NotFound,
}

/// The type returned when putting a model
#[derive(Debug, Serialize, Deserialize)]
pub struct PutModelResponse {
    pub result: PutResult,
    #[serde(default)]
    pub total_versions: usize,
    #[serde(default)]
    pub current_version: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum PutResultSuccess {
    Created,
    NewVersion(String),
}

/// Possible outcomes of a put request
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PutResult {
    Success(PutResultSuccess),
    Error,
}

/// Summary of a given model returned when listing
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ModelSummary {
    pub name: String,
    pub version: String,
    pub description: Option<String>,
    pub deployed_version: Option<String>,
    #[serde(default)]
    pub detailed_status: Status,
    #[deprecated(since = "0.14.0", note = "Use detailed_status instead")]
    pub status: StatusType,
    #[deprecated(since = "0.14.0", note = "Use detailed_status instead")]
    pub status_message: Option<String>,
}

/// The response to a versions request
#[derive(Debug, Serialize, Deserialize)]
pub struct VersionResponse {
    pub result: GetResult,
    #[serde(default)]
    pub message: String,
    pub versions: Vec<VersionInfo>,
}

/// Information about a given version of a model, returned as part of a list of all versions
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct VersionInfo {
    pub version: String,
    pub deployed: bool,
}

/// A request for deleting a model
#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteModelRequest {
    #[serde(default)]
    pub version: Option<String>,
}

/// A response from a delete request
#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteModelResponse {
    pub result: DeleteResult,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub undeploy: bool,
}

/// All possible outcomes of a delete operation
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DeleteResult {
    Deleted,
    Error,
    Noop,
}

/// A request for deploying a model.
///
/// If the given version is empty (or the body is empty), it will deploy the latest version. If the
/// version is set to "latest", it will also deploy the latest version
#[derive(Debug, Serialize, Deserialize)]
pub struct DeployModelRequest {
    pub version: Option<String>,
}

/// A response from a deploy or undeploy request
#[derive(Debug, Serialize, Deserialize)]
pub struct DeployModelResponse {
    pub result: DeployResult,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub version: Option<String>,
}

/// All possible outcomes of a deploy operation
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DeployResult {
    Error,
    Acknowledged,
    NotFound,
}

/// A request to undeploy a model
///
/// Right now this is just an empty struct, but it is reserved for future use
#[derive(Debug, Serialize, Deserialize)]
pub struct UndeployModelRequest {}

/// A response to a status request
#[derive(Debug, Serialize, Deserialize)]
pub struct StatusResponse {
    pub result: StatusResult,
    #[serde(default)]
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<Status>,
}

/// All possible outcomes of a status operation
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StatusResult {
    Error,
    Ok,
    NotFound,
}

/// The current status of a model
#[derive(Debug, Serialize, Deserialize, Default, Clone, PartialEq, Eq)]
pub struct Status {
    #[serde(rename = "status")]
    pub info: StatusInfo,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub scalers: Vec<ScalerStatus>,
    #[serde(default)]
    #[deprecated(since = "0.14.0")]
    pub version: String,
    #[serde(default)]
    #[deprecated(since = "0.14.0")]
    pub components: Vec<ComponentStatus>,
}

impl Status {
    pub fn new(info: StatusInfo, scalers: Vec<ScalerStatus>) -> Self {
        #[allow(deprecated)]
        Status {
            info,
            scalers,
            version: String::with_capacity(0),
            components: Vec::with_capacity(0),
        }
    }
}

/// The current status of a component
#[derive(Debug, Serialize, Deserialize, Default, Clone, Eq, PartialEq)]
pub struct ComponentStatus {
    pub name: String,
    #[serde(rename = "type")]
    pub component_type: String,
    #[serde(rename = "status")]
    pub info: StatusInfo,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub traits: Vec<TraitStatus>,
}

/// The current status of a trait
#[derive(Debug, Serialize, Deserialize, Default, Clone, Eq, PartialEq)]
pub struct TraitStatus {
    #[serde(rename = "type")]
    pub trait_type: String,
    #[serde(rename = "status")]
    pub info: StatusInfo,
}

/// The current status of a scaler
#[derive(Debug, Serialize, Deserialize, Default, Clone, Eq, PartialEq)]
pub struct ScalerStatus {
    /// The id of the scaler
    #[serde(default)]
    pub id: String,
    /// The kind of scaler
    #[serde(default)]
    pub kind: String,
    /// The human-readable name of the scaler
    #[serde(default)]
    pub name: String,
    #[serde(rename = "status")]
    pub info: StatusInfo,
}

/// Common high-level status information
#[derive(Debug, Serialize, Deserialize, Default, Clone, Eq, PartialEq)]
pub struct StatusInfo {
    #[serde(rename = "type")]
    pub status_type: StatusType,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub message: String,
}

impl StatusInfo {
    pub fn undeployed(message: &str) -> Self {
        StatusInfo {
            status_type: StatusType::Undeployed,
            message: message.to_owned(),
        }
    }

    pub fn deployed(message: &str) -> Self {
        StatusInfo {
            status_type: StatusType::Deployed,
            message: message.to_owned(),
        }
    }

    pub fn failed(message: &str) -> Self {
        StatusInfo {
            status_type: StatusType::Failed,
            message: message.to_owned(),
        }
    }

    pub fn reconciling(message: &str) -> Self {
        StatusInfo {
            status_type: StatusType::Reconciling,
            message: message.to_owned(),
        }
    }

    pub fn waiting(message: &str) -> Self {
        StatusInfo {
            status_type: StatusType::Waiting,
            message: message.to_owned(),
        }
    }

    pub fn unhealthy(message: &str) -> Self {
        StatusInfo {
            status_type: StatusType::Unhealthy,
            message: message.to_owned(),
        }
    }
}

/// All possible status types
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Copy, Default)]
#[serde(rename_all = "lowercase")]
pub enum StatusType {
    Waiting,
    #[default]
    Undeployed,
    #[serde(alias = "compensating")]
    Reconciling,
    #[serde(alias = "ready")]
    Deployed,
    Failed,
    Unhealthy,
}

// Implementing add makes it easy for use to get an aggregate status by summing all of them together
impl std::ops::Add for StatusType {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        // If any match, return the same status
        if self == rhs {
            return self;
        }

        // Because we match on exact matches above, we don't have to handle them in the match below.
        // For all of the statuses _except_ ready, they will override the other status. Order of the
        // matching matters below
        match (self, rhs) {
            // Anything that is failed means the whole thing is failed
            (Self::Failed, _) => Self::Failed,
            (_, Self::Failed) => Self::Failed,
            // If anything is undeployed, the whole thing is
            (Self::Undeployed, _) => Self::Undeployed,
            (_, Self::Undeployed) => Self::Undeployed,
            // If anything is waiting, the whole thing is
            (Self::Waiting, _) => Self::Waiting,
            (_, Self::Waiting) => Self::Waiting,
            (Self::Reconciling, _) => Self::Reconciling,
            (_, Self::Reconciling) => Self::Reconciling,
            (Self::Unhealthy, _) => Self::Unhealthy,
            (_, Self::Unhealthy) => Self::Unhealthy,
            // This is technically covered in the first comparison, but we'll be explicit
            (Self::Deployed, Self::Deployed) => Self::Deployed,
        }
    }
}

impl std::iter::Sum for StatusType {
    fn sum<I: Iterator<Item = Self>>(mut iter: I) -> Self {
        // Grab the first status to use as our seed, defaulting to undeployed
        let first = iter.next().unwrap_or_default();
        iter.fold(first, |a, b| a + b)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_status_aggregate() {
        assert!(matches!(
            [StatusType::Deployed, StatusType::Deployed]
                .into_iter()
                .sum(),
            StatusType::Deployed
        ));

        assert!(matches!(
            [StatusType::Undeployed, StatusType::Undeployed]
                .into_iter()
                .sum(),
            StatusType::Undeployed
        ));

        assert!(matches!(
            [StatusType::Undeployed, StatusType::Failed]
                .into_iter()
                .sum(),
            StatusType::Failed
        ));

        assert!(matches!(
            [StatusType::Reconciling, StatusType::Undeployed]
                .into_iter()
                .sum(),
            StatusType::Undeployed
        ));

        assert!(matches!(
            [StatusType::Deployed, StatusType::Undeployed]
                .into_iter()
                .sum(),
            StatusType::Undeployed
        ));

        assert!(matches!(
            [
                StatusType::Deployed,
                StatusType::Reconciling,
                StatusType::Undeployed,
                StatusType::Failed
            ]
            .into_iter()
            .sum(),
            StatusType::Failed
        ));

        assert!(matches!(
            [StatusType::Deployed, StatusType::Unhealthy]
                .into_iter()
                .sum(),
            StatusType::Unhealthy
        ));

        assert!(matches!(
            [StatusType::Reconciling, StatusType::Unhealthy]
                .into_iter()
                .sum(),
            StatusType::Reconciling
        ));

        let empty: Vec<StatusType> = Vec::new();
        assert!(matches!(empty.into_iter().sum(), StatusType::Undeployed));
    }
}
