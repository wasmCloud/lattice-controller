use std::{collections::HashMap, time::Duration};

use async_nats::{jetstream, Subscriber};
use futures::StreamExt;
use helpers::setup_env;
use serde::de::DeserializeOwned;
use wadm::server::*;
use wadm_types::{api::*, *};

mod helpers;

struct TestServer {
    prefix: String,
    client: async_nats::Client,
    notify: Subscriber,
    handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.abort()
    }
}

impl TestServer {
    fn get_topic(&self, topic: &str) -> String {
        format!("{}.{}", self.prefix, topic)
    }

    // NOTE: The given subject should not include the prefix
    async fn get_response<T: DeserializeOwned>(
        &self,
        subject: &str,
        data: Vec<u8>,
        header: Option<(&str, &str)>,
    ) -> T {
        let msg = if let Some((k, v)) = header {
            let mut headers = async_nats::HeaderMap::new();
            headers.insert(k, v);
            self.client
                .request_with_headers(self.get_topic(subject), headers, data.into())
                .await
                .expect("Should be able to perform request")
        } else {
            self.client
                .request(self.get_topic(subject), data.into())
                .await
                .expect("Should be able to perform request")
        };
        serde_json::from_slice(&msg.payload).unwrap_or_else(|e| {
            panic!(
                "Should return a valid response. Body: {}\nError: {e}",
                String::from_utf8_lossy(&msg.payload)
            )
        })
    }

    async fn wait_for_notify(&mut self, contains: &str) {
        let mut timer = tokio::time::interval(Duration::from_secs(2));
        // Consume the first tick
        timer.tick().await;
        loop {
            tokio::select! {
                res = self.notify.next() => {
                    match res {
                        Some(msg) => {
                            if String::from_utf8_lossy(&msg.payload).contains(contains) {
                                return
                            }
                        }
                        None => panic!("Subscriber terminated")
                    }
                }
                _ = timer.tick() => panic!("Timeout waiting for notify event containing {contains}")
            }
        }
    }
}

async fn setup_server(id: &str, client: async_nats::Client) -> TestServer {
    let store = helpers::create_test_store_with_client(id, client.clone()).await;

    let context = jetstream::new(client.clone());
    let status_stream = context
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: "wadm_status".to_string(),
            description: Some(
                "A stream that stores all status updates for wadm applications".to_string(),
            ),
            num_replicas: 1,
            retention: async_nats::jetstream::stream::RetentionPolicy::Limits,
            subjects: vec!["wadm.status.*.*".to_string()],
            max_messages_per_subject: 10,
            max_age: std::time::Duration::from_nanos(0),
            storage: async_nats::jetstream::stream::StorageType::File,
            allow_rollup: false,
            ..Default::default()
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))
        .expect("Should be able to set up status stream for tests");

    status_stream
        .purge()
        .await
        .expect("Should be able to purge status stream for tests");

    let prefix = format!("testing.{id}");
    let server = Server::new(
        store,
        client.clone(),
        Some(id),
        false,
        status_stream,
        ManifestNotifier::new(&prefix, client.clone()),
    )
    .await
    .expect("Should be able to setup server");

    let notify = client
        .subscribe(format!("{prefix}.default.>"))
        .await
        .expect("Unable to set up subscription");

    TestServer {
        prefix: id.to_owned(),
        handle: tokio::spawn(server.serve()),
        client,
        notify,
    }
}

#[tokio::test]
async fn test_crud_operations() {
    let env = setup_env()
        .await
        .expect("should have set up the test environment");
    let nats_client = env
        .nats_client()
        .await
        .expect("should have created a nats client");
    let test_server = setup_server("crud_operations", nats_client).await;

    // First test with a raw file (a common operation)
    let raw = tokio::fs::read("./oam/sqldbpostgres.yaml")
        .await
        .expect("Unable to load file");
    let resp: PutModelResponse = test_server
        .get_response("default.model.put", raw, None)
        .await;

    println!("Response: {resp:?}");

    assert_put_response(
        resp,
        PutResult::Success(PutResultSuccess::Created),
        "v0.0.1",
        1,
    );

    let raw = tokio::fs::read("./oam/simple1.yaml")
        .await
        .expect("Should be able to load file");
    // Get the manifest in memory so we can manipulate data like the version
    let mut manifest: Manifest =
        serde_yaml::from_slice(&raw).expect("Should be able to parse as manifest");

    let resp: PutModelResponse = test_server
        .get_response("default.model.put", raw, None)
        .await;
    // This manifest has no version, so it's assigned on as a ULID
    let example_version = resp.current_version.clone();
    assert_put_response(
        resp,
        PutResult::Success(PutResultSuccess::Created),
        &example_version,
        1,
    );

    // Check that we can get back the manifest
    let resp: GetModelResponse = test_server
        .get_response("default.model.get.my-example-app", Vec::new(), None)
        .await;
    manifest
        .metadata
        .annotations
        .insert(VERSION_ANNOTATION_KEY.to_owned(), example_version.clone());
    assert_manifest(
        &manifest,
        resp.manifest
            .as_ref()
            .expect("Response should have a manifest"),
    );

    // Now check that the data returned is correct
    let ListModelsResponse { models: resp, .. } = test_server
        .get_response("default.model.get", Vec::new(), None)
        .await;

    assert_eq!(resp.len(), 2, "Should have two models in storage");
    let summary = resp
        .iter()
        .find(|m| m.name == "my-example-app")
        .expect("Should be able to find the correct model");
    assert_eq!(
        summary.version, example_version,
        "Should have the correct data"
    );
    let summary = resp
        .iter()
        .find(|m| m.name == "rust-sqldb-postgres-query")
        .expect("Should be able to find the correct model");
    assert_eq!(summary.version, "v0.0.1", "Should have the correct data");

    // Now put two more versions of the same manifest
    manifest
        .metadata
        .annotations
        .insert(VERSION_ANNOTATION_KEY.to_owned(), "v0.0.2".to_owned());
    let resp: PutModelResponse = test_server
        .get_response(
            "default.model.put",
            serde_yaml::to_string(&manifest).unwrap().into_bytes(),
            None,
        )
        .await;
    assert_put_response(
        resp,
        PutResult::Success(PutResultSuccess::NewVersion("v0.0.2".to_string())),
        "v0.0.2",
        2,
    );

    manifest
        .metadata
        .annotations
        .insert(VERSION_ANNOTATION_KEY.to_owned(), "v0.0.3".to_owned());
    let resp: PutModelResponse = test_server
        .get_response(
            "default.model.put",
            serde_yaml::to_string(&manifest).unwrap().into_bytes(),
            None,
        )
        .await;
    assert_put_response(
        resp,
        PutResult::Success(PutResultSuccess::NewVersion("v0.0.3".to_string())),
        "v0.0.3",
        3,
    );

    // Make sure we still only have 2 manifests
    let ListModelsResponse { models: resp, .. } = test_server
        .get_response("default.model.get", Vec::new(), None)
        .await;

    assert_eq!(resp.len(), 2, "Should still have two models in storage");

    // Now list the versions of a manifest
    let resp: VersionResponse = test_server
        .get_response("default.model.versions.my-example-app", Vec::new(), None)
        .await;
    assert!(
        matches!(resp.result, GetResult::Success),
        "Versions should have a success result"
    );
    let mut iter = resp.versions.into_iter();
    assert_eq!(
        iter.next().unwrap().version,
        example_version,
        "Should find the correct version"
    );
    assert_eq!(
        iter.next().unwrap().version,
        "v0.0.2",
        "Should find the correct version"
    );
    assert_eq!(
        iter.next().unwrap().version,
        "v0.0.3",
        "Should find the correct version"
    );
    assert!(iter.next().is_none(), "Should only have 3 versions");

    // Then get a specific version
    let resp: GetModelResponse = test_server
        .get_response(
            "default.model.get.my-example-app",
            serde_json::to_vec(&GetModelRequest {
                version: Some("v0.0.2".to_owned()),
            })
            .unwrap(),
            None,
        )
        .await;
    // Swap back the version so it matches
    manifest
        .metadata
        .annotations
        .insert(VERSION_ANNOTATION_KEY.to_owned(), "v0.0.2".to_owned());
    assert_manifest(
        &manifest,
        resp.manifest.as_ref().expect("Should have manifest set"),
    );

    // Then delete a manifest version
    let resp: DeleteModelResponse = test_server
        .get_response(
            "default.model.del.my-example-app",
            serde_json::to_vec(&DeleteModelRequest {
                version: Some("v0.0.2".to_owned()),
            })
            .unwrap(),
            None,
        )
        .await;
    assert!(
        matches!(resp.result, DeleteResult::Deleted),
        "Should have gotten deleted response"
    );
    assert!(!resp.message.is_empty(), "Should have a message set");

    // Make sure we only receive the proper number of versions now (in the right order)
    let resp: VersionResponse = test_server
        .get_response("default.model.versions.my-example-app", Vec::new(), None)
        .await;
    assert!(
        matches!(resp.result, GetResult::Success),
        "Versions should have a success result"
    );
    let mut iter = resp.versions.into_iter();
    assert_eq!(
        iter.next().unwrap().version,
        example_version,
        "Should find the correct version"
    );
    assert_eq!(
        iter.next().unwrap().version,
        "v0.0.3",
        "Should find the correct version"
    );
    assert!(iter.next().is_none(), "Should only have 2 versions");

    // Delete all
    let resp: DeleteModelResponse = test_server
        .get_response(
            "default.model.del.my-example-app",
            serde_json::to_vec(&DeleteModelRequest { version: None }).unwrap(),
            None,
        )
        .await;
    assert!(
        matches!(resp.result, DeleteResult::Deleted),
        "Should have gotten deleted response"
    );

    // Getting the deleted model should return an error
    let resp: GetModelResponse = test_server
        .get_response("default.model.get.my-example-app", Vec::new(), None)
        .await;
    assert!(
        matches!(resp.result, GetResult::NotFound),
        "Deleted model should no longer exist"
    );

    // Delete last remaining
    let resp: DeleteModelResponse = test_server
        .get_response(
            "default.model.del.rust-sqldb-postgres-query",
            serde_json::to_vec(&DeleteModelRequest {
                version: Some("v0.0.1".to_owned()),
            })
            .unwrap(),
            None,
        )
        .await;
    assert!(
        matches!(resp.result, DeleteResult::Deleted),
        "Should have gotten deleted response"
    );

    let resp: GetModelResponse = test_server
        .get_response(
            "default.model.get.rust-sqldb-postgres-query",
            Vec::new(),
            None,
        )
        .await;
    assert!(
        matches!(resp.result, GetResult::NotFound),
        "Deleting last model should remove model from storage"
    );
}

#[tokio::test]
async fn test_bad_requests() {
    let env = setup_env()
        .await
        .expect("should have set up the test environment");
    let nats_client = env
        .nats_client()
        .await
        .expect("should have created a nats client");
    let test_server = setup_server("bad_requests", nats_client).await;

    // Duplicate version
    let raw = tokio::fs::read("./oam/sqldbpostgres.yaml")
        .await
        .expect("Unable to load file");
    let resp: PutModelResponse = test_server
        .get_response("default.model.put", raw, None)
        .await;

    assert_put_response(
        resp,
        PutResult::Success(PutResultSuccess::Created),
        "v0.0.1",
        1,
    );

    // https://imgflip.com/memegenerator/195657242/Do-it-again
    let raw = tokio::fs::read("./oam/sqldbpostgres.yaml")
        .await
        .expect("Unable to load file");
    let resp: PutModelResponse = test_server
        .get_response("default.model.put", raw, None)
        .await;

    assert!(
        matches!(resp.result, PutResult::Error),
        "Should have gotten an error with a duplicate version"
    );
    assert!(!resp.message.is_empty(), "Should not have an empty message");

    // Setting manifest to latest
    let raw = tokio::fs::read("./oam/sqldbpostgres.yaml")
        .await
        .expect("Unable to load file");
    let mut manifest: Manifest = serde_yaml::from_slice(&raw).unwrap();
    manifest
        .metadata
        .annotations
        .insert(VERSION_ANNOTATION_KEY.to_owned(), "latest".to_owned());
    let resp: PutModelResponse = test_server
        .get_response("default.model.put", raw, None)
        .await;

    assert!(
        matches!(resp.result, PutResult::Error),
        "Should have gotten an error with a latest version"
    );
    assert!(!resp.message.is_empty(), "Should not have an empty message");

    // Mismatched name on put
    let raw = tokio::fs::read("./oam/sqldbpostgres.yaml")
        .await
        .expect("Unable to load file");
    let resp: PutModelResponse = test_server
        .get_response("default.model.put", raw, None)
        .await;

    assert!(
        matches!(resp.result, PutResult::Error),
        "Should have gotten an error with a name mismatch"
    );
    assert!(!resp.message.is_empty(), "Should not have an empty message");
}

#[tokio::test]
async fn test_delete_noop() {
    let env = setup_env()
        .await
        .expect("should have set up the test environment");
    let nats_client = env
        .nats_client()
        .await
        .expect("should have created a nats client");
    let test_server = setup_server("delete_noop", nats_client).await;

    // Delete a model that doesn't exist
    let resp: DeleteModelResponse = test_server
        .get_response(
            "default.model.del.my-example-app",
            serde_json::to_vec(&DeleteModelRequest {
                version: Some("v0.0.2".to_owned()),
            })
            .unwrap(),
            None,
        )
        .await;
    assert!(
        matches!(resp.result, DeleteResult::Noop),
        "Should have gotten noop response"
    );
    assert!(!resp.message.is_empty(), "Should have a message set");

    let resp: DeleteModelResponse = test_server
        .get_response(
            "default.model.del.my-example-app",
            serde_json::to_vec(&DeleteModelRequest { version: None }).unwrap(),
            None,
        )
        .await;
    assert!(
        matches!(resp.result, DeleteResult::Noop),
        "Should have gotten noop response for already deleted model"
    );

    // Delete a non-existent version for an existing model
    let raw = tokio::fs::read("./oam/sqldbpostgres.yaml")
        .await
        .expect("Unable to load file");
    let resp: PutModelResponse = test_server
        .get_response("default.model.put", raw, None)
        .await;

    assert_put_response(
        resp,
        PutResult::Success(PutResultSuccess::Created),
        "v0.0.1",
        1,
    );

    let resp: DeleteModelResponse = test_server
        .get_response(
            "default.model.del.rust-sqldb-postgres-query",
            serde_json::to_vec(&DeleteModelRequest {
                version: Some("v0.0.2".to_owned()),
            })
            .unwrap(),
            None,
        )
        .await;
    assert!(
        matches!(resp.result, DeleteResult::Noop),
        "Should have gotten noop response"
    );
    assert!(!resp.message.is_empty(), "Should have a message set");
}

#[tokio::test]
async fn test_invalid_topics() {
    let env = setup_env()
        .await
        .expect("should have set up the test environment");
    let nats_client = env
        .nats_client()
        .await
        .expect("should have created a nats client");
    let test_server = setup_server("invalid_topics", nats_client).await;

    // Put in a manifest to make sure we have something that could be fetched if we aren't handing
    // invalid topics correctly
    let raw = tokio::fs::read("./oam/sqldbpostgres.yaml")
        .await
        .expect("Unable to load file");
    let resp: PutModelResponse = test_server
        .get_response("default.model.put", raw, None)
        .await;

    assert_put_response(
        resp,
        PutResult::Success(PutResultSuccess::Created),
        "v0.0.1",
        1,
    );

    // Too short
    let resp: HashMap<String, String> = test_server
        .get_response("default.model", Vec::new(), None)
        .await;

    assert_eq!(
        resp.get("result").expect("Response should have valid data"),
        "error"
    );
    assert!(
        resp.get("message")
            .expect("Response should have valid data")
            .starts_with("Invalid subject"),
        "error"
    );

    // Extra things on end
    let resp: HashMap<String, String> = test_server
        .get_response(
            "default.model.get.rust-sqldb-postgres-query.foo.bar",
            Vec::new(),
            None,
        )
        .await;

    assert_eq!(
        resp.get("result").expect("Response should have valid data"),
        "error"
    );
    assert!(
        resp.get("message")
            .expect("Response should have valid data")
            .starts_with("Invalid subject"),
        "error"
    );

    // Random topic
    let resp: HashMap<String, String> = test_server
        .get_response(
            "default.blah.get.rust-sqldb-postgres-query",
            Vec::new(),
            None,
        )
        .await;

    assert_eq!(
        resp.get("result").expect("Response should have valid data"),
        "error"
    );
    assert!(
        resp.get("message")
            .expect("Response should have valid data")
            .starts_with("Unsupported subject"),
        "error"
    );
}

#[tokio::test]
async fn test_manifest_parsing() {
    let env = setup_env()
        .await
        .expect("should have set up the test environment");
    let nats_client = env
        .nats_client()
        .await
        .expect("should have created a nats client");
    let test_server = setup_server("manifest_parsing", nats_client).await;

    // Test json manifest with no hint
    let raw = tokio::fs::read("./oam/simple1.json")
        .await
        .expect("Unable to load file");
    let mut manifest: Manifest = serde_json::from_slice(&raw).unwrap();
    let resp: PutModelResponse = test_server
        .get_response("default.model.put", raw, None)
        .await;

    // This manifest has no version, so it's assigned on as a ULID
    let version = resp.current_version.clone();

    assert_put_response(
        resp,
        PutResult::Success(PutResultSuccess::Created),
        &version,
        1,
    );

    // Test yaml manifest with hint
    let raw = tokio::fs::read("./oam/sqldbpostgres.yaml")
        .await
        .expect("Unable to load file");
    let resp: PutModelResponse = test_server
        .get_response(
            "default.model.put",
            raw,
            Some(("Content-Type", "application/yaml")),
        )
        .await;

    assert_put_response(
        resp,
        PutResult::Success(PutResultSuccess::Created),
        "v0.0.1",
        1,
    );

    // Test json manifest with hint
    manifest
        .metadata
        .annotations
        .insert(VERSION_ANNOTATION_KEY.to_owned(), "v0.0.2".to_string());
    let raw = serde_json::to_vec(&manifest).unwrap();
    let resp: PutModelResponse = test_server
        .get_response(
            "default.model.put",
            raw,
            Some(("Content-Type", "application/json")),
        )
        .await;

    assert_put_response(
        resp,
        PutResult::Success(PutResultSuccess::NewVersion("v0.0.2".to_string())),
        "v0.0.2",
        2,
    );

    // Smoke test to make sure the server can handle the various provider config options
    let raw = tokio::fs::read("./oam/config.yaml")
        .await
        .expect("Unable to load file");
    let resp: PutModelResponse = test_server
        .get_response("default.model.put", raw, None)
        .await;
    // This manifest has no version, so it's assigned on as a ULID
    let version = resp.current_version.clone();

    assert_put_response(
        resp,
        PutResult::Success(PutResultSuccess::Created),
        &version,
        1,
    );
}

#[tokio::test]
async fn test_deploy() {
    let env = setup_env()
        .await
        .expect("should have set up the test environment");
    let nats_client = env
        .nats_client()
        .await
        .expect("should have created a nats client");
    let mut test_server = setup_server("deploy_ops", nats_client).await;

    // Create a manifest with 2 versions
    let raw = tokio::fs::read("./oam/sqldbpostgres.yaml")
        .await
        .expect("Unable to load file");
    let mut manifest: Manifest = serde_yaml::from_slice(&raw).unwrap();
    let resp: PutModelResponse = test_server
        .get_response("default.model.put", raw, None)
        .await;

    assert_put_response(
        resp,
        PutResult::Success(PutResultSuccess::Created),
        "v0.0.1",
        1,
    );
    manifest
        .metadata
        .annotations
        .insert(VERSION_ANNOTATION_KEY.to_owned(), "v0.0.2".to_string());
    let resp: PutModelResponse = test_server
        .get_response(
            "default.model.put",
            serde_yaml::to_string(&manifest).unwrap().into_bytes(),
            None,
        )
        .await;
    assert_put_response(
        resp,
        PutResult::Success(PutResultSuccess::NewVersion("v0.0.2".to_string())),
        "v0.0.2",
        2,
    );

    // Try to deploy and undeploy something that doesn't exist
    let resp: DeployModelResponse = test_server
        .get_response("default.model.deploy.foobar", Vec::new(), None)
        .await;
    assert!(
        matches!(resp.result, DeployResult::NotFound),
        "Should have gotten not found response"
    );

    let resp: DeployModelResponse = test_server
        .get_response("default.model.undeploy.foobar", Vec::new(), None)
        .await;
    assert!(
        matches!(resp.result, DeployResult::NotFound),
        "Should have gotten not found response"
    );

    // Deploy using no body
    let resp: DeployModelResponse = test_server
        .get_response(
            "default.model.deploy.rust-sqldb-postgres-query",
            Vec::new(),
            None,
        )
        .await;
    assert!(
        matches!(resp.result, DeployResult::Acknowledged),
        "Should have gotten acknowledged response: {resp:?}"
    );

    test_server
        .wait_for_notify("com.wadm.manifest_published")
        .await;

    let resp: VersionResponse = test_server
        .get_response(
            "default.model.versions.rust-sqldb-postgres-query",
            Vec::new(),
            None,
        )
        .await;
    for info in resp.versions.into_iter() {
        match info.version.as_str() {
            "v0.0.1" => assert!(!info.deployed, "The correct version should be deployed"),
            "v0.0.2" => assert!(info.deployed, "The correct version should be deployed"),
            _ => panic!("Got unexpected version {}", info.version),
        }
    }

    // Now deploy with a specific version
    let resp: DeployModelResponse = test_server
        .get_response(
            "default.model.deploy.rust-sqldb-postgres-query",
            serde_json::to_vec(&DeployModelRequest {
                version: Some("v0.0.1".to_string()),
            })
            .unwrap(),
            None,
        )
        .await;
    assert!(
        matches!(resp.result, DeployResult::Acknowledged),
        "Should have gotten acknowledged response"
    );

    let resp: VersionResponse = test_server
        .get_response(
            "default.model.versions.rust-sqldb-postgres-query",
            Vec::new(),
            None,
        )
        .await;
    for info in resp.versions.into_iter() {
        match info.version.as_str() {
            "v0.0.1" => assert!(info.deployed, "The correct version should be deployed"),
            "v0.0.2" => assert!(!info.deployed, "The correct version should be deployed"),
            _ => panic!("Got unexpected version {}", info.version),
        }
    }

    // Try to deploy latest
    let resp: DeployModelResponse = test_server
        .get_response(
            "default.model.deploy.rust-sqldb-postgres-query",
            serde_json::to_vec(&DeployModelRequest {
                version: Some("latest".to_string()),
            })
            .unwrap(),
            None,
        )
        .await;
    assert!(
        matches!(resp.result, DeployResult::Acknowledged),
        "Should have gotten acknowledged response"
    );

    let resp: VersionResponse = test_server
        .get_response(
            "default.model.versions.rust-sqldb-postgres-query",
            Vec::new(),
            None,
        )
        .await;
    for info in resp.versions.into_iter() {
        match info.version.as_str() {
            "v0.0.1" => assert!(!info.deployed, "The correct version should be deployed"),
            "v0.0.2" => assert!(info.deployed, "The correct version should be deployed"),
            _ => panic!("Got unexpected version {}", info.version),
        }
    }

    // Undeploy stuff
    let resp: DeployModelResponse = test_server
        .get_response(
            "default.model.undeploy.rust-sqldb-postgres-query",
            Vec::new(),
            None,
        )
        .await;
    assert!(
        matches!(resp.result, DeployResult::Acknowledged),
        "Should have gotten acknowledged response"
    );

    test_server
        .wait_for_notify("com.wadm.manifest_unpublished")
        .await;

    let resp: VersionResponse = test_server
        .get_response(
            "default.model.versions.rust-sqldb-postgres-query",
            Vec::new(),
            None,
        )
        .await;
    assert!(
        resp.versions.into_iter().all(|info| !info.deployed),
        "No version should be deployed"
    );
}

#[tokio::test]
async fn test_delete_deploy() {
    let env = setup_env()
        .await
        .expect("should have set up the test environment");
    let nats_client = env
        .nats_client()
        .await
        .expect("should have created a nats client");
    let mut test_server = setup_server("deploy_delete", nats_client).await;

    // Create a manifest with 2 versions
    let raw = tokio::fs::read("./oam/sqldbpostgres.yaml")
        .await
        .expect("Unable to load file");
    let mut manifest: Manifest = serde_yaml::from_slice(&raw).unwrap();
    let resp: PutModelResponse = test_server
        .get_response("default.model.put", raw, None)
        .await;

    assert_put_response(
        resp,
        PutResult::Success(PutResultSuccess::Created),
        "v0.0.1",
        1,
    );
    manifest
        .metadata
        .annotations
        .insert(VERSION_ANNOTATION_KEY.to_owned(), "v0.0.2".to_string());
    let resp: PutModelResponse = test_server
        .get_response(
            "default.model.put",
            serde_yaml::to_string(&manifest).unwrap().into_bytes(),
            None,
        )
        .await;
    assert_put_response(
        resp,
        PutResult::Success(PutResultSuccess::NewVersion("v0.0.2".to_string())),
        "v0.0.2",
        2,
    );

    let resp: DeployModelResponse = test_server
        .get_response(
            "default.model.deploy.rust-sqldb-postgres-query",
            Vec::new(),
            None,
        )
        .await;
    assert!(
        matches!(resp.result, DeployResult::Acknowledged),
        "Should have gotten acknowledged response"
    );

    let resp: DeleteModelResponse = test_server
        .get_response(
            "default.model.del.rust-sqldb-postgres-query",
            serde_json::to_vec(&DeleteModelRequest {
                version: Some("v0.0.2".to_owned()),
            })
            .unwrap(),
            None,
        )
        .await;
    assert!(
        matches!(resp.result, DeleteResult::Deleted),
        "Should have gotten deleted response"
    );

    test_server
        .wait_for_notify("com.wadm.manifest_unpublished")
        .await;

    // Deploy again and then delete all
    let resp: DeployModelResponse = test_server
        .get_response(
            "default.model.deploy.rust-sqldb-postgres-query",
            Vec::new(),
            None,
        )
        .await;
    assert!(
        matches!(resp.result, DeployResult::Acknowledged),
        "Should have gotten acknowledged response"
    );

    let resp: DeleteModelResponse = test_server
        .get_response(
            "default.model.del.rust-sqldb-postgres-query",
            serde_json::to_vec(&DeleteModelRequest { version: None }).unwrap(),
            None,
        )
        .await;
    assert!(
        matches!(resp.result, DeleteResult::Deleted),
        "Should have gotten deleted response"
    );

    test_server
        .wait_for_notify("com.wadm.manifest_unpublished")
        .await;
}

#[tokio::test]
async fn test_status() {
    let env = setup_env()
        .await
        .expect("should have set up the test environment");
    let nats_client = env
        .nats_client()
        .await
        .expect("should have created a nats client");
    let test_server = setup_server("status", nats_client).await;

    let raw = tokio::fs::read("./oam/sqldbpostgres.yaml")
        .await
        .expect("Unable to load file");
    let resp: PutModelResponse = test_server
        .get_response("default.model.put", raw, None)
        .await;
    assert_put_response(
        resp,
        PutResult::Success(PutResultSuccess::Created),
        "v0.0.1",
        1,
    );

    let resp: StatusResponse = test_server
        .get_response(
            "default.model.status.rust-sqldb-postgres-query",
            Vec::new(),
            None,
        )
        .await;

    // This is just checking it returns a valid default status. e2e tests will have to check actual
    // status updates
    assert_eq!(
        resp.result,
        StatusResult::Ok,
        "Should have the proper result"
    );
    assert_eq!(
        resp.status
            .expect("Should have a status set")
            .info
            .status_type,
        StatusType::Undeployed,
        "Should have the correct default status"
    );
}

fn assert_put_response(
    resp: PutModelResponse,
    result: PutResult,
    current_version: &str,
    total_versions: usize,
) {
    assert_eq!(
        resp.result, result,
        "Should have gotten proper result in response. Error: {}",
        resp.message
    );
    assert_eq!(
        resp.current_version, current_version,
        "Current version should be set correctly"
    );
    assert!(!resp.message.is_empty(), "A message should be set");
    assert_eq!(
        resp.total_versions, total_versions,
        "Total number of versions should be set correctly"
    );
}

fn assert_manifest(expected: &Manifest, received: &Manifest) {
    assert_eq!(
        expected.metadata.name, received.metadata.name,
        "Data should match"
    );
    assert_eq!(
        expected.metadata.annotations, received.metadata.annotations,
        "Data should match"
    );
    // NOTE(thomastaylor312): As we continue to improve wadm, we should probably do some deep
    // equality checking to make sure all the components are right. For now this serves as a basic
    // sanity check
    assert_eq!(
        expected.spec.components.len(),
        received.spec.components.len(),
        "Should have same number of components"
    );
}
