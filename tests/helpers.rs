#![allow(dead_code)]

use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};

use async_nats::{
    jetstream::{
        self,
        kv::{Config as KvConfig, Store},
    },
    Client,
};
use futures::TryStreamExt;
use testcontainers::{
    core::WaitFor, runners::AsyncRunner as _, ContainerAsync, GenericImage, ImageExt,
};
use tokio::process::Command;

use anyhow::{bail, Context as _, Result};
use wadm::consumers::{CommandConsumer, ScopedMessage};

pub const DEFAULT_NATS_PORT: u16 = 4222;
pub const HELLO_IMAGE_REF: &str = "ghcr.io/wasmcloud/components/http-hello-world-rust:0.1.0";
pub const HELLO_COMPONENT_ID: &str = "http_hello_world";
pub const HTTP_SERVER_IMAGE_REF: &str = "ghcr.io/wasmcloud/http-server:0.23.0";
pub const HTTP_SERVER_COMPONENT_ID: &str = "http_server";
pub const HTTP_CLIENT_IMAGE_REF: &str = "ghcr.io/wasmcloud/http-client:0.12.0";

/// Get a TCP random port
fn get_random_tcp_port() -> u16 {
    TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
        .expect("Unable to bind to check for port")
        .local_addr()
        .unwrap()
        .port()
}

pub struct TestEnv {
    nats_server: ContainerAsync<GenericImage>,
    wasmcloud_hosts: Vec<ContainerAsync<GenericImage>>,
}

impl TestEnv {
    pub async fn nats_port(&self) -> Result<u16> {
        self.nats_server
            .get_host_port_ipv4(DEFAULT_NATS_PORT)
            .await
            .context("should have been able to get the host:guest port mapping")
    }

    pub async fn nats_url(&self) -> Result<String> {
        let nats_host = self
            .nats_server
            .get_host()
            .await
            .context("should have been able to query container host url")?;
        let nats_port = self.nats_port().await?;
        Ok(format!("{nats_host}:{nats_port}"))
    }

    pub async fn nats_client(&self) -> Result<async_nats::Client> {
        async_nats::connect(self.nats_url().await?)
            .await
            .context("should have created a nats client")
    }
}

pub async fn setup_env() -> Result<TestEnv> {
    let nats_server = start_nats_server().await?;
    let bridge_nats_server_ip = nats_server.get_bridge_ip_address().await?;
    let wasmcloud_host = start_wasmcloud_host(TestWasmCloudHostConfig {
        nats_ip: bridge_nats_server_ip.to_string(),
        nats_port: DEFAULT_NATS_PORT.to_string(),
        wasmcloud_version: "1.1.0".to_string(),
    })
    .await?;
    Ok(TestEnv {
        nats_server,
        wasmcloud_hosts: vec![wasmcloud_host],
    })
}

async fn start_nats_server() -> Result<ContainerAsync<GenericImage>> {
    GenericImage::new("nats", "2.10.18-alpine")
        .with_exposed_port(DEFAULT_NATS_PORT.into())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_cmd(["-js"])
        .start()
        .await
        .context("should have started nats-server")
}

struct TestWasmCloudHostConfig {
    nats_ip: String,
    nats_port: String,
    wasmcloud_version: String,
}

async fn start_wasmcloud_host(
    config: TestWasmCloudHostConfig,
) -> Result<ContainerAsync<GenericImage>> {
    GenericImage::new("ghcr.io/wasmcloud/wasmcloud", &config.wasmcloud_version)
        .with_wait_for(WaitFor::message_on_stderr("wasmCloud host started"))
        .with_env_var("WASMCLOUD_NATS_HOST", config.nats_ip)
        .with_env_var("WASMCLOUD_NATS_PORT", config.nats_port)
        .start()
        .await
        .context("should have started wasmcloud-host")
}

pub async fn wait_for_server(url: &str) {
    let mut wait_count = 1;
    loop {
        // Magic number: 10 + 1, since we are starting at 1 for humans
        if wait_count >= 11 {
            panic!("Ran out of retries waiting for host to start");
        }
        match tokio::net::TcpStream::connect(url).await {
            Ok(_) => break,
            Err(e) => {
                eprintln!("Waiting for server {url} to come up, attempt {wait_count}. Will retry in 1 second. Got error {e:?}");
                wait_count += 1;
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

/// Runs wash with the given args and makes sure it runs successfully. Returns the contents of
/// stdout
pub async fn run_wash_command<I, S>(args: I) -> Result<Vec<u8>>
where
    I: IntoIterator<Item = S> + std::clone::Clone,
    S: AsRef<std::ffi::OsStr>,
{
    let output = Command::new("wash")
        .args(args.clone())
        .output()
        .await
        .expect("Unable to run wash command");
    if !output.status.success() {
        bail!(
            "wash command ({:?}) didn't exit successfully: {}",
            args.into_iter()
                .map(|i| i.as_ref().to_str().unwrap_or_default().to_owned())
                .collect::<Vec<String>>(),
            String::from_utf8_lossy(&output.stderr),
        );
    }
    Ok(output.stdout)
}

/// Helper function that sets up a store with the given ID as its name using an existing nats client.
/// This ID should be unique per test
pub async fn create_test_store_with_client(id: &str, client: Client) -> Store {
    let context = jetstream::new(client);

    // First make sure we clean up the store. We can't just do a cleanup on `Drop` because
    // `tokio::test` uses a single threaded runtime and blocks forever and it isn't really worth
    // spinning up more cores to handle this. We don't care about the result because it could not
    // exist
    let _ = context.delete_key_value(id).await;

    context
        .create_key_value(KvConfig {
            bucket: id.to_string(),
            history: 1,
            num_replicas: 1,
            storage: jetstream::stream::StorageType::Memory,
            ..Default::default()
        })
        .await
        .expect("Unable to create KV bucket")
}

/// A wrapper around a command consumer
pub struct StreamWrapper {
    pub topic: String,
    pub client: async_nats::Client,
    pub stream: CommandConsumer,
}

impl StreamWrapper {
    /// Sets up a new command consumer stream using the given id as the stream name
    pub async fn new(id: String, client: async_nats::Client) -> StreamWrapper {
        let context = async_nats::jetstream::new(client.clone());
        let topic = format!("{id}.cmd.default");
        // If the stream exists, purge it
        let stream = if let Ok(stream) = context.get_stream(&id).await {
            stream
                .purge()
                .await
                .expect("Should be able to purge stream");
            stream
        } else {
            context
                .create_stream(async_nats::jetstream::stream::Config {
                    name: id.to_owned(),
                    description: Some("A stream that stores all commands".to_string()),
                    num_replicas: 1,
                    retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
                    subjects: vec![topic.clone()],
                    max_age: wadm::DEFAULT_EXPIRY_TIME,
                    storage: async_nats::jetstream::stream::StorageType::Memory,
                    allow_rollup: false,
                    ..Default::default()
                })
                .await
                .expect("Should be able to create test stream")
        };
        let stream = CommandConsumer::new(stream, &topic, "default", None)
            .await
            .expect("Unable to setup stream");
        StreamWrapper {
            topic,
            client,
            stream,
        }
    }

    /// Publish the given command
    pub async fn publish_command(&self, cmd: impl Into<wadm::commands::Command>) {
        self.client
            .publish(
                self.topic.clone(),
                serde_json::to_vec(&cmd.into()).unwrap().into(),
            )
            .await
            .expect("Unable to send command");
    }

    pub async fn wait_for_command(&mut self) -> ScopedMessage<wadm::commands::Command> {
        tokio::time::timeout(std::time::Duration::from_secs(2), self.stream.try_next())
            .await
            .expect("Should have received event before timeout")
            .expect("Stream shouldn't have had an error")
            .expect("Stream shouldn't have ended")
    }
}
