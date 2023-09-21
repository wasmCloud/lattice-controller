#![cfg(feature = "_e2e_tests")]
use std::path::PathBuf;

use futures::StreamExt;
use wadm::server::{DeployResult, PutResult, StatusType};

mod e2e;
mod helpers;

use e2e::{assert_status, check_actors, check_providers, ClientInfo, ExpectedCount};
use helpers::{ECHO_ACTOR_ID, HTTP_SERVER_PROVIDER_ID};

use crate::e2e::check_status;

const MANIFESTS_PATH: &str = "test/data";
const DOCKER_COMPOSE_FILE: &str = "test/docker-compose-e2e-multitenant.yaml";

const MESSAGE_PUB_ACTOR_ID: &str = "MC3QONHYH3FY4KYFCOSVJWIDJG4WA2PVD6FHKR7FFT457GVUTZJYR2TJ";
const NATS_PROVIDER_ID: &str = "VADNMSIML2XGO2X4TPIONTIC55R2UUQGPPDZPAVSC2QD7E76CR77SPW7";
const ACCOUNT_EAST: &str = "Axxx";
const ACCOUNT_WEST: &str = "Ayyy";
const LATTICE_EAST: &str = "wasmcloud-east";
const LATTICE_WEST: &str = "wasmcloud-west";

#[cfg(feature = "_e2e_tests")]
#[tokio::test(flavor = "multi_thread")]
async fn run_multitenant_tests() {
    let root_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("Unable to find repo root"));
    let manifest_dir = root_dir.join(MANIFESTS_PATH);

    let compose_file = root_dir.join(DOCKER_COMPOSE_FILE);

    // Enable multitenancy
    std::env::set_var("WADM_MULTITENANT", "true");
    let mut client_info = ClientInfo::new(manifest_dir, compose_file).await;
    client_info
        .add_ctl_client(LATTICE_EAST, Some("Axxx.wasmbus.ctl"))
        .await;
    client_info
        .add_ctl_client(LATTICE_WEST, Some("Ayyy.wasmbus.ctl"))
        .await;
    client_info.launch_wadm().await;

    // Wait for the first event on both lattice prefixes before we start deploying and checking
    // statuses. Wadm can absolutely handle hosts starting before you start the wadm process, but the first event
    // on the lattice will initialize the lattice monitor and for the following test we quickly assert things.
    let mut east_sub = client_info
        .client
        .subscribe(format!("wadm.evt.{LATTICE_EAST}"))
        .await
        .expect("Should be able to subscribe to east events");
    let mut west_sub = client_info
        .client
        .subscribe(format!("wadm.evt.{LATTICE_WEST}"))
        .await
        .expect("Should be able to subscribe to west events");
    let _ = east_sub.next().await;
    let _ = west_sub.next().await;

    // NOTE(thomastaylor312): A nice to have here, but what I didn't want to figure out now, would
    // be to catch the panics from tests and label the backtrace with the appropriate information
    // about which test failed. Another issue is that only the first panic will be returned, so
    // capturing the backtraces and then printing them nicely would probably be good

    // We run this test first by itself because it is a basic test that wadm only spins up the exact
    // number of resources requested. If we were to run it in parallel, some of the shared resources
    // will be created with other tests (namely providers) and this test will fail
    test_basic_separation(&client_info)
        .await
        .expect("basic multitenant separation to work");
}

async fn test_basic_separation(client_info: &ClientInfo) -> anyhow::Result<()> {
    let stream = client_info.get_status_stream().await;
    stream
        .purge()
        .await
        .expect("shouldn't have errored purging stream");
    let resp = client_info
        .put_manifest_from_file("simple.yaml", Some(ACCOUNT_EAST), Some(LATTICE_EAST))
        .await;
    assert_ne!(
        resp.result,
        PutResult::Error,
        "Shouldn't have errored when creating manifest: {resp:?}"
    );

    let resp = client_info
        .put_manifest_from_file("simple2.yaml", Some(ACCOUNT_WEST), Some(LATTICE_WEST))
        .await;
    assert_ne!(
        resp.result,
        PutResult::Error,
        "Shouldn't have errored when creating manifest: {resp:?}"
    );

    eprintln!("Deploying manifests to east and west");

    let resp = client_info
        .deploy_manifest("echo-simple", Some(ACCOUNT_EAST), Some(LATTICE_EAST), None)
        .await;
    assert_ne!(
        resp.result,
        DeployResult::Error,
        "Shouldn't have errored when deploying manifest: {resp:?}"
    );

    let resp = client_info
        .deploy_manifest(
            "messaging-simple",
            Some(ACCOUNT_WEST),
            Some(LATTICE_WEST),
            None,
        )
        .await;
    assert_ne!(
        resp.result,
        DeployResult::Error,
        "Shouldn't have errored when deploying manifest: {resp:?}"
    );

    // Once manifest is deployed, first status should be compensating
    check_status(
        &stream,
        LATTICE_EAST,
        "echo-simple",
        StatusType::Compensating,
    )
    .await
    .unwrap();
    check_status(
        &stream,
        LATTICE_WEST,
        "messaging-simple",
        StatusType::Compensating,
    )
    .await
    .unwrap();

    // NOTE: This runs for a while, but it's because we're waiting for the provider to download,
    // which can take a bit
    // Ensure echo deployed in east and messaging deployed in west
    assert_status(None, Some(7), || async {
        let east_inventory = client_info.get_all_inventory(LATTICE_EAST).await?;
        let west_inventory = client_info.get_all_inventory(LATTICE_WEST).await?;

        // Check for echo actor and httpserver in east, as well as the link between them
        eprintln!("Ensuring east has echo, httpserver and link");
        check_actors(
            &east_inventory,
            "wasmcloud.azurecr.io/echo:0.3.7",
            "echo-simple",
            4,
        )?;
        check_providers(
            &east_inventory,
            "wasmcloud.azurecr.io/httpserver:0.17.0",
            ExpectedCount::Exactly(1),
        )?;
        let links = client_info
            .ctl_client(LATTICE_EAST)
            .query_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;

        if !links.links.iter().any(|ld| {
            ld.actor_id == ECHO_ACTOR_ID
                && ld.provider_id == HTTP_SERVER_PROVIDER_ID
                && ld.contract_id == "wasmcloud:httpserver"
        }) {
            anyhow::bail!(
                "Link between echo actor and http provider should exist: {:#?}",
                links
            )
        }

        // Check for messaging actor, httpserver and messaging in west, as well as the links between them
        eprintln!("Ensuring west has message-pub, httpserver, messaging and link");
        check_actors(
            &west_inventory,
            "wasmcloud.azurecr.io/message-pub:0.1.3",
            "messaging-simple",
            1,
        )?;
        check_providers(
            &west_inventory,
            "wasmcloud.azurecr.io/httpserver:0.18.2",
            ExpectedCount::Exactly(1),
        )?;
        check_providers(
            &west_inventory,
            "wasmcloud.azurecr.io/nats_messaging:0.17.2",
            ExpectedCount::Exactly(1),
        )?;
        let links = client_info
            .ctl_client(LATTICE_WEST)
            .query_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;

        if !links.links.iter().any(|ld| {
            ld.actor_id == MESSAGE_PUB_ACTOR_ID
                && ld.provider_id == HTTP_SERVER_PROVIDER_ID
                && ld.contract_id == "wasmcloud:httpserver"
        }) {
            anyhow::bail!(
                "Link between messaging actor and http provider should exist: {:#?}",
                links
            )
        }
        if !links.links.iter().any(|ld| {
            ld.actor_id == MESSAGE_PUB_ACTOR_ID
                && ld.provider_id == NATS_PROVIDER_ID
                && ld.contract_id == "wasmcloud:messaging"
        }) {
            anyhow::bail!(
                "Link between messaging actor and nats provider should exist: {:#?}",
                links
            )
        }

        // Check to ensure that no resources from west are running in east and vice versa
        eprintln!("Ensuring east has no west resources and vice versa");
        check_actors(
            &west_inventory,
            "wasmcloud.azurecr.io/echo:0.3.7",
            "echo-simple",
            0,
        )?;
        check_providers(
            &west_inventory,
            "wasmcloud.azurecr.io/httpserver:0.17.0",
            ExpectedCount::Exactly(0),
        )?;
        let links = client_info
            .ctl_client(LATTICE_WEST)
            .query_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;

        if links.links.iter().any(|ld| {
            ld.actor_id == ECHO_ACTOR_ID
                && ld.provider_id == HTTP_SERVER_PROVIDER_ID
                && ld.contract_id == "wasmcloud:httpserver"
        }) {
            anyhow::bail!(
                "Link between echo actor and http provider should not exist: {:#?}",
                links
            )
        }
        check_actors(
            &east_inventory,
            "wasmcloud.azurecr.io/message-pub:0.1.3",
            "messaging-simple",
            0,
        )?;
        check_providers(
            &east_inventory,
            "wasmcloud.azurecr.io/httpserver:0.18.2",
            ExpectedCount::Exactly(0),
        )?;
        check_providers(
            &east_inventory,
            "wasmcloud.azurecr.io/nats_messaging:0.17.2",
            ExpectedCount::Exactly(0),
        )?;
        let links = client_info
            .ctl_client(LATTICE_EAST)
            .query_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;

        if links.links.iter().any(|ld| {
            ld.actor_id == MESSAGE_PUB_ACTOR_ID
                && ld.provider_id == HTTP_SERVER_PROVIDER_ID
                && ld.contract_id == "wasmcloud:httpserver"
        }) {
            anyhow::bail!(
                "Link between messagepub actor and http provider should not exist: {:#?}",
                links
            )
        }
        if links.links.iter().any(|ld| {
            ld.actor_id == MESSAGE_PUB_ACTOR_ID
                && ld.provider_id == NATS_PROVIDER_ID
                && ld.contract_id == "wasmcloud:messaging"
        }) {
            anyhow::bail!(
                "Link between messagepub actor and http provider should not exist: {:#?}",
                links
            )
        }

        check_status(&stream, LATTICE_EAST, "echo-simple", StatusType::Ready)
            .await
            .unwrap();
        check_status(&stream, LATTICE_WEST, "messaging-simple", StatusType::Ready)
            .await
            .unwrap();

        Ok(())
    })
    .await;

    eprintln!("Everything good, undeploying manifests");

    // sleep 10 seconds
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

    // Undeploy manifests
    eprintln!("Undeploying manifest from east and west");
    let resp = client_info
        .undeploy_manifest("echo-simple", Some(ACCOUNT_EAST), Some(LATTICE_EAST))
        .await;
    assert_ne!(
        resp.result,
        DeployResult::Error,
        "Shouldn't have errored when undeploying manifest: {resp:?}"
    );

    let resp = client_info
        .undeploy_manifest("messaging-simple", Some(ACCOUNT_WEST), Some(LATTICE_WEST))
        .await;
    assert_ne!(
        resp.result,
        DeployResult::Error,
        "Shouldn't have errored when undeploying manifest: {resp:?}"
    );

    check_status(&stream, LATTICE_EAST, "echo-simple", StatusType::Undeployed)
        .await
        .unwrap();
    check_status(
        &stream,
        LATTICE_WEST,
        "messaging-simple",
        StatusType::Undeployed,
    )
    .await
    .unwrap();

    // assert that no actors or providers with annotations exist
    assert_status(None, None, || async {
        let east_inventory = client_info.get_all_inventory(LATTICE_EAST).await?;
        println!("east inventory: {:?}", east_inventory);
        let west_inventory = client_info.get_all_inventory(LATTICE_WEST).await?;
        println!("west inventory: {:?}", west_inventory);

        eprintln!("Ensuring resources stopped in east");
        check_actors(
            &east_inventory,
            "wasmcloud.azurecr.io/echo:0.3.7",
            "echo-simple",
            0,
        )?;
        check_providers(
            &east_inventory,
            "wasmcloud.azurecr.io/httpserver:0.17.0",
            ExpectedCount::Exactly(0),
        )?;

        eprintln!("Ensuring resources stopped in west");
        check_actors(
            &west_inventory,
            "wasmcloud.azurecr.io/message-pub:0.1.3",
            "messaging-simple",
            0,
        )?;
        check_providers(
            &west_inventory,
            "wasmcloud.azurecr.io/httpserver:0.18.2",
            ExpectedCount::Exactly(0),
        )?;
        check_providers(
            &west_inventory,
            "wasmcloud.azurecr.io/nats_messaging:0.17.2",
            ExpectedCount::Exactly(0),
        )?;

        Ok(())
    })
    .await;

    Ok(())
}
