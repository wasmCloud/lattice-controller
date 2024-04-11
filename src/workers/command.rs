use tracing::{instrument, trace};

use crate::{
    commands::*,
    consumers::{
        manager::{WorkError, WorkResult, Worker},
        ScopedMessage,
    },
};

use super::insert_managed_annotations;

/// A worker implementation for handling incoming commands
#[derive(Clone)]
pub struct CommandWorker {
    client: wasmcloud_control_interface::Client,
}

impl CommandWorker {
    /// Creates a new command worker with the given connection pool.
    pub fn new(ctl_client: wasmcloud_control_interface::Client) -> CommandWorker {
        CommandWorker { client: ctl_client }
    }
}

#[async_trait::async_trait]
impl Worker for CommandWorker {
    type Message = Command;

    #[instrument(level = "trace", skip_all)]
    async fn do_work(&self, mut message: ScopedMessage<Self::Message>) -> WorkResult<()> {
        let res = match message.as_ref() {
            Command::ScaleComponent(actor) => {
                trace!(command = ?actor, "Handling scale actor command");
                // Order here is intentional to prevent scalers from overwriting managed annotations
                let mut annotations = actor.annotations.clone();
                insert_managed_annotations(&mut annotations, &actor.model_name);
                self.client
                    .scale_component(
                        &actor.host_id,
                        &actor.reference,
                        &actor.component_id,
                        actor.count,
                        Some(annotations.into_iter().collect()),
                        actor.config.clone(),
                    )
                    .await
            }
            Command::StartProvider(prov) => {
                trace!(command = ?prov, "Handling start provider command");
                // Order here is intentional to prevent scalers from overwriting managed annotations
                let mut annotations = prov.annotations.clone();
                insert_managed_annotations(&mut annotations, &prov.model_name);
                self.client
                    .start_provider(
                        &prov.host_id,
                        &prov.reference,
                        &prov.provider_id,
                        Some(annotations.into_iter().collect()),
                        prov.config.clone(),
                    )
                    .await
            }
            Command::StopProvider(prov) => {
                trace!(command = ?prov, "Handling stop provider command");
                // Order here is intentional to prevent scalers from overwriting managed annotations
                let mut annotations = prov.annotations.clone();
                insert_managed_annotations(&mut annotations, &prov.model_name);
                self.client
                    .stop_provider(&prov.host_id, &prov.provider_id)
                    .await
            }
            Command::PutLink(ld) => {
                trace!(command = ?ld, "Handling put linkdef command");
                // TODO(thomastaylor312): We should probably change ScopedMessage to allow us `pub`
                // access to the inner type so we don't have to clone, but no need to worry for now
                self.client.put_link(ld.clone().into()).await
            }
            Command::DeleteLink(ld) => {
                trace!(command = ?ld, "Handling delete linkdef command");
                self.client
                    .delete_link(
                        &ld.source_id,
                        &ld.link_name,
                        &ld.wit_namespace,
                        &ld.wit_package,
                    )
                    .await
            }
            Command::PutConfig(put_config) => {
                trace!(command = ?put_config, "Handling put config command");
                self.client
                    .put_config(&put_config.config_name, put_config.config.clone())
                    .await
            }
            Command::DeleteConfig(config_name) => {
                trace!("Handling delete config command");
                self.client.delete_config(config_name).await
            }
        }
        .map_err(|e| anyhow::anyhow!("{e:?}"));

        match res {
            Ok(ack) if !ack.success => {
                message.nack().await;
                Err(WorkError::Other(anyhow::anyhow!("{}", ack.message).into()))
            }
            Ok(_) => message.ack().await.map_err(WorkError::from),
            Err(e) => {
                message.nack().await;
                Err(WorkError::Other(e.into()))
            }
        }
    }
}
