<img align="right" src="./static/images/wadm_128.png" alt="wadm logo" />

# wasmCloud Application Deployment Manager (wadm)

The wasmCloud Application Deployment Manager (**wadm**) enables declarative wasmCloud applications.
It's responsible for managing a set of application deployment specifications, monitoring the current
state of an entire [lattice](https://wasmcloud.com/docs/deployment/lattice/), and issuing the
appropriate lattice control commands required to close the gap between observed and desired state.

## Using wadm

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://github.com/codespaces/new?hide_repo_select=true&ref=main&repo=401352358&machine=standardLinux32gb&location=EastUs)

### Install & Run

You can easily run **wadm** by downloading the [`wash`](https://wasmcloud.com/docs/installation) CLI, which automatically launches wadm alongside NATS and a wasmCloud host when you run `wash up`. You can use `wash` to query, create, and deploy applications.

```
wash up -d    # Start NATS, wasmCloud, and wadm in the background
```

Follow the [wasmCloud quickstart](https://wasmcloud.com/docs/tour/hello-world) to get started building and deploying an application, or follow the **Deploying an application** example below to simply try a deploy.

If you prefer to run **wadm** separately and/or connect to running wasmCloud hosts, you can instead opt for using the latest GitHub release artifact and executing the binary. Simply replace the latest version, your operating system, and architecture below. Please note that wadm requires a wasmCloud host version >=0.63.0

```
# Install wadm
curl -fLO https://github.com/wasmCloud/wadm/releases/download/<version>/wadm-<version>-<os>-<arch>.tar.gz
tar -xvf wadm-<version>-<os>-<arch>.tar.gz
cd wadm-<version>-<os>-<arch>
./wadm
```

### Deploying an application

Copy the following manifest and save it locally as `hello.yaml` (you can also find it in the `oam`
[directory](./oam/hello.yaml)):

```yaml
# Metadata
apiVersion: core.oam.dev/v1beta1
kind: Application
metadata:
  name: hello-world
  annotations:
    description: 'HTTP hello world demo'
spec:
  components:
    - name: http-component
      type: component
      properties:
        # Run components from OCI registries as below or from a local .wasm component binary.
        image: ghcr.io/wasmcloud/components/http-hello-world-rust:0.1.0
      traits:
        # One replica of this component will run
        - type: spreadscaler
          properties:
            instances: 1
    # The httpserver capability provider, started from the official wasmCloud OCI artifact
    - name: httpserver
      type: capability
      properties:
        image: ghcr.io/wasmcloud/http-server:0.21.0
      traits:
        # Link the HTTP server and set it to listen on the local machine's port 8080
        - type: link
          properties:
            target: http-component
            namespace: wasi
            package: http
            interfaces: [incoming-handler]
            source_config:
              - name: default-http
                properties:
                  ADDRESS: 127.0.0.1:8080
```

Then use `wash` to deploy the manifest:

```
wash app deploy hello.yaml
```

🎉 You've just launched your first application with **wadm**! Try `curl localhost:8080`.

When you're done, you can use `wash` to undeploy the application:

```
wash app undeploy hello-world
```

### Modifying applications

**wadm** supports upgrading applications by deploying new versions of manifests. Try changing the manifest you created above by updating the number of instances.

```yaml
<<ELIDED>>
metadata:
  name: hello-world
  annotations:
    description: 'HTTP hello world demo'
spec:
  components:
    - name: http-component
      type: component
      properties:
        image: ghcr.io/wasmcloud/components/http-hello-world-rust:0.1.0
      traits:
        - type: spreadscaler
          properties:
            instances: 10 # Let's have 10!
<<ELIDED>>
```

Then simply deploy the new manifest:

```
wash app deploy hello.yaml
```

Now wasmCloud is configured to automatically scale your component to 10 instances based on incoming load.

## Responsibilities

**wadm** has a very small set of responsibilities, which actually contributes to its power.

- **Manage Application Specifications** - Manage models consisting of _desired state_. This includes
  the creation and deletion and _rollback_ of models to previous versions. Application
  specifications are defined using the [Open Application Model](https://oam.dev/). For more
  information on wadm's specific OAM features, see our [OAM README](./oam/README.md).
- **Observe State** - Monitor wasmCloud [CloudEvents](https://wasmcloud.com/docs/reference/cloud-event-list) from all hosts in a lattice to build the current state.
- **Take Compensating Actions** - When indicated, issue commands to the [lattice control
  interface](https://github.com/wasmCloud/interfaces/tree/main/lattice-control) to bring about the
  changes necessary to make the desired and observed state match.

## 🚧 Advanced

You can find a Docker Compose file for deploying an end-to-end multi-tenant example in the [test](https://github.com/wasmCloud/wadm/blob/main/tests/docker-compose-e2e-multitenant.yaml) directory.

In advanced use cases, **wadm** is also capable of:

- Monitoring multiple lattices.
- Running multiple instances to distribute load among multiple processes, or for a high-availability
  architecture.

🚧 Multi-lattice and multi-process functionality is somewhat tested, but not as rigorously as a single instance monitoring
a single lattice. Proceed with caution while we do further testing.

### API

Interacting with **wadm** is done over NATS on the root topic `wadm.api.{prefix}` where `prefix` is
the lattice namespace prefix. For more information on this API, please consult the [wadm
Reference](https://wasmcloud.com/docs/ecosystem/wadm/).

## Known Issues/Missing functionality

As this is a new project there are some things we know are missing or buggy. A non-exhaustive list
of these can be found below:

- It is _technically_ possible as things stand right now for a race condition with manifests when a
  manifest is updated/created and deleted simultaneously. In this case, one of the operations will
  win and you will end up with a manifest that still exists after you delete it or a manifest that
  does not exist after you create it. This is a very unlikely scenario as only one person or process
  is interacting with a specific, but it is possible. If this becomes a problem for you, please let
  us know and we will consider additional ways of how we can address it.
- Manifest validation is implemented, but slightly clunky. Any PRs that make this better would be
  more than welcome!

## References

The wasmCloud Application Deployment Manager (**wadm**) originally came from [the autonomous lattice
controller RFC](https://github.com/wasmCloud/wasmcloud-otp/issues/177) and then was reworked in
version `0.4` in response to [the tidying and productioning
RFC](https://github.com/wasmCloud/wadm/issues/40).

- [Autonomous Agents](https://www.sciencedirect.com/topics/computer-science/autonomous-agent)
- [Autonomous Agent (Wikipedia)](https://en.wikipedia.org/wiki/Autonomous_agent)
- [Control Loop](https://en.wikipedia.org/wiki/Control_loop)
- [Hashicorp Vault](https://www.vaultproject.io/)
