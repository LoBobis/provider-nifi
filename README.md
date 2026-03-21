# provider-nifi

A [Crossplane](https://crossplane.io) provider for [Apache NiFi](https://nifi.apache.org/) that enables Kubernetes-native management of NiFi dataflow infrastructure.

Declare your NiFi processors, connections, parameter contexts, and entire registry-based flows as Kubernetes custom resources — and let the provider reconcile them against a live NiFi cluster. The standout feature is **ManagedFlow**, a high-level abstraction that provides **automated blue-green rollouts** for registry-based flows with health checking, queue draining, and auto-update from Git-based registries.

## Overview

| CRD | Description |
|-----|-------------|
| **ManagedFlow** | Deploy flows from a NiFi Registry with blue-green rollouts, auto-update, and inline parameter contexts |
| **RegistryFlow** | Simple one-shot import of a versioned flow from a NiFi Registry |
| **ProcessGroup** | A NiFi process group container |
| **Processor** | An individual NiFi processor |
| **Connection** | A connection (queue) between two NiFi components |
| **ControllerService** | A shared controller service (e.g., DBCPConnectionPool) |
| **ParameterContext** | A named set of parameters that can be bound to process groups |

## Quick Start

### 1. Install the Provider

```yaml
apiVersion: pkg.crossplane.io/v1
kind: Provider
metadata:
  name: provider-nifi
spec:
  package: docker.io/<your-org>/provider-nifi:v0.3
```

### 2. Configure NiFi Connection

Create a Secret with your NiFi API credentials and a ProviderConfig that references it:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: nifi-credentials
  namespace: crossplane-system
type: Opaque
stringData:
  credentials: |
    {
      "url": "https://nifi.example.com:8443/nifi-api",
      "username": "admin",
      "password": "changeme",
      "tlsSkipVerify": true
    }
---
apiVersion: nifi.crossplane.io/v1alpha1
kind: ProviderConfig
metadata:
  name: default
spec:
  credentials:
    source: Secret
    secretRef:
      name: nifi-credentials
      namespace: crossplane-system
      key: credentials
```

### 3. Deploy a Flow

```yaml
apiVersion: nifi.crossplane.io/v1alpha1
kind: ManagedFlow
metadata:
  name: my-pipeline
  namespace: crossplane-system
spec:
  forProvider:
    parentGroupId: "root"
    registryId: "your-registry-id"
    bucketId: "your-bucket-id"
    flowId: "your-flow-id"
    flowVersion: 0            # 0 = latest version
    autoUpdate: true
    branch: "main"            # for Git-based registries
    desiredState: RUNNING
    parameterContext:
      name: "my-pipeline"
      parameters:
        - name: input_topic
          value: "events"
        - name: db_password
          sensitive: true
          valueFromSecret:
            name: nifi-secrets
            key: db-password
    rollout:
      strategy: BlueGreen
      healthCheck:
        stabilizationWindow: "30s"
      drainTimeout: "60s"
  providerConfigRef:
    name: default
```

---

## ManagedFlow

ManagedFlow is not a 1-to-1 mapping to a NiFi resource. It is a **higher-level controller** that orchestrates multiple NiFi API calls to deliver safe, automated flow deployments. Think of it as a "Deployment" for NiFi flows — it manages the lifecycle of process groups imported from a NiFi Registry, including version upgrades and rollbacks.

### Why Not Just Use RegistryFlow?

A RegistryFlow performs a simple one-shot import: it creates a process group from a registry snapshot and keeps it running. But in production, you need more:

- **Zero-downtime upgrades** — you can't just delete the old process group and import the new one; in-flight data would be lost
- **Automated rollback** — if the new version has bugs, you want the old version restored automatically
- **Auto-update** — when a developer pushes a new commit to the Git-based registry, the flow should update without manual intervention
- **Parameter management** — parameters should be declared inline with the flow, not managed as a separate resource

ManagedFlow solves all of these.

### What It Does

When you create a ManagedFlow, the controller:

1. **Imports** the specified flow version from the NiFi Registry as a new process group
2. **Creates** an inline parameter context (if specified) and binds it to the process group
3. **Enables** all controller services within the process group
4. **Starts** all processors (if `desiredState: RUNNING`)

When you update the flow (new version in the registry or changed parameters), the controller performs a **blue-green rollout**:

1. **Imports** the new version as a separate process group alongside the old one
2. **Configures** and starts the new process group
3. **Runs health checks** — monitors for bulletin errors during a stabilization window
4. **Stops** the old process group's input processors to drain in-flight data
5. **Waits** for connection queues to empty (with a configurable drain timeout)
6. **Cuts over** — deletes the old process group, renames the new one
7. **Rolls back** automatically if health checks fail

### Lifecycle Phases

```
Creating ──> Importing ──> EnablingServices ──> Starting ──> HealthCheck ──> Draining ──> Active
                                                    |                          |
                                                    └──── RollingBack <────────┘
```

| Phase | Description |
|-------|-------------|
| `Creating` | Initial import of the flow from the registry |
| `Importing` | Blue-green: importing the new version alongside the active one |
| `EnablingServices` | Enabling controller services in the (new) process group |
| `Starting` | Starting all processors |
| `HealthCheck` | Monitoring for bulletin errors during the stabilization window |
| `Draining` | Old PG's input processors stopped; waiting for queues to empty |
| `Active` | Flow is running and healthy |
| `RollingBack` | Health check failed — reverting to the previous version |

### Auto-Update

When `autoUpdate: true` and `flowVersion: 0`, the controller polls the NiFi Registry for new versions on every reconciliation cycle. When a new version is detected, it automatically triggers a blue-green rollout.

This works with both traditional NiFi Registry (integer version comparison) and **Git-based registries** (NiFi 2.x), where versions are identified by commit SHAs. The controller tracks deployed and latest commit SHAs to detect changes — important because NiFi's Git registry only keeps a rolling window of ~10 version entries.

```
$ kubectl get managedflow
NAME             READY   SYNCED   PHASE    DEPLOYED                                     AGE
my-pipeline      True    True     Active   a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2     5m
```

The `DEPLOYED` column shows the commit SHA of the currently running version. Use `kubectl get managedflow -o wide` to also see the `LATEST` column with the most recent version available in the registry.

### Sensitive Parameters with `valueFromSecret`

ManagedFlow (and ParameterContext) supports referencing Kubernetes Secrets for sensitive parameter values. The provider reads the secret at reconciliation time and passes the value to NiFi as a sensitive parameter — the secret value never appears in any Kubernetes custom resource spec or status:

```yaml
parameters:
  - name: db_password
    sensitive: true
    valueFromSecret:
      name: nifi-secrets            # Kubernetes Secret name
      key: db-password              # key within the Secret's data
      namespace: crossplane-system  # optional, defaults to the resource's namespace
```

### Rollout Configuration

```yaml
rollout:
  strategy: BlueGreen           # only strategy supported today
  healthCheck:
    stabilizationWindow: "30s"  # how long the new PG must run error-free
  drainTimeout: "60s"           # max time to wait for old PG queues to drain
```

If the new process group produces bulletin errors during the stabilization window, the controller automatically **rolls back**: it deletes the new PG and keeps the old one running. The `failedFlowVersion` is recorded in the status to prevent retry loops.

### Duplicate Prevention

The controller includes multiple layers of protection against creating duplicate process groups (a common issue with Kubernetes controllers where status writes can fail between reconciliation cycles):

- **Observe recovery**: If the external name annotation points to a deleted PG (stale after a blue-green cutover), the controller recovers by checking the `ActiveProcessGroupID` from status, then searching NiFi for PGs matching the ManagedFlow's name
- **Create guard**: Before importing a new PG, checks if one already exists in NiFi and adopts it instead
- **Rollout guard**: Before starting a blue-green rollout, checks NiFi for existing pending PGs and resumes them instead of creating duplicates

---

## Other CRDs

### RegistryFlow

A simpler alternative to ManagedFlow for one-shot flow imports without blue-green rollouts or auto-update. The controller imports the specified version and keeps the process group running, but does not perform version upgrades.

```yaml
apiVersion: nifi.crossplane.io/v1alpha1
kind: RegistryFlow
metadata:
  name: simple-flow
  namespace: crossplane-system
spec:
  forProvider:
    parentGroupId: "root"
    registryId: "your-registry-id"
    bucketId: "your-bucket-id"
    flowId: "your-flow-id"
    flowVersion: 3
    desiredState: RUNNING
    position:
      x: 0
      y: 0
  providerConfigRef:
    name: default
```

### Processor

```yaml
apiVersion: nifi.crossplane.io/v1alpha1
kind: Processor
metadata:
  name: generate-flowfile
  namespace: crossplane-system
spec:
  forProvider:
    parentGroupId: "your-pg-id"
    type: "org.apache.nifi.processors.standard.GenerateFlowFile"
    name: "Generate Test Data"
    desiredState: RUNNING
    config:
      schedulingStrategy: TIMER_DRIVEN
      schedulingPeriod: "10 sec"
      properties:
        "File Size": "1 KB"
        "Batch Size": "1"
      autoTerminatedRelationships:
        - success
    position:
      x: 0
      y: 0
  providerConfigRef:
    name: default
```

### Connection

```yaml
apiVersion: nifi.crossplane.io/v1alpha1
kind: Connection
metadata:
  name: generate-to-log
  namespace: crossplane-system
spec:
  forProvider:
    parentGroupId: "your-pg-id"
    source:
      id: "source-processor-id"
      type: PROCESSOR
      groupId: "your-pg-id"
    destination:
      id: "destination-processor-id"
      type: PROCESSOR
      groupId: "your-pg-id"
    selectedRelationships:
      - success
    backPressureObjectThreshold: 10000
    backPressureDataSizeThreshold: "1 GB"
  providerConfigRef:
    name: default
```

### ParameterContext

```yaml
apiVersion: nifi.crossplane.io/v1alpha1
kind: ParameterContext
metadata:
  name: my-params
  namespace: crossplane-system
spec:
  forProvider:
    name: "my-params"
    description: "Application parameters"
    parameters:
      - name: input_topic
        value: "events"
        description: "Kafka topic to consume from"
      - name: api_key
        sensitive: true
        valueFromSecret:
          name: app-secrets
          key: api-key
  providerConfigRef:
    name: default
```

### ControllerService

```yaml
apiVersion: nifi.crossplane.io/v1alpha1
kind: ControllerService
metadata:
  name: dbcp-pool
  namespace: crossplane-system
spec:
  forProvider:
    parentGroupId: "your-pg-id"
    type: "org.apache.nifi.dbcp.DBCPConnectionPool"
    name: "Database Pool"
    desiredState: ENABLED
    properties:
      "Database Connection URL": "jdbc:postgresql://db:5432/mydb"
      "Database Driver Class Name": "org.postgresql.Driver"
      "Database User": "app"
      "Password": "secret"
  providerConfigRef:
    name: default
```

### ProcessGroup

```yaml
apiVersion: nifi.crossplane.io/v1alpha1
kind: ProcessGroup
metadata:
  name: my-group
  namespace: crossplane-system
spec:
  forProvider:
    parentGroupId: "root"
    name: "My Process Group"
    desiredState: RUNNING
    comments: "Managed by Crossplane"
    position:
      x: 200
      y: 100
  providerConfigRef:
    name: default
```

---

## Building

### Build the Provider Binary

```bash
go build -o provider cmd/provider/main.go
```

### Build and Push Docker Image

```bash
docker build -t <your-registry>/provider-nifi:v0.3 .
docker push <your-registry>/provider-nifi:v0.3
```

### Build Crossplane Package (XPKG)

```bash
# Install crossplane CLI: https://docs.crossplane.io/latest/cli/
crossplane xpkg build \
  --package-root=package \
  --embed-runtime-image=<your-registry>/provider-nifi:v0.3 \
  -o provider-nifi.xpkg

crossplane xpkg push <your-registry>/provider-nifi:v0.3 -f provider-nifi.xpkg
```

### Run Locally (Out-of-Cluster)

```bash
# Requires a kubeconfig with access to the cluster and CRDs installed
go run cmd/provider/main.go --debug
```

---

## NiFi API JSON Schemas

The `schemas/nifi-api/` directory contains strict JSON schemas (`additionalProperties: false`) for all NiFi API endpoints used by this provider. They are organized into 4 files by HTTP method:

| File | Endpoints | Description |
|------|-----------|-------------|
| `GET.json` | 14 | All read operations (get processor, list PGs, poll versions, check status, etc.) |
| `POST.json` | 9 | All create operations (import flows, create processors, auth token, etc.) |
| `PUT.json` | 9 | All update operations (update configs, change run status, schedule components, etc.) |
| `DELETE.json` | 5 | All delete operations (remove processors, PGs, connections, etc.) |

Each endpoint entry documents **request** (path parameters, query parameters, body) and **response** schemas, with all shared type definitions in `_definitions.json`.

These schemas can be used for proxy validation, API documentation, or contract testing.

---

## Compatibility

| Component | Version |
|-----------|---------|
| Crossplane | 1.x and 2.x |
| Apache NiFi | 1.x and 2.x |
| NiFi Registry | Traditional and Git-based |
| Kubernetes | 1.25+ |

## License

Apache-2.0
