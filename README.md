# KEDA External Pub/Sub Push Scaler

A high-performance, event-driven external scaler for KEDA that provides "Cloud Run-like" instant scaling for GCP Pub/Sub.

## Why this exists?
The standard KEDA GCP Pub/Sub scaler relies on Cloud Monitoring metrics, which can have a 2-3 minute lag. This scaler uses a **Streaming Pull** "Starter Motor" architecture:
1.  **Instant-On:** Maintains an ephemeral subscription to the topic and signals KEDA to scale up the moment a message is published.
2.  **Metric-Backed Hold:** Once active, it keeps the pods up for a configurable `holdDuration`.
3.  **Clean Shutdown:** Uses PromQL (PQL) to verify the worker subscription's backlog is empty before allowing a scale-to-zero.

## Features
- **Sub-second scaling lag.**
- **Supports multiple topics/subscriptions** in a single instance.
- **Strictly one-way data flow:** Decoupled listener and manager logic.
- **GCP Native PQL support:** Uses the modern Prometheus API for backlog checks.
- **mTLS support** for secure gRPC communication.

---

## Deployment: Running as a KEDA Operator Sidecar

The most efficient way to run this scaler is as a sidecar to the KEDA Operator. This removes network latency and simplifies authentication.

### 1. Update KEDA Helm Values
Add the following to your `values.yaml` when installing or upgrading KEDA. 

The scaler uses **Application Default Credentials (ADC)**. It is highly recommended to use **Workload Identity Federation (WIF)** by annotating the KEDA Operator's Service Account with a GCP IAM Service Account that has the required permissions.

```yaml
operator:
  extraContainers:
    - name: gcp-push-scaler
      image: ghcr.io/nd-ecobee/keda-external-pubsub:latest
      ports:
        - containerPort: 9090
      env:
        # Optional: Overrides automatic project detection
        # - name: GOOGLE_CLOUD_PROJECT
        #   value: "your-project-id"
      # If using mTLS
      # volumeMounts:
      #   - name: certs
      #     mountPath: /certs
      #     readOnly: true

  # If using mTLS
  # extraVolumes:
  #   - name: certs
  #     secret:
  #       secretName: keda-external-scaler-certs
```

### 2. Configure ScaledObject
Because the scaler is running in the same pod as the operator, you can point KEDA to `localhost:9090`.

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: fast-pubsub-worker
spec:
  scaleTargetRef:
    name: your-deployment
  minReplicaCount: 0
  maxReplicaCount: 20
Receive error for topic     # How often to check PQL metrics
```

---

## Development & Build

### Requirements
- Go 1.26+
- Pack CLI (for container builds)

### Locally
```bash
go build -o keda-external-pubsub .
```

### Container Build (Paketo)
```bash
# Build for local architecture
make build

# Build and Push to registry
make publish CN=ghcr.io/your-org/keda-external-pubsub:v1.0.0 PLATFORM=linux/amd64
```

## Security
- **Authentication:** The scaler uses Google Application Default Credentials (ADC). Ensure the KEDA Operator's Service Account has `roles/pubsub.viewer` and `roles/monitoring.viewer`.
- **mTLS:** Optional mTLS is supported via `TLS_CERT_PATH`, `TLS_KEY_PATH`, and `TLS_CA_PATH`.
- **Default Listener:** For safety in sidecar deployments, the scaler defaults to listening on `127.0.0.1`. Use the `HOST` environment variable to override (e.g. `0.0.0.0` for standalone deployment).

