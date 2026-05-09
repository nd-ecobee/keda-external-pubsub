# KEDA External Pub/Sub Push Scaler

A high-performance, event-driven external scaler for KEDA that provides "Cloud Run-like" instant scaling for GCP Pub/Sub.

## Why this exists?
The standard KEDA GCP Pub/Sub scaler relies on Cloud Monitoring metrics, which can have a 2-3 minute lag. This scaler uses a pure **Streaming Pull** "Starter Motor" architecture:
1.  **Instant-On:** Maintains an ephemeral subscription to the topic and signals KEDA to scale up the moment a message is published.
2.  **Metric-Free Hold:** Once active, it pulls a single message, Nacks it, and keeps the scaling target alive for a configurable `holdDuration` without querying GCP API metrics, saving cost and reducing latency to zero.

## Features
- **Sub-second scaling lag.**
- **Supports multiple topics** in a single instance.
- **Zero API Polling Cost:** Purely event-driven, relying only on Pub/Sub streaming pull.
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
Because the scaler is running in the same pod as the operator, you can point KEDA to `localhost:9090`. We combine this "starter motor" with standard Prometheus triggers to handle 0-to-1 instantly, while letting metrics handle 1-to-N scaling.

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
  triggers:
    # 1. The Starter Motor: Instantly scales from 0 -> 1 on first message
    - type: external-push
      metadata:
        scalerAddress: localhost:9090
        topic: "projects/your-project/topics/your-topic"
        holdDuration: "5m"      # Keep pods alive for 5m after last message
        checkInterval: "1m"     # How often to check topic queue/backlog
    
    # 2. Topic Publish Rate: Keeps scaler active if messages are flowing
    - type: prometheus
      metadata:
        serverAddress: https://monitoring.googleapis.com/v1/projects/your-project/location/global/prometheus
        metricName: topic_publish_rate
        query: sum(rate({__name__="pubsub.googleapis.com/topic/send_request_count", monitored_resource="pubsub_topic", topic_id="your-topic"}[1m]))
        threshold: "1"
        authModes: "bearer"

    # 3. Subscription Backlog: Scales 1 -> N based on queue depth
    - type: prometheus
      metadata:
        serverAddress: https://monitoring.googleapis.com/v1/projects/your-project/location/global/prometheus
        metricName: subscription_backlog
        query: max_over_time({__name__="pubsub.googleapis.com/subscription/num_undelivered_messages", monitored_resource="pubsub_subscription", subscription_id="your-worker-sub"}[1m])
        threshold: "10" # Target 10 messages per pod
        authModes: "bearer"
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

