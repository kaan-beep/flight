# Flight Radar - Kubernetes Deployment

## Prerequisites

- Kubernetes cluster (minikube, Docker Desktop K8s, EKS, AKS, GKE)
- `kubectl` CLI configured
- Docker installed
- `bash` shell

## Quick Start

### 1. Prepare Environment Variables (Optional)
```bash
export OPENSKY_USERNAME="your_username"
export OPENSKY_PASSWORD="your_password"
```

### 2. Build and Deploy

**Option A: Automated (Recommended)**
```bash
bash setup-kubernetes.sh
```

**Option B: Manual**
```bash
# Build images
docker build -f Dockerfile.producer -t flight-radar-producer:latest .
docker build -f Dockerfile.consumer -t flight-radar-consumer:latest .

# Deploy
kubectl apply -f kubernetes.yaml

# Check status
kubectl get pods -n flight-radar
kubectl get services -n flight-radar
```

### 3. Access Services

**WebSocket (Frontend)**
```
ws://localhost:30765
```

**Kafka**
```
kafka:9092 (inside cluster)
localhost:30092 (from host)
```

**PostgreSQL**
```
postgres:5432 (inside cluster)
localhost:5432 (from host - configure port-forward)
```

## Port Forwarding (if needed)

```bash
# PostgreSQL
kubectl port-forward -n flight-radar svc/postgres 5432:5432

# Kafka
kubectl port-forward -n flight-radar svc/kafka 9092:9092
```

## Check Logs

```bash
# Producer logs
kubectl logs -n flight-radar deployment/producer -f

# Consumer logs
kubectl logs -n flight-radar deployment/consumer -f

# Kafka logs
kubectl logs -n flight-radar statefulset/kafka -f
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                    │
│  ┌─────────────────────────────────────────────────────┐│
│  │                  flight-radar namespace              ││
│  │                                                       ││
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐           ││
│  │  │ Producer │─▶│  Kafka   │◀─│ Consumer │           ││
│  │  └──────────┘  └──────────┘  └──────────┘           ││
│  │       │                              │               ││
│  │       ▼                              │               ││
│  │  ┌──────────┐                   ┌────────────┐      ││
│  │  │PostgreSQL│                   │ WebSocket  │      ││
│  │  └──────────┘                   │ (port 8765)│      ││
│  │                                  └────────────┘      ││
│  │  ┌──────────┐                                       ││
│  │  │Zookeeper │                                       ││
│  │  └──────────┘                                       ││
│  └─────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────┘
```

## Data Persistence

All critical data is stored in Persistent Volumes:
- **PostgreSQL**: 10Gi PVC
- **Kafka**: 20Gi PVC
- **failed_flights.jsonl**: Pod volume (restart safe)

## Resource Limits

| Service      | Memory | CPU   |
|--------------|--------|-------|
| Producer     | 256Mi  | 500m  |
| Consumer     | 256Mi  | 500m  |
| Kafka        | 512Mi  | 1000m |
| PostgreSQL   | 256Mi  | 500m  |
| Zookeeper    | 128Mi  | 250m  |

## Scaling

### Add more consumers (horizontal scaling)

```bash
kubectl scale deployment consumer -n flight-radar --replicas=3
```

### Modify resource limits

Edit `kubernetes.yaml` and update `resources` section:

```yaml
resources:
  limits:
    memory: "512Mi"
    cpu: "1000m"
```

## Cleanup

```bash
kubectl delete namespace flight-radar
```

## Troubleshooting

**Pods not starting?**
```bash
kubectl describe pod <pod-name> -n flight-radar
```

**Kafka not connecting?**
```bash
kubectl logs -n flight-radar statefulset/kafka
```

**Producer failing?**
```bash
kubectl logs -n flight-radar deployment/producer
# Check failed_flights.jsonl in pod
kubectl exec -it -n flight-radar deployment/producer -- cat failed_flights.jsonl
```

## Next Steps

1. Deploy with Docker Registry (for production)
2. Configure Ingress for WebSocket
3. Add monitoring (Prometheus + Grafana)
4. Setup auto-scaling (HPA)
