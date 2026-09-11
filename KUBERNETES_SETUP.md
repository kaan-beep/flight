# Flight Radar - Kubernetes Deployment Guide

## Prerequisites

- Kubernetes cluster (1.24+)
- Docker Registry (Azure Container Registry, Docker Hub, etc.)
- `kubectl` configured
- `docker` CLI

## Step 1: Set Up Registry Credentials

```bash
# Azure Container Registry örneği
ACR_REGISTRY="your-registry.azurecr.io"
ACR_USERNAME="<username>"
ACR_PASSWORD="<password>"

# Registry credentials secret oluştur
kubectl create secret docker-registry registry-credentials \
  --docker-server=$ACR_REGISTRY \
  --docker-username=$ACR_USERNAME \
  --docker-password=$ACR_PASSWORD \
  --docker-email=your-email@example.com \
  -n flight-radar
```

## Step 2: Build & Push Docker Images

```bash
# Registry address (değiştir)
REGISTRY="your-registry.azurecr.io"

# Producer image build
docker build -t $REGISTRY/flight-producer:latest -f Dockerfile.producer .
docker push $REGISTRY/flight-producer:latest

# Consumer image build
docker build -t $REGISTRY/flight-consumer:latest -f Dockerfile.consumer .
docker push $REGISTRY/flight-consumer:latest
```

## Step 3: Update kubernetes.yaml

kubernetes.yaml dosyasında registry adresini güncelle:

```yaml
# Bul ve değiştir:
your-registry.azurecr.io → your-actual-registry.azurecr.io
```

## Step 4: Deploy to Kubernetes

```bash
# Namespace'i kontrol et
kubectl get namespace flight-radar || kubectl create namespace flight-radar

# Secrets ve ConfigMap'ları güncelle (optional)
kubectl -n flight-radar create secret generic flight-secrets \
  --from-literal=PG_PASSWORD=your-password \
  --from-literal=OPENSKY_USERNAME=your-username \
  --from-literal=OPENSKY_PASSWORD=your-password \
  -o yaml | kubectl apply -f -

# Tüm resources deploy et
kubectl apply -f kubernetes.yaml

# Pod'ları kontrol et
kubectl -n flight-radar get pods

# Logs'ları takip et
kubectl -n flight-radar logs -f deployment/producer
kubectl -n flight-radar logs -f deployment/consumer
kubectl -n flight-radar logs -f deployment/postgres
kubectl -n flight-radar logs -f deployment/kafka
```

## Step 5: Verify Deployment

```bash
# Pod status
kubectl -n flight-radar get pods
kubectl -n flight-radar describe pod <pod-name>

# Services
kubectl -n flight-radar get svc

# PVC status
kubectl -n flight-radar get pvc

# WebSocket consumer port
kubectl -n flight-radar port-forward svc/consumer 8765:8765
# Then access: ws://localhost:8765
```

## Storage Configuration

### Persistent Volumes

Kubernetes deployment 3 PV kullanır:

- **pg-pvc** (10Gi) - PostgreSQL data
- **kafka-pvc** (20Gi) - Kafka logs + offsets  
- **zk-pvc** (5Gi) - Zookeeper data

**Production için:** Storage class kullan (AWS EBS, Azure Disk, etc.)

```yaml
# Example: Azure Disk Storage Class
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: azure-disk
provisioner: disk.csi.azure.com
parameters:
  skuname: Standard_LRS
```

## Scaling

### Horizontal Scaling - Consumer

```bash
# 3 consumer replicas
kubectl -n flight-radar scale deployment consumer --replicas=3

# Auto-scaling (HPA)
kubectl autoscale deployment consumer \
  --min=1 --max=5 --cpu-percent=80 \
  -n flight-radar
```

### Kafka Replication

Production için Kafka'yı multi-broker setup'a çevir:

```yaml
# kafka-broker-2.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-broker-2
  namespace: flight-radar
spec:
  replicas: 1
  # ...
  env:
  - name: KAFKA_BROKER_ID
    value: "2"
  # ... (same as kafka-broker-1)
```

## Monitoring

```bash
# Resource usage
kubectl -n flight-radar top pods
kubectl -n flight-radar top nodes

# Events
kubectl -n flight-radar get events --sort-by='.lastTimestamp'

# Pod logs
kubectl -n flight-radar logs -f deployment/producer --tail=100
kubectl -n flight-radar logs -f deployment/consumer --tail=100
```

## Troubleshooting

### Pod not starting

```bash
kubectl -n flight-radar describe pod <pod-name>
kubectl -n flight-radar logs <pod-name>
```

### Registry auth failed

```bash
# Debug image pull
kubectl -n flight-radar get events | grep -i pull
kubectl -n flight-radar get pods -o wide
```

### Kafka broker not ready

```bash
# Check Kafka service
kubectl -n flight-radar get svc kafka
kubectl -n flight-radar logs deployment/kafka

# Port forward for testing
kubectl -n flight-radar port-forward svc/kafka 9092:9092
# From another terminal: telnet localhost 9092
```

### PostgreSQL connection failed

```bash
# Test DB connection
kubectl -n flight-radar exec -it deployment/postgres -- \
  psql -U postgres -d flightradar -c "SELECT 1;"
```

## Cleanup

```bash
# Tüm resources sil
kubectl delete namespace flight-radar

# Sadece Deployments sil
kubectl -n flight-radar delete deployment --all

# PVC'leri sil
kubectl -n flight-radar delete pvc --all
```

## Environment Variables

kubernetes.yaml içindeki ConfigMap ve Secret'ları özelleştir:

### ConfigMap (flight-config)
- `KAFKA_BROKER`: Kafka address
- `KAFKA_TOPIC`: Topic name
- `PG_HOST`: PostgreSQL host
- `PG_PORT`: PostgreSQL port
- `PG_DB`: Database name
- `PG_USER`: Database user

### Secret (flight-secrets)
- `PG_PASSWORD`: Database password
- `OPENSKY_USERNAME`: OpenSky API username
- `OPENSKY_PASSWORD`: OpenSky API password

## Resource Limits

Deployment'larda ayarlanmış resource limits:

- **Producer**: 256Mi memory, 500m CPU
- **Consumer**: 256Mi memory, 500m CPU
- **PostgreSQL**: 256Mi memory, 500m CPU
- **Kafka**: 512Mi memory, 1000m CPU
- **Zookeeper**: 128Mi memory, 200m CPU

Production için requirements'ınıza göre artırın.

## Health Checks

Her pod liveness ve readiness probe'lar ile kurulu:

- **Liveness**: Pod sağlıklı mı? (Interval: 30s, Timeout: 10s)
- **Readiness**: Pod traffic kabul edebilir mi? (Interval: 10s, Timeout: 5s)

## Next Steps

1. ✅ Docker images build & push
2. ✅ kubernetes.yaml'i özelleştir
3. ✅ `kubectl apply -f kubernetes.yaml`
4. ✅ Pod'ları monitor et
5. ✅ Consumer WebSocket'ine bağlan
