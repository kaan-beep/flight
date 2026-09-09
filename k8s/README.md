# Flight Radar - Kubernetes Deployment

## Gereksinimler

- Kubernetes cluster (1.20+)
- kubectl kurulu
- Docker images registry'de

## Kubernetes Mimarisi

```
Namespace: flight-radar
│
├─ StatefulSet:
│  ├─ postgres-0 (PVC: 10Gi)
│  ├─ zookeeper-0 (PVC: 5Gi)
│  └─ kafka-0 (PVC: 10Gi)
│
├─ Deployment:
│  ├─ backend (2 replicas)
│  └─ frontend (2 replicas)
│
└─ Service:
   ├─ postgres-service (ClusterIP)
   ├─ zookeeper-service (ClusterIP)
   ├─ kafka-service (ClusterIP)
   ├─ backend-service (ClusterIP)
   └─ frontend-service (LoadBalancer)
```

## Deploy Adımları

### 1. Docker Images Oluştur

```bash
# Backend image oluştur
docker build -t flight-radar:latest .

# Frontend image oluştur
docker build -t flight-radar-frontend:latest ./frontend

# Registry'e gönder (opsiyonel)
docker tag flight-radar:latest your-registry/flight-radar:latest
docker push your-registry/flight-radar:latest
```

### 2. Kubernetes Deploy Et

```bash
cd k8s

# Deploy script'i çalıştır
chmod +x deploy.sh
./deploy.sh

# Veya manuel olarak:
kubectl apply -f namespace.yaml
kubectl apply -f configmap.yaml
kubectl apply -f secret.yaml
kubectl apply -f postgres.yaml
kubectl apply -f kafka.yaml
kubectl apply -f backend.yaml
kubectl apply -f frontend.yaml
```

### 3. Status Kontrol Et

```bash
# Pod durumu
kubectl get pods -n flight-radar

# Service'ler
kubectl get svc -n flight-radar

# Logs göster
kubectl logs -n flight-radar -l app=backend

# Pod'a gir
kubectl exec -it -n flight-radar pod/backend-xxxx -- bash
```

### 4. Frontend'e Erişim

```bash
# Port forward
kubectl port-forward -n flight-radar svc/frontend-service 8080:80

# Tarayıcıda aç
http://localhost:8080
```

## Scaling

```bash
# Backend'i scale et (3 replicas)
kubectl scale deployment backend -n flight-radar --replicas=3

# Frontend'i scale et
kubectl scale deployment frontend -n flight-radar --replicas=3
```

## Logs

```bash
# Tüm backend logs
kubectl logs -n flight-radar -l app=backend -f

# Specific pod
kubectl logs -n flight-radar backend-abc123

# PostgreSQL logs
kubectl logs -n flight-radar postgres-0
```

## Silme

```bash
# Tüm namespace'i sil
kubectl delete namespace flight-radar

# Sadece deployment'ları sil
kubectl delete deployment -n flight-radar --all
```

## Notlar

- **Persistent Storage:** PostgreSQL, Zookeeper, Kafka için PVC kullanılıyor
- **Resource Limits:** Her pod için CPU/Memory limitleri ayarlandı
- **Health Checks:** Liveness ve readiness probes yapılandırıldı
- **Replicas:** Backend ve Frontend 2 replica ile başlıyor (scale edebilir)
- **Image Pull:** imagePullPolicy: Always (her deployment'da pull)

## Production İyileştirmeleri

1. **Namespace Isolation:** RBAC, Network Policies
2. **Monitoring:** Prometheus, Grafana
3. **Logging:** ELK Stack, Loki
4. **Ingress:** Nginx Ingress Controller, TLS
5. **Backup:** PostgreSQL backup strategy
6. **HPA:** Horizontal Pod Autoscaler
7. **PDB:** Pod Disruption Budget
