# Flight Radar - Deployment Guide

## 🎯 Architecture

```
OpenSky API (uçuş verileri)
         ↓
   Backend (Python/asyncio)
         ↓
   ┌─────────────┐
   │  PostgreSQL │  (data storage)
   └─────────────┘
         ↓
   ┌─────────────┐
   │   Kafka     │  (event streaming)
   └─────────────┘
         ↓
   WebSocket Server
         ↓
   Frontend (HTML/Leaflet)
         ↓
   🗺️ Harita (Live Flight Tracking)
```

## 🐳 Docker Compose (Development)

### Quick Start

```bash
# Otomatik başlat
chmod +x start-docker.sh
./start-docker.sh
```

### Manual Start

```bash
# Build
docker-compose build --no-cache

# Start
docker-compose up -d

# Check status
docker-compose ps

# Logs
docker-compose logs -f app
```

### Access

- **Frontend**: http://localhost:8080
- **Backend API**: ws://localhost:8765
- **Kafka**: localhost:9092
- **PostgreSQL**: localhost:5432
- **Zookeeper**: localhost:2181

### Stop

```bash
docker-compose down
```

---

## ☸️ Kubernetes (Production)

### Requirements

- Kubernetes cluster (1.20+)
- kubectl configured
- (Optional) Helm for package management

### Quick Deploy

```bash
# Otomatik deploy et
chmod +x k8s/deploy-auto.sh
./k8s/deploy-auto.sh
```

### Manual Deploy

```bash
cd k8s

# 1. Namespace ve config
kubectl apply -f namespace.yaml
kubectl apply -f configmap.yaml
kubectl apply -f secret.yaml

# 2. Databases
kubectl apply -f postgres.yaml
kubectl apply -f kafka.yaml

# 3. Application
kubectl apply -f backend.yaml
kubectl apply -f frontend.yaml

# Check status
kubectl get pods -n flight-radar
kubectl get svc -n flight-radar
```

### Access Frontend

```bash
# Port forward
kubectl port-forward -n flight-radar svc/frontend-service 8080:80

# Open browser
http://localhost:8080
```

### View Logs

```bash
# Backend logs (real-time)
kubectl logs -n flight-radar -l app=backend -f

# Specific pod
kubectl logs -n flight-radar backend-xyz123 -f

# Previous crash logs
kubectl logs -n flight-radar backend-xyz123 --previous
```

### Scaling

```bash
# Scale backend to 3 replicas
kubectl scale deployment backend -n flight-radar --replicas=3

# Scale frontend
kubectl scale deployment frontend -n flight-radar --replicas=2

# Check autoscaling (HPA)
kubectl get hpa -n flight-radar
```

### Delete Deployment

```bash
# Delete namespace (removes everything)
kubectl delete namespace flight-radar

# Or delete specific resources
kubectl delete deployment,statefulset,service -n flight-radar --all
```

---

## 🔄 Data Flow Explained

### 1. **OpenSky API → Backend**
```python
# fetch_states() - runs every 25 seconds
GET https://opensky-network.org/api/states/all
  ↓
Parse flight data (4000+ planes per request)
  ↓
Insert into PostgreSQL (flight_states table)
  ↓
Produce to Kafka topic "flights"
```

### 2. **Kafka Message Format**
```json
{
  "icao24": "a00001",
  "callsign": "PGT",
  "origin_country": "Turkey",
  "latitude": 41.2753,
  "longitude": 28.7519,
  "altitude_m": 9150.0,
  "on_ground": false,
  "velocity_ms": 250.5,
  "heading_deg": 45.0,
  "vertical_rate_ms": 2.5,
  "timestamp": 1694329801
}
```

### 3. **Kafka Consumer → WebSocket**
```
Kafka Consumer Group: "flight-radar-consumer"
  ↓
Listen on topic "flights"
  ↓
For each message:
  - Parse JSON
  - Send to all connected WebSocket clients
  - Update lastSeen timestamp
```

### 4. **Frontend Processing**
```javascript
WebSocket.onmessage = (event) => {
  const flight = JSON.parse(event.data);
  flights.set(flight.icao24, flight);  // Store
  scheduleRedraw();                     // Update canvas
}

// Canvas rendering
redraw() {
  for (const flight of visibleFlights) {
    drawPlane(
      x, y,
      heading,
      altitudeColor(altitude),
      scale
    );
  }
}
```

---

## 🔧 Configuration

### Environment Variables

**Docker (.env)**
```env
OPENSKY_USERNAME=your_username
OPENSKY_PASSWORD=your_password
```

**Kubernetes (k8s/secret.yaml)**
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: opensky-creds
  namespace: flight-radar
type: Opaque
stringData:
  username: your_username
  password: your_password
```

### Kafka Settings

- **Topic**: `flights`
- **Partitions**: 1 (auto-created)
- **Replication Factor**: 1
- **Auto-create**: enabled
- **Consumer Group**: `flight-radar-consumer`
- **Auto-commit**: enabled

### PostgreSQL Schema

```sql
CREATE TABLE flight_states (
  id              BIGSERIAL PRIMARY KEY,
  icao24          VARCHAR(10) NOT NULL,
  callsign        VARCHAR(20),
  origin_country  VARCHAR(100),
  longitude       DOUBLE PRECISION,
  latitude        DOUBLE PRECISION,
  altitude_m      DOUBLE PRECISION,
  on_ground       BOOLEAN,
  velocity_ms     DOUBLE PRECISION,
  heading_deg     DOUBLE PRECISION,
  vertical_rate_ms DOUBLE PRECISION,
  recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_flight_states_icao24 ON flight_states (icao24);
CREATE INDEX idx_flight_states_recorded_at ON flight_states (recorded_at);
```

---

## 📊 Monitoring

### Health Checks

#### Docker
```bash
# Check specific service
docker-compose exec postgres pg_isready -U postgres
docker-compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

#### Kubernetes
```bash
# Pod health
kubectl get pods -n flight-radar -o wide

# Describe pod for events
kubectl describe pod -n flight-radar backend-xyz123

# Check resource usage
kubectl top pods -n flight-radar
kubectl top nodes
```

### Performance Metrics

**Frontend**
- Connected clients: shown in status bar
- Active flights: live count
- Frames/second: canvas redraw rate

**Backend**
- Flights processed per request
- Kafka produce/consume latency
- PostgreSQL insert time

**Kafka**
```bash
# Consumer lag (Docker)
docker exec flight-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group flight-radar-consumer \
  --describe

# Pod metrics (K8s)
kubectl top pod -n flight-radar kafka-0
```

---

## 🐛 Troubleshooting

### Docker Issues

**"Unable to bootstrap from kafka"**
- Kafka not fully started yet
- Solution: Wait 30-60 seconds, retry logs

**WebSocket connection fails**
- Frontend URL wrong (using localhost instead of app)
- Solution: Check `connectWebSocket()` in index.html

**Database connection refused**
- PostgreSQL not ready
- Solution: Check `docker-compose ps` for health status

### Kubernetes Issues

**Pods stuck in Pending**
```bash
# Check node resources
kubectl top nodes
kubectl describe pod -n flight-radar <pod-name>
```

**CrashLoopBackOff**
```bash
# See previous logs
kubectl logs -n flight-radar <pod-name> --previous
kubectl describe pod -n flight-radar <pod-name>
```

**Service unreachable**
```bash
# Check DNS resolution
kubectl exec -it -n flight-radar <pod-name> -- nslookup kafka-service
kubectl get endpoints -n flight-radar
```

---

## 📈 Production Considerations

### 1. Security
- [ ] Use secrets for credentials (done in K8s)
- [ ] Enable TLS/SSL for WebSocket (wss://)
- [ ] Implement RBAC policies
- [ ] Network policies (pod-to-pod communication)

### 2. High Availability
- [ ] Multiple Kafka brokers (3+ replicas)
- [ ] PostgreSQL replication/failover
- [ ] Load balancer for frontend
- [ ] Pod disruption budgets (PDB)

### 3. Monitoring & Logging
- [ ] Prometheus metrics collection
- [ ] Grafana dashboards
- [ ] ELK Stack or Loki for logs
- [ ] Alert rules (Pod restarts, errors)

### 4. Backup & Recovery
- [ ] PostgreSQL automated backups
- [ ] Volume snapshots
- [ ] Disaster recovery plan
- [ ] Regular restore testing

### 5. Performance
- [ ] Horizontal Pod Autoscaler (HPA)
- [ ] Resource requests/limits tuning
- [ ] Connection pooling (PgBouncer)
- [ ] Kafka consumer performance tuning

---

## 📝 Useful Commands

### Docker
```bash
# Clean everything
docker-compose down -v

# Rebuild specific service
docker-compose build --no-cache app

# Enter container
docker exec -it flight-app bash

# Check network
docker network inspect flight_default
```

### Kubernetes
```bash
# Tail logs
kubectl logs -f -n flight-radar -l app=backend --all-containers=true

# Execute command in pod
kubectl exec -it -n flight-radar backend-xyz -- bash

# Port forward
kubectl port-forward -n flight-radar pod/backend-xyz 8765:8765

# Get YAML of running resource
kubectl get deployment backend -n flight-radar -o yaml
```

---

## 🚀 Next Steps

1. **Add authentication** to frontend
2. **Implement flight filtering** (by airline, altitude, etc.)
3. **Add 3D visualization** (altitude profile)
4. **Create admin dashboard** (system metrics)
5. **Setup CI/CD** (GitHub Actions → Docker Registry → K8s)

---

## 📞 Support

For issues:
1. Check logs: `kubectl logs -n flight-radar -l app=backend`
2. Verify connectivity: `kubectl port-forward` + curl test
3. Check resources: `kubectl top pods/nodes -n flight-radar`

---

**Last Updated**: 2026-09-10  
**Maintainer**: Flight Radar Team
