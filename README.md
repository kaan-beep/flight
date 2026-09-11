# Flight Radar - Real-Time Aircraft Tracking

Real-time flight tracking system using OpenSky Network API, Kafka, PostgreSQL, and WebSocket.

## Architecture

```
OpenSky API (4000-5000 flights every 25s)
    ↓
Producer Container
├─→ PostgreSQL (historical data)
└─→ Kafka (real-time streaming)
    ↓
Consumer Container
└─→ WebSocket (frontend broadcast)
    ↓
Frontend (8080)
```

## Components

- **Producer** (producer.py)
  - Fetches flight data from OpenSky Network API
  - Saves to PostgreSQL (persistent storage)
  - Publishes to Kafka (real-time streaming)
  - Retry logic with Dead Letter Queue (DLQ)

- **Consumer** (consumer.py)
  - Reads from Kafka (consumer group offset persistence)
  - Broadcasts via WebSocket (8765)
  - Auto-commit offset every 5 seconds
  
- **Infrastructure**
  - PostgreSQL: Historical data storage
  - Kafka: Message broker with 30-day retention
  - Zookeeper: Kafka coordination
  - Redis: Optional caching

## Quick Start

### 1. Prerequisites
```bash
docker --version  # 20.10+
docker-compose --version  # 2.0+
```

### 2. Environment Variables
```bash
# .env file (create if needed)
OPENSKY_USERNAME=your-username
OPENSKY_PASSWORD=your-password
```

### 3. Start Services
```bash
docker-compose up -d

# Check status
docker-compose ps
docker-compose logs -f producer
docker-compose logs -f consumer
```

### 4. Access Services
- **Frontend**: http://localhost:8080
- **WebSocket**: ws://localhost:8765
- **PostgreSQL**: localhost:5432
- **Kafka**: localhost:9092

## Data Persistence

### Volumes
- `pg_data` - PostgreSQL database
- `kafka_data` - Kafka logs + consumer offsets (30 days)
- `zookeeper_data` - Zookeeper state

### Guarantee
✅ **Zero data loss**: 
- API → PostgreSQL (persist immediately)
- PostgreSQL → Kafka (with 3x retry)
- Failed writes → Dead Letter Queue (DLQ)
- DLQ retry every 60 seconds

## Monitoring

### Container Health
```bash
docker-compose ps
docker-compose logs -f [service]  # producer, consumer, kafka, postgres
```

### Database
```bash
docker-compose exec postgres psql -U postgres -d flightradar -c "SELECT COUNT(*) FROM flight_states;"
```

### Kafka Topics
```bash
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic flights --from-beginning
```

### Dead Letter Queue
```bash
docker-compose exec producer cat failed_flights.jsonl
```

## Scaling

### Horizontal Scaling
```bash
# Scale consumer to 3 replicas
docker-compose up -d --scale consumer=3
```

### Message Retention
Edit `docker-compose.yml`:
```yaml
kafka:
  environment:
    KAFKA_LOG_RETENTION_HOURS: 720  # 30 days (default)
```

## Troubleshooting

### Producer not running
```bash
docker-compose logs producer
# Check: Kafka connection, PostgreSQL connection
```

### Consumer WebSocket not responding
```bash
docker-compose logs consumer
# Check: Kafka connection, port 8765
```

### Database connection failed
```bash
docker-compose exec postgres pg_isready
docker-compose logs postgres
```

### Kafka broker not ready
```bash
docker-compose logs kafka
docker-compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

## Performance Tuning

### Kafka
- `KAFKA_NUM_NETWORK_THREADS`: 8
- `KAFKA_NUM_IO_THREADS`: 8
- `KAFKA_LOG_RETENTION_HOURS`: 720
- `KAFKA_LOG_RETENTION_BYTES`: -1 (unlimited)

### Producer
- Retry interval: 2 seconds
- Max retries: 3
- DLQ check interval: 60 seconds

### Consumer
- Auto-commit interval: 5 seconds
- Session timeout: 30 seconds
- Heartbeat interval: 10 seconds

## Cleanup

```bash
# Stop all services
docker-compose down

# Remove volumes (DELETE DATA)
docker-compose down -v

# Remove images
docker rmi flight-producer flight-consumer
```

## Development

### Running Locally (without Docker)

```bash
# Install dependencies
pip install -r requirements.txt

# Start services (except app containers)
docker-compose up postgres kafka zookeeper

# Run producer
python producer.py

# Run consumer (in another terminal)
python consumer.py
```

### Debugging
```bash
# Enable debug logging
export DEBUG=1
python producer.py
```

## Kubernetes Deployment

See [KUBERNETES_SETUP.md](KUBERNETES_SETUP.md) for Kubernetes deployment guide.

## API Documentation

### WebSocket Format
```json
{
  "icao24": "a00001",
  "callsign": "THY123",
  "origin_country": "Turkey",
  "longitude": 29.0,
  "latitude": 41.0,
  "altitude_m": 10000,
  "on_ground": false,
  "velocity_ms": 250.5,
  "heading_deg": 180,
  "vertical_rate_ms": 5.0,
  "timestamp": 1726901234
}
```

## Monitoring & Alerting

### Health Checks
- Producer: Kafka broker connectivity
- Consumer: WebSocket port availability
- PostgreSQL: Database readiness
- Kafka: Broker API availability

### Metrics to Monitor
- Flight count per cycle
- Kafka offset lag
- Message processing latency
- Database size
- Failed write count (DLQ size)

## License

MIT

## Author

Flight Radar Team
