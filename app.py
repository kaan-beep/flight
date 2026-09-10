"""
Flight Radar -- Kafka entegrasyonlu sürüm.

Akış:
  1. OpenSky API'den uçuş verisi çeker           (fetch_states)
  2. Veriyi PostgreSQL'e kaydeder                (insert_flights)
  3. Veriyi Kafka'ya yazıncı olarak gönderir     (produce_to_kafka)
  4. Kafka'dan okur ve WebSocket ile yayınlar   (consume_from_kafka)

Kullanım:
    python app.py
"""

import asyncio
import json
import os
import sys
import time

import psycopg2
import requests
from psycopg2.extras import execute_values
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

# ---------------------------------------------------------------------------
# Ayarlar
# ---------------------------------------------------------------------------

OPENSKY_URL = "https://opensky-network.org/api/states/all"
POLL_INTERVAL_SECONDS = 25
RATE_LIMIT_BACKOFF_SECONDS = 120
WS_HOST = "0.0.0.0"
WS_PORT = 8765

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flights")

REGION_BBOX = (25.0, -12.0, 60.0, 55.0)  # (lamin, lomin, lamax, lomax)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS flight_states (
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
CREATE INDEX IF NOT EXISTS idx_flight_states_icao24 ON flight_states (icao24);
CREATE INDEX IF NOT EXISTS idx_flight_states_recorded_at ON flight_states (recorded_at);
"""

INSERT_SQL = """
INSERT INTO flight_states (
    icao24, callsign, origin_country, longitude, latitude,
    altitude_m, on_ground, velocity_ms, heading_deg, vertical_rate_ms
) VALUES %s
"""

connected_clients: set = set()


# ---------------------------------------------------------------------------
# 1) OpenSky'dan veri çekme
# ---------------------------------------------------------------------------

def fetch_states(bbox: tuple[float, float, float, float] | None = None) -> list[dict]:
    params = {}
    if bbox:
        lamin, lomin, lamax, lomax = bbox
        params.update(lamin=lamin, lomin=lomin, lamax=lamax, lomax=lomax)

    auth = None
    username = os.getenv("OPENSKY_USERNAME")
    password = os.getenv("OPENSKY_PASSWORD")
    if username and password:
        auth = (username, password)

    response = requests.get(OPENSKY_URL, params=params, auth=auth, timeout=15)
    response.raise_for_status()
    payload = response.json()

    states = payload.get("states") or []
    return [parse_state(state) for state in states]


def parse_state(state: list) -> dict:
    """OpenSky'nin ham state vector dizisini okunabilir bir sözlüğe çevirir."""
    return {
        "icao24": state[0],
        "callsign": (state[1] or "").strip(),
        "origin_country": state[2],
        "longitude": state[5],
        "latitude": state[6],
        "altitude_m": state[7],
        "on_ground": state[8],
        "velocity_ms": state[9],
        "heading_deg": state[10],
        "vertical_rate_ms": state[11],
        "squawk": state[14] if len(state) > 14 else None,
        "category": state[17] if len(state) > 17 else 0,
        "timestamp": int(time.time()),
    }


# ---------------------------------------------------------------------------
# 2) PostgreSQL'e kaydetme
# ---------------------------------------------------------------------------

def get_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DB", "flightradar"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "postgres"),
    )


def init_db(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()


def insert_flights(conn, flights: list[dict]) -> int:
    if not flights:
        return 0

    rows = [
        (
            f["icao24"], f["callsign"], f["origin_country"], f["longitude"],
            f["latitude"], f["altitude_m"], f["on_ground"], f["velocity_ms"],
            f["heading_deg"], f["vertical_rate_ms"],
        )
        for f in flights
    ]

    with conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, rows)
    conn.commit()
    return len(rows)


def connect_with_retry(delay_seconds: float = 3.0):
    attempt = 0
    while True:
        attempt += 1
        try:
            return get_connection()
        except Exception as exc:
            print(f"PostgreSQL'e bağlanılamadı (deneme {attempt}): {exc}", file=sys.stderr)
            time.sleep(delay_seconds)


async def connect_kafka_producer_with_retry(bootstrap_servers: str, delay_seconds: float = 3.0):
    attempt = 0
    while True:
        attempt += 1
        try:
            producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
            await producer.start()
            return producer
        except Exception as exc:
            print(f"Kafka Producer bağlantısı başarısız (deneme {attempt}): {exc}", file=sys.stderr)
            await asyncio.sleep(delay_seconds)


async def connect_kafka_consumer_with_retry(
    topic: str, bootstrap_servers: str, delay_seconds: float = 3.0
):
    attempt = 0
    while True:
        attempt += 1
        try:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',
                group_id='flight-radar-consumer',
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                max_poll_interval_ms=300000,
                enable_auto_commit=True,
            )
            await consumer.start()
            return consumer
        except Exception as exc:
            print(f"Kafka Consumer bağlantısı başarısız (deneme {attempt}): {exc}", file=sys.stderr)
            await asyncio.sleep(delay_seconds)


# ---------------------------------------------------------------------------
# 3) Kafka ile mesaj hareketi
# ---------------------------------------------------------------------------

async def produce_to_kafka(producer: AIOKafkaProducer, flights: list[dict]) -> None:
    """Uçak verilerini Kafka'ya yazıncı olarak gönder"""
    for flight in flights:
        await producer.send_and_wait(
            KAFKA_TOPIC,
            json.dumps(flight).encode("utf-8")
        )


# ---------------------------------------------------------------------------
# 4) WebSocket istemci yönetimi
# ---------------------------------------------------------------------------

async def handle_client(websocket) -> None:
    connected_clients.add(websocket)
    print(f"Yeni istemci bağlandı. Toplam: {len(connected_clients)}")
    try:
        async for _ in websocket:
            pass
    except ConnectionClosed:
        pass
    finally:
        connected_clients.discard(websocket)
        print(f"İstemci ayrıldı. Toplam: {len(connected_clients)}")


# ---------------------------------------------------------------------------
# 5) Ana döngüler
# ---------------------------------------------------------------------------

async def fetch_loop(loop: asyncio.AbstractEventLoop, conn, producer: AIOKafkaProducer) -> None:
    """OpenSky'dan veri çek → DB'ye kaydet → Kafka'ya gönder"""
    while True:
        wait_seconds = POLL_INTERVAL_SECONDS
        try:
            flights = await loop.run_in_executor(None, fetch_states, REGION_BBOX)
            await loop.run_in_executor(None, insert_flights, conn, flights)
            await produce_to_kafka(producer, flights)
            print(f"[{time.strftime('%H:%M:%S')}] {len(flights)} uçuş işlendi (Kafka'ya yazıldı).")
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status == 429:
                wait_seconds = RATE_LIMIT_BACKOFF_SECONDS
                print(f"[{time.strftime('%H:%M:%S')}] OpenSky rate limit (429), {wait_seconds}s bekleniyor.", file=sys.stderr)
            else:
                print(f"[{time.strftime('%H:%M:%S')}] Hata, atlanıyor: {exc}", file=sys.stderr)
        except Exception as exc:
            print(f"[{time.strftime('%H:%M:%S')}] Hata, atlanıyor: {exc}", file=sys.stderr)
        await asyncio.sleep(wait_seconds)


async def kafka_consumer_task(consumer: AIOKafkaConsumer) -> None:
    """Kafka'dan oku ve WebSocket istemcilerine gönder"""
    try:
        async for message in consumer:
            try:
                flight = json.loads(message.value.decode("utf-8"))
                if not connected_clients:
                    continue

                stale = []
                for client in list(connected_clients):
                    try:
                        await client.send(json.dumps(flight))
                    except ConnectionClosed:
                        stale.append(client)

                for client in stale:
                    connected_clients.discard(client)
            except Exception as exc:
                print(f"Kafka mesajı işlenirken hata: {exc}", file=sys.stderr)
    except Exception as exc:
        print(f"Kafka Consumer bağlantı hatası: {exc}", file=sys.stderr)


# ---------------------------------------------------------------------------
# 6) Başlangıç
# ---------------------------------------------------------------------------

async def main() -> None:
    # PostgreSQL bağlantı
    conn = connect_with_retry()
    init_db(conn)
    print("PostgreSQL hazır.")

    # Kafka Producer
    producer = await connect_kafka_producer_with_retry(KAFKA_BROKER)
    print(f"Kafka Producer başladı: {KAFKA_BROKER}")

    # Kafka Consumer
    consumer = await connect_kafka_consumer_with_retry(KAFKA_TOPIC, KAFKA_BROKER)
    print(f"Kafka Consumer başladı: {KAFKA_TOPIC}")

    loop = asyncio.get_running_loop()

    # WebSocket sunucusu ve tüm görevler beraber çalışsın
    async with serve(handle_client, WS_HOST, WS_PORT):
        print(f"WebSocket sunucusu ws://{WS_HOST}:{WS_PORT} adresinde çalışıyor.")

        # Görevleri paralel olarak başlat
        tasks = [
            fetch_loop(loop, conn, producer),
            kafka_consumer_task(consumer),
        ]

        try:
            await asyncio.gather(*tasks)
        finally:
            await producer.stop()
            await consumer.stop()
            print("Kapatılıyor...")


if __name__ == "__main__":
    asyncio.run(main())
