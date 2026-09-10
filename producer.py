"""
Flight Radar Producer - OpenSky API'den veri çekip Kafka'ya yazıyor.

Görev:
  1. OpenSky API'den uçuş verilerini çek (~4200 uçak her 25s)
  2. PostgreSQL'e kaydet (historical data)
  3. Kafka topic'ine publish et (real-time streaming)
"""

import asyncio
import json
import os
import sys
import time

import psycopg2
import requests
from psycopg2.extras import execute_values
from aiokafka import AIOKafkaProducer

# OpenSky Network API
OPENSKY_URL = "https://opensky-network.org/api/states/all"
POLL_INTERVAL_SECONDS = 25
RATE_LIMIT_BACKOFF_SECONDS = 120

# Kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flights")

# Coğrafi sınır - Türkiye + komşu ülkeler
REGION_BBOX = (25.0, -12.0, 60.0, 55.0)

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


def fetch_states(bbox=None):
    """OpenSky API'den canlı uçuş verilerini çek."""
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


def parse_state(state):
    """OpenSky state vector'ü JSON dict'e dönüştür."""
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


def get_connection():
    """PostgreSQL'e bağlan."""
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DB", "flightradar"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "postgres"),
    )


def init_db(conn):
    """Tablo ve index'leri oluştur."""
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    print("✅ PostgreSQL schema hazır")


def insert_flights(conn, flights):
    """Batch insert - tüm flights'ı bir sorguyla yaz."""
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


def connect_with_retry(delay_seconds=3.0):
    """PostgreSQL'e bağlan - retry et."""
    attempt = 0
    while True:
        attempt += 1
        try:
            conn = get_connection()
            print(f"✅ PostgreSQL bağlantı başarılı (deneme {attempt})")
            return conn
        except Exception as exc:
            print(f"⏳ PostgreSQL'e bağlanılamadı (deneme {attempt}): {exc}", file=sys.stderr)
            time.sleep(delay_seconds)


async def connect_kafka_producer_with_retry(bootstrap_servers, delay_seconds=3.0):
    """Kafka Producer'a bağlan - retry et."""
    attempt = 0
    while True:
        attempt += 1
        try:
            producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
            await producer.start()
            print(f"✅ Kafka Producer başarıyla bağlandı (deneme {attempt})")
            return producer
        except Exception as exc:
            print(f"⏳ Kafka Producer bağlantısı başarısız (deneme {attempt}): {exc}", file=sys.stderr)
            await asyncio.sleep(delay_seconds)


async def produce_to_kafka(producer, flights):
    """Uçak verilerini Kafka topic'ine gönder."""
    for flight in flights:
        message = json.dumps(flight).encode("utf-8")
        await producer.send_and_wait(KAFKA_TOPIC, message)


async def fetch_loop(loop, conn, producer):
    """Ana veri çekme döngüsü - API → DB → Kafka."""
    while True:
        wait_seconds = POLL_INTERVAL_SECONDS

        try:
            # OpenSky API'den veri çek
            flights = await loop.run_in_executor(None, fetch_states, REGION_BBOX)

            # PostgreSQL'e kaydet
            await loop.run_in_executor(None, insert_flights, conn, flights)

            # Kafka'ya gönder
            await produce_to_kafka(producer, flights)

            print(f"[{time.strftime('%H:%M:%S')}] ✅ {len(flights)} uçuş işlendi → Kafka'ya yazıldı")

        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status == 429:
                wait_seconds = RATE_LIMIT_BACKOFF_SECONDS
                print(f"[{time.strftime('%H:%M:%S')}] ⚠️  Rate limit (429) - {wait_seconds}s bekleniyor", file=sys.stderr)
            else:
                print(f"[{time.strftime('%H:%M:%S')}] ❌ API Hatası ({status}): {exc}", file=sys.stderr)

        except Exception as exc:
            print(f"[{time.strftime('%H:%M:%S')}] ❌ Beklenmeyen hata: {exc}", file=sys.stderr)

        await asyncio.sleep(wait_seconds)


async def main():
    """Producer başlat."""
    conn = connect_with_retry()
    init_db(conn)

    producer = await connect_kafka_producer_with_retry(KAFKA_BROKER)

    loop = asyncio.get_running_loop()

    print(f"\n🚀 Flight Radar Producer başladı!")
    print(f"   📡 API: {OPENSKY_URL}")
    print(f"   💾 PostgreSQL: connected")
    print(f"   🔌 Kafka: {KAFKA_BROKER}\n")

    try:
        await fetch_loop(loop, conn, producer)
    finally:
        print("\n🛑 Kapatılıyor...")
        await producer.stop()
        conn.close()
        print("✅ Producer kapatıldı")


if __name__ == "__main__":
    asyncio.run(main())
