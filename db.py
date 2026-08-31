"""
PostgreSQL bağlantısı ve uçuş verisi kayıt katmanı.

Bağlantı bilgileri ortam değişkenlerinden okunur (bkz. .env.example):
  PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
"""

import os

import psycopg2
from psycopg2.extras import execute_values

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
            f["icao24"],
            f["callsign"],
            f["origin_country"],
            f["longitude"],
            f["latitude"],
            f["altitude_m"],
            f["on_ground"],
            f["velocity_ms"],
            f["heading_deg"],
            f["vertical_rate_ms"],
        )
        for f in flights
    ]

    with conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, rows)
    conn.commit()
    return len(rows)
