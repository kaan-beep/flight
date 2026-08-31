"""
Aşama 2/4: Py Backend Consume -- Kafka'daki 'live_flights' topic'ini dinler,
gelen her uçuş kaydını PostgreSQL'e yazar.

Kullanım:
    python consumer_db.py
"""

import json
import os
import sys
import time

from confluent_kafka import Consumer, KafkaException

from db import get_connection, init_db, insert_flights

TOPIC = "live_flights"
GROUP_ID = "flight-db-writer"
BATCH_SIZE = 50
POLL_TIMEOUT_SECONDS = 5.0


def make_consumer() -> Consumer:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": GROUP_ID,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )


def connect_with_retry(retries: int = 10, delay_seconds: float = 3.0):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return get_connection()
        except Exception as exc:
            last_error = exc
            print(
                f"PostgreSQL'e bağlanılamadı (deneme {attempt}/{retries}): {exc}",
                file=sys.stderr,
            )
            time.sleep(delay_seconds)
    raise last_error


def main() -> None:
    conn = connect_with_retry()
    init_db(conn)

    try:
        consumer = make_consumer()
    except KafkaException as exc:
        print(f"Kafka consumer oluşturulamadı: {exc}", file=sys.stderr)
        sys.exit(1)

    consumer.subscribe([TOPIC])
    print(f"'{TOPIC}' dinleniyor, PostgreSQL'e yazılacak (Ctrl+C ile durdur).")

    buffer: list[dict] = []
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if buffer:
                    _flush(conn, buffer)
                continue
            if msg.error():
                print(f"Kafka hatası: {msg.error()}", file=sys.stderr)
                continue

            flight = json.loads(msg.value())
            buffer.append(flight)

            if len(buffer) >= BATCH_SIZE:
                _flush(conn, buffer)
    except KeyboardInterrupt:
        print("\nDurduruldu.")
    finally:
        if buffer:
            _flush(conn, buffer)
        consumer.close()
        conn.close()


def _flush(conn, buffer: list[dict]) -> None:
    inserted = insert_flights(conn, buffer)
    print(f"{inserted} kayıt PostgreSQL'e yazıldı.")
    buffer.clear()


if __name__ == "__main__":
    main()
