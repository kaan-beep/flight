"""
Aşama 2: Py Producer -- OpenSky API'den çektiği uçuş verisini Kafka'ya basar.

Kullanım:
    python producer.py             # sürekli döngü, her 10 saniyede bir çeker
    python producer.py --once      # tek seferlik çalışıp çıkar

Kafka broker adresi KAFKA_BOOTSTRAP_SERVERS ortam değişkeninden okunur
(varsayılan: localhost:9092).
"""

import argparse
import json
import os
import sys
import time

import requests
from confluent_kafka import KafkaException, Producer

from opensky_client import fetch_states

POLL_INTERVAL_SECONDS = 30
RATE_LIMIT_BACKOFF_SECONDS = 120
TOPIC = "live_flights"


def make_producer() -> Producer:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return Producer({"bootstrap.servers": bootstrap_servers})


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Mesaj gönderilemedi: {err}", file=sys.stderr)


def publish_flights(producer: Producer, flights: list[dict]) -> int:
    for flight in flights:
        producer.produce(
            TOPIC,
            key=flight["icao24"],
            value=json.dumps(flight),
            callback=delivery_report,
        )
    producer.poll(0)
    producer.flush(timeout=10)
    return len(flights)


def run_once(producer: Producer) -> None:
    flights = fetch_states()
    published = publish_flights(producer, flights)
    print(f"[{time.strftime('%H:%M:%S')}] {published} uçuş '{TOPIC}' topic'ine gönderildi.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="tek seferlik çalıştır")
    args = parser.parse_args()

    try:
        producer = make_producer()
    except KafkaException as exc:
        print(f"Kafka producer oluşturulamadı: {exc}", file=sys.stderr)
        sys.exit(1)

    if args.once:
        run_once(producer)
        return

    print(f"Sürekli producer döngüsü başladı, hedef topic: '{TOPIC}' (Ctrl+C ile durdur).")
    try:
        while True:
            wait_seconds = POLL_INTERVAL_SECONDS
            try:
                run_once(producer)
            except requests.exceptions.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if status == 429:
                    # OpenSky rate limit'e takıldık -- normalden çok daha uzun bekle.
                    wait_seconds = RATE_LIMIT_BACKOFF_SECONDS
                    print(
                        f"[{time.strftime('%H:%M:%S')}] OpenSky rate limit (429), "
                        f"{wait_seconds}s bekleniyor.",
                        file=sys.stderr,
                    )
                else:
                    print(f"[{time.strftime('%H:%M:%S')}] Hata, atlanıyor: {exc}", file=sys.stderr)
            except Exception as exc:
                # OpenSky API zaman zaman bağlantıyı kesebiliyor (ağ sorunu vb.)
                # -- döngüyü çökertmek yerine loglayıp devam et.
                print(f"[{time.strftime('%H:%M:%S')}] Hata, atlanıyor: {exc}", file=sys.stderr)
            time.sleep(wait_seconds)
    except KeyboardInterrupt:
        print("\nDurduruldu.")


if __name__ == "__main__":
    main()
