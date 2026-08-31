"""
Aşama 1: OpenSky API'den periyodik veri çekip PostgreSQL'e kaydeder.

Kullanım:
    python main.py                # sürekli döngü, her 15 saniyede bir çeker
    python main.py --once          # tek seferlik çalışıp çıkar
"""

import argparse
import sys
import time

from db import get_connection, init_db, insert_flights
from opensky_client import fetch_states

TURKEY_BBOX = (35.0, 25.0, 43.0, 45.0)
POLL_INTERVAL_SECONDS = 15


def run_once(conn) -> None:
    flights = fetch_states(bbox=TURKEY_BBOX)
    inserted = insert_flights(conn, flights)
    print(f"[{time.strftime('%H:%M:%S')}] {inserted} kayıt eklendi.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="tek seferlik çalıştır")
    args = parser.parse_args()

    try:
        conn = get_connection()
    except Exception as exc:
        print(f"PostgreSQL'e bağlanılamadı: {exc}", file=sys.stderr)
        print(
            "Docker ile hızlıca ayağa kaldırmak için:\n"
            "  docker run --name flight-postgres -e POSTGRES_PASSWORD=postgres "
            "-e POSTGRES_DB=flightradar -p 5432:5432 -d postgres:16",
            file=sys.stderr,
        )
        sys.exit(1)

    init_db(conn)

    if args.once:
        run_once(conn)
        return

    print("Sürekli veri çekme döngüsü başladı (Ctrl+C ile durdur).")
    try:
        while True:
            run_once(conn)
            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nDurduruldu.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
