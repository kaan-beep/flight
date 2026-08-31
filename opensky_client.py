"""
Aşama 1: OpenSky Network API'den anlık uçuş verisi çekme.

OpenSky REST API'si kimlik doğrulama olmadan da çalışır (rate-limit düşüktür),
ama kayıtlı bir hesabın varsa OPENSKY_USERNAME / OPENSKY_PASSWORD ortam
değişkenlerini ayarlayarak daha yüksek limitlerden faydalanabilirsin.
"""

import os
import time

import requests

OPENSKY_URL = "https://opensky-network.org/api/states/all"


def fetch_states(bbox: tuple[float, float, float, float] | None = None) -> list[dict]:
    """OpenSky'dan anlık uçuş durumu (state vector) listesini çeker.

    bbox verilirse sadece o coğrafi kutudaki uçaklar döner:
    (lamin, lomin, lamax, lomax) -- örn. Türkiye için (35.0, 25.0, 43.0, 45.0)
    """
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
    """OpenSky'nin ham state vector dizisini okunabilir bir sözlüğe çevirir.

    Alan sırası OpenSky dokümantasyonuna göre sabittir:
    https://openskynetwork.github.io/opensky-api/rest.html#response
    """
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
        "timestamp": int(time.time()),
    }


if __name__ == "__main__":
    # Türkiye çevresindeki uçakları çekelim
    turkey_bbox = (35.0, 25.0, 43.0, 45.0)
    flights = fetch_states(bbox=turkey_bbox)

    print(f"{len(flights)} uçak bulundu.\n")
    for flight in flights[:10]:
        print(
            f"{flight['callsign'] or flight['icao24']:10s} "
            f"| {flight['origin_country']:20s} "
            f"| lat={flight['latitude']} lon={flight['longitude']} "
            f"| alt={flight['altitude_m']}m | hız={flight['velocity_ms']}m/s"
        )
