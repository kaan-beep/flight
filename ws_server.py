"""
Aşama 3: Frontend Consumer WS -- Kafka'daki 'live_flights' topic'ini dinler,
gelen her uçuş kaydını bağlı tüm WebSocket istemcilerine (tarayıcılara) anlık basar.

Kullanım:
    python ws_server.py
    # Sonra frontend/index.html dosyasını tarayıcıda aç.
"""

import asyncio
import json
import os

from confluent_kafka import Consumer
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

TOPIC = "live_flights"
GROUP_ID = "flight-ws-broadcaster"
# 0.0.0.0: container içinden dışarıya (host port mapping'ine) erişilebilmesi için
WS_HOST = os.getenv("WS_HOST", "0.0.0.0")
WS_PORT = 8765

connected_clients: set = set()


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


async def handle_client(websocket) -> None:
    connected_clients.add(websocket)
    print(f"Yeni istemci bağlandı. Toplam: {len(connected_clients)}")
    try:
        async for _ in websocket:
            pass  # bu servis tek yönlü (server -> client) yayın yapıyor
    except ConnectionClosed:
        pass
    finally:
        connected_clients.discard(websocket)
        print(f"İstemci ayrıldı. Toplam: {len(connected_clients)}")


async def broadcast(message: str) -> None:
    if not connected_clients:
        return
    stale = []
    for client in list(connected_clients):
        try:
            await client.send(message)
        except ConnectionClosed:
            stale.append(client)
    for client in stale:
        connected_clients.discard(client)


async def kafka_reader_loop() -> None:
    consumer = make_consumer()
    consumer.subscribe([TOPIC])
    loop = asyncio.get_running_loop()
    print(f"'{TOPIC}' dinleniyor, WebSocket istemcilerine yayınlanacak.")
    try:
        while True:
            msg = await loop.run_in_executor(None, consumer.poll, 1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka hatası: {msg.error()}")
                continue
            await broadcast(msg.value().decode("utf-8"))
    finally:
        consumer.close()


async def main() -> None:
    async with serve(handle_client, WS_HOST, WS_PORT):
        print(f"WebSocket sunucusu ws://{WS_HOST}:{WS_PORT} adresinde çalışıyor.")
        await kafka_reader_loop()


if __name__ == "__main__":
    asyncio.run(main())
