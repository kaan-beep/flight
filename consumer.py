"""
Flight Radar Consumer - Kafka'dan veri okuyor ve WebSocket üzerinden yayınlıyor.

Görev:
  1. Kafka topic'inden flight mesajlarını oku (consumer group offset'ten)
  2. Tüm bağlı WebSocket client'lara real-time yayınla
  3. Offset otomatik commit et (Docker volume'da persist)
"""

import asyncio
import json
import os
import sys

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed
from aiokafka import AIOKafkaConsumer

# Kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flights")

# WebSocket
WS_HOST = "0.0.0.0"
WS_PORT = 8765

# Bağlı WebSocket client'ları sakla
connected_clients: set = set()


async def connect_kafka_consumer_with_retry(topic, bootstrap_servers, delay_seconds=3.0):
    """
    Kafka Consumer'a bağlan - kaldığı yerden devam et.

    Offset Persistence:
    - Consumer group'lar offset'leri Kafka broker'ında tutarlar
    - Docker volume'lar (kafka_data) sayesinde kalıcı depolanır
    - Consumer restart → aynı group_id ile başarsa → offset restore olur

    Session management ayarları:
    - session_timeout_ms=30000: Consumer 30s cevap vermezse group'tan çıkar
    - heartbeat_interval_ms=10000: Her 10s'de heartbeat gönder
    - max_poll_interval_ms=300000: Mesaj işlerken max 5 dakika alabilir
    - auto_commit_interval_ms=5000: Her 5s'de offset'i otomatik kaydet
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                # ⭐ OFFSET STRATEGY - Kaldığı yerden devam et
                auto_offset_reset='earliest',            # Topic'e yeni gelişte: en başından oku
                group_id='flight-radar-consumer',        # Consumer group - offset state'i sakla
                enable_auto_commit=True,                 # Offset'i otomatik commit et
                auto_commit_interval_ms=5000,            # Her 5 saniye commit et
                # Session management - Network kesintisi toleransı
                session_timeout_ms=30000,                # 30s: Heartbeat timeout
                heartbeat_interval_ms=10000,             # 10s: Heartbeat frequency
                max_poll_interval_ms=300000,             # 5 min: Max processing time
            )
            await consumer.start()
            print(f"✅ Kafka Consumer başarıyla bağlandı (deneme {attempt})")
            print(f"   📍 Group ID: flight-radar-consumer (offset persist enabled)")
            return consumer
        except Exception as exc:
            print(f"⏳ Kafka Consumer bağlantısı başarısız (deneme {attempt}): {exc}", file=sys.stderr)
            await asyncio.sleep(delay_seconds)


async def handle_client(websocket) -> None:
    """Yeni WebSocket client'ını kayıt et ve bağlantısını yönet."""
    connected_clients.add(websocket)
    print(f"✅ WebSocket client bağlandı. Toplam: {len(connected_clients)}")

    try:
        # Client bağlı kaldığı sürece dinle (veri göndermesi beklenmez)
        async for _ in websocket:
            pass
    except ConnectionClosed:
        pass
    finally:
        connected_clients.discard(websocket)
        print(f"❌ WebSocket client ayrıldı. Toplam: {len(connected_clients)}")


async def kafka_consumer_task(consumer: AIOKafkaConsumer) -> None:
    """
    Kafka'dan flight mesajlarını oku ve tüm WebSocket istemcilerine yayınla.

    Offset Strategy: Auto-commit (5s interval) - Docker volume'da persist edilir

    Akış:
    1. Kafka'dan bir flight mesajı oku (consumer group offset'ten)
    2. JSON decode et
    3. Tüm bağlı WebSocket istemcilerine gönder
    4. Offset otomatik commit edilir (Docker volume'da saklanır)
    5. Kapanan bağlantıları temizle

    Recovery:
    - Consumer crash → Restart → Aynı group_id → Offset restore
    - Docker container kapandı → Volume persist ediyor → Data korunur
    - Kafka mesajı 7 gün saklanır (KAFKA_LOG_RETENTION_HOURS=168)
    """
    try:
        # Kafka topic'in mesajlarını stream olarak oku (consumer group offset'ten)
        async for message in consumer:
            try:
                # Kafka mesajını JSON flight object'e dönüştür
                flight = json.loads(message.value.decode("utf-8"))

                # Eğer hiç client bağlı değilse veriyi yolla (sonra temizle)
                if not connected_clients:
                    continue

                # Bağlı tüm WebSocket client'lara mesaj gönder
                stale = []  # Kapanan bağlantılar
                for client in list(connected_clients):
                    try:
                        # Flight JSON'ını client'a gönder
                        await client.send(json.dumps(flight))
                    except ConnectionClosed:
                        # Bu client kapandı - sonra kaldıracağız
                        stale.append(client)

                # Kapanan bağlantıları set'ten çıkar
                for client in stale:
                    connected_clients.discard(client)

                # ⭐ Offset otomatik commit edilir (enable_auto_commit=True + 5s interval)
                # Mesaj başarıyla işlendikten sonra Kafka offset'i güncellenir

            except json.JSONDecodeError as exc:
                # JSON parsing hatası - skip et, sonraki mesaja geç
                print(f"⚠️  JSON parsing hatası: {exc}", file=sys.stderr)
            except Exception as exc:
                # Diğer hata'lar - loglama
                print(f"❌ Kafka mesajı işlenirken hata: {exc}", file=sys.stderr)

    except Exception as exc:
        # Consumer bağlantısı koptu - kritik hata
        print(f"❌ Kafka Consumer bağlantı hatası: {exc}", file=sys.stderr)
        print("⚠️  Offset kaydedilmiştir (restart'ta kaldığı yerden devam edilecek)", file=sys.stderr)


async def main():
    """Consumer başlat."""
    # Kafka Consumer'a bağlan
    consumer = await connect_kafka_consumer_with_retry(KAFKA_TOPIC, KAFKA_BROKER)

    # WebSocket sunucusu başlat
    async with serve(handle_client, WS_HOST, WS_PORT):
        print(f"\n🚀 Flight Radar Consumer başladı!")
        print(f"   📡 WebSocket: ws://{WS_HOST}:{WS_PORT}")
        print(f"   🔌 Kafka: {KAFKA_BROKER}\n")

        try:
            # Kafka consumer task'ını çalıştır
            await kafka_consumer_task(consumer)
        finally:
            print("\n🛑 Kapatılıyor...")
            await consumer.stop()
            print("✅ Consumer kapatıldı")


if __name__ == "__main__":
    asyncio.run(main())
