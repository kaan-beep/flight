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
# Ayarlar (Environment + Constants)
# ---------------------------------------------------------------------------

# OpenSky Network API endpoint - 4000-5000 uçak bilgisi her çağrıda
OPENSKY_URL = "https://opensky-network.org/api/states/all"

# API polling aralığı (saniye) - Rate limit yüzünden 25s minimum
POLL_INTERVAL_SECONDS = 25

# Rate limit (429) hatası aldığında bekleme süresi
RATE_LIMIT_BACKOFF_SECONDS = 120

# WebSocket sunucusu - Frontend bağlantısı
WS_HOST = "0.0.0.0"  # Tüm arayüzler üzerinde dinle
WS_PORT = 8765       # ws://localhost:8765

# Kafka konfigürasyonu - Docker Compose'da "app" servisi Kafka'ya bağlanır
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")  # bootstrap.servers
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flights")            # Mesaj topic'i

# Coğrafi sınır - Sadece Türkiye + komşu ülkeler
# (lamin=25.0=min latitude, lomin=-12.0=min longitude, lamax=60.0=max lat, lomax=55.0=max lon)
REGION_BBOX = (25.0, -12.0, 60.0, 55.0)

# PostgreSQL Tablo Şeması - Tüm uçuş verilerini saklar
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS flight_states (
    -- Benzersiz kayıt ID'si
    id              BIGSERIAL PRIMARY KEY,

    -- ICAO 24-bit uçak tanımlayıcısı (ör: a00001)
    icao24          VARCHAR(10) NOT NULL,

    -- Çağrı işareti - Uçak registration (ör: PGT, THY123)
    callsign        VARCHAR(20),

    -- Uçağın tescil edildiği ülke
    origin_country  VARCHAR(100),

    -- GPS koordinatları (dereceler)
    longitude       DOUBLE PRECISION,
    latitude        DOUBLE PRECISION,

    -- Barometrik irtifa (metre)
    altitude_m      DOUBLE PRECISION,

    -- Yerde mi uçuşta mı
    on_ground       BOOLEAN,

    -- Hız (metre/saniye)
    velocity_ms     DOUBLE PRECISION,

    -- Başlık yönü (derece, 0-360)
    heading_deg     DOUBLE PRECISION,

    -- Dikey hız (metre/saniye, += tırmanış, -= iniş)
    vertical_rate_ms DOUBLE PRECISION,

    -- Kayıt zamanı (otomatik şu anki zaman)
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- İndeks: ICAO24 ile hızlı arama (her 25 saniye aynı uçağı güncellerken)
CREATE INDEX IF NOT EXISTS idx_flight_states_icao24 ON flight_states (icao24);

-- İndeks: Tarih aralığında arama (geçmiş veriler sorgulamak için)
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
    """
    OpenSky Network API'den canlı uçuş verilerini çek.

    Args:
        bbox: (min_lat, min_lon, max_lat, max_lon) - coğrafi sınırlar
              None ise tüm dünya

    Returns:
        Uçak objeleri listesi [{"icao24": ..., "callsign": ..., ...}, ...]

    Raises:
        requests.HTTPError: API hatası (401=auth, 429=rate limit, 503=server down)
    """
    params = {}

    # Bounding box filtresi - sadece Türkiye + komşu bölge
    if bbox:
        lamin, lomin, lamax, lomax = bbox
        params.update(lamin=lamin, lomin=lomin, lamax=lamax, lomax=lomax)

    # Kimlik doğrulama - Premium özellikler ve yüksek rate limit için
    auth = None
    username = os.getenv("OPENSKY_USERNAME")
    password = os.getenv("OPENSKY_PASSWORD")
    if username and password:
        auth = (username, password)  # HTTP Basic Auth

    # HTTP isteği - timeout 15s çünkü 4000+ uçak büyük yanıt olabilir
    response = requests.get(OPENSKY_URL, params=params, auth=auth, timeout=15)
    response.raise_for_status()  # 4xx/5xx hataları exception fırlat
    payload = response.json()

    # JSON içinden "states" dizisini çıkar (4000-5000 uçak)
    states = payload.get("states") or []

    # Her raw state'i okunabilir dict'e dönüştür
    return [parse_state(state) for state in states]


def parse_state(state: list) -> dict:
    """
    OpenSky API'nin "state vector" liste formatını JSON dict'e dönüştür.

    OpenSky API state array index'leri:
    [0]=icao24, [1]=callsign, [2]=origin_country, [3]=time_position,
    [4]=last_contact, [5]=longitude, [6]=latitude, [7]=baro_altitude,
    [8]=on_ground, [9]=velocity, [10]=true_track, [11]=vertical_rate,
    [12]=sensors, [13]=geo_altitude, [14]=squawk, [15]=spi,
    [16]=position_source, [17]=category

    Çıktı: Frontend'in anlayacağı JSON formatında uçak verisi
    """
    return {
        "icao24": state[0],                              # Uçak kimliği
        "callsign": (state[1] or "").strip(),            # Uçuş numarası
        "origin_country": state[2],                      # Ülke
        "longitude": state[5],                           # X koordinat
        "latitude": state[6],                            # Y koordinat
        "altitude_m": state[7],                          # Yükseklik
        "on_ground": state[8],                           # Yerde mi
        "velocity_ms": state[9],                         # Hız
        "heading_deg": state[10],                        # Yön (0-360)
        "vertical_rate_ms": state[11],                   # Dikey hız (± hızı)
        "squawk": state[14] if len(state) > 14 else None,  # Acil kodu
        "category": state[17] if len(state) > 17 else 0,   # Uçak tipi
        "timestamp": int(time.time()),                   # Sunucu zamanı
    }


# ---------------------------------------------------------------------------
# 2) PostgreSQL'e kaydetme
# ---------------------------------------------------------------------------

def get_connection():
    """PostgreSQL'e bağlan - docker-compose env vars'dan config oku."""
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),      # Docker: "postgres" container name
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DB", "flightradar"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "postgres"),
    )


def init_db(conn) -> None:
    """Tablo ve index'leri oluştur (ilk çalışmada)."""
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    print("✅ PostgreSQL schema hazır")


def insert_flights(conn, flights: list[dict]) -> int:
    """
    Batch insert - 4000+ uçak kaydını bir SQL sorgusuyla yaz.

    Avantaj: her uçak için ayrı INSERT yapmak yerine tek sorguyla hepsi
    Performance: 4000 flights ≈ 100ms vs 4000*INSERT ≈ 10s

    Args:
        conn: PostgreSQL bağlantısı
        flights: [{"icao24": ..., "callsign": ..., ...}, ...] list

    Returns:
        Kaç satır eklendi
    """
    if not flights:
        return 0

    # List of tuples format'ında hazırla - execute_values bu format'ı bekliyor
    rows = [
        (
            f["icao24"], f["callsign"], f["origin_country"], f["longitude"],
            f["latitude"], f["altitude_m"], f["on_ground"], f["velocity_ms"],
            f["heading_deg"], f["vertical_rate_ms"],
        )
        for f in flights
    ]

    # execute_values: Tek SQL statement'te tüm rows'ları INSERT et
    # Örnek: INSERT INTO ... VALUES (row1), (row2), (row3), ...
    with conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, rows)
    conn.commit()
    return len(rows)


def connect_with_retry(delay_seconds: float = 3.0):
    """
    PostgreSQL'e bağlan - başarılı olana kadar retry et.

    Docker başladığında Postgres hemen hazır olmayabilir (5-10 saniye sürebilir).
    Bu fonksiyon backend'in PostgreSQL'e bağlanmasını garantiler.

    Args:
        delay_seconds: Retry aralığı (saniye)

    Returns:
        Bağlı PostgreSQL connection
    """
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


async def connect_kafka_producer_with_retry(bootstrap_servers: str, delay_seconds: float = 3.0):
    """
    Kafka Producer'a bağlan - başarılı olana kadar retry et.

    Docker başlanırken Kafka hemen hazır olmayabilir.
    Bu fonksiyon Broker hazır olana kadar bekler.

    Args:
        bootstrap_servers: "kafka:9092" (Docker) ya da "localhost:9092" (local)
        delay_seconds: Retry aralığı (3 saniye)

    Returns:
        Bağlı Kafka producer instance
    """
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


async def connect_kafka_consumer_with_retry(
    topic: str, bootstrap_servers: str, delay_seconds: float = 3.0
):
    """
    Kafka Consumer'a bağlan - başarılı olana kadar retry et.

    Consumer group'lar topic'i offset'ten okurlar. Offset'ler Kafka'nın
    __consumer_offsets topic'inde saklanır, böylece consumer restart olsa bile
    kaldığı yerden devam edebilir.

    Offset Persistence:
    - Consumer group'lar offset'leri Kafka broker'ında tutarlar
    - Docker volume'lar (kafka_data) sayesinde kalıcı depolanır
    - Consumer restart → aynı group_id ile başarsa → offset restore olur

    Session management ayarları:
    - session_timeout_ms=30000: Consumer 30s cevap vermezse group'tan çıkar
    - heartbeat_interval_ms=10000: Her 10s'de heartbeat gönder (alive signal)
    - max_poll_interval_ms=300000: Mesaj işlerken max 5 dakika alabilir
    - auto_commit_interval_ms=5000: Her 5s'de offset'i otomatik kaydet

    Args:
        topic: "flights" (Kafka topic name)
        bootstrap_servers: "kafka:9092"
        delay_seconds: Retry aralığı

    Returns:
        Bağlı Kafka consumer instance
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


# ---------------------------------------------------------------------------
# 3) Kafka ile mesaj hareketi
# ---------------------------------------------------------------------------

async def produce_to_kafka(producer: AIOKafkaProducer, flights: list[dict]) -> None:
    """
    Tüm uçak verilerini Kafka topic'ine gönder.

    Her uçak = 1 Kafka mesaj
    Format: JSON string, UTF-8 encoded

    Kafka broker tüm mesajları "flights" topic'ine yazar,
    consumer group'lar onları okuyarak Frontend'e yayınlar.

    Args:
        producer: Async Kafka producer instance
        flights: [{"icao24": ..., "callsign": ..., ...}, ...]

    Note:
        send_and_wait: Kafka broker'a yazıldığını bekle (ack=all)
        Async loop'ta güvenli çalışır
    """
    for flight in flights:
        # JSON serialize + UTF-8 byte'a dönüştür
        message = json.dumps(flight).encode("utf-8")

        # Kafka'ya gönder - Broker cevap verene kadar bekle
        await producer.send_and_wait(
            KAFKA_TOPIC,          # Topic name: "flights"
            message                # Byte content: b'{"icao24":"a00001",...}'
        )


# ---------------------------------------------------------------------------
# 4) WebSocket istemci yönetimi
# ---------------------------------------------------------------------------

async def handle_client(websocket) -> None:
    """
    Yeni WebSocket istemcisini kayıt et.

    Frontend'in ws://localhost:8765'e bağlanması bu handler'ı çağırır.
    Istemci bağlı kaldığı sürece mesajları consume_from_kafka'dan alır.

    Args:
        websocket: WebSocket connection object (asyncio.Protocol)
    """
    # Global set'e ekle - mesaj gönderirken tüm clients'a ulaşmak için
    connected_clients.add(websocket)
    print(f"✅ Yeni WebSocket client bağlandı. Toplam: {len(connected_clients)}")

    try:
        # Istemci bağlı kaldığı sürece mesajları yok sayıp dinle
        # (Frontend veri göndermez, sadece alır - bu loop bağlantıyı açık tutar)
        async for _ in websocket:
            pass
    except ConnectionClosed:
        # Normal kapanış
        pass
    finally:
        # Client kapandı - set'ten çıkar
        connected_clients.discard(websocket)
        print(f"❌ WebSocket client ayrıldı. Toplam: {len(connected_clients)}")


# ---------------------------------------------------------------------------
# 5) Ana döngüler
# ---------------------------------------------------------------------------

async def fetch_loop(loop: asyncio.AbstractEventLoop, conn, producer: AIOKafkaProducer) -> None:
    """
    Ana uçuş veri çekme ve işleme döngüsü.

    Her 25 saniye:
    1. OpenSky API'den ~4200 uçak bilgisi çek
    2. PostgreSQL'e kaydedebilmek için parse et
    3. Kafka producer'a gönder (Frontend'e yayınlanması için)
    4. Hata varsa (rate limit, network) retry et

    Rate limiting: OpenSky free tier = 1 request/10 saniye
                  Premium tier = 4 requests/10 saniye
    Biz 25 saniye bekliyoruz = güvenli zona

    Args:
        loop: asyncio event loop - sync kod çalıştırmak için
        conn: PostgreSQL connection (blocking I/O)
        producer: Kafka producer (async)
    """
    while True:
        wait_seconds = POLL_INTERVAL_SECONDS  # Normal: 25 saniye

        try:
            # 1️⃣ OpenSky API'den veri çek (blocking call - executor ile async yapıyoruz)
            flights = await loop.run_in_executor(None, fetch_states, REGION_BBOX)

            # 2️⃣ PostgreSQL'e insert et (blocking - executor ile)
            await loop.run_in_executor(None, insert_flights, conn, flights)

            # 3️⃣ Kafka'ya produce et (async - doğrudan await)
            await produce_to_kafka(producer, flights)

            # Log - Kaç uçak işlendi
            print(f"[{time.strftime('%H:%M:%S')}] ✅ {len(flights)} uçuş işlendi → Kafka'ya yazıldı")

        except requests.exceptions.HTTPError as exc:
            # HTTP hataları - Rate limit, auth, server error
            status = exc.response.status_code if exc.response is not None else None

            if status == 429:
                # Rate limit hatası - 2 dakika bekle
                wait_seconds = RATE_LIMIT_BACKOFF_SECONDS
                print(f"[{time.strftime('%H:%M:%S')}] ⚠️  OpenSky rate limit (429) - {wait_seconds}s bekleniyor", file=sys.stderr)
            else:
                # Diğer HTTP hataları - şu sefer atla, sonra tekrar dene
                print(f"[{time.strftime('%H:%M:%S')}] ❌ API Hatası ({status}): {exc}", file=sys.stderr)

        except Exception as exc:
            # Network timeout, JSON parse error, vs
            print(f"[{time.strftime('%H:%M:%S')}] ❌ Beklenmeyen hata: {exc}", file=sys.stderr)

        # Bekleme - sonra döngü baştan başlasın
        await asyncio.sleep(wait_seconds)


async def kafka_consumer_task(consumer: AIOKafkaConsumer) -> None:
    """
    Kafka'dan flight mesajlarını oku ve tüm WebSocket istemcilerine yayınla.

    Kafka Consumer group: "flight-radar-consumer"
    Topic: "flights"
    Offset Strategy: Auto-commit (5s interval) - Kalıcı disk'e kaydedilir

    Akış:
    1. Kafka'dan bir flight mesajı oku (consumer group offset'ten)
    2. JSON decode et
    3. Tüm bağlı WebSocket istemcilerine gönder
    4. Offset otomatik commit edilir (Docker volume'da saklanır)
    5. Kapanan bağlantıları temizle

    Recovery:
    - Consumer crash → Restart → Aynı group_id → Offset restore
    - Docker container kapandı → Volume persist ediyor → Data korunu
    - Kafka mesajı 7 gün saklanır (KAFKA_LOG_RETENTION_HOURS=168)

    Args:
        consumer: AIOKafkaConsumer instance
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


# ---------------------------------------------------------------------------
# 6) Başlangıç
# ---------------------------------------------------------------------------

async def main() -> None:
    """
    Ana başlangıç fonksiyonu - tüm bileşenleri orchestrate et.

    Başlangıç sırası:
    1. PostgreSQL'e bağlan
    2. Tablo şemasını oluştur
    3. Kafka Producer'a bağlan
    4. Kafka Consumer'a bağlan
    5. WebSocket sunucusu başlat
    6. fetch_loop ve kafka_consumer_task'ı paralel çalıştır

    Architecture:
    ┌─────────────────────────────────────────────────────────┐
    │         WebSocket Server (handle_client)               │
    │         ↑                                                │
    │         └─── Connected Clients (Frontend)              │
    │                                                          │
    │ kafka_consumer_task()                                   │
    │ ├─ Kafka Consumer ← Topic "flights"                    │
    │ └─ Send to WebSocket clients                           │
    │                                                          │
    │ fetch_loop()                                            │
    │ ├─ OpenSky API → flights list                          │
    │ ├─ PostgreSQL INSERT                                   │
    │ └─ Kafka Producer → Topic "flights"                    │
    └─────────────────────────────────────────────────────────┘
    """

    # 1️⃣ PostgreSQL'e bağlan (retry logic ile)
    conn = connect_with_retry()
    init_db(conn)

    # 2️⃣ Kafka Producer'a bağlan (retry logic ile)
    producer = await connect_kafka_producer_with_retry(KAFKA_BROKER)

    # 3️⃣ Kafka Consumer'a bağlan (retry logic ile)
    consumer = await connect_kafka_consumer_with_retry(KAFKA_TOPIC, KAFKA_BROKER)

    # 4️⃣ Event loop referansı al (executor için)
    loop = asyncio.get_running_loop()

    # 5️⃣ WebSocket sunucusu başlat
    async with serve(handle_client, WS_HOST, WS_PORT):
        print(f"\n🚀 Flight Radar Backend başladı!")
        print(f"   📡 WebSocket: ws://{WS_HOST}:{WS_PORT}")
        print(f"   🔌 Kafka: {KAFKA_BROKER}")
        print(f"   💾 PostgreSQL: connected\n")

        # 6️⃣ İki ana görev paralel olarak çalışsın
        tasks = [
            # Görev 1: OpenSky API'den data çek → DB → Kafka
            fetch_loop(loop, conn, producer),

            # Görev 2: Kafka'dan oku → WebSocket clients'a gönder
            kafka_consumer_task(consumer),
        ]

        try:
            # asyncio.gather: Her iki task'ı paralel çalıştır
            # Birisi fail olursa, error propagate olur
            await asyncio.gather(*tasks)
        finally:
            # Shutdown - bağlantıları kapat
            print("\n🛑 Kapatılıyor...")
            await producer.stop()
            await consumer.stop()
            print("✅ Tüm kaynaklar temizlendi")


if __name__ == "__main__":
    """
    Entry point - Python interpreter bunu çalıştırırken girer.

    asyncio.run(main()):
    - Event loop oluştur
    - main() coroutine'i çalıştır
    - Sonuna kadar bekle (infinite gather loop)
    - Hata veya KeyboardInterrupt'ta cleanup et

    Başlatma:
        python app.py
        # veya Docker'da:
        docker-compose up
    """
    asyncio.run(main())
