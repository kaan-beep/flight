#!/bin/bash
set -e

echo "🚀 Flight Radar - Docker Başlatıyor..."

# Önceki containerları temizle
echo "📦 Eski containerları temizliyorum..."
docker-compose down 2>/dev/null || true

# Build et
echo "🔨 Docker image'larını oluşturuyorum..."
docker-compose build --no-cache

# Başlat
echo "🚀 Containerları başlatıyorum..."
docker-compose up -d

# Health check
echo "⏳ Servislerin başlamasını bekliyorum..."
max_attempts=30
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if docker-compose exec -T postgres pg_isready -U postgres >/dev/null 2>&1 && \
       docker-compose exec -T zookeeper echo ruok | nc localhost 2181 >/dev/null 2>&1; then
        echo "✅ Tüm servisler hazır!"
        break
    fi
    attempt=$((attempt + 1))
    sleep 2
done

# Status
echo ""
echo "📊 Konteyner Durumu:"
docker-compose ps

echo ""
echo "✨ Flight Radar Başlatıldı!"
echo "🌐 Frontend:  http://localhost:8080"
echo "📡 Backend:   ws://localhost:8765"
echo "🔌 Postgres:  localhost:5432"
echo "📨 Kafka:     localhost:9092"
