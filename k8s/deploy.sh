#!/bin/bash

# Flight Radar Kubernetes Deployment Script

echo "🚀 Flight Radar Kubernetes Deploy Başlıyor..."

# 1. Namespace oluştur
echo "📦 Namespace oluşturuluyor..."
kubectl apply -f namespace.yaml

# 2. ConfigMap ve Secret oluştur
echo "⚙️ ConfigMap ve Secret yapılandırılıyor..."
kubectl apply -f configmap.yaml
kubectl apply -f secret.yaml

# 3. PostgreSQL deploy et
echo "🗄️ PostgreSQL deploy ediliyor..."
kubectl apply -f postgres.yaml

# PostgreSQL'ün hazır olmasını bekle
echo "⏳ PostgreSQL başlamasını bekleniyor..."
kubectl wait --for=condition=ready pod -l app=postgres -n flight-radar --timeout=300s

# 4. Zookeeper ve Kafka deploy et
echo "🔄 Zookeeper ve Kafka deploy ediliyor..."
kubectl apply -f kafka.yaml

# Kafka'nın hazır olmasını bekle
echo "⏳ Kafka başlamasını bekleniyor..."
kubectl wait --for=condition=ready pod -l app=kafka -n flight-radar --timeout=300s

# 5. Backend deploy et
echo "⚡ Backend deploy ediliyor..."
kubectl apply -f backend.yaml

# 6. Frontend deploy et
echo "🎨 Frontend deploy ediliyor..."
kubectl apply -f frontend.yaml

# 7. Status göster
echo ""
echo "✅ Deploy tamamlandı!"
echo ""
echo "📊 Pod durumu:"
kubectl get pods -n flight-radar

echo ""
echo "🔗 Service'ler:"
kubectl get svc -n flight-radar

echo ""
echo "💡 Frontend'e erişim:"
echo "kubectl port-forward -n flight-radar svc/frontend-service 8080:80"
