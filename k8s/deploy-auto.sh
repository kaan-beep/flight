#!/bin/bash
set -e

NAMESPACE="flight-radar"

echo "🚀 Flight Radar - Kubernetes Deploy Otomatik"
echo "==============================================="

# 1. Namespace kontrol et
echo "📦 Namespace kontrol ediliyor..."
if ! kubectl get namespace $NAMESPACE >/dev/null 2>&1; then
    echo "   → $NAMESPACE oluşturuluyor..."
    kubectl create namespace $NAMESPACE
else
    echo "   ✅ $NAMESPACE zaten var"
fi

# 2. ConfigMap ve Secret kontrol et
echo "🔐 ConfigMap ve Secret kontrol ediliyor..."
kubectl apply -f namespace.yaml
kubectl apply -f configmap.yaml
kubectl apply -f secret.yaml

# 3. Stateful Services deploy et
echo "📊 Stateful Services deploy ediliyor..."
kubectl apply -f postgres.yaml
kubectl apply -f kafka.yaml

# 4. Backend ve Frontend deploy et
echo "🌐 Backend ve Frontend deploy ediliyor..."
kubectl apply -f backend.yaml
kubectl apply -f frontend.yaml

# 5. Rollout status kontrol et
echo "⏳ Deployment'ları kontrol ediliyor..."
echo "   → PostgreSQL başlanıyor..."
kubectl rollout status statefulset/postgres -n $NAMESPACE --timeout=5m || true

echo "   → Kafka başlanıyor..."
kubectl rollout status statefulset/kafka -n $NAMESPACE --timeout=5m || true

echo "   → Backend başlanıyor..."
kubectl rollout status deployment/backend -n $NAMESPACE --timeout=5m || true

echo "   → Frontend başlanıyor..."
kubectl rollout status deployment/frontend -n $NAMESPACE --timeout=5m || true

# 6. Service URL'leri göster
echo ""
echo "✨ Deployment Tamamlandı!"
echo "==============================================="
echo "📊 Pod Durumu:"
kubectl get pods -n $NAMESPACE

echo ""
echo "🔗 Service'ler:"
kubectl get svc -n $NAMESPACE

echo ""
echo "🌐 Frontend Erişimi:"
echo "   kubectl port-forward -n $NAMESPACE svc/frontend-service 8080:80"
echo "   Sonra: http://localhost:8080"

echo ""
echo "📊 Logs (Backend):"
echo "   kubectl logs -n $NAMESPACE -l app=backend -f"

echo ""
echo "✅ Ready!"
