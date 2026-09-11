#!/bin/bash

set -e

echo "🚀 Flight Radar Kubernetes Setup"
echo "================================"

# 1. Build Docker Images
echo ""
echo "1️⃣ Building Docker images..."
docker build -f Dockerfile.producer -t flight-radar-producer:latest .
docker build -f Dockerfile.consumer -t flight-radar-consumer:latest .
echo "✅ Docker images built"

# 2. Create Namespace
echo ""
echo "2️⃣ Creating Kubernetes namespace..."
kubectl create namespace flight-radar --dry-run=client -o yaml | kubectl apply -f -
echo "✅ Namespace created"

# 3. Deploy to Kubernetes
echo ""
echo "3️⃣ Deploying to Kubernetes..."
kubectl apply -f kubernetes.yaml
echo "✅ Deployed to Kubernetes"

# 4. Wait for resources
echo ""
echo "4️⃣ Waiting for resources to be ready..."
kubectl wait --for=condition=ready pod -l app=postgres -n flight-radar --timeout=300s || true
kubectl wait --for=condition=ready pod -l app=zookeeper -n flight-radar --timeout=60s || true
kubectl wait --for=condition=ready pod -l app=kafka -n flight-radar --timeout=120s || true
kubectl wait --for=condition=ready pod -l app=producer -n flight-radar --timeout=120s || true
kubectl wait --for=condition=ready pod -l app=consumer -n flight-radar --timeout=120s || true
echo "✅ Resources ready"

# 5. Get service info
echo ""
echo "5️⃣ Service Information:"
echo "========================"
echo ""
echo "PostgreSQL:"
kubectl get service postgres -n flight-radar
echo ""
echo "Kafka:"
kubectl get service kafka -n flight-radar
echo ""
echo "WebSocket (Consumer):"
kubectl get service consumer -n flight-radar
echo ""
echo "Pod Status:"
kubectl get pods -n flight-radar
echo ""
echo "✅ Setup Complete!"
echo ""
echo "Access WebSocket at: ws://localhost:30765 (or NODE_IP:30765)"
