#!/bin/bash

# Deploy Feast with Redis Vector Support to Kubernetes
set -e

echo "🚀 Deploying Feast with Redis Vector Support to Kubernetes"

# Check if kubectl is available
if ! command -v kubectl &> /dev/null; then
    echo "❌ kubectl is not installed or not in PATH"
    exit 1
fi

# Check if we can connect to Kubernetes cluster
if ! kubectl cluster-info &> /dev/null; then
    echo "❌ Cannot connect to Kubernetes cluster"
    exit 1
fi

echo "✅ Kubernetes cluster connection verified"

# Step 1: Create namespace and RBAC
echo "📝 Creating namespace and RBAC..."
kubectl apply -f k8s-manifests/namespace.yaml

# Step 2: Deploy Redis Stack with RediSearch
echo "🔴 Deploying Redis Stack with RediSearch..."
kubectl apply -f k8s-manifests/redis-stack.yaml

# Wait for Redis to be ready
echo "⏳ Waiting for Redis to be ready..."
kubectl wait --for=condition=available --timeout=300s deployment/redis-stack -n feast

# Step 3: Deploy Feast Feature Server
echo "🍽️ Deploying Feast Feature Server..."
kubectl apply -f k8s-manifests/feast-feature-server.yaml

# Wait for Feast to be ready
echo "⏳ Waiting for Feast Feature Server to be ready..."
kubectl wait --for=condition=available --timeout=300s deployment/feast-feature-server -n feast

echo "✅ Deployment completed successfully!"
echo ""
echo "📋 Deployment Status:"
kubectl get pods -n feast
echo ""
echo "🔗 Services:"
kubectl get services -n feast
echo ""
echo "🧪 Testing Redis Vector functionality:"
echo "  1. Port forward to Redis Insight: kubectl port-forward svc/redis-insight 8001:8001 -n feast"
echo "  2. Port forward to Feast: kubectl port-forward svc/feast-feature-server 6566:6566 -n feast"
echo "  3. Test vector operations with your demo application"
echo ""
echo "📊 Monitor logs:"
echo "  - Redis: kubectl logs -f deployment/redis-stack -n feast"
echo "  - Feast: kubectl logs -f deployment/feast-feature-server -n feast"
