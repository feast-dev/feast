#!/bin/bash

# Build script for Feast with Redis Vector Support
# This script builds custom Docker images with your Redis vector database implementation

set -e

# Configuration
export REGISTRY="your-registry.com"  # Change this to your container registry
export VERSION="redis-vector-$(date +%Y%m%d-%H%M%S)"
export FEAST_VERSION="redis-vector-dev"

echo "🚀 Building Feast with Redis Vector Support"
echo "Registry: $REGISTRY"
echo "Version: $VERSION"

# Step 1: Install dependencies and build Python package
echo "📦 Installing dependencies..."
source venv/bin/activate
pip install -e "sdk/python[redis]"

# Step 2: Compile protobuf files
echo "🔧 Compiling protobuf files..."
make compile-protos-python

# Step 3: Build Feature Server Docker image
echo "🐳 Building Feature Server Docker image..."
docker buildx build \
    -t ${REGISTRY}/feast-feature-server:${VERSION} \
    -t ${REGISTRY}/feast-feature-server:latest \
    -f sdk/python/feast/infra/feature_servers/multicloud/Dockerfile \
    --load sdk/python/feast/infra/feature_servers/multicloud

# Step 4: Build Feature Transformation Server Docker image
echo "🐳 Building Feature Transformation Server Docker image..."
docker buildx build --build-arg VERSION=${FEAST_VERSION} \
    -t ${REGISTRY}/feast-transformation-server:${VERSION} \
    -t ${REGISTRY}/feast-transformation-server:latest \
    -f sdk/python/feast/infra/transformation_servers/Dockerfile --load .

# Step 5: Build Java Feature Server (optional)
echo "🐳 Building Java Feature Server Docker image..."
make build-java-no-tests REVISION=${FEAST_VERSION}
docker buildx build --build-arg VERSION=${FEAST_VERSION} \
    -t ${REGISTRY}/feast-feature-server-java:${VERSION} \
    -t ${REGISTRY}/feast-feature-server-java:latest \
    -f java/infra/docker/feature-server/Dockerfile --load .

# Step 6: Build Go Feature Server (optional)
echo "🐳 Building Go Feature Server Docker image..."
docker buildx build --build-arg VERSION=${FEAST_VERSION} \
    -t ${REGISTRY}/feast-feature-server-go:${VERSION} \
    -t ${REGISTRY}/feast-feature-server-go:latest \
    -f go/infra/docker/feature-server/Dockerfile --load .

echo "✅ Build completed successfully!"
echo ""
echo "📋 Built images:"
echo "  - ${REGISTRY}/feast-feature-server:${VERSION}"
echo "  - ${REGISTRY}/feast-transformation-server:${VERSION}"
echo "  - ${REGISTRY}/feast-feature-server-java:${VERSION}"
echo "  - ${REGISTRY}/feast-feature-server-go:${VERSION}"
echo ""
echo "🚀 Next steps:"
echo "  1. Push images: ./push-feast-images.sh"
echo "  2. Deploy to Kubernetes: kubectl apply -f k8s-manifests/"
echo "  3. Test Redis vector functionality"
