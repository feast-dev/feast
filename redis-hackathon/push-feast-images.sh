#!/bin/bash

# Push script for Feast Redis Vector images
set -e

# Configuration (should match build script)
export REGISTRY="your-registry.com"  # Change this to your container registry
export VERSION="redis-vector-$(date +%Y%m%d-%H%M%S)"

echo "🚀 Pushing Feast Redis Vector images to registry..."

# Login to registry (uncomment and modify as needed)
# docker login $REGISTRY

# Push all images
echo "📤 Pushing Feature Server..."
docker push ${REGISTRY}/feast-feature-server:${VERSION}
docker push ${REGISTRY}/feast-feature-server:latest

echo "📤 Pushing Transformation Server..."
docker push ${REGISTRY}/feast-transformation-server:${VERSION}
docker push ${REGISTRY}/feast-transformation-server:latest

echo "📤 Pushing Java Feature Server..."
docker push ${REGISTRY}/feast-feature-server-java:${VERSION}
docker push ${REGISTRY}/feast-feature-server-java:latest

echo "📤 Pushing Go Feature Server..."
docker push ${REGISTRY}/feast-feature-server-go:${VERSION}
docker push ${REGISTRY}/feast-feature-server-go:latest

echo "✅ All images pushed successfully!"
echo ""
echo "📋 Pushed images:"
echo "  - ${REGISTRY}/feast-feature-server:${VERSION}"
echo "  - ${REGISTRY}/feast-transformation-server:${VERSION}"
echo "  - ${REGISTRY}/feast-feature-server-java:${VERSION}"
echo "  - ${REGISTRY}/feast-feature-server-go:${VERSION}"
