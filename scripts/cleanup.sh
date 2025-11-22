#!/bin/bash

# Cleanup Script for DQ Framework
# Removes all deployed resources

set -e

echo "=============================================================================="
echo "DQ Framework - Cleanup Script"
echo "=============================================================================="

echo ""
echo "This will remove:"
echo "  - Kubernetes resources (namespace: dq-framework)"
echo "  - Docker images (dq-runner:latest)"
echo "  - Local reports and warehouse data"
echo ""
read -p "Are you sure? (yes/no): " confirmation

if [ "$confirmation" != "yes" ]; then
    echo "Cleanup cancelled."
    exit 0
fi

# Delete Kubernetes resources
echo ""
echo "[1/4] Deleting Kubernetes resources..."
kubectl delete namespace dq-framework --ignore-not-found=true
echo "  ✓ Kubernetes resources deleted"

# Remove Docker image
echo ""
echo "[2/4] Removing Docker image..."
docker rmi dq-runner:latest --force 2>/dev/null || echo "  - Image not found"
echo "  ✓ Docker image removed"

# Clean local data
echo ""
echo "[3/4] Cleaning local data..."
rm -rf reports/allure-results/*
rm -rf reports/allure-report/*
rm -rf warehouse/*
echo "  ✓ Local data cleaned"

# Summary
echo ""
echo "[4/4] Cleanup complete!"
echo "=============================================================================="
echo "All DQ Framework resources have been removed."
echo ""
echo "To redeploy, run: ./scripts/deploy.sh"
echo "=============================================================================="

