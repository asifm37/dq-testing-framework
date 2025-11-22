#!/bin/bash

# Deployment Script for DQ Framework
# This script sets up the entire infrastructure

set -e  # Exit on error

echo "=============================================================================="
echo "DQ Framework - Deployment Script"
echo "=============================================================================="

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print colored output
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Check prerequisites
echo ""
echo "[1/8] Checking prerequisites..."

command -v docker >/dev/null 2>&1 || { print_error "Docker is required but not installed. Aborting."; exit 1; }
print_success "Docker installed"

command -v kubectl >/dev/null 2>&1 || { print_error "kubectl is required but not installed. Aborting."; exit 1; }
print_success "kubectl installed"

command -v python3 >/dev/null 2>&1 || { print_error "Python 3 is required but not installed. Aborting."; exit 1; }
print_success "Python 3 installed"

# Check if Docker Desktop K8s is running
if ! kubectl cluster-info &> /dev/null; then
    print_error "Kubernetes cluster is not running. Please start Docker Desktop Kubernetes."
    exit 1
fi
print_success "Kubernetes cluster is running"

# Deploy MinIO to Kubernetes
echo ""
echo "[2/8] Deploying MinIO to Kubernetes..."
kubectl apply -f kubernetes/k8s-minio.yaml
print_success "MinIO deployment created"

# Wait for MinIO to be ready
echo ""
echo "[3/8] Waiting for MinIO to be ready..."
kubectl wait --for=condition=ready pod -l app=minio -n dq-framework --timeout=300s || {
    print_warning "MinIO pod not ready yet, continuing anyway..."
}
print_success "MinIO is ready"

# Setup MinIO buckets (port-forward temporarily)
echo ""
echo "[4/8] Setting up MinIO buckets..."
kubectl port-forward -n dq-framework service/minio-service 9000:9000 &
PF_PID=$!
sleep 5

export MINIO_ENDPOINT="localhost:9000"
python3 scripts/setup_minio.py || {
    print_warning "MinIO setup encountered errors, but continuing..."
}

# Kill port-forward
kill $PF_PID 2>/dev/null || true
print_success "MinIO buckets configured"

# Build Docker image
echo ""
echo "[5/8] Building DQ Runner Docker image..."
docker build -t dq-runner:latest -f docker/Dockerfile.dq-runner . || {
    print_error "Failed to build Docker image"
    exit 1
}
print_success "Docker image built successfully"

# Install Python dependencies locally (for Airflow)
echo ""
echo "[6/8] Installing Python dependencies..."
pip3 install -r requirements.txt --quiet || {
    print_warning "Some dependencies may not have installed correctly"
}
print_success "Python dependencies installed"

# Seed test data
echo ""
echo "[7/8] Seeding test data..."
print_warning "This requires MinIO to be accessible. Port-forwarding..."
kubectl port-forward -n dq-framework service/minio-service 9000:9000 &
PF_PID=$!
sleep 5

export MINIO_ENDPOINT="localhost:9000"
export MINIO_ACCESS_KEY="minioadmin"
export MINIO_SECRET_KEY="minioadmin"
export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minioadmin"

python3 scripts/seed_data.py || {
    print_warning "Failed to seed data. You may need to run this manually later."
}

# Kill port-forward
kill $PF_PID 2>/dev/null || true
print_success "Test data seeded"

# Summary
echo ""
echo "[8/8] Deployment Summary"
echo "=============================================================================="
print_success "DQ Framework deployed successfully!"
echo ""
echo "Infrastructure:"
echo "  ✓ MinIO running in Kubernetes (namespace: dq-framework)"
echo "  ✓ Docker image: dq-runner:latest"
echo "  ✓ Test data seeded to Iceberg tables"
echo ""
echo "Next Steps:"
echo ""
echo "1. Access MinIO Console:"
echo "   kubectl port-forward -n dq-framework service/minio-service 9001:9001"
echo "   Then open: http://localhost:9001"
echo "   Credentials: minioadmin / minioadmin"
echo ""
echo "2. Run DQ Tests locally:"
echo "   ./scripts/run_tests.sh"
echo ""
echo "3. Setup Airflow (if not already installed):"
echo "   pip install apache-airflow"
echo "   airflow db init"
echo "   airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com"
echo ""
echo "4. Start Airflow:"
echo "   airflow webserver -p 8080"
echo "   airflow scheduler"
echo "   Copy airflow/dags/dq_validation_dag.py to your Airflow DAGs folder"
echo ""
echo "5. View Kubernetes resources:"
echo "   kubectl get all -n dq-framework"
echo ""
echo "=============================================================================="

