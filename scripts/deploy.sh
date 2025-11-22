#!/bin/bash
set -e

echo "=== DQ Framework Deployment (Iceberg + Unstructured) ==="
echo ""

# Check prerequisites
echo "[1/4] Checking prerequisites..."
command -v docker >/dev/null 2>&1 || { echo "Error: Docker not found"; exit 1; }
echo "✓ Docker OK"

# Build Docker image
echo ""
echo "[2/4] Building Docker image..."
docker build -t dq-runner:latest -f docker/Dockerfile.dq-runner . -q
echo "✓ Docker image built"

# Create directories
echo ""
echo "[3/4] Creating directories..."
mkdir -p warehouse warehouse/unstructured reports/allure-results reports/allure-report
echo "✓ Directories ready"

# Seed data (Iceberg tables + JSON logs)
echo ""
echo "[4/4] Seeding data (Iceberg tables + JSON logs)..."
docker run --rm \
  -v $(pwd)/warehouse:/app/warehouse \
  dq-runner:latest \
  python3 /app/scripts/seed_data_iceberg.py --mode initial

echo "✓ Data seeded"

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "What was created:"
echo "  ✓ Iceberg tables: local.datalake_bronze.transactions, user_profiles"
echo "  ✓ JSON logs: warehouse/unstructured/event_logs/"
echo ""
echo "Next steps:"
echo "  1. Run tests: ./scripts/run_tests.sh"
echo "  2. View report: open reports/allure-report/index.html"
echo "  3. (Optional) Deploy Airflow DAG: cp airflow/dags/dq_validation_dag.py ~/airflow/dags/"
echo ""
