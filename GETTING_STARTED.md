# Getting Started Guide

This guide walks you through deploying and running the DQ Framework from scratch.

## Step 1: Prerequisites Check

### Hardware Requirements
- Apple M1/M2/M3 Mac (or Intel Mac with 16GB+ RAM)
- At least 20GB free disk space

### Software Requirements

```bash
# Check Docker
docker --version
# Expected: Docker version 24.x or higher

# Check Kubernetes (Docker Desktop)
kubectl version --short
# Expected: Client and Server versions displayed

# Check Python
python3 --version
# Expected: Python 3.11 or higher

# Check pip
pip3 --version
```

### Enable Kubernetes in Docker Desktop

1. Open Docker Desktop
2. Go to Settings → Kubernetes
3. Check "Enable Kubernetes"
4. Click "Apply & Restart"
5. Wait for Kubernetes to start (green indicator)

## Step 2: Clone/Setup Project

```bash
# Navigate to home directory
cd ~

# If you received the project as a zip, extract it
# Otherwise, if it's already in ~/dq-testing-framework, navigate there
cd dq-testing-framework

# Verify structure
ls -la
# Should see: airflow/, docker/, dq_framework/, tests/, scripts/, etc.
```

## Step 3: Deploy Infrastructure

```bash
# Run the automated deployment script
./scripts/deploy.sh
```

This script will:
1. ✓ Check prerequisites (Docker, kubectl, Python)
2. ✓ Deploy MinIO to Kubernetes
3. ✓ Wait for MinIO to be ready
4. ✓ Create MinIO buckets (datalake, logs, metadata)
5. ✓ Build Docker image (dq-runner:latest)
6. ✓ Install Python dependencies
7. ✓ Seed test data (transactions, user_profiles)

**Expected Duration**: 5-10 minutes

**Troubleshooting**:
- If MinIO fails to start, check: `kubectl get pods -n dq-framework`
- If Docker build fails, ensure you have internet connection for downloading packages

## Step 4: Verify Deployment

```bash
# Check Kubernetes resources
kubectl get all -n dq-framework

# Expected output:
# NAME                         READY   STATUS    RESTARTS   AGE
# pod/minio-xxxxxxxxx-xxxxx   1/1     Running   0          2m
# 
# NAME                    TYPE       CLUSTER-IP      EXTERNAL-IP   PORT(S)
# service/minio-service   NodePort   10.96.xxx.xxx   <none>        9000:30900/TCP,9001:30901/TCP

# Check Docker image
docker images | grep dq-runner

# Expected output:
# dq-runner   latest   xxxxxxxxx   X minutes ago   X.XXG
```

## Step 5: Access MinIO Console (Optional)

```bash
# Port-forward MinIO console
kubectl port-forward -n dq-framework service/minio-service 9001:9001

# Open browser: http://localhost:9001
# Login: minioadmin / minioadmin

# You should see:
# - Bucket: datalake (with folders: warehouse/, raw/, bronze/, etc.)
# - Bucket: logs
# - Bucket: metadata
```

Press `Ctrl+C` to stop port-forwarding when done.

## Step 6: Run DQ Tests Locally

```bash
# Run the test suite
./scripts/run_tests.sh
```

**What happens**:
1. Tests run in sequence (metadata → data quality)
2. Circuit breaker activates if metadata tests fail
3. Results are saved to `reports/allure-results/`
4. HTML report is generated in `reports/allure-report/`

**Expected Output**:
```
==============================================================================
DQ Framework - Test Runner
==============================================================================
...
[2/4] Running DQ tests...
tests/test_metadata.py::test_table_exists[transactions] PASSED
tests/test_metadata.py::test_table_schema[transactions] PASSED
tests/test_data_quality.py::test_not_null[transactions] PASSED
...
```

## Step 7: View Allure Report

```bash
# Option 1: Live server (recommended)
allure serve reports/allure-results

# Option 2: Open static HTML
open reports/allure-report/index.html  # macOS
# or
xdg-open reports/allure-report/index.html  # Linux
```

**What to look for**:
- 📊 Overall pass/fail statistics
- 🔍 Individual test details
- 🚨 Circuit breaker alerts (if any)
- 📎 Validation details (JSON attachments)

## Step 8: Setup Airflow (Optional)

If you want to run tests on a schedule via Airflow:

```bash
# Install Airflow
pip install apache-airflow==2.8.0 apache-airflow-providers-cncf-kubernetes==7.9.0

# Initialize Airflow database
airflow db init

# Create admin user
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com

# Copy DAG to Airflow DAGs folder
mkdir -p ~/airflow/dags
cp airflow/dags/dq_validation_dag.py ~/airflow/dags/

# Start Airflow webserver (Terminal 1)
airflow webserver -p 8080

# Start Airflow scheduler (Terminal 2 - open new terminal)
cd ~/dq-testing-framework
airflow scheduler
```

**Access Airflow UI**:
1. Open: http://localhost:8080
2. Login: `admin` / `admin`
3. Find DAG: `dq_validation_hourly`
4. Toggle to enable
5. Click "Trigger DAG" to run manually

## Step 9: Customize for Your Data

### Add a New Table

Edit `dq_framework/config/schema_registry.json`:

```json
{
  "tables": {
    "my_new_table": {
      "type": "structured",
      "format": "iceberg",
      "namespace": "datalake.bronze",
      "incremental": true,
      "timestamp_column": "created_at",
      "schema": {
        "id": "string",
        "name": "string",
        "value": "decimal(10,2)",
        "created_at": "timestamp"
      },
      "validations": {
        "schema_check": {
          "enabled": true,
          "strict_mode": true
        },
        "not_null": {
          "columns": ["id", "name"]
        },
        "range": {
          "value": {
            "min": 0,
            "max": 10000
          }
        }
      },
      "quality_threshold": 0.10,
      "circuit_breaker": {
        "metadata_stop_on_failure": true,
        "data_stop_on_threshold": true
      }
    }
  }
}
```

### Create Test Data

```python
# Add to scripts/seed_data.py or create a new script
from dq_framework.utils import IcebergManager, get_spark_session

spark = get_spark_session()
iceberg_mgr = IcebergManager(spark)

# Create your data
data = [
    Row(id="1", name="Test", value=Decimal("100.00"), created_at=datetime.now()),
    # ... more rows
]
df = spark.createDataFrame(data)

# Write to Iceberg
iceberg_mgr.create_table("datalake.bronze", "my_new_table", schema_dict)
iceberg_mgr.write_data(df, "datalake.bronze", "my_new_table")
```

### Run Tests

```bash
# Run tests for specific table
python3 -m pytest -v -k "my_new_table" tests/
```

## Step 10: Cleanup (When Done)

```bash
# Remove all resources
./scripts/cleanup.sh

# This will:
# - Delete Kubernetes namespace (dq-framework)
# - Remove Docker image (dq-runner:latest)
# - Clean local reports and warehouse data
```

---

## Common Issues

### Issue: Port already in use

```bash
# If port 9000 or 9001 is already in use
lsof -ti:9000 | xargs kill -9  # Kill process on port 9000
lsof -ti:9001 | xargs kill -9  # Kill process on port 9001
```

### Issue: Python module not found

```bash
# Ensure you're in the project directory
cd ~/dq-testing-framework

# Re-install dependencies
pip3 install -r requirements.txt

# Add project to PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

### Issue: Kubernetes pod won't start

```bash
# Check pod logs
kubectl logs -n dq-framework <pod-name>

# Describe pod for events
kubectl describe pod -n dq-framework <pod-name>

# Common fix: Restart Kubernetes
# Docker Desktop → Settings → Kubernetes → Reset Kubernetes Cluster
```

### Issue: Tests fail with "Table not found"

```bash
# Re-run seed data script
python3 scripts/seed_data.py

# Check if MinIO is accessible
kubectl port-forward -n dq-framework service/minio-service 9000:9000
# In another terminal:
curl http://localhost:9000/minio/health/live
# Should return: HTTP 200 OK
```

---

## Next Steps

1. ✅ Explore Allure reports to understand test results
2. ✅ Add your own tables to the schema registry
3. ✅ Customize validation rules
4. ✅ Set up Airflow for scheduled runs
5. ✅ Scale to more tables by adding parallel tasks in the DAG

**Congratulations!** 🎉 You now have a production-ready Data Quality framework running locally.

