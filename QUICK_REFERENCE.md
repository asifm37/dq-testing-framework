# Quick Reference Card

## 🚀 Essential Commands

### Initial Setup (One-Time)
```bash
cd ~/dq-testing-framework
./scripts/deploy.sh          # Deploy everything (5-10 min)
```

### Daily Operations
```bash
# Run DQ tests locally
./scripts/run_tests.sh

# View reports
allure serve reports/allure-results
# or
open reports/allure-report/index.html

# Re-seed test data
python3 scripts/seed_data.py

# Clean up everything
./scripts/cleanup.sh
```

### Kubernetes Operations
```bash
# Check all resources
kubectl get all -n dq-framework

# View MinIO logs
kubectl logs -n dq-framework -l app=minio

# Access MinIO Console
kubectl port-forward -n dq-framework service/minio-service 9001:9001
# Then: http://localhost:9001 (minioadmin/minioadmin)

# Run test pod manually
kubectl run dq-test --image=dq-runner:latest -n dq-framework --rm -it -- /bin/bash
```

### Airflow Operations
```bash
# Start Airflow
airflow webserver -p 8080    # Terminal 1
airflow scheduler             # Terminal 2

# Access UI: http://localhost:8080 (admin/admin)

# Trigger DAG manually
airflow dags trigger dq_validation_hourly
```

### Docker Operations
```bash
# Rebuild image
docker build -t dq-runner:latest -f docker/Dockerfile.dq-runner .

# Test locally with Docker Compose
docker-compose -f docker/docker-compose.yml up -d
docker exec -it dq-runner bash
```

---

## 📁 Key Files

| File                                      | Purpose                          |
|-------------------------------------------|----------------------------------|
| `dq_framework/config/schema_registry.json`| Table definitions & rules        |
| `airflow/dags/dq_validation_dag.py`       | Orchestration DAG                |
| `tests/test_metadata.py`                  | Circuit breaker tests            |
| `tests/test_data_quality.py`              | DQ validation tests              |
| `scripts/seed_data.py`                    | Generate test data               |

---

## 🎯 Common Tasks

### Add a New Table
1. Edit `dq_framework/config/schema_registry.json`
2. Add entry with schema and validation rules
3. Run tests: `./scripts/run_tests.sh`

### Modify Validation Rules
1. Edit table config in `schema_registry.json`
2. Available rules:
   - `not_null`: Required columns
   - `range`: Min/max bounds
   - `regex`: Pattern matching
   - `cross_column`: Inter-column checks

### Scale to More Tables
1. Edit `airflow/dags/dq_validation_dag.py`
2. Add parallel `KubernetesPodOperator` tasks
3. Group tables by domain/size

---

## 🔍 Troubleshooting Quick Fixes

### MinIO not accessible
```bash
kubectl get pods -n dq-framework
kubectl logs -n dq-framework <minio-pod-name>
kubectl port-forward -n dq-framework service/minio-service 9000:9000
```

### Tests fail with import errors
```bash
cd ~/dq-testing-framework
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
pip3 install -r requirements.txt
```

### Kubernetes pod won't start
```bash
# Check pod status
kubectl describe pod -n dq-framework <pod-name>

# Restart Kubernetes (Docker Desktop)
# Settings → Kubernetes → Reset Kubernetes Cluster
```

### Docker image not found
```bash
# Verify image exists
docker images | grep dq-runner

# Rebuild if needed
docker build -t dq-runner:latest -f docker/Dockerfile.dq-runner .
```

---

## 📊 Environment Variables

| Variable                | Default           | Used By            |
|-------------------------|-------------------|--------------------|
| `MINIO_ENDPOINT`        | `localhost:9000`  | All components     |
| `MINIO_ACCESS_KEY`      | `minioadmin`      | All components     |
| `MINIO_SECRET_KEY`      | `minioadmin`      | All components     |
| `DQ_THRESHOLD`          | `0.10`            | Data validator     |
| `DATA_INTERVAL_START`   | (from Airflow)    | Incremental tests  |
| `DATA_INTERVAL_END`     | (from Airflow)    | Incremental tests  |

---

## 📝 Test Execution Markers

Run specific test categories:

```bash
# Only metadata tests
pytest -v -m metadata tests/

# Only data quality tests
pytest -v -m data_quality tests/

# Only structured data tests
pytest -v -m structured tests/

# Only unstructured data tests
pytest -v -m unstructured tests/

# Specific table
pytest -v -k "transactions" tests/
```

---

## 🎨 Allure Report Commands

```bash
# Generate and view report
allure serve reports/allure-results

# Generate static report
allure generate reports/allure-results -o reports/allure-report --clean

# Open existing report
open reports/allure-report/index.html

# Clean old results
rm -rf reports/allure-results/*
rm -rf reports/allure-report/*
```

---

## 🔧 Configuration Snippets

### Add NOT NULL validation
```json
"validations": {
  "not_null": {
    "columns": ["user_id", "amount", "timestamp"]
  }
}
```

### Add Range validation
```json
"validations": {
  "range": {
    "age": {"min": 18, "max": 120},
    "amount": {"min": 0.01, "max": 1000000.00}
  }
}
```

### Add Regex validation
```json
"validations": {
  "regex": {
    "email": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",
    "user_id": "^USR[0-9]{8}$"
  }
}
```

### Add Cross-column validation
```json
"validations": {
  "cross_column": [
    {
      "name": "date_check",
      "expression": "start_date <= end_date",
      "description": "Start date must be before end date"
    }
  ]
}
```

---

## 📞 Getting Help

1. Check `GETTING_STARTED.md` for setup issues
2. Check `ARCHITECTURE.md` for design questions
3. Check `README.md` for general usage
4. Review logs: `kubectl logs -n dq-framework <pod-name>`
5. Check Allure reports for test details

---

## 🎯 Performance Tips

1. **Use column pruning**: Only read columns you validate
2. **Use incremental mode**: Validate only new data
3. **Parallelize table groups**: Run multiple pods
4. **Partition tables**: Use Iceberg partitioning
5. **Adjust thresholds**: Fine-tune based on data quality

---

**Print this card for quick reference!** 📋

