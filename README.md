# Automated Data Quality (DQ) Testing Framework

> **Production-Ready POC**: Scalable, end-to-end data quality validation framework deployed on Apple M1 Pro Mac with Docker Desktop Kubernetes

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.0-orange.svg)](https://spark.apache.org/)
[![Airflow](https://img.shields.io/badge/Airflow-2.8.0-green.svg)](https://airflow.apache.org/)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-1.28-blue.svg)](https://kubernetes.io/)

---

## 🎯 Overview

This framework provides **automated, scalable data quality testing** for data lakes, supporting:

- ✅ **1000s of tables** on hourly schedules
- ✅ **Incremental validation** using Airflow's `data_interval_start/end`
- ✅ **Column pruning optimization** to minimize I/O
- ✅ **Circuit breaker pattern** to prevent bad data propagation
- ✅ **Apache Iceberg** tables backed by Parquet
- ✅ **Structured & Unstructured** data support
- ✅ **Allure reporting** with beautiful HTML dashboards

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         LOCAL MAC (M1 Pro)                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌──────────────┐      Triggers      ┌─────────────────────────┐   │
│  │   Airflow    │ ─────────────────> │  KubernetesPodOperator  │   │
│  │  (Native)    │                    └───────────┬─────────────┘   │
│  └──────────────┘                                │                   │
│                                                   │                   │
│  ┌────────────────────────────────────────────────▼────────────┐   │
│  │             Docker Desktop Kubernetes                         │   │
│  │                                                               │   │
│  │  ┌───────────────┐     ┌──────────────────────────────┐    │   │
│  │  │  MinIO (S3)   │     │   DQ Runner Pod              │    │   │
│  │  │  Storage      │ <── │  - PySpark 3.5               │    │   │
│  │  │               │     │  - Pytest                    │    │   │
│  │  └───────────────┘     │  - Allure Reporter           │    │   │
│  │                        │  - Iceberg Integration       │    │   │
│  │  ┌───────────────┐     └──────────────────────────────┘    │   │
│  │  │ Iceberg       │                   ▲                      │   │
│  │  │ Warehouse     │ ──────────────────┘                      │   │
│  │  │ (Parquet)     │                                          │   │
│  │  └───────────────┘                                          │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                       │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  Host Volumes (Docker-to-Host mounting)                      │   │
│  │  - /reports/allure-results/  (visible on Mac)               │   │
│  │  - /reports/allure-report/   (HTML dashboard)               │   │
│  │  - /warehouse/               (Iceberg tables)                │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📋 Features

### 1. **Circuit Breaker Pattern**

```
┌────────────────────┐       PASS       ┌────────────────────┐
│ Metadata Tests     │ ──────────────> │ Data Quality Tests │
│ - Table exists?    │                  │ - NOT NULL         │
│ - Schema valid?    │                  │ - Range checks     │
└────────────────────┘                  │ - Regex patterns   │
         │                               │ - Cross-column     │
         │ FAIL                          └────────────────────┘
         │                                        │
         ▼                                        │
  ┌────────────────┐                             │
  │ STOP Pipeline  │ <───────────────────────────┘
  │ (No DQ tests)  │         FAIL (>10% threshold)
  └────────────────┘
```

### 2. **Validation Rules**

| Rule Type        | Description                           | Example                               |
|------------------|---------------------------------------|---------------------------------------|
| Schema           | Table exists, schema matches          | Check column names and types          |
| NOT NULL         | Required columns have no nulls        | `user_id`, `transaction_id`           |
| Range            | Numeric values within bounds          | `age BETWEEN 18 AND 120`              |
| Regex            | String patterns match format          | `email` matches email regex           |
| Cross-Column     | Inter-column relationships hold       | `transaction_timestamp <= NOW()`      |

### 3. **Optimization: Column Pruning**

```python
# Traditional approach: Read ALL columns (slow!)
df = spark.table("transactions")  # Reads 50+ columns

# DQ Framework: Read ONLY what you validate (fast!)
df = iceberg_manager.read_table(
    "transactions",
    columns=["user_id", "amount"]  # Only 2 columns!
)
```

**Result**: Up to **10x faster** on wide tables.

### 4. **Incremental Validation**

```python
# Validate only NEW data since last run
df = iceberg_manager.get_incremental_data(
    table="transactions",
    timestamp_column="transaction_timestamp",
    start_time="{{ data_interval_start }}",  # From Airflow
    end_time="{{ data_interval_end }}"
)
```

---

## 🚀 Quick Start

### Prerequisites

- **Hardware**: Apple M1/M2 Mac (or any Mac with Docker Desktop)
- **Software**:
  - Docker Desktop with Kubernetes enabled
  - Python 3.11+
  - kubectl CLI
  - Airflow (optional for DAG orchestration)

### Installation

```bash
# 1. Clone/Navigate to project
cd ~/dq-testing-framework

# 2. Deploy infrastructure (MinIO, build Docker image, seed data)
./scripts/deploy.sh

# 3. Run tests locally
./scripts/run_tests.sh

# 4. View Allure report
allure serve reports/allure-results
# Or open reports/allure-report/index.html
```

---

## 📁 Project Structure

```
dq-testing-framework/
├── README.md                           # This file
├── requirements.txt                    # Python dependencies
├── .gitignore                          # Git ignore patterns
│
├── airflow/
│   └── dags/
│       └── dq_validation_dag.py        # Airflow DAG with KubernetesPodOperator
│
├── docker/
│   ├── Dockerfile.dq-runner            # Docker image for DQ tests
│   └── docker-compose.yml              # Local testing (without K8s)
│
├── kubernetes/
│   └── k8s-minio.yaml                  # MinIO deployment manifest
│
├── dq_framework/                       # Core DQ framework
│   ├── config/
│   │   └── schema_registry.json        # Table definitions & validation rules
│   ├── validators/
│   │   ├── metadata_validator.py       # Table/schema checks
│   │   └── data_validator.py           # Data quality checks
│   ├── reporters/
│   │   └── allure_reporter.py          # Allure integration
│   └── utils/
│       ├── spark_utils.py              # Spark session & optimizations
│       └── iceberg_utils.py            # Iceberg operations
│
├── tests/                              # Pytest test suite
│   ├── conftest.py                     # Fixtures & circuit breaker logic
│   ├── test_metadata.py                # Metadata tests (run first)
│   └── test_data_quality.py            # Data quality tests
│
├── scripts/                            # Utility scripts
│   ├── deploy.sh                       # Full deployment automation
│   ├── cleanup.sh                      # Remove all resources
│   ├── seed_data.py                    # Generate test data
│   ├── setup_minio.py                  # Configure MinIO buckets
│   └── run_tests.sh                    # Run tests locally
│
└── reports/                            # Allure reports (host-mounted)
    ├── allure-results/                 # Test results (JSON)
    └── allure-report/                  # HTML dashboard
```

---

## 🧪 Test Data

The framework includes a seed data generator that creates **positive**, **negative**, and **edge cases**:

```bash
python3 scripts/seed_data.py
```

**Generated Tables**:

| Table            | Rows  | Data Quality Issues          |
|------------------|-------|------------------------------|
| transactions     | 1000  | ~5% invalid (nulls, ranges)  |
| user_profiles    | 500   | ~5% invalid (regex, dates)   |

**Issue Types**:
- ✅ **Positive**: Valid data that passes all checks
- ❌ **Negative**: NULL violations, range violations, regex violations
- ⚠️ **Edge**: Future dates, boundary values (age=18, amount=0.01)

---

## 🎨 Allure Reports

The framework generates beautiful, interactive HTML reports:

![Allure Report Example](https://docs.qameta.io/allure/images/overview.png)

**Features**:
- 📊 Pass/Fail statistics
- 📈 Historical trends
- 🔍 Detailed test logs
- 🚨 Circuit breaker alerts
- 📎 Validation details (JSON attachments)

**View Reports**:
```bash
# Live server (auto-refresh)
allure serve reports/allure-results

# Static HTML
open reports/allure-report/index.html
```

---

## ⚙️ Configuration

### Schema Registry

Edit `dq_framework/config/schema_registry.json` to add/modify tables:

```json
{
  "tables": {
    "your_table": {
      "type": "structured",
      "format": "iceberg",
      "namespace": "datalake.bronze",
      "schema": {
        "col1": "string",
        "col2": "integer"
      },
      "validations": {
        "not_null": {
          "columns": ["col1"]
        },
        "range": {
          "col2": {"min": 0, "max": 100}
        }
      },
      "quality_threshold": 0.10
    }
  }
}
```

### Environment Variables

| Variable                | Default           | Description                       |
|-------------------------|-------------------|-----------------------------------|
| `MINIO_ENDPOINT`        | `localhost:9000`  | MinIO API endpoint                |
| `MINIO_ACCESS_KEY`      | `minioadmin`      | MinIO access key                  |
| `MINIO_SECRET_KEY`      | `minioadmin`      | MinIO secret key                  |
| `DQ_THRESHOLD`          | `0.10`            | Max allowed invalid data (10%)    |
| `DATA_INTERVAL_START`   | (from Airflow)    | Incremental start time            |
| `DATA_INTERVAL_END`     | (from Airflow)    | Incremental end time              |

---

## 🔄 Airflow Integration

### Setup Airflow

```bash
# Install Airflow
pip install apache-airflow==2.8.0

# Initialize database
airflow db init

# Create admin user
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com

# Copy DAG to Airflow
cp airflow/dags/dq_validation_dag.py ~/airflow/dags/

# Start Airflow
airflow webserver -p 8080  # In terminal 1
airflow scheduler            # In terminal 2
```

### Access Airflow UI

1. Open http://localhost:8080
2. Login: `admin` / `admin`
3. Enable `dq_validation_hourly` DAG
4. Trigger manually or wait for hourly schedule

---

## 🐳 Docker & Kubernetes

### Build Docker Image

```bash
docker build -t dq-runner:latest -f docker/Dockerfile.dq-runner .
```

### Deploy to Kubernetes

```bash
# Apply MinIO deployment
kubectl apply -f kubernetes/k8s-minio.yaml

# Check status
kubectl get all -n dq-framework

# Access MinIO Console
kubectl port-forward -n dq-framework service/minio-service 9001:9001
# Open http://localhost:9001
```

### Run Tests in Kubernetes Pod

```bash
# Manual pod creation (for testing)
kubectl run dq-test \
  --image=dq-runner:latest \
  --namespace=dq-framework \
  --command -- sleep 3600

# Execute tests
kubectl exec -it dq-test -n dq-framework -- python3 -m pytest -v

# Copy reports to local machine
kubectl cp dq-framework/dq-test:/app/reports ./reports
```

---

## 📊 Scaling to 1000s of Tables

The framework supports massive scale through:

### 1. **Parallel Execution**

Modify `airflow/dags/dq_validation_dag.py` to create parallel tasks:

```python
# Group tables by domain/size
table_groups = {
    "group_1": ["table1", "table2", ...],  # 100 tables
    "group_2": ["table101", "table102", ...],  # 100 tables
    # ... 10 groups = 1000 tables
}

# Create parallel K8s pods
tasks = []
for group_name, tables in table_groups.items():
    task = KubernetesPodOperator(
        task_id=f"dq_group_{group_name}",
        arguments=["-k", "|".join(tables)],  # Pytest filter
        ...
    )
    tasks.append(task)

# All run in parallel
tasks >> generate_report
```

### 2. **Resource Optimization**

```yaml
# K8s Pod resources
resources:
  requests:
    memory: "4Gi"
    cpu: "2"
  limits:
    memory: "8Gi"
    cpu: "4"
```

### 3. **Incremental Processing**

Only validate **new data** since last run:
- Reads only records where `timestamp >= data_interval_start`
- Uses Iceberg metadata for efficient filtering
- Skips unchanged data

**Result**: 1000 tables × 1 hour cadence = **sustainable at scale**.

---

## 🛠️ Troubleshooting

### MinIO not accessible

```bash
# Check pod status
kubectl get pods -n dq-framework

# Port-forward to access locally
kubectl port-forward -n dq-framework service/minio-service 9000:9000
```

### Docker image not found in K8s

```bash
# Tag and load image to K8s (Docker Desktop)
docker build -t dq-runner:latest -f docker/Dockerfile.dq-runner .

# Verify image exists
docker images | grep dq-runner
```

### Tests failing with connection errors

```bash
# Ensure MinIO is running
kubectl get svc -n dq-framework

# Check environment variables
echo $MINIO_ENDPOINT
echo $MINIO_ACCESS_KEY
```

### Allure reports not generated

```bash
# Install Allure CLI
brew install allure  # macOS

# Or download from: https://github.com/allure-framework/allure2/releases
```

---

## 📝 Example Test Run

```bash
$ ./scripts/run_tests.sh

==============================================================================
DQ Framework - Test Runner
==============================================================================

[1/4] Cleaning previous reports...
  ✓ Reports directory cleaned

[2/4] Running DQ tests...
  - Test suite: tests/
  - Allure results: ./reports/allure-results

tests/test_metadata.py::test_table_exists[transactions] PASSED     [ 10%]
tests/test_metadata.py::test_table_schema[transactions] PASSED     [ 20%]
tests/test_data_quality.py::test_not_null[transactions] PASSED     [ 30%]
tests/test_data_quality.py::test_range[transactions] FAILED        [ 40%]  ← 5% violations
...

[3/4] Generating Allure report...
  ✓ Allure report generated: ./reports/allure-report/index.html

[4/4] Test Summary
==============================================================================
✗ Some tests failed (exit code: 1)
==============================================================================

Reports:
  - Allure Results: ./reports/allure-results
  - Allure Report:  ./reports/allure-report/index.html
```

---

## 🤝 Contributing

This is a POC framework. To extend:

1. **Add new validation rules**: Edit `dq_framework/validators/data_validator.py`
2. **Add new tables**: Update `dq_framework/config/schema_registry.json`
3. **Add new tests**: Create test files in `tests/`
4. **Customize reports**: Modify `dq_framework/reporters/allure_reporter.py`

---

## 📄 License

This project is a proof-of-concept (POC) for demonstration purposes.

---

## 🙏 Acknowledgments

- **Apache Spark** & **Iceberg** for scalable data processing
- **Airflow** for orchestration
- **Allure Framework** for beautiful reporting
- **MinIO** for S3-compatible storage
- **Kubernetes** for container orchestration

---

## 📞 Support

For questions or issues:

1. Check the **Troubleshooting** section above
2. Review logs: `kubectl logs <pod-name> -n dq-framework`
3. Inspect Allure reports for detailed error messages

---

**Built with ❤️ for Data Quality Excellence**

