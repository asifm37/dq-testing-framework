"""
Simplified Pytest configuration for Parquet-based DQ tests
"""

import pytest
import os
from pyspark.sql import SparkSession

from dq_framework.validators import MetadataValidator, DataQualityValidator
from dq_framework.reporters import AllureReporter


# Global flag for circuit breaker
_metadata_validation_passed = {}


@pytest.fixture(scope="session")
def spark_session():
    """
    Create a Spark session with Iceberg support and optimizations.
    """
    spark = SparkSession.builder \
        .appName("DQ-Testing-Iceberg") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "file:///app/warehouse") \
        .config("spark.sql.defaultCatalog", "local") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✓ Spark Session with Iceberg Initialized")
    
    yield spark
    spark.stop()


@pytest.fixture(scope="function")
def metadata_validator(spark_session):
    """Create metadata validator for each test"""
    return MetadataValidator(spark_session)


@pytest.fixture(scope="function")
def data_validator(spark_session):
    """Create data quality validator with configured threshold"""
    threshold = float(os.environ.get("DQ_THRESHOLD", "0.10"))
    return DataQualityValidator(spark_session, quality_threshold=threshold)


@pytest.fixture(scope="function")
def allure_reporter():
    """Create Allure reporter"""
    return AllureReporter()


@pytest.fixture(scope="function")
def incremental_time_range():
    """
    Get incremental time range from Airflow execution date.
    Falls back to last 1 hour if not running in Airflow.
    """
    from datetime import datetime, timedelta
    
    start_time = os.environ.get("DATA_INTERVAL_START")
    end_time = os.environ.get("DATA_INTERVAL_END")
    
    if not start_time or not end_time:
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=1)
        return {
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S")
        }
    
    return {
        "start_time": start_time,
        "end_time": end_time
    }


def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "metadata: mark test as metadata validation (circuit breaker)"
    )
    config.addinivalue_line(
        "markers", "data_quality: mark test as data quality validation"
    )
    config.addinivalue_line(
        "markers", "structured: mark test for structured data"
    )
    config.addinivalue_line(
        "markers", "unstructured: mark test for unstructured data"
    )


def set_metadata_validation_status(table_name: str, passed: bool):
    """Set metadata validation status for circuit breaker"""
    global _metadata_validation_passed
    _metadata_validation_passed[table_name] = passed


def get_metadata_validation_status(table_name: str) -> bool:
    """Get metadata validation status for circuit breaker"""
    global _metadata_validation_passed
    return _metadata_validation_passed.get(table_name, False)


@pytest.fixture(scope="session")
def circuit_breaker():
    """Fixture to provide circuit breaker functions"""
    return {
        "set_status": set_metadata_validation_status,
        "get_status": get_metadata_validation_status
    }


def pytest_collection_modifyitems(config, items):
    """
    Modify test collection to enforce circuit breaker order.
    Metadata tests must run before data quality tests.
    """
    metadata_tests = []
    data_quality_tests = []
    other_tests = []
    
    for item in items:
        if "metadata" in item.keywords:
            metadata_tests.append(item)
        elif "data_quality" in item.keywords:
            data_quality_tests.append(item)
        else:
            other_tests.append(item)
    
    items[:] = metadata_tests + data_quality_tests + other_tests

