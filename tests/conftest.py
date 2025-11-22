"""Pytest configuration and fixtures for DQ Framework tests"""

import pytest
import os
from pyspark.sql import SparkSession

from dq_framework.config import get_config_manager
from dq_framework.utils import get_spark_session, IcebergManager
from dq_framework.validators import MetadataValidator, DataQualityValidator
from dq_framework.reporters import AllureReporter


# Global flag for circuit breaker
_metadata_validation_passed = {}


@pytest.fixture(scope="session")
def spark_session():
    """
    Create a Spark session for the test session.
    Reused across all tests for efficiency.
    """
    spark = get_spark_session(app_name="DQ-Testing-Session")
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def config_manager():
    """Get configuration manager"""
    return get_config_manager()


@pytest.fixture(scope="session")
def iceberg_manager(spark_session):
    """Create Iceberg manager"""
    return IcebergManager(spark_session)


@pytest.fixture(scope="function")
def metadata_validator(spark_session):
    """Create metadata validator for each test"""
    return MetadataValidator(spark_session)


@pytest.fixture(scope="function")
def data_validator(spark_session, config_manager):
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
    
    # Try to get from Airflow context
    start_time = os.environ.get("DATA_INTERVAL_START")
    end_time = os.environ.get("DATA_INTERVAL_END")
    
    if not start_time or not end_time:
        # Fallback: last 1 hour
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
    config.addinivalue_line(
        "markers", "incremental: mark test for incremental validation"
    )


def set_metadata_validation_status(table_name: str, passed: bool):
    """
    Set metadata validation status for circuit breaker.
    
    Args:
        table_name: Name of the table
        passed: Whether metadata validation passed
    """
    global _metadata_validation_passed
    _metadata_validation_passed[table_name] = passed


def get_metadata_validation_status(table_name: str) -> bool:
    """
    Get metadata validation status for circuit breaker.
    
    Args:
        table_name: Name of the table
    
    Returns:
        True if metadata validation passed, False otherwise
    """
    global _metadata_validation_passed
    return _metadata_validation_passed.get(table_name, False)


@pytest.fixture
def circuit_breaker():
    """
    Fixture to provide circuit breaker functions.
    """
    return {
        "set_status": set_metadata_validation_status,
        "get_status": get_metadata_validation_status
    }


# Hook to handle test failures with circuit breaker
def pytest_runtest_makereport(item, call):
    """
    Hook to capture test results and implement circuit breaker logic.
    """
    if call.when == "call":
        # Check if this is a metadata test
        if "metadata" in item.keywords:
            # Extract table name from test
            table_name = None
            if hasattr(item, 'callspec'):
                table_name = item.callspec.params.get('table_name')
            
            if table_name:
                # Set status based on test outcome
                passed = call.excinfo is None
                set_metadata_validation_status(table_name, passed)


def pytest_collection_modifyitems(config, items):
    """
    Modify test collection to enforce circuit breaker order.
    Metadata tests must run before data quality tests.
    """
    # Separate metadata and data quality tests
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
    
    # Reorder: metadata first, then data quality, then others
    items[:] = metadata_tests + data_quality_tests + other_tests

