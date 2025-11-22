"""
Metadata tests for Iceberg tables with Circuit Breaker Pattern
"""

import pytest
import allure
from dq_framework.reporters import AllureReporter
from dq_framework.utils import IcebergManager


@pytest.mark.metadata
@pytest.mark.structured
@pytest.mark.parametrize("table_name", ["transactions", "user_profiles"])
def test_iceberg_table_exists(
    table_name,
    spark_session,
    circuit_breaker,
    allure_reporter
):
    """Test 1: Verify Iceberg table exists"""
    AllureReporter.add_table_metadata(
        table_name=table_name,
        table_type="structured",
        namespace="datalake.bronze",
        incremental=True
    )
    
    with allure.step(f"Check if Iceberg table exists: local.datalake_bronze.{table_name}"):
        iceberg = IcebergManager(spark_session)
        exists = iceberg.table_exists("datalake_bronze", table_name)
        
        if exists:
            df = iceberg.read_table("datalake_bronze", table_name)
            row_count = df.count()
            allure.attach(
                f"✓ Iceberg table exists: local.datalake_bronze.{table_name}\n✓ Row count: {row_count:,}",
                name="Table Existence",
                attachment_type=allure.attachment_type.TEXT
            )
            circuit_breaker["set_status"](table_name, True)
        else:
            allure.attach(
                f"✗ Iceberg table does NOT exist: local.datalake_bronze.{table_name}",
                name="Table Existence Error",
                attachment_type=allure.attachment_type.TEXT
            )
            circuit_breaker["set_status"](table_name, False)
            pytest.fail(f"Circuit Breaker: Iceberg table {table_name} does not exist")
    
    assert exists, f"Iceberg table should exist: {table_name}"


@pytest.mark.metadata
@pytest.mark.structured
@pytest.mark.parametrize("table_name,expected_columns", [
    ("transactions", ["transaction_id", "user_id", "amount", "transaction_timestamp", "status", "merchant_id", "category"]),
    ("user_profiles", ["user_id", "email", "age", "registration_date", "account_balance", "is_active", "updated_at"])
])
def test_iceberg_schema(
    table_name,
    expected_columns,
    spark_session,
    circuit_breaker
):
    """Test 2: Validate Iceberg table schema"""
    if not circuit_breaker["get_status"](table_name):
        pytest.skip(f"Circuit Breaker: Table {table_name} does not exist")
    
    AllureReporter.add_table_metadata(
        table_name=table_name,
        table_type="structured",
        namespace="datalake.bronze"
    )
    
    with allure.step(f"Validate schema for Iceberg table: {table_name}"):
        iceberg = IcebergManager(spark_session)
        df = iceberg.read_table("datalake_bronze", table_name)
        actual_columns = df.columns
        
        missing_cols = set(expected_columns) - set(actual_columns)
        extra_cols = set(actual_columns) - set(expected_columns)
        
        errors = []
        if missing_cols:
            errors.append(f"Missing columns: {missing_cols}")
        if extra_cols:
            errors.append(f"Extra columns: {extra_cols}")
        
        is_valid = len(errors) == 0
        
        schema_comparison = f"""
        Expected Columns: {expected_columns}
        Actual Columns: {actual_columns}
        Errors: {errors if errors else 'None'}
        """
        
        allure.attach(schema_comparison, name="Schema Validation", attachment_type=allure.attachment_type.TEXT)
        
        current_status = circuit_breaker["get_status"](table_name)
        circuit_breaker["set_status"](table_name, current_status and is_valid)
        
        if not is_valid:
            AllureReporter.mark_circuit_breaker_failure(
                f"Schema validation failed for {table_name}: {errors}"
            )
            pytest.fail(f"Circuit Breaker: Schema validation failed: {errors}")
    
    assert is_valid, f"Schema should be valid. Errors: {errors}"


@pytest.mark.metadata
@pytest.mark.structured
@pytest.mark.parametrize("table_name", ["transactions", "user_profiles"])
def test_iceberg_not_empty(
    table_name,
    spark_session,
    circuit_breaker
):
    """Test 3: Verify table has data"""
    if not circuit_breaker["get_status"](table_name):
        pytest.skip(f"Circuit Breaker: Previous metadata checks failed for {table_name}")
    
    with allure.step(f"Get row count for Iceberg table: {table_name}"):
        iceberg = IcebergManager(spark_session)
        df = iceberg.read_table("datalake_bronze", table_name)
        row_count = df.count()
    
    allure.attach(
        f"Table has {row_count:,} rows",
        name="Row Count",
        attachment_type=allure.attachment_type.TEXT
    )
    
    assert row_count > 0, f"Table {table_name} should not be empty"

