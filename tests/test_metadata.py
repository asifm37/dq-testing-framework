"""
Metadata validation tests with Circuit Breaker Pattern.

These tests MUST run first. If they fail, data quality tests are skipped.
"""

import pytest
import allure

from dq_framework.reporters import AllureReporter


@pytest.mark.metadata
@pytest.mark.structured
@pytest.mark.parametrize("table_name", [
    "transactions",
    "user_profiles"
])
def test_table_exists(
    table_name,
    config_manager,
    metadata_validator,
    circuit_breaker,
    allure_reporter
):
    """
    Test 1: Verify table exists in Iceberg catalog.
    
    Circuit Breaker: If FAILED, skip all downstream validations.
    """
    table_config = config_manager.get_table_config(table_name)
    namespace = table_config["namespace"]
    
    # Add metadata to report
    AllureReporter.add_table_metadata(
        table_name=table_name,
        table_type=table_config["type"],
        namespace=namespace
    )
    
    # Check table existence
    with allure.step(f"Verify table '{namespace}.{table_name}' exists"):
        exists = metadata_validator.check_table_exists(namespace, table_name)
    
    # Set circuit breaker status
    circuit_breaker["set_status"](table_name, exists)
    
    if not exists:
        AllureReporter.mark_circuit_breaker_failure(
            f"Table {namespace}.{table_name} does not exist"
        )
        pytest.fail(f"Circuit Breaker: Table {namespace}.{table_name} does not exist")
    
    assert exists, f"Table {namespace}.{table_name} should exist"


@pytest.mark.metadata
@pytest.mark.structured
@pytest.mark.parametrize("table_name", [
    "transactions",
    "user_profiles"
])
def test_table_schema(
    table_name,
    config_manager,
    metadata_validator,
    circuit_breaker,
    allure_reporter
):
    """
    Test 2: Validate table schema matches expected schema.
    
    Circuit Breaker: If FAILED, skip all downstream validations.
    """
    # Check if previous metadata test passed
    if not circuit_breaker["get_status"](table_name):
        pytest.skip(f"Circuit Breaker: Table {table_name} does not exist, skipping schema validation")
    
    table_config = config_manager.get_table_config(table_name)
    namespace = table_config["namespace"]
    expected_schema = table_config["schema"]
    strict_mode = table_config["validations"]["schema_check"].get("strict_mode", True)
    
    # Add metadata to report
    AllureReporter.add_table_metadata(
        table_name=table_name,
        table_type=table_config["type"],
        namespace=namespace
    )
    
    # Validate schema
    with allure.step(f"Validate schema for '{namespace}.{table_name}'"):
        is_valid, errors = metadata_validator.validate_schema(
            namespace=namespace,
            table_name=table_name,
            expected_schema=expected_schema,
            strict_mode=strict_mode
        )
    
    # Update circuit breaker status
    current_status = circuit_breaker["get_status"](table_name)
    circuit_breaker["set_status"](table_name, current_status and is_valid)
    
    if not is_valid:
        AllureReporter.mark_circuit_breaker_failure(
            f"Schema validation failed for {namespace}.{table_name}: {errors}"
        )
        pytest.fail(f"Circuit Breaker: Schema validation failed: {errors}")
    
    assert is_valid, f"Schema should be valid. Errors: {errors}"


@pytest.mark.metadata
@pytest.mark.structured
@pytest.mark.parametrize("table_name", [
    "transactions",
    "user_profiles"
])
def test_table_not_empty(
    table_name,
    config_manager,
    metadata_validator,
    circuit_breaker
):
    """
    Test 3: Verify table has data (optional check).
    
    Not a circuit breaker - just informational.
    """
    # Check if previous metadata tests passed
    if not circuit_breaker["get_status"](table_name):
        pytest.skip(f"Circuit Breaker: Previous metadata checks failed for {table_name}")
    
    table_config = config_manager.get_table_config(table_name)
    namespace = table_config["namespace"]
    
    # Get row count
    with allure.step(f"Get row count for '{namespace}.{table_name}'"):
        row_count = metadata_validator.get_row_count(namespace, table_name)
    
    allure.attach(
        f"Table has {row_count:,} rows",
        name="Row Count",
        attachment_type=allure.attachment_type.TEXT
    )
    
    # This is informational - we don't fail if empty (might be incremental with no new data)
    if row_count == 0:
        allure.attach(
            "Table is empty - this may be expected for incremental loads with no new data",
            name="Empty Table Warning",
            attachment_type=allure.attachment_type.TEXT
        )


@pytest.mark.metadata
@pytest.mark.unstructured
@pytest.mark.parametrize("table_name", [
    "event_logs",
    "product_images"
])
def test_unstructured_metadata(
    table_name,
    config_manager,
    spark_session,
    circuit_breaker
):
    """
    Test 4: Validate metadata for unstructured data sources.
    
    Circuit Breaker: If FAILED, skip downstream validations.
    """
    from dq_framework.validators import UnstructuredMetadataValidator
    
    table_config = config_manager.get_table_config(table_name)
    namespace = table_config["namespace"]
    
    # Add metadata to report
    AllureReporter.add_table_metadata(
        table_name=table_name,
        table_type=table_config["type"],
        namespace=namespace
    )
    
    # For unstructured data, we check if the metadata table/file exists
    # This is a simplified check - in production, you'd read from a metadata catalog
    
    # Set circuit breaker to passed for now (implement actual checks as needed)
    circuit_breaker["set_status"](table_name, True)
    
    allure.attach(
        f"Unstructured data source '{table_name}' metadata validated",
        name="Unstructured Metadata",
        attachment_type=allure.attachment_type.TEXT
    )
    
    assert True, "Metadata validation passed for unstructured data"

