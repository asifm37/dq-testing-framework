"""
Data quality validation tests with threshold checking.

These tests only run if metadata validation passes (Circuit Breaker Pattern).
"""

import pytest
import allure

from dq_framework.reporters import AllureReporter, DQReportFormatter


@pytest.mark.data_quality
@pytest.mark.structured
@pytest.mark.parametrize("table_name", [
    "transactions",
    "user_profiles"
])
def test_not_null_constraints(
    table_name,
    config_manager,
    iceberg_manager,
    data_validator,
    circuit_breaker,
    incremental_time_range
):
    """
    Test: Validate NOT NULL constraints on required columns.
    
    Uses column pruning to read only columns being validated.
    """
    # Circuit breaker check
    if not circuit_breaker["get_status"](table_name):
        AllureReporter.mark_circuit_breaker_failure(
            f"Metadata validation failed for {table_name}"
        )
        pytest.skip(f"Circuit Breaker: Metadata validation failed for {table_name}")
    
    table_config = config_manager.get_table_config(table_name)
    namespace = table_config["namespace"]
    not_null_cols = table_config["validations"].get("not_null", {}).get("columns", [])
    
    if not not_null_cols:
        pytest.skip(f"No NOT NULL validations configured for {table_name}")
    
    # Add metadata to report
    AllureReporter.add_table_metadata(
        table_name=table_name,
        table_type=table_config["type"],
        namespace=namespace,
        incremental=table_config.get("incremental", False)
    )
    
    # Read data with column pruning (only columns being validated)
    with allure.step(f"Read data from '{namespace}.{table_name}' with column pruning"):
        if table_config.get("incremental", False):
            timestamp_col = table_config.get("timestamp_column")
            df = iceberg_manager.get_incremental_data(
                namespace=namespace,
                table_name=table_name,
                timestamp_column=timestamp_col,
                start_time=incremental_time_range["start_time"],
                end_time=incremental_time_range["end_time"],
                columns=not_null_cols  # Column pruning optimization
            )
            allure.attach(
                f"Incremental load: {incremental_time_range['start_time']} to {incremental_time_range['end_time']}",
                name="Time Range",
                attachment_type=allure.attachment_type.TEXT
            )
        else:
            df = iceberg_manager.read_table(
                namespace=namespace,
                table_name=table_name,
                columns=not_null_cols  # Column pruning optimization
            )
    
    if df is None:
        pytest.fail(f"Failed to read table {namespace}.{table_name}")
    
    # Validate NOT NULL
    with allure.step(f"Validate NOT NULL constraints for {len(not_null_cols)} columns"):
        results = data_validator.validate_not_null(
            df=df,
            columns=not_null_cols,
            table_name=table_name
        )
    
    # Add to report
    AllureReporter.add_validation_summary(
        validation_type="NOT NULL",
        passed=results["passed"],
        total_rows=results["total_rows"],
        violations=results["total_violations"],
        threshold=data_validator.quality_threshold
    )
    
    AllureReporter.attach_validation_details(results, "NOT NULL Validation Results")
    
    # Threshold check
    if not results["passed"]:
        alert = DQReportFormatter.format_threshold_alert(
            table_name=table_name,
            violation_percentage=results["violation_percentage"],
            threshold=data_validator.quality_threshold,
            total_violations=results["total_violations"]
        )
        allure.attach(alert, name="THRESHOLD EXCEEDED", attachment_type=allure.attachment_type.TEXT)
        pytest.fail(f"NOT NULL validation failed: {results['violation_percentage']:.2%} exceeds threshold")
    
    assert results["passed"], "NOT NULL validation should pass"


@pytest.mark.data_quality
@pytest.mark.structured
@pytest.mark.parametrize("table_name", [
    "transactions",
    "user_profiles"
])
def test_range_constraints(
    table_name,
    config_manager,
    iceberg_manager,
    data_validator,
    circuit_breaker,
    incremental_time_range
):
    """
    Test: Validate range (min/max) constraints on numeric columns.
    """
    # Circuit breaker check
    if not circuit_breaker["get_status"](table_name):
        pytest.skip(f"Circuit Breaker: Metadata validation failed for {table_name}")
    
    table_config = config_manager.get_table_config(table_name)
    namespace = table_config["namespace"]
    range_config = table_config["validations"].get("range", {})
    
    if not range_config:
        pytest.skip(f"No range validations configured for {table_name}")
    
    # Add metadata to report
    AllureReporter.add_table_metadata(
        table_name=table_name,
        table_type=table_config["type"],
        namespace=namespace,
        incremental=table_config.get("incremental", False)
    )
    
    # Read data with column pruning
    columns_to_validate = list(range_config.keys())
    with allure.step(f"Read data with column pruning ({len(columns_to_validate)} columns)"):
        if table_config.get("incremental", False):
            timestamp_col = table_config.get("timestamp_column")
            df = iceberg_manager.get_incremental_data(
                namespace=namespace,
                table_name=table_name,
                timestamp_column=timestamp_col,
                start_time=incremental_time_range["start_time"],
                end_time=incremental_time_range["end_time"],
                columns=columns_to_validate
            )
        else:
            df = iceberg_manager.read_table(
                namespace=namespace,
                table_name=table_name,
                columns=columns_to_validate
            )
    
    if df is None:
        pytest.fail(f"Failed to read table {namespace}.{table_name}")
    
    # Validate range
    with allure.step(f"Validate range constraints for {len(range_config)} columns"):
        results = data_validator.validate_range(
            df=df,
            range_config=range_config,
            table_name=table_name
        )
    
    # Add to report
    AllureReporter.add_validation_summary(
        validation_type="Range",
        passed=results["passed"],
        total_rows=results["total_rows"],
        violations=results["total_violations"],
        threshold=data_validator.quality_threshold
    )
    
    AllureReporter.attach_validation_details(results, "Range Validation Results")
    
    # Threshold check
    if not results["passed"]:
        alert = DQReportFormatter.format_threshold_alert(
            table_name=table_name,
            violation_percentage=results["violation_percentage"],
            threshold=data_validator.quality_threshold,
            total_violations=results["total_violations"]
        )
        allure.attach(alert, name="THRESHOLD EXCEEDED", attachment_type=allure.attachment_type.TEXT)
        pytest.fail(f"Range validation failed: {results['violation_percentage']:.2%} exceeds threshold")
    
    assert results["passed"], "Range validation should pass"


@pytest.mark.data_quality
@pytest.mark.structured
@pytest.mark.parametrize("table_name", [
    "transactions",
    "user_profiles"
])
def test_regex_patterns(
    table_name,
    config_manager,
    iceberg_manager,
    data_validator,
    circuit_breaker,
    incremental_time_range
):
    """
    Test: Validate regex patterns on string columns.
    """
    # Circuit breaker check
    if not circuit_breaker["get_status"](table_name):
        pytest.skip(f"Circuit Breaker: Metadata validation failed for {table_name}")
    
    table_config = config_manager.get_table_config(table_name)
    namespace = table_config["namespace"]
    regex_config = table_config["validations"].get("regex", {})
    
    if not regex_config:
        pytest.skip(f"No regex validations configured for {table_name}")
    
    # Add metadata to report
    AllureReporter.add_table_metadata(
        table_name=table_name,
        table_type=table_config["type"],
        namespace=namespace,
        incremental=table_config.get("incremental", False)
    )
    
    # Read data with column pruning
    columns_to_validate = list(regex_config.keys())
    with allure.step(f"Read data with column pruning ({len(columns_to_validate)} columns)"):
        if table_config.get("incremental", False):
            timestamp_col = table_config.get("timestamp_column")
            df = iceberg_manager.get_incremental_data(
                namespace=namespace,
                table_name=table_name,
                timestamp_column=timestamp_col,
                start_time=incremental_time_range["start_time"],
                end_time=incremental_time_range["end_time"],
                columns=columns_to_validate
            )
        else:
            df = iceberg_manager.read_table(
                namespace=namespace,
                table_name=table_name,
                columns=columns_to_validate
            )
    
    if df is None:
        pytest.fail(f"Failed to read table {namespace}.{table_name}")
    
    # Validate regex
    with allure.step(f"Validate regex patterns for {len(regex_config)} columns"):
        results = data_validator.validate_regex(
            df=df,
            regex_config=regex_config,
            table_name=table_name
        )
    
    # Add to report
    AllureReporter.add_validation_summary(
        validation_type="Regex",
        passed=results["passed"],
        total_rows=results["total_rows"],
        violations=results["total_violations"],
        threshold=data_validator.quality_threshold
    )
    
    AllureReporter.attach_validation_details(results, "Regex Validation Results")
    
    # Threshold check
    if not results["passed"]:
        alert = DQReportFormatter.format_threshold_alert(
            table_name=table_name,
            violation_percentage=results["violation_percentage"],
            threshold=data_validator.quality_threshold,
            total_violations=results["total_violations"]
        )
        allure.attach(alert, name="THRESHOLD EXCEEDED", attachment_type=allure.attachment_type.TEXT)
        pytest.fail(f"Regex validation failed: {results['violation_percentage']:.2%} exceeds threshold")
    
    assert results["passed"], "Regex validation should pass"


@pytest.mark.data_quality
@pytest.mark.structured
@pytest.mark.parametrize("table_name", [
    "transactions",
    "user_profiles"
])
def test_cross_column_constraints(
    table_name,
    config_manager,
    iceberg_manager,
    data_validator,
    circuit_breaker,
    incremental_time_range
):
    """
    Test: Validate cross-column constraints (e.g., col_a < col_b).
    """
    # Circuit breaker check
    if not circuit_breaker["get_status"](table_name):
        pytest.skip(f"Circuit Breaker: Metadata validation failed for {table_name}")
    
    table_config = config_manager.get_table_config(table_name)
    namespace = table_config["namespace"]
    cross_col_rules = table_config["validations"].get("cross_column", [])
    
    if not cross_col_rules:
        pytest.skip(f"No cross-column validations configured for {table_name}")
    
    # Add metadata to report
    AllureReporter.add_table_metadata(
        table_name=table_name,
        table_type=table_config["type"],
        namespace=namespace,
        incremental=table_config.get("incremental", False)
    )
    
    # Read full table (cross-column may need multiple columns)
    with allure.step(f"Read data for cross-column validation"):
        if table_config.get("incremental", False):
            timestamp_col = table_config.get("timestamp_column")
            df = iceberg_manager.get_incremental_data(
                namespace=namespace,
                table_name=table_name,
                timestamp_column=timestamp_col,
                start_time=incremental_time_range["start_time"],
                end_time=incremental_time_range["end_time"]
            )
        else:
            df = iceberg_manager.read_table(
                namespace=namespace,
                table_name=table_name
            )
    
    if df is None:
        pytest.fail(f"Failed to read table {namespace}.{table_name}")
    
    # Validate cross-column
    with allure.step(f"Validate {len(cross_col_rules)} cross-column rules"):
        results = data_validator.validate_cross_column(
            df=df,
            cross_column_rules=cross_col_rules,
            table_name=table_name
        )
    
    # Add to report
    AllureReporter.add_validation_summary(
        validation_type="Cross-Column",
        passed=results["passed"],
        total_rows=results["total_rows"],
        violations=results["total_violations"],
        threshold=data_validator.quality_threshold
    )
    
    AllureReporter.attach_validation_details(results, "Cross-Column Validation Results")
    
    # Threshold check
    if not results["passed"]:
        alert = DQReportFormatter.format_threshold_alert(
            table_name=table_name,
            violation_percentage=results["violation_percentage"],
            threshold=data_validator.quality_threshold,
            total_violations=results["total_violations"]
        )
        allure.attach(alert, name="THRESHOLD EXCEEDED", attachment_type=allure.attachment_type.TEXT)
        pytest.fail(f"Cross-column validation failed: {results['violation_percentage']:.2%} exceeds threshold")
    
    assert results["passed"], "Cross-column validation should pass"

