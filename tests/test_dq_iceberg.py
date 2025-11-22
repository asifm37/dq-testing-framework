"""
Data Quality tests for Iceberg tables
Includes: NOT NULL, Range, Regex, and Cross-Column validations
"""

import pytest
import allure
from dq_framework.reporters import AllureReporter, DQReportFormatter
from dq_framework.utils import IcebergManager


# Validation rules for each table
VALIDATION_RULES = {
    "transactions": {
        "not_null": ["transaction_id", "user_id", "amount", "transaction_timestamp"],
        "range": {
            "amount": {"min": 0.01, "max": 1000000.00}
        },
        "regex": {
            "transaction_id": r"^TXN[0-9]{10}$",
            "status": r"^(COMPLETED|PENDING|FAILED|REFUNDED)$"
        },
        "cross_column": [
            {
                "name": "timestamp_validity",
                "expression": "transaction_timestamp <= current_timestamp()",
                "description": "Transaction timestamp cannot be in the future"
            }
        ]
    },
    "user_profiles": {
        "not_null": ["user_id", "email", "registration_date"],
        "range": {
            "age": {"min": 18, "max": 120},
            "account_balance": {"min": 0.00, "max": 999999.99}
        },
        "regex": {
            "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            "user_id": r"^USR[0-9]{8}$"
        },
        "cross_column": [
            {
                "name": "registration_date_validity",
                "expression": "registration_date <= current_date()",
                "description": "Registration date cannot be in the future"
            }
        ]
    }
}


@pytest.mark.data_quality
@pytest.mark.structured
@pytest.mark.parametrize("table_name", ["transactions", "user_profiles"])
def test_not_null_constraints(table_name, spark_session, data_validator, circuit_breaker):
    """Test: NOT NULL constraints"""
    if not circuit_breaker["get_status"](table_name):
        AllureReporter.mark_circuit_breaker_failure(f"Metadata validation failed for {table_name}")
        pytest.skip(f"Circuit Breaker: Metadata validation failed for {table_name}")
    
    rules = VALIDATION_RULES[table_name]
    not_null_cols = rules.get("not_null", [])
    
    if not not_null_cols:
        pytest.skip(f"No NOT NULL validations configured for {table_name}")
    
    AllureReporter.add_table_metadata(table_name=table_name, table_type="structured", namespace="datalake.bronze")
    
    with allure.step(f"Read Iceberg table: local.datalake_bronze.{table_name}"):
        iceberg = IcebergManager(spark_session)
        df = iceberg.read_table("datalake_bronze", table_name, columns=not_null_cols)  # Column pruning
    
    with allure.step(f"Validate NOT NULL constraints for {len(not_null_cols)} columns"):
        results = data_validator.validate_not_null(df=df, columns=not_null_cols, table_name=table_name)
    
    AllureReporter.add_validation_summary(
        validation_type="NOT NULL", passed=results["passed"], total_rows=results["total_rows"],
        violations=results["total_violations"], threshold=data_validator.quality_threshold
    )
    
    AllureReporter.attach_validation_details(results, "NOT NULL Validation Results")
    
    if not results["passed"]:
        alert = DQReportFormatter.format_threshold_alert(
            table_name=table_name, violation_percentage=results["violation_percentage"],
            threshold=data_validator.quality_threshold, total_violations=results["total_violations"]
        )
        allure.attach(alert, name="THRESHOLD EXCEEDED", attachment_type=allure.attachment_type.TEXT)
        pytest.fail(f"NOT NULL validation failed: {results['violation_percentage']:.2%} exceeds threshold")
    
    assert results["passed"], "NOT NULL validation should pass"


@pytest.mark.data_quality
@pytest.mark.structured
@pytest.mark.parametrize("table_name", ["transactions", "user_profiles"])
def test_range_constraints(table_name, spark_session, data_validator, circuit_breaker):
    """Test: Range (min/max) constraints"""
    if not circuit_breaker["get_status"](table_name):
        pytest.skip(f"Circuit Breaker: Metadata validation failed for {table_name}")
    
    rules = VALIDATION_RULES[table_name]
    range_config = rules.get("range", {})
    
    if not range_config:
        pytest.skip(f"No range validations configured for {table_name}")
    
    AllureReporter.add_table_metadata(table_name=table_name, table_type="structured", namespace="datalake.bronze")
    
    columns_to_validate = list(range_config.keys())
    with allure.step(f"Read Iceberg table with column pruning ({len(columns_to_validate)} columns)"):
        iceberg = IcebergManager(spark_session)
        df = iceberg.read_table("datalake_bronze", table_name, columns=columns_to_validate)
    
    with allure.step(f"Validate range constraints for {len(range_config)} columns"):
        results = data_validator.validate_range(df=df, range_config=range_config, table_name=table_name)
    
    AllureReporter.add_validation_summary(
        validation_type="Range", passed=results["passed"], total_rows=results["total_rows"],
        violations=results["total_violations"], threshold=data_validator.quality_threshold
    )
    
    AllureReporter.attach_validation_details(results, "Range Validation Results")
    
    if not results["passed"]:
        alert = DQReportFormatter.format_threshold_alert(
            table_name=table_name, violation_percentage=results["violation_percentage"],
            threshold=data_validator.quality_threshold, total_violations=results["total_violations"]
        )
        allure.attach(alert, name="THRESHOLD EXCEEDED", attachment_type=allure.attachment_type.TEXT)
        pytest.fail(f"Range validation failed: {results['violation_percentage']:.2%} exceeds threshold")
    
    assert results["passed"], "Range validation should pass"


@pytest.mark.data_quality
@pytest.mark.structured
@pytest.mark.parametrize("table_name", ["transactions", "user_profiles"])
def test_regex_patterns(table_name, spark_session, data_validator, circuit_breaker):
    """Test: Regex pattern validation"""
    if not circuit_breaker["get_status"](table_name):
        pytest.skip(f"Circuit Breaker: Metadata validation failed for {table_name}")
    
    rules = VALIDATION_RULES[table_name]
    regex_config = rules.get("regex", {})
    
    if not regex_config:
        pytest.skip(f"No regex validations configured for {table_name}")
    
    AllureReporter.add_table_metadata(table_name=table_name, table_type="structured", namespace="datalake.bronze")
    
    columns_to_validate = list(regex_config.keys())
    with allure.step(f"Read Iceberg table with column pruning ({len(columns_to_validate)} columns)"):
        iceberg = IcebergManager(spark_session)
        df = iceberg.read_table("datalake_bronze", table_name, columns=columns_to_validate)
    
    with allure.step(f"Validate regex patterns for {len(regex_config)} columns"):
        results = data_validator.validate_regex(df=df, regex_config=regex_config, table_name=table_name)
    
    AllureReporter.add_validation_summary(
        validation_type="Regex", passed=results["passed"], total_rows=results["total_rows"],
        violations=results["total_violations"], threshold=data_validator.quality_threshold
    )
    
    AllureReporter.attach_validation_details(results, "Regex Validation Results")
    
    if not results["passed"]:
        alert = DQReportFormatter.format_threshold_alert(
            table_name=table_name, violation_percentage=results["violation_percentage"],
            threshold=data_validator.quality_threshold, total_violations=results["total_violations"]
        )
        allure.attach(alert, name="THRESHOLD EXCEEDED", attachment_type=allure.attachment_type.TEXT)
        pytest.fail(f"Regex validation failed: {results['violation_percentage']:.2%} exceeds threshold")
    
    assert results["passed"], "Regex validation should pass"


@pytest.mark.data_quality
@pytest.mark.structured
@pytest.mark.parametrize("table_name", ["transactions", "user_profiles"])
def test_cross_column_constraints(table_name, spark_session, data_validator, circuit_breaker):
    """Test: Cross-column validations (e.g., timestamp <= current_timestamp)"""
    if not circuit_breaker["get_status"](table_name):
        pytest.skip(f"Circuit Breaker: Metadata validation failed for {table_name}")
    
    rules = VALIDATION_RULES[table_name]
    cross_column_rules = rules.get("cross_column", [])
    
    if not cross_column_rules:
        pytest.skip(f"No cross-column validations configured for {table_name}")
    
    AllureReporter.add_table_metadata(table_name=table_name, table_type="structured", namespace="datalake.bronze")
    
    with allure.step(f"Read full Iceberg table for cross-column validation"):
        iceberg = IcebergManager(spark_session)
        df = iceberg.read_table("datalake_bronze", table_name)
    
    with allure.step(f"Validate {len(cross_column_rules)} cross-column rules"):
        results = data_validator.validate_cross_column(
            df=df, cross_column_rules=cross_column_rules, table_name=table_name
        )
    
    AllureReporter.add_validation_summary(
        validation_type="Cross-Column", passed=results["passed"], total_rows=results["total_rows"],
        violations=results["total_violations"], threshold=data_validator.quality_threshold
    )
    
    AllureReporter.attach_validation_details(results, "Cross-Column Validation Results")
    
    if not results["passed"]:
        alert = DQReportFormatter.format_threshold_alert(
            table_name=table_name, violation_percentage=results["violation_percentage"],
            threshold=data_validator.quality_threshold, total_violations=results["total_violations"]
        )
        allure.attach(alert, name="THRESHOLD EXCEEDED", attachment_type=allure.attachment_type.TEXT)
        pytest.fail(f"Cross-column validation failed: {results['violation_percentage']:.2%} exceeds threshold")
    
    assert results["passed"], "Cross-column validation should pass"

