"""
Unstructured data validation tests (JSON logs)
Tests file existence and required metadata keys
"""

import pytest
import allure
import os
import json
from dq_framework.reporters import AllureReporter


# JSON log file location
JSON_LOG_DIR = "/app/warehouse/unstructured/event_logs"

# Required metadata keys per schema_registry.json
REQUIRED_KEYS = ["event_id", "event_type", "event_time", "user_id", "payload"]


@pytest.mark.unstructured
def test_json_log_directory_exists():
    """Test: JSON log directory exists"""
    AllureReporter.add_table_metadata(
        table_name="event_logs",
        table_type="unstructured",
        namespace="datalake.raw"
    )
    
    with allure.step(f"Check if directory exists: {JSON_LOG_DIR}"):
        exists = os.path.exists(JSON_LOG_DIR) and os.path.isdir(JSON_LOG_DIR)
        allure.attach(
            f"Directory: {JSON_LOG_DIR}\nExists: {exists}",
            name="Directory Check",
            attachment_type=allure.attachment_type.TEXT
        )
        assert exists, f"JSON log directory not found: {JSON_LOG_DIR}"


@pytest.mark.unstructured
def test_json_files_exist():
    """Test: JSON files exist in directory"""
    with allure.step("Count JSON files"):
        if not os.path.exists(JSON_LOG_DIR):
            pytest.skip("JSON log directory not found")
        
        json_files = [f for f in os.listdir(JSON_LOG_DIR) if f.endswith('.json')]
        file_count = len(json_files)
        
        allure.attach(
            f"Directory: {JSON_LOG_DIR}\nJSON files: {file_count}\nSample: {json_files[:5]}",
            name="File Count",
            attachment_type=allure.attachment_type.TEXT
        )
        
        assert file_count > 0, "No JSON files found"


@pytest.mark.unstructured
def test_json_metadata_keys(circuit_breaker):
    """Test: JSON files have required metadata keys"""
    if not circuit_breaker["get_status"]("event_logs_metadata"):
        pytest.skip("Directory existence check failed")
    
    if not os.path.exists(JSON_LOG_DIR):
        pytest.skip("JSON log directory not found")
    
    json_files = [f for f in os.listdir(JSON_LOG_DIR) if f.endswith('.json')]
    if not json_files:
        pytest.skip("No JSON files to validate")
    
    total_files = len(json_files)
    invalid_files = 0
    invalid_details = []
    
    with allure.step(f"Validate {total_files} JSON files for required keys"):
        for filename in json_files:
            filepath = os.path.join(JSON_LOG_DIR, filename)
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)
                
                missing_keys = [key for key in REQUIRED_KEYS if key not in data]
                if missing_keys:
                    invalid_files += 1
                    invalid_details.append({
                        "file": filename,
                        "missing_keys": missing_keys
                    })
            except Exception as e:
                invalid_files += 1
                invalid_details.append({
                    "file": filename,
                    "error": str(e)
                })
        
        violation_percentage = invalid_files / total_files if total_files > 0 else 0
        threshold = 0.10
        passed = violation_percentage <= threshold
        
        report = f"""
        Total Files: {total_files}
        Invalid Files: {invalid_files}
        Violation %: {violation_percentage:.2%}
        Threshold: {threshold:.2%}
        Status: {'PASSED' if passed else 'FAILED'}
        
        Required Keys: {REQUIRED_KEYS}
        Invalid Details: {json.dumps(invalid_details[:10], indent=2)}
        """
        
        allure.attach(report, name="Metadata Validation Results", attachment_type=allure.attachment_type.TEXT)
        
        if not passed:
            pytest.fail(f"Metadata validation failed: {violation_percentage:.2%} exceeds threshold {threshold:.2%}")
        
        assert passed, "JSON metadata validation should pass"


@pytest.fixture(scope="session", autouse=True)
def setup_unstructured_circuit_breaker(circuit_breaker):
    """Set circuit breaker for unstructured tests"""
    # If directory exists, mark as ready for metadata validation
    if os.path.exists(JSON_LOG_DIR):
        circuit_breaker["set_status"]("event_logs_metadata", True)
    else:
        circuit_breaker["set_status"]("event_logs_metadata", False)

