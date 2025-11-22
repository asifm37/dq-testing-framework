"""Allure reporting integration for DQ Framework"""

import os
import json
from typing import Dict, Any, List
from datetime import datetime
import allure


class AllureReporter:
    """
    Allure report utilities for Data Quality framework.
    
    Provides methods to:
    - Add custom metadata to reports
    - Attach validation results
    - Generate summary reports
    - Track historical trends
    """
    
    def __init__(self, results_dir: str = None):
        """
        Initialize Allure Reporter.
        
        Args:
            results_dir: Directory to store Allure results
        """
        self.results_dir = results_dir or os.environ.get(
            "ALLURE_RESULTS_DIR",
            "/app/reports/allure-results"
        )
        os.makedirs(self.results_dir, exist_ok=True)
    
    @staticmethod
    def add_environment_info(info: Dict[str, str]):
        """
        Add environment information to Allure report.
        
        Args:
            info: Dictionary of environment key-value pairs
        """
        for key, value in info.items():
            allure.dynamic.parameter(key, value)
    
    @staticmethod
    def add_table_metadata(
        table_name: str,
        table_type: str,
        namespace: str,
        incremental: bool = False
    ):
        """
        Add table metadata to test.
        
        Args:
            table_name: Name of the table
            table_type: Type (structured/unstructured)
            namespace: Database namespace
            incremental: Whether this is incremental validation
        """
        allure.dynamic.feature(f"Table: {table_name}")
        allure.dynamic.story(f"Namespace: {namespace}")
        allure.dynamic.tag(table_type)
        if incremental:
            allure.dynamic.tag("incremental")
    
    @staticmethod
    def add_validation_summary(
        validation_type: str,
        passed: bool,
        total_rows: int,
        violations: int,
        threshold: float
    ):
        """
        Add validation summary to report.
        
        Args:
            validation_type: Type of validation
            passed: Whether validation passed
            total_rows: Total rows validated
            violations: Number of violations
            threshold: Quality threshold
        """
        severity = allure.severity_level.CRITICAL if not passed else allure.severity_level.NORMAL
        allure.dynamic.severity(severity)
        
        summary = f"""
        Validation Type: {validation_type}
        Status: {'PASSED ✓' if passed else 'FAILED ✗'}
        Total Rows: {total_rows:,}
        Violations: {violations:,}
        Violation Rate: {(violations/total_rows*100) if total_rows > 0 else 0:.2f}%
        Threshold: {threshold*100:.2f}%
        """
        
        allure.attach(
            summary,
            name="Validation Summary",
            attachment_type=allure.attachment_type.TEXT
        )
    
    @staticmethod
    def attach_validation_details(results: Dict[str, Any], name: str = "Validation Details"):
        """
        Attach detailed validation results as JSON.
        
        Args:
            results: Validation results dictionary
            name: Attachment name
        """
        allure.attach(
            json.dumps(results, indent=2, default=str),
            name=name,
            attachment_type=allure.attachment_type.JSON
        )
    
    @staticmethod
    def attach_failed_records_sample(
        failed_records: List[Dict[str, Any]],
        limit: int = 100
    ):
        """
        Attach sample of failed records for debugging.
        
        Args:
            failed_records: List of failed record dictionaries
            limit: Maximum number of records to attach
        """
        sample = failed_records[:limit]
        allure.attach(
            json.dumps(sample, indent=2, default=str),
            name=f"Failed Records Sample (showing {len(sample)} of {len(failed_records)})",
            attachment_type=allure.attachment_type.JSON
        )
    
    @staticmethod
    def create_test_case(
        test_name: str,
        description: str,
        feature: str,
        story: str
    ):
        """
        Decorator to create a test case with metadata.
        
        Usage:
            @AllureReporter.create_test_case(...)
            def test_something():
                pass
        """
        def decorator(func):
            @allure.title(test_name)
            @allure.description(description)
            @allure.feature(feature)
            @allure.story(story)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return decorator
    
    @staticmethod
    def mark_circuit_breaker_failure(reason: str):
        """
        Mark a test as failed due to circuit breaker.
        
        Args:
            reason: Reason for circuit breaker activation
        """
        allure.dynamic.tag("circuit_breaker")
        allure.dynamic.severity(allure.severity_level.BLOCKER)
        allure.attach(
            f"Circuit Breaker Activated: {reason}\n\n"
            "Downstream validations were skipped to prevent data lake contamination.",
            name="Circuit Breaker Alert",
            attachment_type=allure.attachment_type.TEXT
        )
    
    @staticmethod
    def generate_summary_report(all_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate overall summary report from all validation results.
        
        Args:
            all_results: List of all validation results
        
        Returns:
            Summary dictionary
        """
        summary = {
            "timestamp": datetime.now().isoformat(),
            "total_validations": len(all_results),
            "passed": sum(1 for r in all_results if r.get("passed", False)),
            "failed": sum(1 for r in all_results if not r.get("passed", True)),
            "total_rows_validated": sum(r.get("total_rows", 0) for r in all_results),
            "total_violations": sum(r.get("total_violations", 0) for r in all_results),
            "validations_by_type": {},
            "tables_validated": list(set(r.get("table_name") for r in all_results if r.get("table_name")))
        }
        
        # Group by validation type
        for result in all_results:
            val_type = result.get("validation_type", "unknown")
            if val_type not in summary["validations_by_type"]:
                summary["validations_by_type"][val_type] = {
                    "count": 0,
                    "passed": 0,
                    "failed": 0
                }
            summary["validations_by_type"][val_type]["count"] += 1
            if result.get("passed", False):
                summary["validations_by_type"][val_type]["passed"] += 1
            else:
                summary["validations_by_type"][val_type]["failed"] += 1
        
        allure.attach(
            json.dumps(summary, indent=2),
            name="Overall Summary Report",
            attachment_type=allure.attachment_type.JSON
        )
        
        return summary
    
    def save_results_to_file(self, results: Dict[str, Any], filename: str):
        """
        Save validation results to a file in the results directory.
        
        Args:
            results: Results dictionary
            filename: Output filename
        """
        filepath = os.path.join(self.results_dir, filename)
        with open(filepath, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"✓ Saved results to: {filepath}")


class DQReportFormatter:
    """
    Formats DQ validation results for better readability in reports.
    """
    
    @staticmethod
    def format_threshold_alert(
        table_name: str,
        violation_percentage: float,
        threshold: float,
        total_violations: int
    ) -> str:
        """
        Format a threshold alert message.
        
        Args:
            table_name: Name of the table
            violation_percentage: Actual violation percentage
            threshold: Allowed threshold
            total_violations: Number of violations
        
        Returns:
            Formatted alert message
        """
        return f"""
        🚨 DATA QUALITY ALERT 🚨
        
        Table: {table_name}
        Status: FAILED - Threshold Exceeded
        
        Violation Rate: {violation_percentage:.2%}
        Allowed Threshold: {threshold:.2%}
        Total Violations: {total_violations:,}
        
        Action Required:
        - Pipeline has been stopped to prevent data lake contamination
        - Investigate root cause in source data
        - Fix data quality issues before re-running
        
        This alert was triggered by the Circuit Breaker pattern
        to protect downstream consumers from bad data.
        """
    
    @staticmethod
    def format_validation_passed(
        table_name: str,
        total_rows: int,
        validations_count: int
    ) -> str:
        """
        Format a success message.
        
        Args:
            table_name: Name of the table
            total_rows: Total rows validated
            validations_count: Number of validations passed
        
        Returns:
            Formatted success message
        """
        return f"""
        ✓ Data Quality Validation PASSED
        
        Table: {table_name}
        Total Rows: {total_rows:,}
        Validations Passed: {validations_count}
        
        All data quality checks passed successfully.
        Data is safe for downstream consumption.
        """

