"""Metadata validator with circuit breaker pattern"""

from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
import allure


class MetadataValidator:
    """
    Validates table metadata (existence, schema).
    
    Implements Circuit Breaker Pattern:
    - If metadata validation fails, downstream data validation should NOT run
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.validation_results = {}
    
    @allure.step("Check table existence")
    def check_table_exists(
        self,
        namespace: str,
        table_name: str
    ) -> bool:
        """
        Check if table exists in the catalog.
        
        Args:
            namespace: Database/namespace name
            table_name: Table name
        
        Returns:
            True if table exists, False otherwise
        """
        full_table_name = f"local.{namespace}.{table_name}"
        
        try:
            self.spark.table(full_table_name)
            allure.attach(
                f"Table {full_table_name} exists",
                name="Table Existence",
                attachment_type=allure.attachment_type.TEXT
            )
            return True
        except Exception as e:
            allure.attach(
                f"Table {full_table_name} does NOT exist: {str(e)}",
                name="Table Existence Error",
                attachment_type=allure.attachment_type.TEXT
            )
            return False
    
    @allure.step("Validate table schema")
    def validate_schema(
        self,
        namespace: str,
        table_name: str,
        expected_schema: Dict[str, str],
        strict_mode: bool = True
    ) -> tuple[bool, list]:
        """
        Validate table schema against expected schema.
        
        Args:
            namespace: Database/namespace name
            table_name: Table name
            expected_schema: Expected schema as {column: type}
            strict_mode: If True, fail on extra/missing columns
        
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        full_table_name = f"local.{namespace}.{table_name}"
        errors = []
        
        try:
            df = self.spark.table(full_table_name)
            actual_schema = {field.name: str(field.dataType) for field in df.schema.fields}
            
            # Check for missing columns
            missing_cols = set(expected_schema.keys()) - set(actual_schema.keys())
            if missing_cols:
                error_msg = f"Missing columns: {missing_cols}"
                errors.append(error_msg)
            
            # Check for extra columns (only in strict mode)
            if strict_mode:
                extra_cols = set(actual_schema.keys()) - set(expected_schema.keys())
                if extra_cols:
                    error_msg = f"Extra columns: {extra_cols}"
                    errors.append(error_msg)
            
            # Check data types for common columns
            for col, expected_type in expected_schema.items():
                if col in actual_schema:
                    actual_type = actual_schema[col]
                    # Normalize type comparison (e.g., DecimalType(10,2) vs decimal(10,2))
                    if not self._types_compatible(expected_type, actual_type):
                        error_msg = f"Column '{col}': expected {expected_type}, got {actual_type}"
                        errors.append(error_msg)
            
            is_valid = len(errors) == 0
            
            # Attach to Allure report
            schema_comparison = f"""
            Expected Schema: {expected_schema}
            Actual Schema: {actual_schema}
            Errors: {errors if errors else 'None'}
            """
            allure.attach(
                schema_comparison,
                name="Schema Validation",
                attachment_type=allure.attachment_type.TEXT
            )
            
            return is_valid, errors
            
        except Exception as e:
            error_msg = f"Error validating schema: {str(e)}"
            errors.append(error_msg)
            allure.attach(
                error_msg,
                name="Schema Validation Error",
                attachment_type=allure.attachment_type.TEXT
            )
            return False, errors
    
    def _types_compatible(self, expected: str, actual: str) -> bool:
        """
        Check if two type strings are compatible.
        Handles variations like 'StringType()' vs 'string', 'DecimalType(10,2)' vs 'decimal(10,2)'
        """
        # Normalize types to lowercase
        expected_norm = expected.lower().replace("type()", "").replace("()", "")
        actual_norm = actual.lower().replace("type()", "").replace("()", "")
        
        # Simple string contains check (can be enhanced)
        if expected_norm in actual_norm or actual_norm in expected_norm:
            return True
        
        # Handle decimal types specially
        if "decimal" in expected_norm and "decimal" in actual_norm:
            return True
        
        # Handle timestamp/date types
        if expected_norm in ["timestamp", "date"] and actual_norm in ["timestamptype", "datetype", "timestamp", "date"]:
            return True
        
        return expected_norm == actual_norm
    
    @allure.step("Get table row count")
    def get_row_count(
        self,
        namespace: str,
        table_name: str,
        filter_condition: Optional[str] = None
    ) -> int:
        """
        Get row count for table (optionally with filter).
        
        Args:
            namespace: Database/namespace name
            table_name: Table name
            filter_condition: Optional SQL filter
        
        Returns:
            Row count
        """
        full_table_name = f"local.{namespace}.{table_name}"
        
        try:
            df = self.spark.table(full_table_name)
            if filter_condition:
                df = df.filter(filter_condition)
            count = df.count()
            
            allure.attach(
                f"Row count: {count}",
                name="Table Row Count",
                attachment_type=allure.attachment_type.TEXT
            )
            
            return count
        except Exception as e:
            allure.attach(
                f"Error getting row count: {str(e)}",
                name="Row Count Error",
                attachment_type=allure.attachment_type.TEXT
            )
            return 0


class UnstructuredMetadataValidator:
    """
    Validates metadata for unstructured data (files, blobs).
    
    Checks:
    - File existence
    - Required metadata keys
    - File size constraints
    - File extensions
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    @allure.step("Validate file metadata")
    def validate_file_metadata(
        self,
        metadata_df: DataFrame,
        required_keys: list,
        file_size_min: Optional[int] = None,
        file_size_max: Optional[int] = None,
        allowed_extensions: Optional[list] = None
    ) -> tuple[bool, Dict[str, Any]]:
        """
        Validate metadata for unstructured files.
        
        Args:
            metadata_df: DataFrame containing file metadata
            required_keys: List of required metadata columns
            file_size_min: Minimum file size in bytes
            file_size_max: Maximum file size in bytes
            allowed_extensions: List of allowed file extensions
        
        Returns:
            Tuple of (is_valid, validation_report)
        """
        report = {
            "total_files": 0,
            "missing_keys": [],
            "size_violations": 0,
            "extension_violations": 0,
            "errors": []
        }
        
        try:
            report["total_files"] = metadata_df.count()
            
            # Check required keys (columns)
            actual_columns = set(metadata_df.columns)
            missing_keys = set(required_keys) - actual_columns
            if missing_keys:
                report["missing_keys"] = list(missing_keys)
                report["errors"].append(f"Missing metadata keys: {missing_keys}")
            
            # Check file sizes if constraints provided
            if file_size_min or file_size_max:
                if "file_size" in actual_columns:
                    size_violations = metadata_df.filter(
                        (metadata_df.file_size < file_size_min) |
                        (metadata_df.file_size > file_size_max)
                    ).count()
                    report["size_violations"] = size_violations
                    if size_violations > 0:
                        report["errors"].append(f"{size_violations} files violate size constraints")
            
            # Check file extensions if provided
            if allowed_extensions and "file_path" in actual_columns:
                # This is a simplified check; enhance as needed
                report["extension_violations"] = 0  # Implement actual check
            
            is_valid = len(report["errors"]) == 0
            
            allure.attach(
                str(report),
                name="Unstructured Metadata Validation",
                attachment_type=allure.attachment_type.JSON
            )
            
            return is_valid, report
            
        except Exception as e:
            report["errors"].append(f"Validation error: {str(e)}")
            allure.attach(
                str(report),
                name="Unstructured Metadata Validation Error",
                attachment_type=allure.attachment_type.TEXT
            )
            return False, report

