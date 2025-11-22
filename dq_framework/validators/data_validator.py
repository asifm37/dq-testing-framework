"""Data quality validator with optimized column pruning and threshold checking"""

from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import re
import allure


class DataQualityValidator:
    """
    Performs data quality validations with optimization for large-scale tables.
    
    Features:
    - Column pruning: Only reads columns being validated
    - Threshold checking: Fails task if invalid rows exceed threshold
    - Incremental validation: Supports time-based filtering
    """
    
    def __init__(
        self,
        spark: SparkSession,
        quality_threshold: float = 0.10
    ):
        """
        Initialize Data Quality Validator.
        
        Args:
            spark: SparkSession
            quality_threshold: Max allowed % of invalid rows (0.10 = 10%)
        """
        self.spark = spark
        self.quality_threshold = quality_threshold
        self.validation_results = {}
    
    @allure.step("Validate NOT NULL constraints")
    def validate_not_null(
        self,
        df: DataFrame,
        columns: List[str],
        table_name: str
    ) -> Dict[str, Any]:
        """
        Validate NOT NULL constraints on specified columns.
        
        Only reads the columns being validated (column pruning).
        
        Args:
            df: DataFrame (can be pre-filtered for incremental)
            columns: List of columns that should not be null
            table_name: Table name for reporting
        
        Returns:
            Validation results dictionary
        """
        total_rows = df.count()
        results = {
            "validation_type": "not_null",
            "table_name": table_name,
            "total_rows": total_rows,
            "columns_validated": columns,
            "violations": {},
            "total_violations": 0,
            "violation_percentage": 0.0,
            "passed": True
        }
        
        if total_rows == 0:
            allure.attach(
                "No data to validate (empty DataFrame)",
                name=f"NOT NULL - {table_name}",
                attachment_type=allure.attachment_type.TEXT
            )
            return results
        
        # Check each column for nulls
        for col in columns:
            if col in df.columns:
                null_count = df.filter(F.col(col).isNull()).count()
                if null_count > 0:
                    results["violations"][col] = null_count
                    results["total_violations"] += null_count
        
        # Calculate violation percentage
        if results["total_violations"] > 0:
            results["violation_percentage"] = results["total_violations"] / (total_rows * len(columns))
            results["passed"] = results["violation_percentage"] <= self.quality_threshold
        
        # Attach to Allure report
        report_text = f"""
        Table: {table_name}
        Total Rows: {total_rows}
        Columns Validated: {columns}
        Violations: {results['violations']}
        Total Violations: {results['total_violations']}
        Violation %: {results['violation_percentage']:.2%}
        Threshold: {self.quality_threshold:.2%}
        Status: {'PASSED' if results['passed'] else 'FAILED'}
        """
        allure.attach(
            report_text,
            name=f"NOT NULL Validation - {table_name}",
            attachment_type=allure.attachment_type.TEXT
        )
        
        return results
    
    @allure.step("Validate range constraints")
    def validate_range(
        self,
        df: DataFrame,
        range_config: Dict[str, Dict[str, float]],
        table_name: str
    ) -> Dict[str, Any]:
        """
        Validate range constraints (min/max) on numeric columns.
        
        Args:
            df: DataFrame
            range_config: {column: {"min": val, "max": val}}
            table_name: Table name for reporting
        
        Returns:
            Validation results dictionary
        """
        total_rows = df.count()
        results = {
            "validation_type": "range",
            "table_name": table_name,
            "total_rows": total_rows,
            "columns_validated": list(range_config.keys()),
            "violations": {},
            "total_violations": 0,
            "violation_percentage": 0.0,
            "passed": True
        }
        
        if total_rows == 0:
            return results
        
        # Check range for each column
        for col, constraints in range_config.items():
            if col not in df.columns:
                continue
            
            min_val = constraints.get("min")
            max_val = constraints.get("max")
            
            # Build filter condition
            condition = None
            if min_val is not None and max_val is not None:
                condition = (F.col(col) < min_val) | (F.col(col) > max_val)
            elif min_val is not None:
                condition = F.col(col) < min_val
            elif max_val is not None:
                condition = F.col(col) > max_val
            
            if condition is not None:
                violation_count = df.filter(condition).count()
                if violation_count > 0:
                    results["violations"][col] = {
                        "count": violation_count,
                        "constraint": constraints
                    }
                    results["total_violations"] += violation_count
        
        # Calculate violation percentage
        if results["total_violations"] > 0:
            results["violation_percentage"] = results["total_violations"] / (total_rows * len(range_config))
            results["passed"] = results["violation_percentage"] <= self.quality_threshold
        
        # Attach to Allure report
        report_text = f"""
        Table: {table_name}
        Total Rows: {total_rows}
        Range Constraints: {range_config}
        Violations: {results['violations']}
        Total Violations: {results['total_violations']}
        Violation %: {results['violation_percentage']:.2%}
        Threshold: {self.quality_threshold:.2%}
        Status: {'PASSED' if results['passed'] else 'FAILED'}
        """
        allure.attach(
            report_text,
            name=f"Range Validation - {table_name}",
            attachment_type=allure.attachment_type.TEXT
        )
        
        return results
    
    @allure.step("Validate regex patterns")
    def validate_regex(
        self,
        df: DataFrame,
        regex_config: Dict[str, str],
        table_name: str
    ) -> Dict[str, Any]:
        """
        Validate regex patterns on string columns.
        
        Args:
            df: DataFrame
            regex_config: {column: "regex_pattern"}
            table_name: Table name for reporting
        
        Returns:
            Validation results dictionary
        """
        total_rows = df.count()
        results = {
            "validation_type": "regex",
            "table_name": table_name,
            "total_rows": total_rows,
            "columns_validated": list(regex_config.keys()),
            "violations": {},
            "total_violations": 0,
            "violation_percentage": 0.0,
            "passed": True
        }
        
        if total_rows == 0:
            return results
        
        # Check regex for each column
        for col, pattern in regex_config.items():
            if col not in df.columns:
                continue
            
            # Count rows that DON'T match the pattern
            violation_count = df.filter(
                ~F.col(col).rlike(pattern) & F.col(col).isNotNull()
            ).count()
            
            if violation_count > 0:
                results["violations"][col] = {
                    "count": violation_count,
                    "pattern": pattern
                }
                results["total_violations"] += violation_count
        
        # Calculate violation percentage
        if results["total_violations"] > 0:
            results["violation_percentage"] = results["total_violations"] / (total_rows * len(regex_config))
            results["passed"] = results["violation_percentage"] <= self.quality_threshold
        
        # Attach to Allure report
        report_text = f"""
        Table: {table_name}
        Total Rows: {total_rows}
        Regex Patterns: {regex_config}
        Violations: {results['violations']}
        Total Violations: {results['total_violations']}
        Violation %: {results['violation_percentage']:.2%}
        Threshold: {self.quality_threshold:.2%}
        Status: {'PASSED' if results['passed'] else 'FAILED'}
        """
        allure.attach(
            report_text,
            name=f"Regex Validation - {table_name}",
            attachment_type=allure.attachment_type.TEXT
        )
        
        return results
    
    @allure.step("Validate cross-column constraints")
    def validate_cross_column(
        self,
        df: DataFrame,
        cross_column_rules: List[Dict[str, str]],
        table_name: str
    ) -> Dict[str, Any]:
        """
        Validate cross-column constraints (e.g., col_a < col_b).
        
        Args:
            df: DataFrame
            cross_column_rules: List of rules with 'name', 'expression', 'description'
            table_name: Table name for reporting
        
        Returns:
            Validation results dictionary
        """
        total_rows = df.count()
        results = {
            "validation_type": "cross_column",
            "table_name": table_name,
            "total_rows": total_rows,
            "rules_validated": [r.get("name") for r in cross_column_rules],
            "violations": {},
            "total_violations": 0,
            "violation_percentage": 0.0,
            "passed": True
        }
        
        if total_rows == 0:
            return results
        
        # Evaluate each rule
        for rule in cross_column_rules:
            rule_name = rule.get("name", "unnamed_rule")
            expression = rule.get("expression")
            description = rule.get("description", "")
            
            if not expression:
                continue
            
            try:
                # Count rows that violate the rule (where expression is False)
                violation_count = df.filter(~F.expr(expression)).count()
                
                if violation_count > 0:
                    results["violations"][rule_name] = {
                        "count": violation_count,
                        "expression": expression,
                        "description": description
                    }
                    results["total_violations"] += violation_count
            except Exception as e:
                results["violations"][rule_name] = {
                    "error": str(e),
                    "expression": expression
                }
        
        # Calculate violation percentage
        if results["total_violations"] > 0:
            results["violation_percentage"] = results["total_violations"] / (total_rows * len(cross_column_rules))
            results["passed"] = results["violation_percentage"] <= self.quality_threshold
        
        # Attach to Allure report
        report_text = f"""
        Table: {table_name}
        Total Rows: {total_rows}
        Cross-Column Rules: {[r.get('name') for r in cross_column_rules]}
        Violations: {results['violations']}
        Total Violations: {results['total_violations']}
        Violation %: {results['violation_percentage']:.2%}
        Threshold: {self.quality_threshold:.2%}
        Status: {'PASSED' if results['passed'] else 'FAILED'}
        """
        allure.attach(
            report_text,
            name=f"Cross-Column Validation - {table_name}",
            attachment_type=allure.attachment_type.TEXT
        )
        
        return results
    
    def aggregate_results(self, results_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Aggregate multiple validation results.
        
        Args:
            results_list: List of validation result dictionaries
        
        Returns:
            Aggregated results
        """
        total_validations = len(results_list)
        passed_validations = sum(1 for r in results_list if r.get("passed", False))
        failed_validations = total_validations - passed_validations
        
        aggregated = {
            "total_validations": total_validations,
            "passed": passed_validations,
            "failed": failed_validations,
            "overall_passed": failed_validations == 0,
            "results": results_list
        }
        
        return aggregated

