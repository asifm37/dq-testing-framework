"""Iceberg utilities for DQ Framework"""

import os
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime


class IcebergManager:
    """
    Manager for Iceberg table operations.
    
    Handles:
    - Table creation with proper schema
    - Data insertion
    - Incremental reads
    - Metadata operations
    """
    
    def __init__(self, spark: SparkSession, warehouse_path: str = None):
        """
        Initialize Iceberg Manager.
        
        Args:
            spark: SparkSession with Iceberg configured
            warehouse_path: Path to Iceberg warehouse (defaults to env var)
        """
        self.spark = spark
        self.warehouse_path = warehouse_path or os.environ.get(
            "ICEBERG_WAREHOUSE_PATH",
            "s3a://datalake/warehouse"
        )
    
    def create_namespace(self, namespace: str):
        """Create Iceberg namespace if it doesn't exist"""
        try:
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{namespace}")
            return True
        except Exception as e:
            print(f"Error creating namespace {namespace}: {e}")
            return False
    
    def create_table(
        self,
        namespace: str,
        table_name: str,
        schema: Dict[str, str],
        partition_by: Optional[list] = None
    ) -> bool:
        """
        Create an Iceberg table with specified schema.
        
        Args:
            namespace: Database/namespace name
            table_name: Table name
            schema: Dictionary mapping column names to types
            partition_by: Optional list of partition columns
        
        Returns:
            True if successful, False otherwise
        """
        full_table_name = f"local.{namespace}.{table_name}"
        
        # Build schema string
        schema_str = ", ".join([f"{col} {dtype}" for col, dtype in schema.items()])
        
        # Build partition clause
        partition_clause = ""
        if partition_by:
            partition_clause = f"PARTITIONED BY ({', '.join(partition_by)})"
        
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                {schema_str}
            )
            USING iceberg
            {partition_clause}
        """
        
        try:
            self.spark.sql(create_sql)
            print(f"✓ Created/verified table: {full_table_name}")
            return True
        except Exception as e:
            print(f"✗ Error creating table {full_table_name}: {e}")
            return False
    
    def write_data(
        self,
        df: DataFrame,
        namespace: str,
        table_name: str,
        mode: str = "append"
    ) -> bool:
        """
        Write data to Iceberg table.
        
        Args:
            df: DataFrame to write
            namespace: Database/namespace name
            table_name: Table name
            mode: Write mode (append, overwrite)
        
        Returns:
            True if successful, False otherwise
        """
        full_table_name = f"local.{namespace}.{table_name}"
        
        try:
            df.writeTo(full_table_name).using("iceberg").mode(mode).create()
            print(f"✓ Written data to: {full_table_name}")
            return True
        except Exception as e:
            # Table might already exist, try regular write
            try:
                df.writeTo(full_table_name).mode(mode).append()
                print(f"✓ Appended data to: {full_table_name}")
                return True
            except Exception as e2:
                print(f"✗ Error writing to {full_table_name}: {e2}")
                return False
    
    def read_table(
        self,
        namespace: str,
        table_name: str,
        columns: Optional[list] = None,
        filter_condition: Optional[str] = None
    ) -> Optional[DataFrame]:
        """
        Read from Iceberg table with optional column pruning and filtering.
        
        Args:
            namespace: Database/namespace name
            table_name: Table name
            columns: Optional list of columns to read (column pruning)
            filter_condition: Optional SQL filter condition
        
        Returns:
            DataFrame or None if error
        """
        full_table_name = f"local.{namespace}.{table_name}"
        
        try:
            df = self.spark.table(full_table_name)
            
            # Column pruning
            if columns:
                df = df.select(*columns)
            
            # Predicate pushdown
            if filter_condition:
                df = df.filter(filter_condition)
            
            return df
        except Exception as e:
            print(f"✗ Error reading {full_table_name}: {e}")
            return None
    
    def table_exists(self, namespace: str, table_name: str) -> bool:
        """Check if an Iceberg table exists"""
        full_table_name = f"local.{namespace}.{table_name}"
        
        try:
            self.spark.table(full_table_name)
            return True
        except Exception:
            return False
    
    def get_table_schema(self, namespace: str, table_name: str) -> Optional[Dict[str, str]]:
        """Get table schema as dictionary"""
        full_table_name = f"local.{namespace}.{table_name}"
        
        try:
            df = self.spark.table(full_table_name)
            return {field.name: str(field.dataType) for field in df.schema.fields}
        except Exception as e:
            print(f"✗ Error getting schema for {full_table_name}: {e}")
            return None
    
    def get_table_count(self, namespace: str, table_name: str) -> int:
        """Get total row count for a table"""
        full_table_name = f"local.{namespace}.{table_name}"
        
        try:
            return self.spark.table(full_table_name).count()
        except Exception:
            return 0
    
    def get_incremental_data(
        self,
        namespace: str,
        table_name: str,
        timestamp_column: str,
        start_time: str,
        end_time: str,
        columns: Optional[list] = None
    ) -> Optional[DataFrame]:
        """
        Read incremental data based on timestamp range.
        
        Args:
            namespace: Database/namespace name
            table_name: Table name
            timestamp_column: Timestamp column for filtering
            start_time: Start timestamp (ISO format)
            end_time: End timestamp (ISO format)
            columns: Optional columns for pruning
        
        Returns:
            DataFrame with incremental data
        """
        filter_condition = (
            f"{timestamp_column} >= timestamp('{start_time}') AND "
            f"{timestamp_column} < timestamp('{end_time}')"
        )
        
        return self.read_table(
            namespace=namespace,
            table_name=table_name,
            columns=columns,
            filter_condition=filter_condition
        )

