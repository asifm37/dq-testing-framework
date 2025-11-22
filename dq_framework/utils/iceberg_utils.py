"""
Simple Iceberg utilities using Hadoop catalog (file-based)
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Dict, List, Optional


class IcebergManager:
    """Simple Iceberg table manager using Hadoop catalog"""
    
    def __init__(self, spark: SparkSession, warehouse_path: str = "/app/warehouse"):
        self.spark = spark
        self.warehouse_path = warehouse_path
        # Don't configure here - should be done before SparkSession creation
    
    def create_table(self, namespace: str, table_name: str, df: DataFrame, partition_cols: Optional[List[str]] = None):
        """Create Iceberg table from DataFrame"""
        full_name = f"local.{namespace}.{table_name}"
        
        # Create namespace if not exists
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{namespace}")
        
        # Drop table if exists
        self.spark.sql(f"DROP TABLE IF EXISTS {full_name}")
        
        # Write as Iceberg table
        writer = df.writeTo(full_name).using("iceberg")
        
        if partition_cols:
            writer = writer.partitionedBy(*partition_cols)
        
        writer.create()
    
    def append_data(self, namespace: str, table_name: str, df: DataFrame):
        """Append data to existing Iceberg table"""
        full_name = f"local.{namespace}.{table_name}"
        df.writeTo(full_name).append()
    
    def read_table(self, namespace: str, table_name: str, columns: Optional[List[str]] = None) -> DataFrame:
        """Read Iceberg table with optional column pruning"""
        full_name = f"local.{namespace}.{table_name}"
        df = self.spark.table(full_name)
        
        if columns:
            df = df.select(*columns)
        
        return df
    
    def table_exists(self, namespace: str, table_name: str) -> bool:
        """Check if Iceberg table exists"""
        full_name = f"local.{namespace}.{table_name}"
        try:
            self.spark.table(full_name)
            return True
        except:
            return False
    
    def get_schema(self, namespace: str, table_name: str) -> Dict:
        """Get table schema"""
        full_name = f"local.{namespace}.{table_name}"
        df = self.spark.table(full_name)
        return {field.name: str(field.dataType) for field in df.schema.fields}

