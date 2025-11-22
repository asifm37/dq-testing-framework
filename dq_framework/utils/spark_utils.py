"""Spark utilities for DQ Framework with optimization for column pruning"""

import os
from typing import Optional, List
from pyspark.sql import SparkSession
from pyspark import SparkConf


def get_spark_session(
    app_name: str = "DQ-Framework",
    extra_configs: Optional[dict] = None
) -> SparkSession:
    """
    Create and configure a Spark session optimized for data quality checks.
    
    Includes:
    - Iceberg integration
    - S3/MinIO support
    - Parquet optimization (column pruning, vectorized reader)
    - Adaptive query execution
    
    Args:
        app_name: Name of the Spark application
        extra_configs: Additional Spark configurations
    
    Returns:
        Configured SparkSession
    """
    
    # Base configuration
    conf = SparkConf()
    conf.setAppName(app_name)
    
    # MinIO/S3 Configuration
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    
    base_configs = {
        # AWS/S3 Configuration for MinIO
        "spark.hadoop.fs.s3a.endpoint": f"http://{minio_endpoint}",
        "spark.hadoop.fs.s3a.access.key": access_key,
        "spark.hadoop.fs.s3a.secret.key": secret_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        
        # Iceberg Configuration
        "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.local.type": "hadoop",
        "spark.sql.catalog.local.warehouse": "file:///app/warehouse",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": "local",
        
        # Parquet Optimization - Column Pruning & Vectorization
        "spark.sql.parquet.enableVectorizedReader": "true",
        "spark.sql.parquet.columnarReaderBatchSize": "4096",
        "spark.sql.parquet.filterPushdown": "true",
        "spark.sql.parquet.mergeSchema": "false",
        
        # Adaptive Query Execution
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        
        # Performance tuning
        "spark.sql.shuffle.partitions": "10",
        "spark.default.parallelism": "8",
        "spark.driver.memory": "2g",
        "spark.executor.memory": "4g",
        
        # Broadcast optimization
        "spark.sql.autoBroadcastJoinThreshold": "10485760",  # 10MB
    }
    
    # Merge with extra configs
    if extra_configs:
        base_configs.update(extra_configs)
    
    # Set all configurations
    for key, value in base_configs.items():
        conf.set(key, value)
    
    # Create Spark session
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def optimize_column_read(
    spark: SparkSession,
    table_path: str,
    columns: List[str],
    filter_condition: Optional[str] = None
):
    """
    Read only specified columns from a table using Parquet column pruning.
    
    This is optimized for reading only the columns needed for validation,
    significantly reducing I/O for tables with many columns.
    
    Args:
        spark: SparkSession
        table_path: Path to the table (Iceberg or Parquet)
        columns: List of column names to read
        filter_condition: Optional SQL filter condition
    
    Returns:
        DataFrame with only specified columns
    """
    # Read with column pruning (Spark will only read specified columns from Parquet)
    df = spark.read.format("iceberg").load(table_path).select(*columns)
    
    # Apply filter if provided (predicate pushdown)
    if filter_condition:
        df = df.filter(filter_condition)
    
    return df


def get_incremental_data(
    spark: SparkSession,
    table_path: str,
    timestamp_column: str,
    start_time: str,
    end_time: str,
    columns: Optional[List[str]] = None
):
    """
    Read incremental data based on timestamp range with column pruning.
    
    Optimized for processing only new data in hourly/batch schedules.
    
    Args:
        spark: SparkSession
        table_path: Path to the Iceberg table
        timestamp_column: Name of the timestamp column
        start_time: Start timestamp (ISO format)
        end_time: End timestamp (ISO format)
        columns: Optional list of columns to read (None = all columns)
    
    Returns:
        DataFrame with incremental data
    """
    df = spark.read.format("iceberg").load(table_path)
    
    # Apply time filter (predicate pushdown to Iceberg)
    df = df.filter(
        f"{timestamp_column} >= timestamp('{start_time}') AND "
        f"{timestamp_column} < timestamp('{end_time}')"
    )
    
    # Apply column pruning if specified
    if columns:
        df = df.select(*columns)
    
    return df

