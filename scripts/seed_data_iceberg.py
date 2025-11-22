#!/usr/bin/env python3
"""
Seed data generator for DQ Framework using Apache Iceberg tables
Generates: Structured (transactions, user_profiles) + Unstructured (JSON logs)
"""

import os
import sys
import json
import argparse
from datetime import datetime, timedelta
import random

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession, Row
from dq_framework.utils import IcebergManager


def get_spark_with_iceberg():
    """Create Spark session with Iceberg support"""
    spark = SparkSession.builder \
        .appName("DQ-Seed-Iceberg") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/app/warehouse") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def generate_transactions(num_rows, start_time=None, end_time=None):
    """Generate transactions with 95% valid, 5% invalid data"""
    data = []
    
    if not start_time:
        start_time = datetime.now() - timedelta(days=7)
    if not end_time:
        end_time = datetime.now()
    
    time_range = (end_time - start_time).total_seconds()
    statuses = ["COMPLETED", "PENDING", "FAILED", "REFUNDED"]
    
    for i in range(num_rows):
        ts = start_time + timedelta(seconds=random.uniform(0, time_range))
        uid = 1000000000 + i  # Generate 10-digit ID starting from 1000000000
        has_issue = random.random() < 0.05
        
        if has_issue:
            issue_type = random.choice(["null", "range", "regex", "future"])
            if issue_type == "null":
                row = Row(transaction_id=f"TXN{uid:010d}", user_id=None, amount=float(random.uniform(10, 1000)),
                         transaction_timestamp=ts, status=random.choice(statuses), merchant_id=f"M{random.randint(1000,9999)}", category="RETAIL")
            elif issue_type == "range":
                row = Row(transaction_id=f"TXN{uid:010d}", user_id=f"USR{random.randint(10000000,99999999)}",
                         amount=-50.0, transaction_timestamp=ts, status=random.choice(statuses),
                         merchant_id=f"M{random.randint(1000,9999)}", category="RETAIL")
            elif issue_type == "regex":
                # Only violate ONE column, not both
                if random.random() < 0.5:
                    row = Row(transaction_id=f"INVALID{uid}", user_id=f"USR{random.randint(10000000,99999999)}",
                             amount=float(random.uniform(10, 1000)), transaction_timestamp=ts, status=random.choice(statuses),
                             merchant_id=f"M{random.randint(1000,9999)}", category="RETAIL")
                else:
                    row = Row(transaction_id=f"TXN{uid:010d}", user_id=f"USR{random.randint(10000000,99999999)}",
                             amount=float(random.uniform(10, 1000)), transaction_timestamp=ts, status="UNKNOWN",
                             merchant_id=f"M{random.randint(1000,9999)}", category="RETAIL")
            else:
                row = Row(transaction_id=f"TXN{uid:010d}", user_id=f"USR{random.randint(10000000,99999999)}",
                         amount=float(random.uniform(10, 1000)), transaction_timestamp=datetime.now() + timedelta(days=365),
                         status=random.choice(statuses), merchant_id=f"M{random.randint(1000,9999)}", category="RETAIL")
        else:
            row = Row(transaction_id=f"TXN{uid:010d}", user_id=f"USR{random.randint(10000000,99999999)}",
                     amount=float(random.uniform(0.01, 10000)), transaction_timestamp=ts,
                     status=random.choice(statuses), merchant_id=f"M{random.randint(1000,9999)}", category="RETAIL")
        
        data.append(row)
    
    return data


def generate_users(num_rows, start_time=None, end_time=None):
    """Generate user profiles with 95% valid, 5% invalid data"""
    data = []
    
    if not start_time:
        start_time = datetime.now() - timedelta(hours=1)
    if not end_time:
        end_time = datetime.now()
    
    time_range = (end_time - start_time).total_seconds()
    base_date = datetime.now() - timedelta(days=365)
    
    for i in range(num_rows):
        ts = start_time + timedelta(seconds=random.uniform(0, time_range))
        uid = 10000000 + i  # Generate 8-digit ID starting from 10000000
        has_issue = random.random() < 0.05
        
        if has_issue:
            issue_type = random.choice(["null", "range_age", "range_balance", "regex"])
            if issue_type == "null":
                row = Row(user_id=f"USR{uid:08d}", email=None, age=random.randint(18,80),
                         registration_date=(base_date + timedelta(days=i%365)).date(),
                         account_balance=float(random.uniform(0,10000)), is_active=True, updated_at=ts)
            elif issue_type == "range_age":
                row = Row(user_id=f"USR{uid:08d}", email=f"user{uid}@example.com", age=150,
                         registration_date=(base_date + timedelta(days=i%365)).date(),
                         account_balance=float(random.uniform(0,10000)), is_active=True, updated_at=ts)
            elif issue_type == "range_balance":
                row = Row(user_id=f"USR{uid:08d}", email=f"user{uid}@example.com", age=random.randint(18,80),
                         registration_date=(base_date + timedelta(days=i%365)).date(),
                         account_balance=-1000.0, is_active=True, updated_at=ts)
            else:  # regex
                # Only violate ONE column, not both
                if random.random() < 0.5:
                    row = Row(user_id=f"INVALID{uid}", email=f"user{uid}@example.com", age=random.randint(18,80),
                             registration_date=(base_date + timedelta(days=i%365)).date(),
                             account_balance=float(random.uniform(0,10000)), is_active=True, updated_at=ts)
                else:
                    row = Row(user_id=f"USR{uid:08d}", email="not-email", age=random.randint(18,80),
                             registration_date=(base_date + timedelta(days=i%365)).date(),
                             account_balance=float(random.uniform(0,10000)), is_active=True, updated_at=ts)
        else:
            row = Row(user_id=f"USR{uid:08d}", email=f"user{uid}@example.com", age=random.randint(18,80),
                     registration_date=(base_date + timedelta(days=i%365)).date(),
                     account_balance=float(random.uniform(0,50000)), is_active=random.choice([True,False]), updated_at=ts)
        
        data.append(row)
    
    return data


def generate_json_logs(num_files, output_dir):
    """Generate JSON log files for unstructured validation"""
    os.makedirs(output_dir, exist_ok=True)
    
    for i in range(num_files):
        has_issue = random.random() < 0.05
        
        if has_issue:
            # Missing required keys or invalid format
            log_data = {
                "event_id": f"EVT{i:06d}",
                # Missing event_type, event_time, user_id, payload
            }
        else:
            log_data = {
                "event_id": f"EVT{i:06d}",
                "event_type": random.choice(["page_view", "click", "purchase", "logout"]),
                "event_time": (datetime.now() - timedelta(hours=random.randint(0,24))).isoformat(),
                "user_id": f"USR{random.randint(10000000,99999999)}",
                "payload": {"key": f"value{i}"}
            }
        
        filepath = os.path.join(output_dir, f"event_{i:06d}.json")
        with open(filepath, 'w') as f:
            json.dump(log_data, f)


def main():
    parser = argparse.ArgumentParser(description='Iceberg DQ Data Generator')
    parser.add_argument('--mode', choices=['initial', 'append'], default='initial')
    parser.add_argument('--start-time', type=str)
    parser.add_argument('--end-time', type=str)
    parser.add_argument('--num-transactions', type=int, default=None)
    parser.add_argument('--num-users', type=int, default=None)
    parser.add_argument('--num-logs', type=int, default=None)
    
    args = parser.parse_args()
    
    start_time = datetime.strptime(args.start_time, "%Y-%m-%d %H:%M:%S") if args.start_time else None
    end_time = datetime.strptime(args.end_time, "%Y-%m-%d %H:%M:%S") if args.end_time else None
    
    if args.mode == 'initial':
        num_txn = args.num_transactions or 1000
        num_usr = args.num_users or 500
        num_log = args.num_logs or 100
    else:
        num_txn = args.num_transactions or 100
        num_usr = args.num_users or 50
        num_log = args.num_logs or 10
    
    print(f"=== Iceberg Data Generator ({args.mode}) ===")
    print(f"Transactions: {num_txn}, Users: {num_usr}, Logs: {num_log}")
    
    spark = get_spark_with_iceberg()
    iceberg = IcebergManager(spark)
    
    # Generate structured data
    print("\n[1/3] Generating transactions...")
    txn_data = generate_transactions(num_txn, start_time, end_time)
    txn_df = spark.createDataFrame(txn_data)
    
    if args.mode == 'initial':
        iceberg.create_table("datalake_bronze", "transactions", txn_df)
        print(f"✓ Created Iceberg table: local.datalake_bronze.transactions")
    else:
        iceberg.append_data("datalake_bronze", "transactions", txn_df)
        print(f"✓ Appended to Iceberg table")
    
    print("\n[2/3] Generating user profiles...")
    usr_data = generate_users(num_usr, start_time, end_time)
    usr_df = spark.createDataFrame(usr_data)
    
    if args.mode == 'initial':
        iceberg.create_table("datalake_bronze", "user_profiles", usr_df)
        print(f"✓ Created Iceberg table: local.datalake_bronze.user_profiles")
    else:
        iceberg.append_data("datalake_bronze", "user_profiles", usr_df)
        print(f"✓ Appended to Iceberg table")
    
    # Generate unstructured data (JSON logs)
    print("\n[3/3] Generating JSON logs...")
    log_dir = "/app/warehouse/unstructured/event_logs"
    generate_json_logs(num_log, log_dir)
    print(f"✓ Generated {num_log} JSON log files")
    
    print("\n=== Complete ===")
    print(f"Structured: Iceberg tables in /app/warehouse")
    print(f"Unstructured: JSON logs in {log_dir}")
    
    spark.stop()


if __name__ == "__main__":
    main()

