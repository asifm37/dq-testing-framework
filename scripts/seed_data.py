#!/usr/bin/env python3
"""
Seed Data Generator for DQ Framework

Generates test data with:
- Positive cases (valid data)
- Negative cases (invalid data to trigger DQ failures)
- Edge cases (boundary conditions)

This script creates Iceberg tables with intentional data quality issues
to demonstrate the framework's validation capabilities.
"""

import os
import sys
from datetime import datetime, timedelta
from decimal import Decimal
import random

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import Row
from dq_framework.utils import get_spark_session, IcebergManager
from dq_framework.config import get_config_manager


def generate_transactions_data(num_rows: int = 1000, include_issues: bool = True):
    """
    Generate transactions data with positive, negative, and edge cases.
    
    Args:
        num_rows: Number of rows to generate
        include_issues: If True, include ~5% invalid data
    
    Returns:
        List of Row objects
    """
    data = []
    base_time = datetime.now() - timedelta(days=7)
    
    statuses = ["COMPLETED", "PENDING", "FAILED", "REFUNDED"]
    categories = ["RETAIL", "FOOD", "TRAVEL", "ENTERTAINMENT", "UTILITIES"]
    
    for i in range(num_rows):
        # Decide if this row should have issues (5% of rows)
        has_issue = include_issues and random.random() < 0.05
        
        if has_issue:
            # Generate row with intentional issues
            issue_type = random.choice(["null", "range", "regex", "future_date"])
            
            if issue_type == "null":
                row = Row(
                    transaction_id=f"TXN{i:010d}",
                    user_id=None,  # NULL violation
                    amount=Decimal(f"{random.uniform(10, 1000):.2f}"),
                    transaction_timestamp=base_time + timedelta(hours=i % 168),
                    status=random.choice(statuses),
                    merchant_id=f"MERCH{random.randint(1000, 9999)}",
                    category=random.choice(categories)
                )
            elif issue_type == "range":
                row = Row(
                    transaction_id=f"TXN{i:010d}",
                    user_id=f"USR{random.randint(10000000, 99999999)}",
                    amount=Decimal("-50.00"),  # Negative amount (range violation)
                    transaction_timestamp=base_time + timedelta(hours=i % 168),
                    status=random.choice(statuses),
                    merchant_id=f"MERCH{random.randint(1000, 9999)}",
                    category=random.choice(categories)
                )
            elif issue_type == "regex":
                row = Row(
                    transaction_id=f"INVALID{i}",  # Regex violation
                    user_id=f"USR{random.randint(10000000, 99999999)}",
                    amount=Decimal(f"{random.uniform(10, 1000):.2f}"),
                    transaction_timestamp=base_time + timedelta(hours=i % 168),
                    status="UNKNOWN",  # Invalid status (regex violation)
                    merchant_id=f"MERCH{random.randint(1000, 9999)}",
                    category=random.choice(categories)
                )
            else:  # future_date
                row = Row(
                    transaction_id=f"TXN{i:010d}",
                    user_id=f"USR{random.randint(10000000, 99999999)}",
                    amount=Decimal(f"{random.uniform(10, 1000):.2f}"),
                    transaction_timestamp=datetime.now() + timedelta(days=365),  # Future date
                    status=random.choice(statuses),
                    merchant_id=f"MERCH{random.randint(1000, 9999)}",
                    category=random.choice(categories)
                )
        else:
            # Generate valid row
            row = Row(
                transaction_id=f"TXN{i:010d}",
                user_id=f"USR{random.randint(10000000, 99999999)}",
                amount=Decimal(f"{random.uniform(0.01, 10000):.2f}"),
                transaction_timestamp=base_time + timedelta(hours=i % 168),
                status=random.choice(statuses),
                merchant_id=f"MERCH{random.randint(1000, 9999)}",
                category=random.choice(categories)
            )
        
        data.append(row)
    
    return data


def generate_user_profiles_data(num_rows: int = 500, include_issues: bool = True):
    """
    Generate user profiles data with positive, negative, and edge cases.
    
    Args:
        num_rows: Number of rows to generate
        include_issues: If True, include ~5% invalid data
    
    Returns:
        List of Row objects
    """
    data = []
    base_date = datetime.now() - timedelta(days=365)
    
    for i in range(num_rows):
        # Decide if this row should have issues (5% of rows)
        has_issue = include_issues and random.random() < 0.05
        
        if has_issue:
            issue_type = random.choice(["null", "range_age", "range_balance", "regex", "future_date"])
            
            if issue_type == "null":
                row = Row(
                    user_id=f"USR{i:08d}",
                    email=None,  # NULL violation
                    age=random.randint(18, 80),
                    registration_date=(base_date + timedelta(days=i % 365)).date(),
                    account_balance=Decimal(f"{random.uniform(0, 10000):.2f}"),
                    is_active=True,
                    updated_at=datetime.now()
                )
            elif issue_type == "range_age":
                row = Row(
                    user_id=f"USR{i:08d}",
                    email=f"user{i}@example.com",
                    age=150,  # Age too high (range violation)
                    registration_date=(base_date + timedelta(days=i % 365)).date(),
                    account_balance=Decimal(f"{random.uniform(0, 10000):.2f}"),
                    is_active=True,
                    updated_at=datetime.now()
                )
            elif issue_type == "range_balance":
                row = Row(
                    user_id=f"USR{i:08d}",
                    email=f"user{i}@example.com",
                    age=random.randint(18, 80),
                    registration_date=(base_date + timedelta(days=i % 365)).date(),
                    account_balance=Decimal("-1000.00"),  # Negative balance (range violation)
                    is_active=True,
                    updated_at=datetime.now()
                )
            elif issue_type == "regex":
                row = Row(
                    user_id=f"INVALID_{i}",  # Invalid user_id format (regex violation)
                    email="not-an-email",  # Invalid email format (regex violation)
                    age=random.randint(18, 80),
                    registration_date=(base_date + timedelta(days=i % 365)).date(),
                    account_balance=Decimal(f"{random.uniform(0, 10000):.2f}"),
                    is_active=True,
                    updated_at=datetime.now()
                )
            else:  # future_date
                row = Row(
                    user_id=f"USR{i:08d}",
                    email=f"user{i}@example.com",
                    age=random.randint(18, 80),
                    registration_date=(datetime.now() + timedelta(days=365)).date(),  # Future date
                    account_balance=Decimal(f"{random.uniform(0, 10000):.2f}"),
                    is_active=True,
                    updated_at=datetime.now()
                )
        else:
            # Generate valid row
            row = Row(
                user_id=f"USR{i:08d}",
                email=f"user{i}@example.com",
                age=random.randint(18, 80),
                registration_date=(base_date + timedelta(days=i % 365)).date(),
                account_balance=Decimal(f"{random.uniform(0, 50000):.2f}"),
                is_active=random.choice([True, False]),
                updated_at=datetime.now()
            )
        
        data.append(row)
    
    return data


def main():
    """Main function to seed all tables"""
    print("=" * 80)
    print("DQ Framework - Seed Data Generator")
    print("=" * 80)
    
    # Initialize Spark and managers
    print("\n[1/5] Initializing Spark session...")
    spark = get_spark_session(app_name="DQ-Seed-Data")
    
    print("[2/5] Initializing Iceberg manager...")
    iceberg_mgr = IcebergManager(spark)
    config_mgr = get_config_manager()
    
    # Create namespaces
    print("[3/5] Creating Iceberg namespaces...")
    iceberg_mgr.create_namespace("datalake")
    iceberg_mgr.create_namespace("datalake.bronze")
    iceberg_mgr.create_namespace("datalake.raw")
    
    # Seed transactions table
    print("\n[4/5] Seeding transactions table...")
    print("  - Generating 1000 rows (95% valid, 5% invalid)...")
    transactions_data = generate_transactions_data(num_rows=1000, include_issues=True)
    transactions_df = spark.createDataFrame(transactions_data)
    
    print("  - Creating Iceberg table...")
    table_config = config_mgr.get_table_config("transactions")
    iceberg_mgr.create_table(
        namespace="datalake.bronze",
        table_name="transactions",
        schema=table_config["schema"],
        partition_by=["status"]
    )
    
    print("  - Writing data to Iceberg...")
    iceberg_mgr.write_data(
        df=transactions_df,
        namespace="datalake.bronze",
        table_name="transactions",
        mode="overwrite"
    )
    
    row_count = iceberg_mgr.get_table_count("datalake.bronze", "transactions")
    print(f"  ✓ Transactions table created with {row_count:,} rows")
    
    # Seed user_profiles table
    print("\n[5/5] Seeding user_profiles table...")
    print("  - Generating 500 rows (95% valid, 5% invalid)...")
    users_data = generate_user_profiles_data(num_rows=500, include_issues=True)
    users_df = spark.createDataFrame(users_data)
    
    print("  - Creating Iceberg table...")
    table_config = config_mgr.get_table_config("user_profiles")
    iceberg_mgr.create_table(
        namespace="datalake.bronze",
        table_name="user_profiles",
        schema=table_config["schema"]
    )
    
    print("  - Writing data to Iceberg...")
    iceberg_mgr.write_data(
        df=users_df,
        namespace="datalake.bronze",
        table_name="user_profiles",
        mode="overwrite"
    )
    
    row_count = iceberg_mgr.get_table_count("datalake.bronze", "user_profiles")
    print(f"  ✓ User profiles table created with {row_count:,} rows")
    
    print("\n" + "=" * 80)
    print("✓ Seed data generation completed successfully!")
    print("=" * 80)
    print("\nData Overview:")
    print("  - transactions: ~1000 rows with ~5% data quality issues")
    print("  - user_profiles: ~500 rows with ~5% data quality issues")
    print("\nThe data includes:")
    print("  ✓ Positive cases: Valid data that passes all checks")
    print("  ✗ Negative cases: NULL violations, range violations, regex violations")
    print("  ⚠ Edge cases: Future dates, boundary values")
    print("\nYou can now run the DQ tests to validate this data!")
    print("=" * 80)
    
    spark.stop()


if __name__ == "__main__":
    main()

