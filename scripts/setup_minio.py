#!/usr/bin/env python3
"""
Setup MinIO buckets and initial configuration

This script:
1. Connects to MinIO
2. Creates required buckets (datalake, etc.)
3. Sets up initial folder structure
"""

import os
import sys
from minio import Minio
from minio.error import S3Error


def setup_minio():
    """Setup MinIO with required buckets and structure"""
    
    # MinIO configuration
    endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    
    print("=" * 80)
    print("MinIO Setup Script")
    print("=" * 80)
    print(f"\nConnecting to MinIO at: {endpoint}")
    
    # Create MinIO client
    try:
        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False  # Set to True if using HTTPS
        )
        print("✓ Connected to MinIO successfully")
    except Exception as e:
        print(f"✗ Failed to connect to MinIO: {e}")
        sys.exit(1)
    
    # Create buckets
    buckets = [
        "datalake",
        "logs",
        "metadata"
    ]
    
    print("\nCreating buckets...")
    for bucket_name in buckets:
        try:
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)
                print(f"  ✓ Created bucket: {bucket_name}")
            else:
                print(f"  - Bucket already exists: {bucket_name}")
        except S3Error as e:
            print(f"  ✗ Failed to create bucket {bucket_name}: {e}")
    
    # Create folder structure in datalake bucket
    print("\nCreating folder structure in 'datalake' bucket...")
    folders = [
        "warehouse/",
        "raw/",
        "bronze/",
        "silver/",
        "gold/"
    ]
    
    for folder in folders:
        try:
            # MinIO creates folders implicitly when uploading objects
            # We'll create a .gitkeep placeholder
            from io import BytesIO
            client.put_object(
                "datalake",
                f"{folder}.gitkeep",
                BytesIO(b""),
                0
            )
            print(f"  ✓ Created folder: datalake/{folder}")
        except S3Error as e:
            print(f"  ✗ Failed to create folder {folder}: {e}")
    
    print("\n" + "=" * 80)
    print("✓ MinIO setup completed successfully!")
    print("=" * 80)
    print("\nMinIO Console URL: http://localhost:9001")
    print(f"Access Key: {access_key}")
    print(f"Secret Key: {secret_key}")
    print("\nBuckets created:")
    for bucket in buckets:
        print(f"  - {bucket}")
    print("=" * 80)


if __name__ == "__main__":
    setup_minio()

