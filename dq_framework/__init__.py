"""
Data Quality Testing Framework
Scalable, production-ready framework for automated data quality validation
"""

__version__ = "1.0.0"
__author__ = "DQ Framework Team"

from .utils.spark_utils import get_spark_session
from .utils.iceberg_utils import IcebergManager

__all__ = [
    "get_spark_session",
    "IcebergManager",
]

