"""
File to store constants
"""

from enum import Enum


class S3SourceConfig(Enum):
    """
    configuration for source bucket
    """

    INPUT_DATE_FORMAT = "%Y-%m-%d"


class S3FileFormats(Enum):
    """
    supported file formats for S3BucketConnector
    """

    CSV = "csv"
    PARQUET = "parquet"


class MetaFileConfig(Enum):
    """
    formation for MetaFile class
    """

    META_DATE_FORMAT = "%Y-%m-%d"
    META_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
    META_DATE_COL = "source_date"
    META_TIMESTAMP_COL = "datetime_of_processing"
    META_FILE_FORMAT = "csv"
    META_KEY = "meta_file.csv"
