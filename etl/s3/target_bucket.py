from etl.s3.base_bucket import BaseBucketConnector
from etl.common.constants import MetaFileConfig, S3FileFormats


class TargetBucketConnector(BaseBucketConnector):
    """
    interface to target bucket: xetra-report
    """

    meta_key = MetaFileConfig.META_KEY.value
    meta_date_col = MetaFileConfig.META_DATE_COL.value
    meta_timestamp_col = MetaFileConfig.META_TIMESTAMP_COL.value

    csv_format = S3FileFormats.CSV.value
    parquet_format = S3FileFormats.PARQUET.value
