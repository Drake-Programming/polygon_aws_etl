from etl.common.constants import MetaFileConfig
from etl.polygon.source_polygon import SourcePolygonConnector
from etl.s3.target_bucket import TargetBucketConnector
from etl.etl_transformations.config import ETLSourceConfig, ETLTargetConfig
from etl.etl_transformations.etl import ETL
from tests.polygon.make_test_stock_data import MockStockData
from unittest.mock import patch
import boto3
from moto import mock_aws
import os
import pandas as pd
import unittest
from datetime import datetime


@mock_aws
class TestBaseETL(unittest.TestCase):
    """
    base class for testing ETL methods,
    """

    @patch("etl.polygon.base_polygon.RESTClient.list_aggs")
    @patch("os.getenv", return_value="fake_api_key")
    def setUp(self, mock_getenv, mock_list_aggs):
        #  mock aws connection start
        self.mock_aws = mock_aws()
        self.mock_aws.start()
        # Create an instance of MockStockData
        fake_stock_data = MockStockData()
        # Define the return value for the mock
        mock_list_aggs.return_value = fake_stock_data.make_stock_data(limit=1)

        self.bucket_config = {
            "s3_access_key": "AWS_ACCESS_KEY_ID",
            "s3_secret_key": "AWS_SECRET_ACCESS_KEY",
            "s3_bucket_name": "test_bucket",
            "s3_bucket_location": "us-west-1",
        }
        os.environ[self.bucket_config["s3_access_key"]] = "KEY1"
        os.environ[self.bucket_config["s3_secret_key"]] = "KEY2"
        self.s3_conn = boto3.resource(service_name="s3")
        self.s3_conn.create_bucket(
            Bucket=self.bucket_config["s3_bucket_name"],
            CreateBucketConfiguration={
                "LocationConstraint": self.bucket_config["s3_bucket_location"]
            },
        )
        self.bucket = self.s3_conn.Bucket(self.bucket_config["s3_bucket_name"])
        # create bucket connector instance
        self.src_polygon_connector = SourcePolygonConnector("POLYGON_API_KEY")
        self.trg_bucket_connector = TargetBucketConnector(*self.bucket_config.values())
        # create target and source configuration
        self.meta_key = MetaFileConfig.META_KEY.value
        config = {
            "source": {
                "src_input_date": "2024-05-30",
                "src_input_date_format": "%Y-%m-%d",
                "src_columns": [
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "vwap",
                    "timestamp",
                    "transaction",
                    "ticker",
                ],
                "src_col_ticker": "ticker",
                "src_col_timestamp": "timestamp",
                "src_col_open": "open",
                "src_col_high": "high",
                "src_col_low": "low",
                "src_col_close": "close",
                "src_col_volume": "volume",
                "src_col_vwap": "vwap",
                "src_col_otc": "otc",
                "src_col_transaction": "transaction",
            },
            "target": {
                "trg_prefix": "daily/",
                "trg_key_date_format": "%Y%m%d",
                "trg_format": "parquet",
                "trg_columns": [
                    "stock_ticker",
                    "date_time",
                    "opening_price",
                    "closing_price",
                    "low_price",
                    "high_price",
                    "traded_volume",
                    "daily_return",
                    "daily_volatility",
                    "intraday_range",
                ],
                "trg_col_open": "opening_price",
                "trg_col_close": "closing_price",
                "trg_col_low": "low_price",
                "trg_col_high": "high_price",
                "trg_col_volume": "traded_volume",
                "trg_col_return": "daily_return",
                "trg_col_volatility": "daily_volatility",
                "trg_col_intraday_range": "intraday_range",
                "trg_col_datetime": "date_time",
                "trg_col_ticker": "stock_ticker",
                "trg_dtypes": {
                    "stock_ticker": "object",
                    "date_time": "datetime64[ns]",
                    "opening_price": "float64",
                    "closing_price": "float64",
                    "low_price": "float64",
                    "high_price": "float64",
                    "traded_volume": "int64",
                    "daily_return": "float64",
                    "daily_volatility": "float64",
                    "intraday_range": "float64",
                },
            },
        }
        # create source and transformed data
        self.src_config = ETLSourceConfig(**config["source"])
        self.trg_config = ETLTargetConfig(**config["target"])
        self.input_date = self.src_config.src_input_date
        self.input_date_format = self.src_config.src_input_date_format
        self.trg_key = (
            f"{self.trg_config.trg_prefix}"
            f"{datetime.strptime(self.input_date, self.input_date_format).strftime(self.trg_config.trg_key_date_format)}."
            f"{self.trg_config.trg_format}"
        )
        self.df_src = pd.DataFrame(
            [
                [
                    194.990,
                    195.2000,
                    194.5800,
                    194.8300,
                    118043.0,
                    194.8244,
                    1717574400000,
                    2422,
                    None,
                    "AAPL",
                ],
                [
                    194.790,
                    196.9000,
                    194.6678,
                    196.0200,
                    21936159.0,
                    195.6760,
                    1717588800000,
                    376029,
                    None,
                    "AAPL",
                ],
                [
                    196.010,
                    196.7000,
                    195.5900,
                    195.9000,
                    22511239.0,
                    196.0724,
                    1717603200000,
                    261315,
                    None,
                    "AAPL",
                ],
            ],
            columns=[
                "open",
                "high",
                "low",
                "close",
                "volume",
                "vwap",
                "timestamp",
                "transaction",
                "otc",
                "ticker",
            ],
        )
        self.trg_bucket_connector.write_to_s3(
            self.df_src.loc[0:0], "2024-06-05/2024-06-05_BINS_POLY13.csv", "csv"
        )
        self.trg_bucket_connector.write_to_s3(
            self.df_src.loc[1:1], "2024-06-05/2024-06-05_BINS_POLY14.csv", "csv"
        )
        self.trg_bucket_connector.write_to_s3(
            self.df_src.loc[2:2], "2024-06-05/2024-06-05_BINS_POLY15.csv", "csv"
        )

        self.df_trg = pd.DataFrame(
            [
                [
                    "AAPL",
                    pd.Timestamp("2024-06-05 08:00:00"),
                    194.99,
                    194.83,
                    194.58,
                    195.2,
                    118043,
                    0.0,
                    0.32,
                    0.62,
                ]
            ],
            columns=[
                "stock_ticker",
                "date_time",
                "opening_price",
                "closing_price",
                "low_price",
                "high_price",
                "traded_volume",
                "daily_return",
                "daily_volatility",
                "intraday_range",
            ],
        )
        self.etl = ETL(
            self.src_polygon_connector,
            self.trg_bucket_connector,
            self.meta_key,
            self.src_config,
            self.trg_config,
        )
        self.df_empty = pd.DataFrame()
        # meta file attributes
        self.meta_date_col = MetaFileConfig.META_DATE_COL.value

    def tearDown(self):
        #  mock aws connection stop
        self.mock_aws.stop()
        for key in self.bucket.objects.all():
            key.delete()
        self.bucket.delete()
