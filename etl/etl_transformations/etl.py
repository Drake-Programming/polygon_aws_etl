from etl.polygon.source_polygon import SourcePolygonConnector
from etl.s3.target_bucket import TargetBucketConnector
from etl.etl_transformations.config import ETLTargetConfig, ETLSourceConfig
from etl.meta.meta_file import MetaFile

import logging
import pandas as pd
from datetime import datetime


class ETL:
    """
    main interface to extract, transform and load polygon data
    """

    def __init__(
        self,
        source_polygon: SourcePolygonConnector,
        target_bucket: TargetBucketConnector,
        meta_key: str,
        source_args: ETLSourceConfig,
        target_args: ETLTargetConfig,
    ):
        """
        Constructor for Polygon ETL pipeline

        :param source_polygon: connection to source polygon connector
        :param target_bucket: connection to target S3 bucket
        :param meta_key: key of the meta file
        :param source_args: dataclass with source configuration data
        :param target_args: dataclass with target configuration data
        """
        self._logger = logging.getLogger(__name__)
        self.src_polygon = source_polygon
        self.trg_bucket = target_bucket
        self.meta_key = meta_key
        self.src_args = source_args
        self.trg_args = target_args
        self.input_date = self.src_args.src_input_date
        self.input_date_format = self.src_args.src_input_date_format

    def extract(self):
        """
        read the source data and concatenates them into one panda dataframe

        :returns: a tuple with two elements
            1. df: the extracted dataframe
            2. transformed: should the dataframe been transformed
        """
        self._logger.info("extracting polygon source data")
        if MetaFile.date_in_meta_file(self.input_date, self.trg_bucket):
            self._logger.info(
                "input date exists in meta file, reading from target bucket"
            )
            key = (
                f"{self.trg_args.trg_prefix}"
                f"{datetime.strptime(self.input_date, self.input_date_format).strftime(self.trg_args.trg_key_date_format)}."
                f"{self.trg_args.trg_format}"
            )
            df = self.trg_bucket.read_object(key, self.trg_args.trg_format)
            self._logger.info("read data from target bucket")
            return df, True
        else:
            self._logger.info(
                "input date does not exist in meta file, reading from source bucket"
            )
            df = self.src_polygon.get_stocks(self.input_date, ['AAPL', 'TSLA'])
            self._logger.info("extracted data from source polygon")
            return df, False

    def transform(self, df: pd.DataFrame, transformed=False):
        if transformed:
            self._logger.info("transformed dataframe, skip transformation")
            return df, True

        return df, False

    def load(self, df: pd.DataFrame, loaded=False):
        if loaded:
            self._logger.info("dataframe has been loaded or is empty, skip loading")
            return df
        else:
            return df

    def run(self):
        df, transformed = self.extract()
        df, loaded = self.transform(df, transformed)
        df = self.load(df, loaded)
        return df
