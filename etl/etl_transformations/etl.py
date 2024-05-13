from etl.polygon.source_polygon import SourcePolygonConnector
from etl.s3.target_bucket import TargetBucketConnector
from etl.etl_transformations.config import ETLTargetConfig, ETLSourceConfig

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
