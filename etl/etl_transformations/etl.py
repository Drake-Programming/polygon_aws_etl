from etl.polygon.source_polygon import SourcePolygonConnector
from etl.s3.target_bucket import TargetBucketConnector
from etl.etl_transformations.config import ETLTargetConfig, ETLSourceConfig
from etl.meta.meta_file import MetaFile
from typing import Tuple
from etl.common.utils import Utils
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

    def extract(self) -> Tuple[pd.DataFrame, bool]:
        """
        read the source data and concatenates them into one panda dataframe

        :returns: a tuple with two elements
            1. df: the extracted dataframe
            2. transformed: should the dataframe been transformed
        """
        self._logger.info("extracting meta file from target bucket")
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
            df = self.src_polygon.get_stocks(self.input_date, self.src_args.src_tickers)
            self._logger.info("extracted data from source polygon")
            return df, False

    def transform(
        self, df: pd.DataFrame, transformed=False
    ) -> Tuple[pd.DataFrame, bool]:
        """
        Apply Transformations to Extracted Data
        :param df:
        :param transformed:
        :return:
        """
        self._logger.debug("Beginning transformation")
        if transformed:
            self._logger.info("transformed dataframe, skip transformation")
            return df, True

        if df.empty:
            self._logger.info("empty dataframe, skip transformation")
            return df, True

        #  drop otc column
        self._logger.debug(f"Dropping column {self.src_args.src_col_otc}")
        df.drop(columns=[self.src_args.src_col_otc], inplace=True)

        #  drop rows with missing values
        self._logger.debug("Dropping rows with NaN values")
        df.dropna(inplace=True)

        #  compute daily return
        self._logger.debug(f"Adding column {self.trg_args.trg_col_return}")
        df[self.trg_args.trg_col_return] = (
            (df[self.src_args.src_col_close] - df[self.src_args.src_col_close].shift(1))
            / df[self.src_args.src_col_close].shift(1)
            * 100
        )
        # Fill NaN with a specific value (e.g., 0)
        # df[self.trg_args.trg_col_return].fillna(0, inplace=True)
        df.fillna({self.trg_args.trg_col_return: 0}, inplace=True)
        #  compute daily volatility
        self._logger.debug(f"Adding column {self.trg_args.trg_col_volatility}")
        df[self.trg_args.trg_col_volatility] = (
            (df[self.src_args.src_col_high] - df[self.src_args.src_col_low])
            / df[self.src_args.src_col_open]
            * 100
        )
        #  compute intraday range
        self._logger.debug(f"Adding column {self.trg_args.trg_col_intraday_range}")
        df[self.trg_args.trg_col_intraday_range] = (
            df[self.src_args.src_col_high] - df[self.src_args.src_col_low]
        )

        # timestamp to date
        self._logger.debug(
            f"Changing column {self.src_args.src_col_timestamp} to {self.trg_args.trg_col_datetime}"
        )
        df[self.trg_args.trg_col_datetime] = pd.to_datetime(
            df[self.src_args.src_col_timestamp], unit="ms"
        )
        #  rename columns
        self._logger.debug("Renaming columns")
        df.rename(
            columns={
                self.src_args.src_col_open: self.trg_args.trg_col_open,
                self.src_args.src_col_close: self.trg_args.trg_col_close,
                self.src_args.src_col_low: self.trg_args.trg_col_low,
                self.src_args.src_col_high: self.trg_args.trg_col_high,
                self.src_args.src_col_volume: self.trg_args.trg_col_volume,
                self.src_args.src_col_ticker: self.trg_args.trg_col_ticker,
            },
            inplace=True,
        )
        #  Drop old columns
        self._logger.debug("Dropping unused columns")
        cleaned_df = df[self.trg_args.trg_columns]

        #  round off columns
        self._logger.debug(f"Rounding off columns")
        # List of columns to round
        columns_to_round = [
            self.trg_args.trg_col_open,
            self.trg_args.trg_col_close,
            self.trg_args.trg_col_low,
            self.trg_args.trg_col_high,
            self.trg_args.trg_col_return,
            self.trg_args.trg_col_volatility,
            self.trg_args.trg_col_intraday_range,
        ]
        # Round each column
        for col in columns_to_round:
            cleaned_df.loc[:, col] = cleaned_df.loc[:, col].round(decimals=2)
        #  Check and change schema
        df = Utils.schema_check(cleaned_df, self.trg_args)

        return df, False

    def load(self, df: pd.DataFrame, loaded=False) -> pd.DataFrame:
        if loaded:
            self._logger.info("Dataframe has been loaded or is empty, skip loading")
            return df
        else:
            formatted_date = datetime.strptime(
                self.input_date, self.input_date_format
            ).strftime(self.trg_args.trg_key_date_format)

            target_key = (
                f"{self.trg_args.trg_prefix}"
                f"{formatted_date}."
                f"{self.trg_args.trg_format}"
            )
            self._logger.info(
                f"Saving transformed data into target bucket {target_key}"
            )
            self.trg_bucket.write_to_s3(df, target_key, self.trg_args.trg_format)
            self._logger.info(f"Saved transformed data into target bucket {target_key}")

            # Updating meta file
            self._logger.info(f"Attempting to update meta file {self.meta_key}")
            MetaFile.update_meta_file(self.input_date, self.trg_bucket)
            self._logger.info("Successfully updated meta file")
            return df

    def run(self) -> pd.DataFrame:
        df, transformed = self.extract()
        df, loaded = self.transform(df, transformed)
        df = self.load(df, loaded)
        return df
