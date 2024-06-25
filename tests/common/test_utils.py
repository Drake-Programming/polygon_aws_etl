import unittest
from etl.common.utils import Utils
import pandas as pd
from etl.common.exceptions import IncorrectColumns
from etl.etl_transformations.config import ETLTargetConfig


class TestUtils(unittest.TestCase):
    """
    test utility functions
    """

    def setUp(self):
        """
        Sets up the configs, dataframes, and schemas for testing
        :return:
        """
        #  target config for testing purposes
        correct_trg_config = {
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
        }

        incorrect_trg_config = {
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
                "open_price": "float64",
                "closing_price": "float64",
                "low_price": "float64",
                "high_price": "float6",
                "traded_volume": "int64",
                "daily_return": "float64",
                "daily_volatility": "float64",
                "intraday_range": "float64",
            },
        }
        #  Schema using the config
        self.test_schema = ETLTargetConfig(**correct_trg_config)
        self.incorrect_test_schema = ETLTargetConfig(**incorrect_trg_config)

        #  dataframe with ints instead of floats
        self.df_ints = pd.DataFrame(
            [
                [
                    "AAPL",
                    pd.Timestamp("2024-06-05 08:00:00"),
                    194,
                    194,
                    194,
                    195,
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

        #  dataframe with proper floats
        self.df_expected = pd.DataFrame(
            [
                [
                    "AAPL",
                    pd.Timestamp("2024-06-05 08:00:00"),
                    194.0,
                    194.0,
                    194.0,
                    195.0,
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
        #  dataframe with an extra column
        self.df_extra_col = pd.DataFrame(
            [
                [
                    "Extra Column",
                    "AAPL",
                    pd.Timestamp("2024-06-05 08:00:00"),
                    194.0,
                    194.0,
                    194.0,
                    195.0,
                    118043,
                    0.0,
                    0.32,
                    0.62,
                ]
            ],
            columns=[
                "extra_col",
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
        #  dataframe with a missing column
        self.df_missing_col = pd.DataFrame(
            [
                [
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

    def test_schema_check(self):
        """
        Tests if the schema check function will raise proper exceptions and will convert dtypes
        :return:
        """
        #  Test for missing columns
        with self.assertRaises(IncorrectColumns):
            Utils.schema_check(self.df_missing_col, self.test_schema)
        #  Test for extra columns
        with self.assertRaises(IncorrectColumns):
            Utils.schema_check(self.df_extra_col, self.test_schema)
        #  Result of schema check on df_ints
        df_result = Utils.schema_check(self.df_ints, self.test_schema)
        #  Checks if the ints in df_ints were converted to floats
        self.assertTrue(df_result.equals(self.df_expected))

    def test_column_check(self):
        """
        Checks if the column check with catch missing and extra columns
        :return:
        """
        #  Tests for expected number of columns
        self.assertTrue(Utils.column_check(self.df_expected, self.test_schema))
        #  Test for extra columns
        self.assertFalse(Utils.column_check(self.df_extra_col, self.test_schema))
        #  Test for missing columns
        self.assertFalse(Utils.column_check(self.df_missing_col, self.test_schema))

    def test_convert_col_dtypes(self):
        """
        Checks if the convert col types will convert the dtypes to the proper schema
        :return:
        """
        #  Result of schema check on df_ints
        df_result = Utils.convert_col_dtypes(self.df_ints, self.test_schema)
        #  Checks if the ints in df_ints were converted to floats
        self.assertTrue(df_result.equals(self.df_expected))


if __name__ == "__main__":
    unittest.main()
