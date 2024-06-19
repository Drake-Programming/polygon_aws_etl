import pandas as pd
import logging
from etl.common.exceptions import IncorrectColumns

logger = logging.getLogger(__name__)


class Utils:
    """
    class for working with ETL
    """

    @staticmethod
    def schema_check(df: pd.DataFrame, trg_args) -> pd.DataFrame:
        """
        Checks number of columns and convert dtypes of columns to specifications of target args
        :param df:
        :param trg_args:
        :return: pd.DataFrame
        """
        #  schema check
        logger.debug("Starting schema check")

        if Utils.column_check(df, trg_args):
            df = Utils.convert_col_dtypes(df, trg_args)
            return df
        else:
            raise IncorrectColumns

    @staticmethod
    def column_check(df: pd.DataFrame, trg_args) -> bool:
        """
        Checks number of dataframe columns with the specified number of columns for the target bucket
        :param trg_args:
        :param df:
        :return: bool
        """
        #  column check
        missing_columns = [col for col in trg_args.trg_columns if col not in df.columns]
        extra_columns = [col for col in df.columns if col not in trg_args.trg_columns]
        if not missing_columns and not extra_columns:
            logger.info("All expected columns are present")
            return True
        else:
            if missing_columns:
                logger.warning(f"Missing columns in dataframe: {missing_columns}")
                return False
            if extra_columns:
                logger.warning(
                    f"Extra columns were found in dataframe: {extra_columns}"
                )
                return False

    @staticmethod
    def convert_col_dtypes(df: pd.DataFrame, trg_args) -> pd.DataFrame:
        """
        Converts dataframe columns to the dtype of the specified target bucket dtype for the columns
        :param trg_args:
        :param df:
        :return:
        """
        #  dtype convert
        logger.info("Converting dtypes of columns to specified types")
        try:
            df = df.astype(trg_args.trg_dtypes)
        except Exception as e:
            logger.error(f"Failed to convert dtypes: {e}")
        return df
