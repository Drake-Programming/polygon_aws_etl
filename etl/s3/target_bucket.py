import pandas as pd  # to create and manipulate with dataframes
from io import StringIO, BytesIO  # read from and write to strings or bytes
from typing import Union, List  # to display the "or" value in typing
from etl.s3.base_bucket import (
    BaseBucketConnector,
)  # class for connecting to a s3 bucket
from etl.common.constants import (
    MetaFileConfig,
    S3FileFormats,
)  # config names and formats for columns and files
from etl.common.exceptions import (
    WrongFileFormatException,
)  # an exception that takes in a file format to display
import re  # provides support for working with regular expressions


class TargetBucketConnector(BaseBucketConnector):
    """
    interface to target bucket: polygon-report
    """

    meta_key = MetaFileConfig.META_KEY.value
    meta_date_col = MetaFileConfig.META_DATE_COL.value
    meta_timestamp_col = MetaFileConfig.META_TIMESTAMP_COL.value

    csv_format = S3FileFormats.CSV.value
    parquet_format = S3FileFormats.PARQUET.value

    prefix = "daily/"

    def __init__(
        self, access_key_name, secret_access_key_name, bucket_name, bucket_region
    ):
        super().__init__(
            access_key_name, secret_access_key_name, bucket_name, bucket_region
        )

    def read_meta_file(self, decoding="utf-8") -> pd.DataFrame:
        """
        Retrieves meta file from s3 bucket

        :param decoding: decoding codes
        :returns: meta file in dataframe, returns empty dataframe if meta file does not exists
        """
        self._logger.info(
            f"Reading meta file '{self.meta_key}' in bucket '{self._bucket.name}'"
        )
        try:
            csv_obj = (
                self._bucket.Object(key=self.meta_key)
                .get()
                .get("Body")
                .read()
                .decode(decoding)
            )
            data = StringIO(csv_obj)
            df = pd.read_csv(data)
        # if there is no meta file, return an empty dataframe with specified columns
        except self.session.client("s3").exceptions.NoSuchKey:
            self._logger.error(
                "Failed to retrieve meta file, returning dataframe with specified columns"
            )
            df = pd.DataFrame(columns=[self.meta_date_col, self.meta_timestamp_col])

        except Exception as e:
            self._logger.error(f"Could not read meta file: {e}")
            df = pd.DataFrame(columns=[self.meta_date_col, self.meta_timestamp_col])
        return df

    def read_object(
        self, key: str, file_format: str, decoding: str = "utf-8"
    ) -> pd.DataFrame:
        """
        read in a s3 object as a pandas dataframe
        used as a caching layer when input date exists in meta file

        :param key: object key
        :param file_format: object file format, support csv or parquet
        :param decoding: file decoding for csv files
        :returns: a dataframe
        """
        self._logger.info(f"Reading file '{key}' in bucket '{self._bucket.name}'")
        if file_format == self.csv_format:
            csv_obj = (
                self._bucket.Object(key=key).get().get("Body").read().decode(decoding)
            )
            df = pd.read_csv(StringIO(csv_obj))
            return df
        if file_format == self.parquet_format:
            parquet_obj = self._bucket.Object(key=key).get().get("Body").read()
            df = pd.read_parquet(BytesIO(parquet_obj))
            return df
        else:
            self._logger.info(f"File format is not correct, needs to be CSV or PARQUET")

    def list_existing_dates(self) -> List[str]:
        """
        list dates whose polygon data has been loaded to target bucket

        :returns: a list of dates, without target prefix
        """
        self._logger.info("Reading existing dates")
        existing_keys = [
            obj.key for obj in self._bucket.objects.filter(Prefix=self.prefix)
        ]
        existing_dates = []
        for key in existing_keys:
            m = re.search(r"(\d+-\d+-\d+)", key)
            date = m.group(1)
            if date not in existing_dates:
                existing_dates.append(date)
        return existing_dates

    def write_to_s3(
        self, df: pd.DataFrame, key: str, file_format: str
    ) -> Union[bool, None]:
        """
        write a dataframe to S3

        supported formats: .csv, .parquet
        :param file_format: saving format
        :param df: the dataframe that should be written
        :param key: key of the saved file in s3
        """
        if df.empty:
            self._logger.warning("The dataframe is empty! No file will be written!")
            return None
        if file_format == self.csv_format:
            out_buffer = StringIO()
            df.to_csv(out_buffer, index=False)
            return self.put_object(out_buffer, key)
        if file_format == self.parquet_format:
            out_buffer = BytesIO()
            df.to_parquet(out_buffer, index=False)
            return self.put_object(out_buffer, key)
        self._logger.info(
            f"file format {file_format} is not supported to be written to s3!"
        )
        raise WrongFileFormatException(file_format)

    def put_object(self, out_buffer: Union[StringIO, BytesIO], key: str) -> bool:
        """
        Helper function for self.write_df_to_s3()
        :param out_buffer:
        :param key:
        :return:
        """
        self._logger.info(f"Writing file to {self._bucket.name} as {key}")
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True
