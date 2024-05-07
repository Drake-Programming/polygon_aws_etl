import pandas as pd
from io import StringIO, BytesIO
from typing import Union
from etl.s3.base_bucket import BaseBucketConnector
from etl.common.constants import MetaFileConfig, S3FileFormats
from etl.common.exceptions import WrongFileFormatException


class TargetBucketConnector(BaseBucketConnector):
    """
    interface to target bucket: polygon-report
    """

    meta_key = MetaFileConfig.META_KEY.value
    meta_date_col = MetaFileConfig.META_DATE_COL.value
    meta_timestamp_col = MetaFileConfig.META_TIMESTAMP_COL.value

    csv_format = S3FileFormats.CSV.value
    parquet_format = S3FileFormats.PARQUET.value

    def __init__(
        self, access_key_name, secret_access_key_name, endpoint_url, bucket_name
    ):
        super().__init__(
            access_key_name, secret_access_key_name, endpoint_url, bucket_name
        )

    def read_meta_file(self, decoding="utf-8"):
        """
        Retrieves meta file from s3 bucket
        :param decoding: decoding codes
        :return: meta file in dataframe, returns empty dataframe if meta file does not exists
        """
        self._logger.info(
            f"Reading meta file at {self.endpoint_url}/{self._bucket.name}/{self.meta_key}"
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
        # if there is not meta file, return an empty dataframe with specified columns
        except self.session.client("s3").exceptions.NoSuchKey:
            df = pd.DataFrame(columns=[self.meta_date_col, self.meta_timestamp_col])
        return df

    def read_object(self, key: str, file_format: str, decoding="utf-8"):
        """
        read in an s3 object as a pandas dataframe
        used as a caching layer when input date exists in meta file

        :param key: object key
        :param file_format: object file format, support csv or parquet
        :param decoding: file decoding for csv files

        returns:
            a dataframe
        """
        self._logger.info(f"reading file {self.endpoint_url}/{self._bucket.name}/{key}")
        if file_format == self.csv_format:
            csv_obj = (
                self._bucket.Object(key=key).get().get("Body").read().decode(decoding)
            )
            df = pd.read_csv(StringIO(csv_obj))
        if file_format == self.parquet_format:
            parquet_obj = self._bucket.Object(key=key).get().get("Body").read()
            df = pd.read_parquet(BytesIO(parquet_obj))
        return df

    def write_df_to_s3(
        self, data_frame: pd.DataFrame, key: str, file_format: str
    ) -> Union[bool, None]:
        """
        Writing a Pandas Dataframe to s3
        Supported file formats: .csv, .parquet
        :param data_frame:
        :param key:
        :param file_format:
        """

        if data_frame.empty:
            self._logger.info("The dataframe is empty! No file will be written")
            return None

        if file_format == S3FileFormats.CSV.value:
            out_buffer = StringIO()
            data_frame.to_csv(out_buffer, index=False)
            return self._put_object(out_buffer, key)

        if file_format == S3FileFormats.PARQUET.value:
            out_buffer = BytesIO()
            data_frame.to_parquet(out_buffer, index=False)
            return self._put_object(out_buffer, key)

        self._logger.info(
            "The file format %s is not supported to be written to s3", file_format
        )
        raise WrongFileFormatException

    def _put_object(self, out_buffer: Union[StringIO, BytesIO], key: str) -> bool:
        """
        Helper function for self.write_df_to_s3()
        :param out_buffer:
        :param key:
        :return:
        """
        self._logger.info(
            "Writing file to %s/%s/%s", self._endpoint_url, self._bucket.name, key
        )
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True
