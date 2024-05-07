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

    def read_csv_to_df(
        self, key: str, encoding: str = "utf-8", sep: str = ","
    ) -> pd.DataFrame:
        """
        Read a csv file from the s3 bucket and return a dataframe
        :param key:
        :param encoding:
        :param sep:
        :return: pd.DataFrame
        """
        self._logger.info(
            "Reading file %s/%s/%s", self._endpoint_url, self._bucket.name, key
        )
        csv_obj = self._bucket.Object(key=key).get().get("Body").read().decode(encoding)
        data = StringIO(csv_obj)
        data_frame = pd.read_csv(data, sep=sep)
        return data_frame

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
