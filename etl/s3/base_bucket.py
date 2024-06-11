import logging
import boto3
import os
import unittest
from dotenv import load_dotenv, find_dotenv

dotenv_path = find_dotenv()
load_dotenv(dotenv_path)


class BaseBucketConnector(unittest.TestCase):
    """
    base class for target bucket
    """

    def __init__(
        self,
        access_key_name: str,
        secret_access_key_name: str,
        bucket_name: str,
    ):
        """
        Constructor for S3BucketConnector

        :param access_key: access key for accessing S3
        :param secret_key: secret key for accessing S3
        :param bucket: s3 bucket name
        """
        self._logger = logging.getLogger(__name__)
        self.session = boto3.Session(
            aws_access_key_id=os.getenv(access_key_name),
            aws_secret_access_key=os.getenv(secret_access_key_name),
        )

        self._s3 = self.session.resource(service_name="s3")
        self._bucket = self._s3.Bucket(bucket_name)
