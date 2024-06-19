"""Test S3 Bucket Connector Methods"""

import unittest
import os

import boto3
from moto import mock_aws

from etl.s3.target_bucket import TargetBucketConnector


@mock_aws
class TestS3BucketConnector(unittest.TestCase):
    """
    base class for testing source and target bucket connector
    """

    def setUp(self):
        """
        Setting up s3 environment
        :return:
        """
        #  mock aws connection start
        self.mock_aws = mock_aws()
        self.mock_aws.start()
        #  Defining the class arguments
        self.s3_access_key = "AWS_ACCESS_KEY_ID"
        self.s3_secret_key = "AWS_SECRET_ACCESS_KEY"
        self.s3_endpoint_url = "https://s3.eu-central-1.amazonaws.com"
        self.s3_bucket_name = "test_bucket"
        self.s3_bucket_location = "eu-central-1"
        #  Creating s3 access keys as environment variables
        os.environ[self.s3_access_key] = "KEY1"
        os.environ[self.s3_secret_key] = "KEY2"
        #  Creating a bucket on the mocked s3
        self.conn = boto3.resource(service_name="s3", endpoint_url=self.s3_endpoint_url)
        self.conn.create_bucket(
            Bucket=self.s3_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": self.s3_bucket_location},
        )
        self.bucket = self.conn.Bucket(self.s3_bucket_name)
        #  Creating a testing instance
        self.trgBucketConnector = TargetBucketConnector(
            self.s3_access_key,
            self.s3_secret_key,
            self.s3_bucket_name,
            self.s3_bucket_location,
        )

    def tearDown(self):
        """
        Executing after unittests
        :return:
        """
        #  mock aws connection stop
        self.mock_aws.stop()


if __name__ == "__main__":
    unittest.main()
