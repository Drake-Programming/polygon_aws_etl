"""Test S3 Bucket Connector Methods"""

import unittest
import os
import boto3
from moto import mock_aws

from etl.s3.target_bucket import TargetBucketConnector


@mock_aws
class TestBaseBucketConnector(unittest.TestCase):
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
        self.config = {
            "s3_access_key": "AWS_ACCESS_KEY_ID",
            "s3_secret_key": "AWS_SECRET_ACCESS_KEY",
            "s3_bucket_name": "test_bucket",
            "s3_bucket_location": "us-west-1",
        }
        #  Creating s3 access keys as environment variables
        os.environ[self.config["s3_access_key"]] = "KEY1"
        os.environ[self.config["s3_secret_key"]] = "KEY2"
        #  Creating a bucket on the mocked s3
        self.s3_conn = boto3.resource(service_name="s3")
        self.s3_conn.create_bucket(
            Bucket=self.config["s3_bucket_name"],
            CreateBucketConfiguration={
                "LocationConstraint": self.config["s3_bucket_location"]
            },
        )
        self.bucket = self.s3_conn.Bucket(self.config["s3_bucket_name"])
        #  Creating a testing instance
        self.trg_bucket_connector = TargetBucketConnector(*self.config.values())

    def tearDown(self):
        """
        Executing after unittests
        :return:
        """
        #  mock aws connection stop
        self.mock_aws.stop()
        for key in self.bucket.objects.all():
            key.delete()
        self.bucket.delete()
