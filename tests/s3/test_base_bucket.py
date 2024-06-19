"""Test S3 Bucket Connector Methods"""

import unittest
import os

import boto3
from moto import mock_aws

from etl.s3.base_bucket import BaseBucketConnector


@mock_aws
class TestBaseBucketConnector(unittest.TestCase):
    """
    base class for testing source and target bucket connector
    """

    def setUp(self):
        config = {
            "access_key_name": "AWS_ACCESS_KEY",
            "secret_access_key_name": "AWS_SECRET_ACCESS_KEY",
            "bucket_name": "test-bucket",
            "bucket_region": "us-west-1",
        }

        # Set the environment variables for AWS credentials
        os.environ[config["access_key_name"]] = "dummy_access_key"
        os.environ[config["secret_access_key_name"]] = "dummy_secret_key"

        #  Create s3 resource
        self.conn = boto3.resource(
            service_name="s3", region_name=config["bucket_region"]
        )

        #  Create bucket with location constraint
        self.conn.create_bucket(
            Bucket=config["bucket_name"],
            CreateBucketConfiguration={"LocationConstraint": config["bucket_region"]},
        )

        self.bucket = self.conn.Bucket(config["bucket_name"])

        #  Creating a testing instance
        self.trg_bucket_connector = BaseBucketConnector(**config)

    def tearDown(self):
        for key in self.bucket.objects.all():
            key.delete()
        self.bucket.delete()
