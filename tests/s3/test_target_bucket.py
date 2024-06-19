from tests.s3.test_base_bucket import TestBaseBucketConnector
from etl.common.exceptions import WrongFileFormatException
from etl.common.constants import MetaFileConfig
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO, BytesIO
import unittest


class TestTargetBucketConnector(TestBaseBucketConnector):
    """
    test target bucket connector method
    """
