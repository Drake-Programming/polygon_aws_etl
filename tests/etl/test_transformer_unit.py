from tests.etl.test_base_transformer import TestBaseETL
import unittest
from unittest.mock import patch


class UnitTestETL(TestBaseETL):
    """
    unit tests for ETL, extract(), transform() and load()
    """
