import os
import unittest
import logging
from polygon import RESTClient
from dotenv import load_dotenv, find_dotenv

dotenv_path = find_dotenv()
load_dotenv(dotenv_path)


class BasePolygonConnector(unittest.TestCase):
    """
    base class for source api
    """

    def __init__(
        self,
        str_key: str,
    ):
        self._polygon_key = os.getenv(str_key)
        self._client = RESTClient(self._polygon_key)
