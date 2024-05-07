import os
import unittest
import logging
from polygon import RESTClient


class BasePolygonConnector(unittest.TestCase):
    """
    base class for source api
    """

    def __init__(
        self,
        key: str,
    ):
        self._client = RESTClient(key)
