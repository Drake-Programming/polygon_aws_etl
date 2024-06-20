"""Test Polygon Connector Methods"""

import unittest
from unittest.mock import patch, MagicMock
from etl.polygon.base_polygon import BasePolygonConnector


class TestBasePolygonConnector(unittest.TestCase):

    @patch("etl.polygon.base_polygon.RESTClient")  # Mock the RESTClient
    @patch("os.getenv", return_value="fake_api_key")
    def test_init(self, mock_getenv, mock_rest_client):
        #  Test Initialization
        connector = BasePolygonConnector("POLYGON_API_KEY")
        #  Test Assertions
        mock_getenv.assert_called_once_with("POLYGON_API_KEY")
        mock_rest_client.assert_called_once_with("fake_api_key")
        self.assertEqual(connector._polygon_key, "fake_api_key")
        self.assertIsInstance(
            connector._client, MagicMock
        )  # Check if _client is a mock instance


if __name__ == "__main__":
    unittest.main()
