import unittest
from etl.polygon.source_polygon import SourcePolygonConnector
from unittest.mock import patch
import pandas as pd
import pandas.testing as pdt
from tests.polygon.make_test_stock_data import MockStockData


class TestSourcePolygonConnector(unittest.TestCase):
    expected_df = pd.DataFrame(
        {
            "open": [194.990],
            "high": [195.2000],
            "low": [194.5800],
            "close": [194.8300],
            "volume": [118043.0],
            "vwap": [194.8244],
            "timestamp": [1717574400000],
            "transactions": [2422],
            "otc": [None],
            "ticker": ["AAPL"],
        }
    )

    @patch("etl.polygon.base_polygon.RESTClient")
    @patch("os.getenv", return_value="fake_api_key")
    def test_dict_to_df(self, mock_getenv, mock_rest_client):
        connector = SourcePolygonConnector("POLYGON_API_KEY")
        fake_stock_data = MockStockData()

        stock_data = fake_stock_data.make_stock_data(limit=1)
        test_dict = {"AAPL": stock_data}

        result_df = connector.dict_to_df(test_dict)
        pdt.assert_frame_equal(result_df, self.expected_df)

    @patch("etl.polygon.base_polygon.RESTClient.list_aggs")
    @patch("os.getenv", return_value="fake_api_key")
    def test_get_stocks(self, mock_getenv, mock_list_aggs):
        """
        Tests the get_stocks method of SourcePolygonConnector
        :param mock_list_aggs:
        :return:
        """
        connector = SourcePolygonConnector("POLYGON_API_KEY")
        start_date = "2024-05-30"
        tickers = ["AAPL"]

        # Create an instance of MockStockData
        fake_stock_data = MockStockData()

        # Define the return value for the mock
        mock_list_aggs.return_value = fake_stock_data.make_stock_data(limit=1)
        result_df = connector.get_stocks(start_date, tickers)

        pdt.assert_frame_equal(result_df, self.expected_df)


if __name__ == "__main__":
    unittest.main()
