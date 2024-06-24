from tests.etl.test_base_transformer import TestBaseETL
import unittest
from unittest.mock import patch
from tests.polygon.make_test_stock_data import MockStockData


@patch("etl.polygon.base_polygon.RESTClient.list_aggs")
@patch("os.getenv", return_value="fake_api_key")
class IntegrationTestETL(TestBaseETL):
    """
    integration unit tests for ETL, extract(), transform() and load()
    """

    def setUp(self):
        super().setUp()
        # Create an instance of MockStockData
        self.fake_stock_data = MockStockData()

    def test_transform(self, mock_getenv, mock_list_aggs):
        """
        test transform works with extract
        """
        # Define the return value for the mock
        mock_list_aggs.return_value = self.fake_stock_data.make_stock_data(limit=1)
        df_extracted, _ = self.etl.extract()
        df_result, _ = self.etl.transform(df_extracted)
        self.assertTrue(df_result.equals(self.df_trg))

    def test_load(self, mock_getenv, mock_list_aggs):
        """
        test load saves dataframe and updates meta file
        """
        # Define the return value for the mock
        mock_list_aggs.return_value = self.fake_stock_data.make_stock_data(limit=1)
        df_extracted, _ = self.etl.extract()
        df_transformed, _ = self.etl.transform(df_extracted)
        df_expected = self.etl.load(df_transformed)

        df_result = self.trg_bucket_connector.read_object(
            self.trg_key, self.trg_config.trg_format
        )
        meta_file = self.trg_bucket_connector.read_meta_file()

        self.assertTrue(df_result.equals(df_expected))
        self.assertIn(self.input_date, (meta_file[self.meta_date_col]).tolist())

    def test_run_existed(self, mock_getenv, mock_list_aggs):
        """
        test run works when the dataframe has been loaded
        """
        # Define the return value for the mock
        mock_list_aggs.return_value = self.fake_stock_data.make_stock_data(limit=1)
        df_extracted, _ = self.etl.extract()
        df_transformed, _ = self.etl.transform(df_extracted)
        self.etl.load(df_transformed)

        df_result, transformed = self.etl.extract()
        _, loaded = self.etl.transform(df_result, transformed)

        self.assertTrue(transformed)
        self.assertTrue(loaded)
        self.assertTrue(df_result.equals(self.df_trg))

    def test_run(self, mock_getenv, mock_list_aggs):
        """
        test run works when receiving new date
        """
        # Define the return value for the mock
        mock_list_aggs.return_value = self.fake_stock_data.make_stock_data(limit=1)
        df_extracted, transformed = self.etl.extract()
        df_transformed, loaded = self.etl.transform(df_extracted)
        df_result = self.etl.load(df_transformed)
        self.assertFalse(transformed)
        self.assertFalse(loaded)
        self.assertTrue(df_result.equals(self.df_trg))
        df_result2 = self.etl.run()
        self.assertTrue(df_result2.equals(self.df_trg))


if __name__ == "__main__":
    unittest.main()
