from tests.etl.test_base_transformer import TestBaseETL
import unittest
from unittest.mock import patch
from tests.polygon.make_test_stock_data import MockStockData


@patch("etl.polygon.base_polygon.RESTClient.list_aggs")
@patch("os.getenv", return_value="fake_api_key")
class UnitTestETL(TestBaseETL):
    """
    unit tests for ETL, extract(), transform() and load()
    """

    def setUp(self):
        super().setUp()
        # Create an instance of MockStockData
        self.fake_stock_data = MockStockData()

    def test_extract(self, mock_getenv, mock_list_aggs):
        """
        test extract works for correct date
        """
        mock_list_aggs.return_value = self.fake_stock_data.make_stock_data(limit=1)
        df_result, transformed = self.etl.extract()
        self.assertTrue(df_result.equals(self.df_src))
        self.assertFalse(transformed)

    def test_transform_transformed_empty(self, mock_getenv, mock_list_aggs):
        """
        test transform is skipped when receiving transformed=True or an empty dataframe
        """
        mock_list_aggs.return_value = self.fake_stock_data.make_stock_data(limit=1)
        log_expected_empty = "empty dataframe, skip transformation"
        log_expected_transformed = "transformed dataframe, skip transformation"

        with self.assertLogs() as log:
            df_result_empty, loaded = self.etl.transform(self.df_empty)
            self.assertTrue(df_result_empty.equals(self.df_empty))
            self.assertTrue(loaded)
            self.assertIn(log_expected_empty, log.output[0])

        with self.assertLogs() as log:
            df_result_transformed, loaded = self.etl.transform(
                self.df_trg, transformed=True
            )
            self.assertTrue(df_result_transformed.equals(self.df_trg))
            self.assertTrue(loaded)
            self.assertIn(log_expected_transformed, log.output[0])

    def test_load_loaded(self, mock_getenv, mock_list_aggs):
        """
        test load is skipped when receiving loaded=True
        """
        mock_list_aggs.return_value = self.fake_stock_data.make_stock_data(limit=1)
        log_expected = "Dataframe has been loaded or is empty, skip loading"
        with self.assertLogs() as log:
            df_result = self.etl.load(self.df_trg, loaded=True)
            self.assertTrue(df_result.equals(self.df_trg))
            self.assertIn(log_expected, log.output[0])

    def test_load(self, mock_getenv, mock_list_aggs):
        """
        test load works in saving dataframe
        """
        mock_list_aggs.return_value = self.fake_stock_data.make_stock_data(limit=1)

        log_expected1 = f"Saving transformed data into target bucket {self.trg_key}"
        log_expected2 = f"Saved transformed data into target bucket {self.trg_key}"
        log_expected3 = f"Attempting to update meta file {self.meta_key}"
        log_expected4 = (
            "Failed to retrieve meta file, returning dataframe with specified columns"
        )
        log_expected5 = "Successfully updated meta file"

        # test logging output
        with self.assertLogs() as log:
            df_result = self.etl.load(self.df_trg)
            # Log test after method execution
            self.assertIn(log_expected1, log.output[0])
            self.assertIn(log_expected2, log.output[1])
            self.assertIn(log_expected3, log.output[2])
            self.assertIn(log_expected4, log.output[3])
            self.assertIn(log_expected5, log.output[4])
        # test returning df
        self.assertTrue(df_result.equals(self.df_trg))
        # test saving df
        df_saved = self.trg_bucket_connector.read_object(
            self.trg_key, self.trg_config.trg_format
        )
        self.assertTrue(df_saved.equals(self.df_trg))
        # test updating meta file
        meta_df = self.trg_bucket_connector.read_meta_file()
        self.assertIn(self.input_date, meta_df[self.meta_date_col].tolist())


if __name__ == "__main__":
    unittest.main()
