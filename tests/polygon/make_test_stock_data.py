from collections import namedtuple
import random


class MockStockData:
    """Generates Fake Stock Data that Mirrors Polygon"""

    # Define the named tuple for stock data
    StockData = namedtuple(
        "StockData",
        [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vwap",
            "timestamp",
            "transactions",
            "otc",
        ],
    )

    def _generate_stock_data_values(self):
        """
        Generate a single instance of stock data with random values.
        """
        open_price = 194.990
        high_price = 195.2000
        low_price = 194.5800
        close_price = 194.8300
        volume = 118043
        vwap = 194.8244
        timestamp = 1717574400000  # Random UNIX timestamp in milliseconds
        transactions = 2422
        otc = None  # Assuming OTC is None for simplicity

        return self.StockData(
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=volume,
            vwap=vwap,
            timestamp=timestamp,
            transactions=transactions,
            otc=otc,
        )

    def _make_stock_data_generator(self, limit=None):
        """
        Generator function to create a sequence of stock data with an optional limit.
        """
        count = 0
        while limit is None or count < limit:
            yield self._generate_stock_data_values()
            count += 1

    def make_stock_data(self, limit=None):
        """
        Returns a generator of named tuples representing stock data with an optional limit.
        """
        return self._make_stock_data_generator(limit)
