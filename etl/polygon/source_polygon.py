from etl.polygon.base_polygon import BasePolygonConnector
from typing import List, Dict
import pandas as pd
from datetime import datetime

today_date = datetime.today().strftime("%Y-%m-%d")


class SourcePolygonConnector(BasePolygonConnector):
    """
    interface to source api: polygon.io
    """

    def __init__(self, key: str):
        super().__init__(key)

    @staticmethod
    def dict_to_df(dict_stock: Dict) -> pd.DataFrame:
        dataframes = []
        for ticker, data in dict_stock.items():
            df = pd.DataFrame(data).assign(ticker=ticker)
            dataframes.append(df)
        return pd.concat(dataframes, ignore_index=True)

    def get_stocks(
        self,
        start_date,
        tickers: List[str],
        end_date=today_date,
        timespan: str = "hour",
    ) -> Dict:
        stock_objects = {}
        for stock in tickers:
            stock_objects[stock] = self._client.list_aggs(
                ticker=stock,
                multiplier=4,
                timespan=timespan,
                from_=start_date,
                to=end_date,
                limit=50000,
            )
        return self.dict_to_df(stock_objects)
