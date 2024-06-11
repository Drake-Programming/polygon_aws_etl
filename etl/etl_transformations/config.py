from dataclasses import dataclass


@dataclass
class ETLSourceConfig:
    """
    dataclass storing source configurations
    """

    src_input_date: str
    src_input_date_format: str
    src_columns: list
    src_col_timestamp: str
    src_col_open: str
    src_col_high: str
    src_col_low: str
    src_col_close: str
    src_col_volume: str
    src_col_vwap: str
    src_col_otc: str
    src_col_ticker: str
    src_col_transaction: str


@dataclass
class ETLTargetConfig:
    """
    dataclass storing target configuration
    """

    trg_prefix: str
    trg_key_date_format: str
    trg_format: str
    trg_dtypes: dict
    trg_columns: list
    trg_col_open: str
    trg_col_close: str
    trg_col_low: str
    trg_col_high: str
    trg_col_volume: str
    trg_col_return: str
    trg_col_volatility: str
    trg_col_intraday_range: str
    trg_col_datetime: str
    trg_col_ticker: str
