from dataclasses import dataclass


@dataclass
class ETLSourceConfig:
    """
    dataclass storing source configurations
    """

    src_input_date: str
    src_input_date_format: str
    src_columns: list
    src_col_time: str
    src_col_open: str
    src_col_high: str
    src_col_low: str
    src_col_close: str
    src_col_volume: str
    src_col_vwap: str
    src_col_ticker: str


@dataclass
class ETLTargetConfig:
    """
    dataclass storing target configuration
    """

    trg_prefix: str
    trg_key_date_format: str
    trg_format: str
