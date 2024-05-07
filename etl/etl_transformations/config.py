from dataclasses import dataclass


@dataclass
class ETLSourceConfig:
    """
    dataclass storing source configurations
    """

    src_input_date: str
    src_input_date_format: str
    src_columns: list


@dataclass
class ETLTargetConfig:
    """
    dataclass storing target configuration
    """

    trg_prefix: str
    trg_key_date_format: str
    trg_format: str
