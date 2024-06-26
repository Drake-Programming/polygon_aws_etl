import logging
import logging.config
import yaml
import argparse
from etl.common.constants import MetaFileConfig
from etl.etl_transformations.etl import ETL
from etl.etl_transformations.config import ETLSourceConfig, ETLTargetConfig
from etl.polygon.source_polygon import SourcePolygonConnector
from etl.s3.target_bucket import TargetBucketConnector
import pandas as pd


def main() -> None:
    #  setup YAML
    parser = argparse.ArgumentParser(description="run polygon etl job")
    parser.add_argument(
        "--config", help="a yaml configuration file", default="configs/config.yaml"
    )
    args = parser.parse_args()
    with open(args.config) as f:
        config = yaml.safe_load(f)

    # configure logging
    log_config = config["logging"]
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)

    #  setup configs strings for source and target
    s3_config = config["s3"]
    polygon_config = config["polygon"]

    #  Instantiate source connector and target bucket
    src_polygon = SourcePolygonConnector(polygon_config["polygon_key"])
    trg_bucket = TargetBucketConnector(
        s3_config["access_key_name"],
        s3_config["secret_access_key_name"],
        s3_config["trg_bucket"],
        s3_config['s3_region']

    )

    #  setup dataclasses for source and target configs
    src_config = ETLSourceConfig(**config["source"])
    trg_config = ETLTargetConfig(**config["target"])

    #  Instantiate ETL
    logger.info(f"polygon job started for {src_config.src_input_date}")
    etl = ETL(
        src_polygon, trg_bucket, MetaFileConfig.META_KEY.value, src_config, trg_config
    )
    # running etl job for polygon report
    df = etl.run()
    logger.info(f"polygon job finished for {src_config.src_input_date}")
    print(
        f"transformed dataframe saved to target bucket {s3_config['trg_bucket']}, example: "
    )
    pd.set_option('display.max_columns', 1000)
    print(df.head())
    return df


if __name__ == "__main__":
    main()
