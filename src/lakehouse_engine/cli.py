import sys
import yaml
import click
from pyspark.sql import SparkSession
from lakehouse_engine.models.config import EntityPipelineConfig
from lakehouse_engine.core.streaming import run_pipeline

@click.command()
@click.option("--config-path", "-c", required=True, help="Path to YAML entity configuration file")
@click.option("--env", "-e", default="prod", help="Target environment (dev, stage, prod)")
def main(config_path: str, env: str):
    """
    Executes the metadata-driven Lakehouse ingestion pipeline.
    """
    with open(config_path, "r") as f:
        raw_config = yaml.safe_load(f)

    config = EntityPipelineConfig.model_validate(raw_config)
    print(f"Loaded pipeline configuration for entity: {config.entity_name} (Environment: {env})")

    spark = (
        SparkSession.builder
        .appName(f"lakehouse-engine-{config.entity_name}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    run_pipeline(spark, config)

if __name__ == "__main__":
    main()
