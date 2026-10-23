import sys
import yaml
import click
from pathlib import Path
from lakehouse_engine.models.config import EntityPipelineConfig
from lakehouse_engine.core.genie import generate_genie_space_documentation

@click.group()
def cli():
    """Lakehouse Engine CLI - Declarative ETL, Governance, and Agentic Metadata Framework"""
    pass

@cli.command("run")
@click.option("--config-path", "-c", required=True, help="Path to YAML entity configuration file")
@click.option("--env", "-e", default="prod", help="Target environment (dev, stage, prod)")
def run(config_path: str, env: str):
    """
    Executes the metadata-driven Lakehouse streaming ingestion pipeline.
    """
    from pyspark.sql import SparkSession
    from lakehouse_engine.core.streaming import run_pipeline

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

@cli.command("backfill")
@click.option("--config-path", "-c", required=True, help="Path to YAML entity configuration file")
@click.option("--start-date", "-s", default=None, help="Backfill start date (YYYY-MM-DD)")
@click.option("--end-date", "-e", default=None, help="Backfill end date (YYYY-MM-DD)")
@click.option("--date-column", default=None, help="Date column to filter on (defaults to prune_column or created_at)")
@click.option("--reset-checkpoint", is_flag=True, default=False, help="Reset streaming checkpoint location after backfill")
def backfill(config_path: str, start_date: str, end_date: str, date_column: str, reset_checkpoint: bool):
    """
    Executes a historical batch backfill without altering streaming offsets.
    """
    from pyspark.sql import SparkSession
    from lakehouse_engine.core.backfill import run_backfill

    with open(config_path, "r") as f:
        raw_config = yaml.safe_load(f)

    config = EntityPipelineConfig.model_validate(raw_config)
    print(f"Loaded backfill configuration for: {config.entity_name}")

    spark = (
        SparkSession.builder
        .appName(f"lakehouse-backfill-{config.entity_name}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    run_backfill(
        spark=spark,
        config=config,
        start_date=start_date,
        end_date=end_date,
        date_column=date_column,
        reset_checkpoint=reset_checkpoint
    )

@cli.command("export-genie")
@click.option("--configs-dir", "-d", default="configs/examples", help="Directory containing entity YAML configs")
@click.option("--output-file", "-o", default="genie_space_instructions.md", help="Output path for Genie Space documentation")
def export_genie(configs_dir: str, output_file: str):
    """
    Compiles all entity configs into a Databricks Genie Space system prompt and constraint DDLs.
    """
    p = Path(configs_dir)
    yaml_files = list(p.glob("*.yml")) + list(p.glob("*.yaml"))

    if not yaml_files:
        print(f"No YAML configs found in {configs_dir}")
        return

    configs = []
    for yf in yaml_files:
        with open(yf) as f:
            data = yaml.safe_load(f)
            if data and "entity_name" in data:
                configs.append(EntityPipelineConfig.model_validate(data))

    doc = generate_genie_space_documentation(configs)
    out_path = Path(output_file)
    out_path.write_text(doc)
    print(f"Successfully generated Databricks Genie Space instructions: {out_path.resolve()}")



@cli.command("triage-dlq")
@click.option("--config-path", "-c", required=True, help="Path to YAML entity configuration file")
@click.option("--sample-file", "-s", default=None, help="Optional path to JSON file with sample DLQ records (for local/offline mode)")
@click.option("--apply-fix", is_flag=True, default=False, help="Automatically apply recommended remediation patches to the YAML config")
@click.option("--limit", "-l", default=500, help="Maximum number of DLQ records to inspect")
def triage_dlq(config_path: str, sample_file: str, apply_fix: bool, limit: int):
    """
    Autonomous DLQ Triage: Analyzes quarantined records, discovers schema drift & expectation violations,
    and derives automated self-healing YAML patches.
    """
    import json
    from lakehouse_engine.core.triage import analyze_quarantined_records, apply_triage_patches_to_yaml

    with open(config_path, "r") as f:
        raw_config = yaml.safe_load(f)

    config = EntityPipelineConfig.model_validate(raw_config)

    records = []
    if sample_file:
        with open(sample_file, "r") as sf:
            raw_sample = json.load(sf)
            records = raw_sample if isinstance(raw_sample, list) else [raw_sample]
    else:
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            if spark and spark.catalog.tableExists(config.gold.dlq_fqdn):
                df = spark.table(config.gold.dlq_fqdn).limit(limit)
                records = [row.asDict(recursive=True) for row in df.collect()]
            else:
                print(f"[WARN] No active Spark session or table {config.gold.dlq_fqdn} not found. Running triage in zero-record diagnostic mode.")
        except ImportError:
            print("[WARN] PySpark not available and --sample-file not provided. Running in zero-record diagnostic mode.")

    report = analyze_quarantined_records(config, records)
    print(report.format_console_report())

    if apply_fix:
        success, diff = apply_triage_patches_to_yaml(config_path, report)
        if success:
            print("\n✨ Successfully applied autonomous self-healing fix to YAML config!")
            print("Diff preview:")
            print(diff)
        else:
            print(f"\n[Notice] {diff}")

@cli.command("mcp-serve")
def mcp_serve():
    """
    Starts the Model Context Protocol (MCP) server over standard I/O (stdio).
    Exposes declarative entity validation, schema listing, and Genie docs to AI agents.
    """
    from lakehouse_engine.mcp.server import run_mcp_server
    run_mcp_server()

def main():
    cli()

if __name__ == "__main__":
    main()
