import logging
from typing import Dict, Any, List
from lakehouse_engine.models.config import EntityPipelineConfig

logger = logging.getLogger(__name__)

def run_table_maintenance(
    spark,
    config: EntityPipelineConfig,
    retention_hours: int = 168
) -> Dict[str, Any]:
    """
    Executes production storage maintenance across all Delta tables defined in an entity config:
      1. OPTIMIZE: Coalesces small files and applies Liquid Clustering index sorting.
      2. VACUUM: Purges obsolete historical parquet files older than the retention window.
    """
    tables_to_maintain: List[str] = [config.gold.fqdn]

    if config.gold.enable_dlq:
        tables_to_maintain.append(config.gold.dlq_fqdn)

    for pii_cfg in config.pii_splits:
        tables_to_maintain.append(pii_cfg.fqdn)

    results: Dict[str, Dict[str, Any]] = {}

    for tbl in tables_to_maintain:
        results[tbl] = {}
        if not spark.catalog.tableExists(tbl):
            logger.info(f"Skipping maintenance on non-existent table: {tbl}")
            results[tbl]["status"] = "SKIPPED_NOT_FOUND"
            continue

        # 1. OPTIMIZE (Liquid Clustering)
        try:
            logger.info(f"Running OPTIMIZE on {tbl}...")
            opt_df = spark.sql(f"OPTIMIZE {tbl}")
            metrics = opt_df.collect()[0].asDict() if not opt_df.isEmpty() else {}
            results[tbl]["optimize"] = "SUCCESS"
            results[tbl]["metrics"] = metrics
            logger.info(f"Successfully optimized {tbl}")
        except Exception as e:
            logger.error(f"Failed to optimize {tbl}: {e}")
            results[tbl]["optimize"] = f"FAILED: {e}"

        # 2. VACUUM
        try:
            logger.info(f"Running VACUUM {tbl} RETAIN {retention_hours} HOURS...")
            spark.sql(f"VACUUM {tbl} RETAIN {retention_hours} HOURS")
            results[tbl]["vacuum"] = f"SUCCESS ({retention_hours}h retention)"
            logger.info(f"Successfully vacuumed {tbl}")
        except Exception as e:
            logger.warning(f"Could not vacuum {tbl}: {e}")
            results[tbl]["vacuum"] = f"FAILED: {e}"

    return {
        "entity_name": config.entity_name,
        "tables_maintained": len(tables_to_maintain),
        "results": results
    }
