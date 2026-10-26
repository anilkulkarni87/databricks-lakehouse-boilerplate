import logging
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType
)
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)

AUDIT_TABLE_SCHEMA = StructType([
    StructField("batch_id", LongType(), False),
    StructField("entity_name", StringType(), False),
    StructField("target_table", StringType(), False),
    StructField("batch_timestamp", TimestampType(), False),
    StructField("execution_duration_ms", LongType(), True),
    StructField("source_rows_read", LongType(), True),
    StructField("target_rows_inserted", LongType(), True),
    StructField("target_rows_updated", LongType(), True),
    StructField("dlq_rows_quarantined", LongType(), True),
    StructField("status", StringType(), False),
    StructField("error_message", StringType(), True),
])

def ensure_audit_table(spark: SparkSession, audit_table_fqdn: str) -> None:
    """
    Ensures the centralized operational audit table exists with liquid clustering on batch_timestamp.
    """
    if not spark.catalog.tableExists(audit_table_fqdn):
        logger.info(f"Creating Lakehouse Engine audit table: {audit_table_fqdn}")
        empty_df = spark.createDataFrame([], AUDIT_TABLE_SCHEMA)
        (
            empty_df.write.format("delta")
            .mode("ignore")
            .clusterBy("batch_timestamp", "entity_name")
            .saveAsTable(audit_table_fqdn)
        )

def extract_delta_merge_metrics(spark: SparkSession, table_fqdn: str) -> Dict[str, Any]:
    """
    Extracts operation metrics (rows inserted, updated, duration) from the latest Delta commit history.
    """
    try:
        delta_table = DeltaTable.forName(spark, table_fqdn)
        latest_commit = delta_table.history(1).collect()[0]
        metrics = latest_commit.asDict().get("operationMetrics", {}) or {}
        return {
            "rows_inserted": int(metrics.get("numTargetRowsInserted", 0)),
            "rows_updated": int(metrics.get("numTargetRowsUpdated", 0)),
            "source_rows": int(metrics.get("numSourceRows", 0)),
            "execution_time_ms": int(metrics.get("executionTimeMs", 0)),
        }
    except Exception as e:
        logger.warning(f"Could not extract Delta merge metrics for {table_fqdn}: {e}")
        return {
            "rows_inserted": 0,
            "rows_updated": 0,
            "source_rows": 0,
            "execution_time_ms": 0,
        }

def log_batch_audit(
    spark: SparkSession,
    audit_table_fqdn: str,
    batch_id: int,
    entity_name: str,
    target_table: str,
    execution_duration_ms: int,
    source_rows_read: int,
    target_rows_inserted: int,
    target_rows_updated: int,
    dlq_rows_quarantined: int,
    status: str = "SUCCESS",
    error_message: Optional[str] = None
) -> None:
    """
    Appends a structured telemetry audit record for the completed micro-batch.
    """
    try:
        ensure_audit_table(spark, audit_table_fqdn)

        now = datetime.now(timezone.utc)
        audit_data = [(
            int(batch_id),
            str(entity_name),
            str(target_table),
            now,
            int(execution_duration_ms),
            int(source_rows_read),
            int(target_rows_inserted),
            int(target_rows_updated),
            int(dlq_rows_quarantined),
            str(status),
            str(error_message) if error_message is not None else None
        )]

        audit_df = spark.createDataFrame(audit_data, schema=AUDIT_TABLE_SCHEMA)

        audit_df.write.format("delta").mode("append").saveAsTable(audit_table_fqdn)
        logger.info(f"Successfully logged batch {batch_id} telemetry to {audit_table_fqdn}")

    except Exception as e:
        logger.error(f"Failed to write audit log for batch {batch_id}: {e}", exc_info=True)
