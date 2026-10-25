import logging
from typing import List, Tuple, Optional
from pyspark.sql import DataFrame, SparkSession, functions as F

logger = logging.getLogger(__name__)

def split_valid_and_dlq(
    df: DataFrame,
    unique_keys: List[str]
) -> Tuple[DataFrame, DataFrame]:
    """
    Validates records against primary key constraints.
    Rows with null or empty primary keys cannot be merged and are segregated to DLQ.
    """
    # Build condition: all unique keys must be non-null and non-empty
    valid_conditions = []
    missing_key_exprs = []

    for key in unique_keys:
        if key in df.columns:
            is_valid = F.col(key).isNotNull() & (F.col(key) != "")
            valid_conditions.append(is_valid)
            missing_key_exprs.append(
                F.when(~is_valid, F.lit(f"MISSING_OR_EMPTY_KEY:{key}")).otherwise(F.lit(None))
            )

    if not valid_conditions:
        # No unique keys defined to validate
        empty_dlq = df.limit(0).withColumn("_dlq_reason", F.lit("")).withColumn("_dlq_timestamp", F.current_timestamp())
        return df, empty_dlq

    combined_valid_condition = valid_conditions[0]
    for cond in valid_conditions[1:]:
        combined_valid_condition = combined_valid_condition & cond

    valid_df = df.filter(combined_valid_condition)

    # Compile reasons for failed rows
    dlq_df = df.filter(~combined_valid_condition)
    reasons_array = F.array_compact(F.array(*missing_key_exprs))
    dlq_df = (
        dlq_df
        .withColumn("_dlq_reason", F.concat_ws(", ", reasons_array))
        .withColumn("_dlq_timestamp", F.current_timestamp())
    )

    return valid_df, dlq_df

def write_dlq_table(
    spark: SparkSession,
    dlq_table_fqdn: str,
    dlq_df: DataFrame
) -> None:
    """
    Appends quarantined records to the Dead-Letter Queue Delta table.
    Enables zero data loss and automated agent recovery.
    """
    if dlq_df.isEmpty():
        return

    logger.warning(f"Writing {dlq_df.count()} invalid record(s) to DLQ table: {dlq_table_fqdn}")

    if not spark.catalog.tableExists(dlq_table_fqdn):
        (
            dlq_df.limit(0)
            .write.format("delta")
            .mode("ignore")
            .clusterBy("_dlq_timestamp")
            .saveAsTable(dlq_table_fqdn)
        )
        try:
            spark.sql(f"""
                ALTER TABLE {dlq_table_fqdn} SET TBLPROPERTIES (
                    'delta.autoOptimize.optimizeWrite' = 'true',
                    'delta.autoOptimize.autoCompact' = 'true'
                )
            """)
        except Exception as e:
            logger.debug(f"Could not set auto-optimize on DLQ table {dlq_table_fqdn}: {e}")

    (
        dlq_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(dlq_table_fqdn)
    )
