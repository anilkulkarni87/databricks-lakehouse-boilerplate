import logging
from typing import List, Optional
from pyspark.sql import DataFrame, functions as F
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)

def deduplicate_batch(df: DataFrame, unique_keys: List[str], order_by_col: Optional[str] = None) -> DataFrame:
    """
    Deduplicates incoming batch rows across unique keys before MERGE.
    Prevents DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE errors.
    """
    if not order_by_col or order_by_col not in df.columns:
        return df.dropDuplicates(unique_keys)

    from pyspark.sql.window import Window
    window_spec = Window.partitionBy([F.col(k) for k in unique_keys]).orderBy(F.col(order_by_col).desc())
    return (
        df.withColumn("_row_rank", F.row_number().over(window_spec))
        .filter(F.col("_row_rank") == 1)
        .drop("_row_rank")
    )

def ensure_delta_table(
    spark,
    table_fqdn: str,
    df: DataFrame,
    cluster_by: List[str]
) -> None:
    """
    Auto-provisions the Delta table with Databricks Liquid Clustering if it does not exist.
    """
    if not spark.catalog.tableExists(table_fqdn):
        logger.info(f"Table {table_fqdn} does not exist. Creating with schema and clustering: {cluster_by}")
        writer = df.limit(0).write.format("delta").mode("ignore")
        if cluster_by:
            writer = writer.clusterBy(*cluster_by)
        writer.saveAsTable(table_fqdn)

def merge_batch_pruned(
    spark,
    target_table_fqdn: str,
    source_df: DataFrame,
    unique_keys: List[str],
    prune_col: Optional[str] = None,
    order_by_col: Optional[str] = None
) -> None:
    """
    Executes an idempotent Delta MERGE with partition/cluster date pruning.
    Avoids full-table scans by limiting target search to partition dates present in current batch.
    """
    if source_df.isEmpty():
        return

    # In-batch deduplication
    clean_source = deduplicate_batch(source_df, unique_keys, order_by_col)

    target_delta = DeltaTable.forName(spark, target_table_fqdn)

    # Base match condition on primary business keys
    join_conditions = [f"target.{k} = source.{k}" for k in unique_keys]

    # Date pruning optimization
    if prune_col and prune_col in source_df.columns:
        distinct_dates = [
            row[0] for row in clean_source.select(F.to_date(F.col(prune_col))).distinct().collect()
            if row[0] is not None
        ]
        if distinct_dates:
            date_strings = ", ".join([f"'{d}'" for d in distinct_dates])
            prune_condition = f"to_date(target.{prune_col}) IN ({date_strings})"
            join_conditions.insert(0, prune_condition)

    full_condition = " AND ".join(join_conditions)

    # Latest-wins upsert
    merge_builder = target_delta.alias("target").merge(
        clean_source.alias("source"),
        full_condition
    )

    if order_by_col and order_by_col in source_df.columns:
        merge_builder = merge_builder.whenMatchedUpdateAll(
            condition=f"source.{order_by_col} >= target.{order_by_col}"
        )
    else:
        merge_builder = merge_builder.whenMatchedUpdateAll()

    merge_builder.whenNotMatchedInsertAll().execute()
