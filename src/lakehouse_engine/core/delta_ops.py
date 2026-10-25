import logging
from typing import List, Optional, Tuple, Dict, Any
try:
    from pyspark.sql import DataFrame, functions as F
    from delta.tables import DeltaTable
except ImportError:
    DataFrame = Any  # type: ignore
    F = None         # type: ignore
    DeltaTable = None # type: ignore
from lakehouse_engine.models.config import FieldDefinition

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

def sync_catalog_comments(
    spark,
    table_fqdn: str,
    table_comment: Optional[str],
    fields: List[FieldDefinition]
) -> None:
    """
    Syncs declarative table and column descriptions into Unity Catalog / Metastore.
    Enables Text-to-SQL AI Agents (e.g. Databricks Genie) to accurately understand schema semantics.
    """
    if table_comment:
        escaped_tbl_comment = table_comment.replace("'", "\\'")
        try:
            spark.sql(f"COMMENT ON TABLE {table_fqdn} IS '{escaped_tbl_comment}'")
            logger.info(f"Updated table comment on {table_fqdn}")
        except Exception as e:
            logger.warning(f"Could not update table comment on {table_fqdn}: {e}")

    for field in fields:
        if field.description:
            escaped_col_comment = field.description.replace("'", "\\'")
            try:
                spark.sql(f"ALTER TABLE {table_fqdn} ALTER COLUMN {field.name} COMMENT '{escaped_col_comment}'")
                logger.debug(f"Updated column comment on {table_fqdn}.{field.name}")
            except Exception as e:
                logger.warning(f"Could not update column comment on {table_fqdn}.{field.name}: {e}")

def apply_unity_catalog_tags(
    spark,
    table_fqdn: str,
    tags: Optional[Dict[str, str]]
) -> None:
    """
    Applies declarative governance and compliance tags to the table in Unity Catalog.
    """
    if not tags:
        return
    tag_assignments = ", ".join([f"'{k}' = '{v}'" for k, v in tags.items()])
    try:
        spark.sql(f"ALTER TABLE {table_fqdn} SET TAGS ({tag_assignments})")
        logger.info(f"Applied Unity Catalog tags to {table_fqdn}: {tags}")
    except Exception as e:
        logger.debug(f"Could not apply Unity Catalog tags on {table_fqdn}: {e}")

def ensure_delta_table(
    spark,
    table_fqdn: str,
    df: DataFrame,
    cluster_by: List[str],
    table_comment: Optional[str] = None,
    fields: Optional[List[FieldDefinition]] = None,
    tags: Optional[Dict[str, str]] = None,
    auto_optimize: bool = True
) -> None:
    """
    Auto-provisions the Delta table with Databricks Liquid Clustering, auto-compaction properties,
    and syncs Unity Catalog metadata (comments and tags).
    """
    table_is_new = not spark.catalog.tableExists(table_fqdn)
    if table_is_new:
        logger.info(f"Table {table_fqdn} does not exist. Creating with schema and clustering: {cluster_by}")
        writer = df.limit(0).write.format("delta").mode("ignore")
        if cluster_by:
            writer = writer.clusterBy(*cluster_by)
        writer.saveAsTable(table_fqdn)

        if auto_optimize:
            try:
                spark.sql(f"""
                    ALTER TABLE {table_fqdn} SET TBLPROPERTIES (
                        'delta.autoOptimize.optimizeWrite' = 'true',
                        'delta.autoOptimize.autoCompact' = 'true'
                    )
                """)
                logger.info(f"Enabled Delta auto-compaction and optimized writes on {table_fqdn}")
            except Exception as e:
                logger.debug(f"Could not set auto-optimize properties on {table_fqdn}: {e}")

    if fields:
        sync_catalog_comments(spark, table_fqdn, table_comment, fields)

    if tags:
        apply_unity_catalog_tags(spark, table_fqdn, tags)

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
