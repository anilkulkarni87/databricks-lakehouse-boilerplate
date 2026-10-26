import time
import logging
from typing import Optional
from pyspark.sql import SparkSession, functions as F
from lakehouse_engine.models.config import EntityPipelineConfig
from lakehouse_engine.core.transformer import transform_entity, register_helpers
from lakehouse_engine.core.pii import extract_pii_split
from lakehouse_engine.core.delta_ops import ensure_delta_table, merge_batch_pruned
from lakehouse_engine.core.dlq import split_valid_and_dlq, write_dlq_table
from lakehouse_engine.core.expectations import evaluate_data_quality_rules
from lakehouse_engine.core.audit import log_batch_audit, extract_delta_merge_metrics

logger = logging.getLogger(__name__)

def run_backfill(
    spark: SparkSession,
    config: EntityPipelineConfig,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    date_column: Optional[str] = None,
    reset_checkpoint: bool = False
) -> None:
    """
    Executes a historical batch backfill without altering streaming offsets.
    Supports targeted partition date filtering, DLQ segregation, and audit logging.
    """
    start_time = time.time()
    register_helpers(spark)

    print(f"\n=======================================================")
    print(f"Starting Historical Backfill for: {config.entity_name}")
    print(f"Source: {config.source.fqdn} -> Target: {config.gold.fqdn}")
    print(f"Date Range: {start_date or 'BEGINNING'} to {end_date or 'NOW'}")
    print(f"=======================================================\n")

    # 1. Read source table in batch mode
    source_df = spark.read.table(config.source.fqdn)

    if config.source.stream_filter_field and config.source.stream_filter_value:
        source_df = source_df.filter(
            f"{config.source.stream_filter_field} = '{config.source.stream_filter_value}'"
        )

    # 2. Apply Date Range Filters if specified
    filter_date_col = date_column or config.gold.prune_column or "created_at"
    # Auto-resolve target field name to source column name if target field was passed
    if filter_date_col not in source_df.columns and config.gold.fields:
        for f in config.gold.fields:
            if f.name == filter_date_col and f.expr in source_df.columns:
                filter_date_col = f.expr
                break

    if filter_date_col in source_df.columns:
        if start_date:
            source_df = source_df.filter(F.to_date(F.col(filter_date_col)) >= F.to_date(F.lit(start_date)))
        if end_date:
            source_df = source_df.filter(F.to_date(F.col(filter_date_col)) <= F.to_date(F.lit(end_date)))
    else:
        logger.warning(f"Backfill date filter column '{filter_date_col}' not found in source. Processing all records.")

    source_rows = source_df.count()
    print(f"Backfill records selected for processing: {source_rows}")

    if source_rows == 0:
        print("No records matched backfill criteria. Exiting.")
        return

    # 3. Transform Gold Entity
    transformed_gold_df = transform_entity(source_df, config.gold, config.source.payload_column, config.source.payload_type)

    # 4. Primary Key DLQ Split
    valid_gold_df, pk_dlq_df = split_valid_and_dlq(transformed_gold_df, config.gold.unique_keys)

    # 5. Data Quality Expectations Evaluation
    clean_gold_df, exp_dlq_df, warnings = evaluate_data_quality_rules(
        valid_gold_df,
        config.gold.expectations
    )

    # Combine DLQ records
    all_dlq_df = pk_dlq_df
    if not exp_dlq_df.isEmpty():
        all_dlq_df = all_dlq_df.unionByName(exp_dlq_df) if not all_dlq_df.isEmpty() else exp_dlq_df

    total_dlq_rows = all_dlq_df.count() if not all_dlq_df.isEmpty() else 0
    if total_dlq_rows > 0 and config.gold.enable_dlq:
        write_dlq_table(spark, config.gold.dlq_fqdn, all_dlq_df)
        print(f"Quarantined {total_dlq_rows} malformed records to {config.gold.dlq_fqdn}")

    # 6. Execute Idempotent Merge into Gold
    if not clean_gold_df.isEmpty():
        ensure_delta_table(
            spark=spark,
            table_fqdn=config.gold.fqdn,
            df=clean_gold_df,
            cluster_by=config.gold.cluster_by,
            table_comment=config.gold.table_comment,
            fields=config.gold.fields
        )
        effective_prune_col = None if (config.guardrails and config.guardrails.allow_unbounded_merge) else config.gold.prune_column

        merge_batch_pruned(
            spark=spark,
            target_table_fqdn=config.gold.fqdn,
            source_df=clean_gold_df,
            unique_keys=config.gold.unique_keys,
            prune_col=effective_prune_col,
            order_by_col="updated_at" if "updated_at" in clean_gold_df.columns else None
        )

        metrics = extract_delta_merge_metrics(spark, config.gold.fqdn)
        print(f"Backfill MERGE complete: {metrics.get('rows_inserted', 0)} inserted, {metrics.get('rows_updated', 0)} updated.")

    # 7. Process PII Splits
    for pii_cfg in config.pii_splits:
        pii_df = extract_pii_split(source_df, pii_cfg)
        if not pii_df.isEmpty():
            ensure_delta_table(
                spark=spark,
                table_fqdn=pii_cfg.fqdn,
                df=pii_df,
                cluster_by=pii_cfg.cluster_by,
                table_comment=pii_cfg.table_comment,
                fields=pii_cfg.fields
            )
            merge_batch_pruned(
                spark=spark,
                target_table_fqdn=pii_cfg.fqdn,
                source_df=pii_df,
                unique_keys=pii_cfg.unique_keys,
                prune_col=None,
                order_by_col=None
            )
            print(f"Backfill PII table merged: {pii_cfg.fqdn}")

    # 8. Optional Checkpoint Reset
    if reset_checkpoint and config.runtime.checkpoint_location:
        try:
            print(f"Resetting streaming checkpoint location: {config.runtime.checkpoint_location}")
            # Try DBFS dbutils or Hadoop Path deletion
            hadoop_conf = spark._jsc.hadoopConfiguration()
            path = spark._jvm.org.apache.hadoop.fs.Path(config.runtime.checkpoint_location)
            fs = path.getFileSystem(hadoop_conf)
            fs.delete(path, True)
            print("Checkpoint directory successfully reset.")
        except Exception as e:
            logger.warning(f"Could not reset checkpoint location automatically: {e}")

    # 9. Log Telemetry
    duration_ms = int((time.time() - start_time) * 1000)
    if config.runtime.audit_table:
        log_batch_audit(
            spark=spark,
            audit_table_fqdn=config.runtime.audit_table,
            batch_id=-1, # -1 signifies ad-hoc batch backfill
            entity_name=config.entity_name,
            target_table=config.gold.fqdn,
            execution_duration_ms=duration_ms,
            source_rows_read=source_rows,
            target_rows_inserted=metrics.get('rows_inserted', 0) if not clean_gold_df.isEmpty() else 0,
            target_rows_updated=metrics.get('rows_updated', 0) if not clean_gold_df.isEmpty() else 0,
            dlq_rows_quarantined=total_dlq_rows,
            status="SUCCESS (BACKFILL)",
            error_message=None
        )

    print(f"\nBackfill successfully finished in {duration_ms / 1000.0:.2f} seconds.\n")
