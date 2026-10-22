import time
import logging
from pyspark.sql import SparkSession
from lakehouse_engine.models.config import EntityPipelineConfig
from lakehouse_engine.core.transformer import transform_entity, register_helpers
from lakehouse_engine.core.pii import extract_pii_split
from lakehouse_engine.core.delta_ops import ensure_delta_table, merge_batch_pruned
from lakehouse_engine.core.dlq import split_valid_and_dlq, write_dlq_table
from lakehouse_engine.core.audit import log_batch_audit, extract_delta_merge_metrics

logger = logging.getLogger(__name__)

def run_pipeline(spark: SparkSession, config: EntityPipelineConfig) -> None:
    register_helpers(spark)

    # 1. Read source stream
    stream_reader = (
        spark.readStream.format("delta")
        .option("maxBytesPerTrigger", f"{config.runtime.max_mbytes_per_trigger}m")
        .table(config.source.fqdn)
    )

    if config.source.stream_filter_field and config.source.stream_filter_value:
        stream_reader = stream_reader.filter(
            f"{config.source.stream_filter_field} = '{config.source.stream_filter_value}'"
        )

    # 2. Define micro-batch foreach handler
    def process_micro_batch(batch_df, batch_id):
        start_time = time.time()
        if batch_df.isEmpty():
            logger.info(f"Batch {batch_id} is empty. Skipping.")
            return

        batch_df.persist()
        source_rows_read = batch_df.count()
        total_dlq_rows = 0
        gold_inserted = 0
        gold_updated = 0

        try:
            # 1. Transform Gold non-PII dataset
            transformed_gold_df = transform_entity(batch_df, config.gold)

            # 2. Phase 1 Hardening: DLQ Split (quarantine malformed rows with missing keys)
            if config.gold.enable_dlq:
                valid_gold_df, dlq_df = split_valid_and_dlq(transformed_gold_df, config.gold.unique_keys)
                if not dlq_df.isEmpty():
                    total_dlq_rows = dlq_df.count()
                    write_dlq_table(spark, config.gold.dlq_fqdn, dlq_df)
            else:
                valid_gold_df = transformed_gold_df

            # 3. Provision & MERGE Valid Records into Gold
            if not valid_gold_df.isEmpty():
                ensure_delta_table(
                    spark=spark,
                    table_fqdn=config.gold.fqdn,
                    df=valid_gold_df,
                    cluster_by=config.gold.cluster_by,
                    table_comment=config.gold.table_comment,
                    fields=config.gold.fields
                )
                merge_batch_pruned(
                    spark=spark,
                    target_table_fqdn=config.gold.fqdn,
                    source_df=valid_gold_df,
                    unique_keys=config.gold.unique_keys,
                    prune_col=config.gold.prune_column,
                    order_by_col="updated_at" if "updated_at" in valid_gold_df.columns else None
                )
                logger.info(f"Batch {batch_id}: Successfully merged Gold table {config.gold.fqdn}")

                # Extract operation metrics from Delta commit log
                metrics = extract_delta_merge_metrics(spark, config.gold.fqdn)
                gold_inserted = metrics.get("rows_inserted", 0)
                gold_updated = metrics.get("rows_updated", 0)

            # 4. Process PII Splits
            for pii_cfg in config.pii_splits:
                pii_df = extract_pii_split(batch_df, pii_cfg)
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
                    logger.info(f"Batch {batch_id}: Successfully merged PII table {pii_cfg.fqdn}")

            duration_ms = int((time.time() - start_time) * 1000)

            # 5. Phase 1 Hardening: Centralized Operational Telemetry Logging
            if config.runtime.audit_table:
                log_batch_audit(
                    spark=spark,
                    audit_table_fqdn=config.runtime.audit_table,
                    batch_id=batch_id,
                    entity_name=config.entity_name,
                    target_table=config.gold.fqdn,
                    execution_duration_ms=duration_ms,
                    source_rows_read=source_rows_read,
                    target_rows_inserted=gold_inserted,
                    target_rows_updated=gold_updated,
                    dlq_rows_quarantined=total_dlq_rows,
                    status="SUCCESS",
                    error_message=None
                )

        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            if config.runtime.audit_table:
                log_batch_audit(
                    spark=spark,
                    audit_table_fqdn=config.runtime.audit_table,
                    batch_id=batch_id,
                    entity_name=config.entity_name,
                    target_table=config.gold.fqdn,
                    execution_duration_ms=duration_ms,
                    source_rows_read=source_rows_read,
                    target_rows_inserted=0,
                    target_rows_updated=0,
                    dlq_rows_quarantined=total_dlq_rows,
                    status="FAILED",
                    error_message=str(e)
                )
            raise e

        finally:
            batch_df.unpersist()

    # 3. Start stream with Trigger
    writer = (
        stream_reader.writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", config.runtime.checkpoint_location)
    )

    if config.runtime.trigger_type == "AvailableNow":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=config.runtime.trigger_type)

    query = writer.start()
    query.awaitTermination()
    logger.info(f"Pipeline for {config.entity_name} completed.")
