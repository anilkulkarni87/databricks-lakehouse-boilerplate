import time
import logging
from typing import Optional
from pyspark.sql import SparkSession
from lakehouse_engine.models.config import EntityPipelineConfig
from lakehouse_engine.core.transformer import transform_entity, register_helpers
from lakehouse_engine.core.pii import extract_pii_split
from lakehouse_engine.core.delta_ops import ensure_delta_table, merge_batch_pruned
from lakehouse_engine.core.dlq import split_valid_and_dlq, write_dlq_table
from lakehouse_engine.core.expectations import evaluate_data_quality_rules
from lakehouse_engine.core.audit import log_batch_audit, extract_delta_merge_metrics
from lakehouse_engine.core.alerts import (
    send_dlq_webhook_alert,
    send_contract_breach_webhook_alert,
    send_volume_anomaly_webhook_alert
)

logger = logging.getLogger(__name__)

def run_pipeline(spark: SparkSession, config: EntityPipelineConfig) -> None:
    register_helpers(spark)

    # Automatically distribute package to streaming workers (Databricks Connect & classic Spark)
    try:
        import lakehouse_engine
        import inspect
        pkg_file = inspect.getfile(lakehouse_engine)
        if hasattr(spark, "sparkContext"):
            spark.sparkContext.addPyFile(pkg_file)
        elif hasattr(spark, "addArtifacts"):
            spark.addArtifacts(pkg_file, pyfile=True)
    except Exception as e:
        logger.debug(f"Auto-distribution of library to workers skipped: {e}")

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
            transformed_gold_df = transform_entity(batch_df, config.gold, config.source.payload_column, config.source.payload_type)

            # 2. Phase 1 Hardening: DLQ Split (primary key validation)
            valid_gold_df, pk_dlq_df = split_valid_and_dlq(transformed_gold_df, config.gold.unique_keys)

            # 3. Phase 2 Hardening: Declarative Data Quality Expectations
            clean_gold_df, exp_dlq_df, warnings = evaluate_data_quality_rules(
                valid_gold_df,
                config.gold.expectations
            )

            # Combine all DLQ quarantined records
            all_dlq_df = pk_dlq_df
            if not exp_dlq_df.isEmpty():
                all_dlq_df = all_dlq_df.unionByName(exp_dlq_df) if not all_dlq_df.isEmpty() else exp_dlq_df

            if not all_dlq_df.isEmpty():
                total_dlq_rows = all_dlq_df.count()
                if config.gold.enable_dlq:
                    write_dlq_table(spark, config.gold.dlq_fqdn, all_dlq_df)

                # Real-Time Operational DLQ Alerting
                if config.runtime.alert_webhook and config.runtime.alert_webhook.enabled:
                    send_dlq_webhook_alert(
                        alert_config=config.runtime.alert_webhook,
                        entity_name=config.entity_name,
                        batch_id=batch_id,
                        quarantined_count=total_dlq_rows,
                        dlq_table=config.gold.dlq_fqdn
                    )

            # 4. Phase 4 Hardening: Data Contract & Quarantine SLA Enforcement
            if config.contract and config.contract.max_quarantine_rate_percent < 100.0 and source_rows_read > 0:
                quarantine_rate = (total_dlq_rows / source_rows_read) * 100.0
                if quarantine_rate > config.contract.max_quarantine_rate_percent:
                    sla_msg = (
                        f"Data Contract SLA Violation on entity '{config.entity_name}' (Batch #{batch_id}): "
                        f"Quarantine rate of {quarantine_rate:.2f}% ({total_dlq_rows}/{source_rows_read} rows) "
                        f"exceeded maximum allowed contract threshold of {config.contract.max_quarantine_rate_percent:.1f}%."
                    )
                    logger.error(sla_msg)

                    if config.runtime.alert_webhook and config.runtime.alert_webhook.enabled:
                        send_contract_breach_webhook_alert(
                            alert_config=config.runtime.alert_webhook,
                            entity_name=config.entity_name,
                            batch_id=batch_id,
                            quarantine_rate=quarantine_rate,
                            threshold_percent=config.contract.max_quarantine_rate_percent,
                            quarantined_count=total_dlq_rows,
                            source_count=source_rows_read
                        )

                    if config.contract.quarantine_action == "fail_batch":
                        raise RuntimeError(sla_msg)

            # 5. Provision & MERGE Valid Records into Gold
            if not clean_gold_df.isEmpty():
                ensure_delta_table(
                    spark=spark,
                    table_fqdn=config.gold.fqdn,
                    df=clean_gold_df,
                    cluster_by=config.gold.cluster_by,
                    table_comment=config.gold.table_comment,
                    fields=config.gold.fields
                )
                effective_order_col = config.gold.order_by_col or (
                    "updated_at" if "updated_at" in clean_gold_df.columns else None
                )
                merge_batch_pruned(
                    spark=spark,
                    target_table_fqdn=config.gold.fqdn,
                    source_df=clean_gold_df,
                    unique_keys=config.gold.unique_keys,
                    prune_col=config.gold.prune_column,
                    order_by_col=effective_order_col
                )
                logger.info(f"Batch {batch_id}: Successfully merged Gold table {config.gold.fqdn}")

                # Extract operation metrics from Delta commit log
                metrics = extract_delta_merge_metrics(spark, config.gold.fqdn)
                gold_inserted = metrics.get("rows_inserted", 0)
                gold_updated = metrics.get("rows_updated", 0)

            # 6. Process PII Splits
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

            # 7. Centralized Operational Telemetry Logging
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
