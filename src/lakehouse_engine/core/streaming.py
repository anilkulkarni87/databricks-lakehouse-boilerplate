import logging
from pyspark.sql import SparkSession
from lakehouse_engine.models.config import EntityPipelineConfig
from lakehouse_engine.core.transformer import transform_entity, register_helpers
from lakehouse_engine.core.pii import extract_pii_split
from lakehouse_engine.core.delta_ops import ensure_delta_table, merge_batch_pruned

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
        if batch_df.isEmpty():
            logger.info(f"Batch {batch_id} is empty. Skipping.")
            return

        batch_df.persist()

        try:
            # Process Gold Non-PII Table
            gold_df = transform_entity(batch_df, config.gold)
            ensure_delta_table(
                spark=spark,
                table_fqdn=config.gold.fqdn,
                df=gold_df,
                cluster_by=config.gold.cluster_by,
                table_comment=config.gold.table_comment,
                fields=config.gold.fields
            )
            merge_batch_pruned(
                spark=spark,
                target_table_fqdn=config.gold.fqdn,
                source_df=gold_df,
                unique_keys=config.gold.unique_keys,
                prune_col=config.gold.prune_column,
                order_by_col="updated_at" if "updated_at" in gold_df.columns else None
            )
            logger.info(f"Batch {batch_id}: Successfully merged Gold table {config.gold.fqdn}")

            # Process PII Splits
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
