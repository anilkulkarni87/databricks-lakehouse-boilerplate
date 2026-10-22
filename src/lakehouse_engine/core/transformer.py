from pyspark.sql import DataFrame, functions as F
from lakehouse_engine.models.config import TargetTableConfig

def register_helpers(spark):
    """
    Registers reusable SQL expression helpers if needed.
    """
    def _extract_attr(attributes, attr_name):
        if not attributes:
            return None
        for item in attributes:
            if isinstance(item, dict) and item.get("name") == attr_name:
                return item.get("value")
        return None

    spark.udf.register("get_attribute", _extract_attr)

def transform_entity(df: DataFrame, config: TargetTableConfig) -> DataFrame:
    """
    Evaluates declarative field expressions from YAML against the payload DataFrame.
    Preserves raw unmapped payload in _rescued_payload if track_rescued_data is enabled.
    """
    select_exprs = []
    for field in config.fields:
        select_exprs.append(F.expr(field.expr).cast(field.type).alias(field.name))

    transformed = df.select(*select_exprs)

    # Phase 1 Hardening: Capture raw unmapped payload to prevent silent data loss
    if config.track_rescued_data and "payload" in df.columns:
        payload_col = df["payload"]
        if str(payload_col.dataType).startswith("StructType"):
            transformed = transformed.withColumn("_rescued_payload", F.to_json(payload_col))
        else:
            transformed = transformed.withColumn("_rescued_payload", payload_col.cast("string"))

    # Add technical lineage timestamps
    transformed = transformed.withColumn("_ingested_at", F.current_timestamp())
    return transformed
