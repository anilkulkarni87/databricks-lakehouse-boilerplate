import logging
from pyspark.sql import DataFrame, functions as F
from lakehouse_engine.models.config import TargetTableConfig

logger = logging.getLogger(__name__)

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

def prepare_payload_column(df: DataFrame, payload_col_name: str = "payload", payload_type: str = "variant") -> DataFrame:
    """
    Ensures the payload column is encoded as Databricks native binary VARIANT
    for sub-column indexing and maximum Photon query performance.
    """
    if payload_col_name not in df.columns:
        return df

    # If payload is a string and variant encoding is requested, parse to native VARIANT
    if payload_type == "variant":
        try:
            # parse_json() is the native Databricks/Spark 4.0 function to produce VARIANT
            return df.withColumn(payload_col_name, F.expr(f"try_parse_json({payload_col_name})"))
        except Exception as e:
            logger.debug(f"Native try_parse_json not available in current Spark session: {e}. Preserving payload.")
            return df

    return df

def transform_entity(df: DataFrame, config: TargetTableConfig, payload_col_name: str = "payload", payload_type: str = "variant") -> DataFrame:
    """
    Evaluates declarative field expressions from YAML against the payload DataFrame.
    Supports Databricks native VARIANT pathing (payload:path.to.field::type).
    Preserves raw unmapped payload in _rescued_payload as VARIANT or JSON string.
    """
    # 1. Prepare payload (convert to VARIANT if needed)
    prepared_df = prepare_payload_column(df, payload_col_name, payload_type)

    # 2. Evaluate expressions
    select_exprs = []
    for field in config.fields:
        if field.type.lower() == "variant" and not any(fn in field.expr.lower() for fn in ["parse_json", "variant"]):
            # Produce parsed binary VARIANT object rather than variant string scalar
            select_exprs.append(F.expr(f"try_parse_json({field.expr})").alias(field.name))
        else:
            select_exprs.append(F.expr(field.expr).cast(field.type).alias(field.name))

    transformed = prepared_df.select(*select_exprs)

    # 3. Phase 1 & 3 Hardening: Capture raw unmapped payload as VARIANT or String
    if config.track_rescued_data and payload_col_name in prepared_df.columns:
        raw_col = prepared_df[payload_col_name]
        if config.store_rescued_as_variant:
            try:
                # Store directly as native binary VARIANT
                transformed = transformed.withColumn("_rescued_payload", F.expr(f"try_parse_json({payload_col_name})"))
            except Exception:
                # Fallback to string for environments without native VARIANT support
                transformed = transformed.withColumn("_rescued_payload", raw_col.cast("string"))
        else:
            transformed = transformed.withColumn("_rescued_payload", raw_col.cast("string"))

    # Add technical lineage timestamps
    transformed = transformed.withColumn("_ingested_at", F.current_timestamp())
    return transformed
