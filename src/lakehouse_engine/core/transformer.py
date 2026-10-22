from pyspark.sql import DataFrame, functions as F
from lakehouse_engine.models.config import TargetTableConfig, FieldDefinition

def register_helpers(spark):
    """
    Registers reusable SQL expression helpers if needed.
    """
    def _extract_attr(attributes, attr_name):
        if not attributes:
            return None
        for item in attributes:
            if item.get("name") == attr_name:
                return item.get("value")
        return None

    spark.udf.register("get_attribute", _extract_attr)

def transform_entity(df: DataFrame, config: TargetTableConfig) -> DataFrame:
    """
    Evaluates declarative field expressions from YAML against the payload DataFrame.
    """
    # Build select expressions
    select_exprs = []
    for field in config.fields:
        select_exprs.append(F.expr(field.expr).cast(field.type).alias(field.name))

    # Add technical lineage timestamps
    transformed = df.select(*select_exprs)
    transformed = transformed.withColumn("_ingested_at", F.current_timestamp())
    return transformed
