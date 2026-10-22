from pyspark.sql import DataFrame, functions as F
from lakehouse_engine.models.config import PiiSplitConfig

def extract_pii_split(df: DataFrame, pii_config: PiiSplitConfig) -> DataFrame:
    """
    Extracts PII fields from payload into a separate isolated DataFrame.
    Generates foreign key link back to the non-PII entity table.
    """
    select_exprs = [
        F.expr(pii_config.foreign_key_expr).alias(pii_config.foreign_key_name)
    ]

    for field in pii_config.fields:
        select_exprs.append(F.expr(field.expr).cast(field.type).alias(field.name))

    pii_df = df.select(*select_exprs)
    pii_df = pii_df.withColumn("_ingested_at", F.current_timestamp())

    # Filter out rows where all PII fields are null
    pii_cols = [field.name for field in pii_config.fields]
    non_null_condition = " OR ".join([f"{c} IS NOT NULL" for c in pii_cols])
    return pii_df.filter(non_null_condition)
