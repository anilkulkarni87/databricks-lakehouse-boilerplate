import logging
from typing import List, Tuple
from pyspark.sql import DataFrame, functions as F
from lakehouse_engine.models.config import ExpectationConfig

logger = logging.getLogger(__name__)

class DataQualityException(Exception):
    """Raised when an expectation with action='fail_batch' is violated."""
    pass

def evaluate_data_quality_rules(
    df: DataFrame,
    expectations: List[ExpectationConfig]
) -> Tuple[DataFrame, DataFrame, List[str]]:
    """
    Evaluates declarative data quality expectations against a transformed DataFrame.
    Actions:
      - 'fail_batch': Halts execution immediately if any row violates the condition.
      - 'quarantine': Segregates violating rows to the Dead-Letter Queue with failure reason.
      - 'warn': Logs warnings and metrics without dropping records.
    """
    if not expectations or df.isEmpty():
        empty_dlq = df.limit(0).withColumn("_dlq_reason", F.lit("")).withColumn("_dlq_timestamp", F.current_timestamp())
        return df, empty_dlq, []

    current_df = df
    quarantine_dfs = []
    warnings = []

    for exp in expectations:
        rule_condition = F.coalesce(F.expr(exp.expr), F.lit(False))

        if exp.action == "fail_batch":
            failing_rows = current_df.filter(~rule_condition)
            failing_count = failing_rows.count()
            if failing_count > 0:
                error_msg = f"Data Quality Circuit Breaker: Expectation '{exp.name}' failed with {failing_count} invalid row(s). Rule: ({exp.expr})"
                logger.error(error_msg)
                raise DataQualityException(error_msg)

        elif exp.action == "quarantine":
            passing_rows = current_df.filter(rule_condition)
            violating_rows = (
                current_df.filter(~rule_condition)
                .withColumn("_dlq_reason", F.lit(f"FAILED_EXPECTATION:{exp.name} ({exp.expr})"))
                .withColumn("_dlq_timestamp", F.current_timestamp())
            )

            if not violating_rows.isEmpty():
                logger.warning(f"Expectation '{exp.name}' quarantined {violating_rows.count()} row(s). Rule: ({exp.expr})")
                quarantine_dfs.append(violating_rows)

            current_df = passing_rows

        elif exp.action == "warn":
            failing_count = current_df.filter(~rule_condition).count()
            if failing_count > 0:
                warn_msg = f"Expectation '{exp.name}' warning: {failing_count} row(s) violated rule ({exp.expr})"
                logger.warning(warn_msg)
                warnings.append(warn_msg)

    # Combine all quarantined rows from expectations
    if quarantine_dfs:
        combined_dlq = quarantine_dfs[0]
        for q_df in quarantine_dfs[1:]:
            combined_dlq = combined_dlq.unionByName(q_df)
    else:
        combined_dlq = df.limit(0).withColumn("_dlq_reason", F.lit("")).withColumn("_dlq_timestamp", F.current_timestamp())

    return current_df, combined_dlq, warnings
