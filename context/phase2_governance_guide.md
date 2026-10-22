# Phase 2 Governance: Declarative Expectations & Historical Backfills

This document explains the two governance capabilities implemented in Phase 2:
1. **Declarative Data Quality Expectations (Circuit Breakers & Quarantining)**
2. **Historical Backfill CLI (`lakehouse-engine backfill`)**

---

## 1. Declarative Data Quality Expectations

In traditional ETL pipelines, data quality checks are either missing, hardcoded in complex PySpark `assert` statements, or deferred to downstream post-load checks.

In this framework, data quality rules are declared directly inside the entity YAML alongside schema definitions:

```yaml
gold:
  expectations:
    # 1. Circuit Breaker (Halts batch execution immediately)
    - name: "valid_mrr_non_negative"
      expr: "mrr_amount >= 0.0"
      action: "fail_batch"
      description: "MRR cannot be negative."

    # 2. Automated Quarantine (Diverts bad rows to DLQ, allows clean rows through)
    - name: "valid_subscription_status"
      expr: "status IN ('ACTIVE', 'PAST_DUE', 'CANCELLED', 'PAUSED')"
      action: "quarantine"
      description: "Unrecognized status codes are quarantined to DLQ."

    # 3. Soft Warning (Logs alert and telemetry without dropping rows)
    - name: "non_future_started_timestamp"
      expr: "started_at <= current_timestamp()"
      action: "warn"
      description: "Alerts if started_at is post-dated."
```

### Expectation Actions Matrix:

| Action | Behavior When Expression Evaluates to `FALSE` | Target Destination |
| :--- | :--- | :--- |
| `fail_batch` | Raises `DataQualityException`. Aborts micro-batch, rolls back transaction, and logs failure in `audit_table`. | Nowhere (Transaction aborted) |
| `quarantine` | Isolates violating rows into `<target_table>_dlq` tagged with `_dlq_reason: FAILED_EXPECTATION:<name>`. Valid rows continue to Gold. | `<target_table>_dlq` |
| `warn` | Emits a warning to logger and telemetry audit log. Rows proceed to Gold. | Target Gold table |

---

## 2. Historical Backfill CLI (`lakehouse-engine backfill`)

### The Challenge with Streaming Pipelines:
When streaming with `Trigger.AvailableNow()`, the pipeline tracks checkpoint offsets. If you need to:
- Re-calculate a column where upstream logic changed 6 months ago.
- Reprocess a historical date window (e.g., January 1 to March 31).

Running the streaming job will **not** process historical data because the offsets have already moved past those records.

### The Solution:
Use the dedicated `backfill` CLI command:

```bash
# Reprocess a specific date range
python3 -m lakehouse_engine.cli backfill \
  --config-path configs/examples/subscriptions.yml \
  --start-date 2026-01-01 \
  --end-date 2026-03-31

# Reprocess all historical data and reset the streaming checkpoint
python3 -m lakehouse_engine.cli backfill \
  --config-path configs/examples/subscriptions.yml \
  --reset-checkpoint
```

### How the Backfill Operates:
1. **Batch Mode**: Reads the source table using batch APIs (`spark.read.table`), bypassing streaming offsets.
2. **Date Pruning Filter**: Evaluates `to_date(prune_column) BETWEEN start_date AND end_date`.
3. **Identical Transform & DLQ Pipeline**: Runs the exact same `transform_entity`, `evaluate_data_quality_rules`, and PII split logic.
4. **Idempotent Merge**: Executes the same pruned Delta MERGE into Gold—updating existing historical rows and inserting any newly discovered rows without duplicates.
5. **Telemetry**: Logs a completion record to `audit_table` with `batch_id: -1` and `status: "SUCCESS (BACKFILL)"`.
6. **Streaming Integrity**: The streaming checkpoint is preserved (unless `--reset-checkpoint` is explicitly passed), allowing the scheduled streaming job to resume seamlessly on the next hourly trigger.
