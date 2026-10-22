# Delta Lake Engine Norms & Optimizations

This document explains the technical Delta Lake execution patterns implemented in `src/lakehouse_engine/core/delta_ops.py`.

---

## 1. Trigger.AvailableNow() Stateful Micro-Batching
Instead of running 24/7 continuous streaming clusters (expensive) or traditional ad-hoc scheduled batch queries (stateless, complex watermark tracking):
- The pipeline runs Spark Structured Streaming with `Trigger.AvailableNow()`.
- Spark processes all outstanding records that arrived since the last run in cost-optimal micro-batches, updates checkpoint state offsets, and terminates.
- Runs on single-node spot instances (`i3.xlarge` / `rgd-fleet.4xlarge`) to maximize DBU and cloud compute efficiency.

---

## 2. Liquid Clustering vs. Legacy Partitioning
- **Legacy Partitioning** creates rigid subdirectories (`/year=2026/month=10/day=21/`). When cardinality is high or query filters don't align with the partition key, this causes small file problems and massive scan overhead.
- **Liquid Clustering (`clusterBy`)**: Databricks reorganizes data files dynamically without directory rewriting.
- **Standard Rule**: Cluster Gold tables by event timestamps (`created_at`, `started_at`, `event_time`).

---

## 3. In-Batch Deduplication Before MERGE
If upstream delivers duplicated keys within the same micro-batch, a standard Delta `MERGE` will fail with:
`DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`

To prevent this:
- The engine runs a window function over incoming primary keys before calling `merge()`:
  ```python
  window_spec = Window.partitionBy(unique_keys).orderBy(F.col(order_by_col).desc())
  clean_source = df.withColumn("_row_rank", F.row_number().over(window_spec)).filter(F.col("_row_rank") == 1).drop("_row_rank")
  ```
- This guarantees exactly one record per key is presented to the target merge.

---

## 4. Date-Pruned Idempotent Delta MERGE
Naive Delta merges scan the entire target table (`target.id = source.id`), causing query runtimes to grow linearly as the table grows to hundreds of gigabytes.

To keep merge performance fast and sub-linear:
1. Extract the distinct event dates present in the current incoming micro-batch.
2. Inject a date filter into the join predicate:
   ```sql
   to_date(target.created_at) IN ('2026-10-20', '2026-10-21') AND target.id = source.id
   ```
3. Delta Lake skips all target files whose date min/max statistics don't overlap with the current batch dates.
