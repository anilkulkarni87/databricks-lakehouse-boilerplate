# Databricks Native VARIANT Data Type Guide

This guide explains the architectural and performance advantages of Databricks' native binary `VARIANT` data type (DBR 15.3+ / Apache Spark 4.0+) implemented in Phase 3.

---

## 1. The Problem with JSON as Strings

In legacy lakehouses, semi-structured data is stored as a raw text string (`StringType`):
```sql
SELECT get_json_object(payload, '$.plan.amountCents') FROM silver_events;
```
### Bottlenecks:
1. **Full String Parsing on Every Read**: Every query must scan and parse megabytes of raw text into temporary memory objects.
2. **Zero Columnar Pruning**: Spark cannot prune sub-fields. Reading one nested field requires scanning the entire multi-kilobyte JSON string from disk.
3. **Photon Inefficiency**: The native C++ Photon execution engine must fall back to JVM text processing for string JSON manipulation.

---

## 2. What is Databricks Native `VARIANT`?

`VARIANT` is a native binary encoding for semi-structured data that automatically **shreds** sub-fields into columnar storage behind the scenes.

```
Raw JSON String (Slow Text) ──> try_parse_json(payload) ──> Native Binary VARIANT
                                                                    │
                                      ┌─────────────────────────────┴─────────────────────────────┐
                                      ▼                                                           ▼
                           Shredded Sub-Columns                                    Unshredded Variant Remainder
                 (Parquet binary columns with min/max statistics)                (Preserves arbitrary new fields)
```

### Key Capabilities:
1. **Sub-Column Pruning**: If you query `payload:plan.amountCents`, Spark only reads the bytes for `amountCents` off storage. The rest of the payload is skipped.
2. **Path Navigation Syntax**:
   - Navigation: `payload:path.to.field`
   - Direct Casting: `payload:path.to.field::double`
   - Array Access: `payload:items[0].id::string`
3. **8–10x Performance Gain**: Benchmarks show query speeds rivaling fully typed native columns while retaining the flexibility of schemaless JSON.

---

## 3. How This Boilerplate Implements `VARIANT`

1. **Automatic Conversion at Ingest**:
   In `src/lakehouse_engine/core/transformer.py`, incoming payload strings are passed through `try_parse_json(payload)`, producing native `VARIANT` for the downstream projection.
2. **Zero-Loss Rescued Data**:
   The engine stores `_rescued_payload` directly as native `VARIANT` (`store_rescued_as_variant: true`). If an upstream API adds new unexpected keys, they are stored in shredded binary format rather than uncompressed string blobs.
3. **Backward Compatibility**:
   If deployed to an environment without native VARIANT support (e.g. local Python/Spark < 3.5), the engine catches the exception and falls back to string representation automatically.
