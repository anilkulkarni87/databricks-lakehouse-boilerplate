import json
import re
from typing import Dict, Any, List, Tuple
from pathlib import Path

PII_KEYWORDS = {"email", "phone", "tax", "ssn", "contact", "address", "postal", "zip", "first_name", "last_name", "full_name"}

def infer_sql_type(value: Any) -> str:
    """Infers standard Spark SQL data type from Python values."""
    if value is None:
        return "string"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "bigint" if abs(value) > 2147483647 else "int"
    if isinstance(value, float):
        return "double"
    if isinstance(value, dict):
        return "variant"
    if isinstance(value, str):
        if re.match(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}", value):
            return "timestamp"
        if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
            return "date"
        return "string"
    return "string"

def is_pii_field(field_name: str) -> bool:
    clean = field_name.lower().replace("-", "_")
    return any(keyword in clean for keyword in PII_KEYWORDS)

def generate_entity_scaffold_yaml(
    entity_name: str,
    source_table: str,
    sample_record: Dict[str, Any],
    catalog: str = "${CATALOG:-main}",
    schema: str = "default"
) -> str:
    """
    Generates a production-ready declarative Lakehouse YAML configuration
    by inspecting a sample event or row.
    """
    gold_fields = []
    pii_fields = []
    pk_candidates = []

    # Source table breakdown
    parts = source_table.split(".")
    src_catalog = parts[0] if len(parts) == 3 else catalog
    src_schema = parts[1] if len(parts) >= 2 else "raw_landing"
    src_tbl = parts[-1]

    for col_name, val in sample_record.items():
        sql_type = infer_sql_type(val)
        
        # Check primary key candidates
        is_pk = False
        lower_name = col_name.lower()
        if lower_name.endswith("id") or lower_name in {"id", "uuid", "key"}:
            pk_candidates.append(col_name)
            if len(pk_candidates) == 1:
                is_pk = True

        if is_pii_field(col_name):
            pii_fields.append({
                "name": col_name,
                "type": sql_type,
                "expr": col_name,
                "description": f"Direct PII field for {col_name}"
            })
        else:
            gold_fields.append({
                "name": col_name,
                "type": sql_type,
                "expr": col_name,
                "is_primary_key": is_pk,
                "description": f"Curated field {col_name}"
            })

    primary_key = pk_candidates[0] if pk_candidates else (gold_fields[0]["name"] if gold_fields else "id")
    has_pii = len(pii_fields) > 0

    lines = [
        f'version: "1.0"',
        f'entity_name: "{entity_name}"',
        f'description: "Curated {entity_name} dimension with automated governance and data quality checks"',
        f'',
        f'source:',
        f'  catalog: "{src_catalog}"',
        f'  schema: "{src_schema}"',
        f'  table: "{src_tbl}"',
        f'',
        f'runtime:',
        f'  trigger_type: "AvailableNow"',
        f'  max_mbytes_per_trigger: 500',
        f'  checkpoint_location: "${{CHECKPOINT_ROOT:-/Volumes/main/default}}/checkpoints/{entity_name}"',
        f'  audit_table: "{catalog}.default.pipeline_batch_audit"',
        f'',
        f'gold:',
        f'  catalog: "{catalog}"',
        f'  schema: "{schema}"',
        f'  table: "dim_{entity_name}"',
        f'  table_comment: "Curated star-schema dimension for {entity_name}"',
        f'  table_type: "dimension"',
        f'  unique_keys: ["{primary_key}"]',
        f'  cluster_by: ["{primary_key}"]',
        f'  enable_dlq: true',
        f'  dlq_table: "{catalog}.{schema}.dim_{entity_name}_dlq"',
        f'  track_rescued_data: true',
        f'',
        f'  expectations:',
        f'    - name: "non_null_{primary_key}"',
        f'      expr: "{primary_key} IS NOT NULL"',
        f'      action: "quarantine"',
        f'      description: "Primary key cannot be null"',
        f'',
        f'  fields:'
    ]

    for f in gold_fields:
        lines.append(f'    - name: "{f["name"]}"')
        lines.append(f'      type: "{f["type"]}"')
        lines.append(f'      expr: "{f["expr"]}"')
        if f.get("is_primary_key"):
            lines.append(f'      is_primary_key: true')
        lines.append(f'      description: "{f["description"]}"')

    if has_pii:
        lines.extend([
            f'',
            f'pii_splits:',
            f'  - name: "{entity_name}_pii"',
            f'    target_catalog: "{catalog}"',
            f'    target_schema: "{schema}"',
            f'    target_table: "dim_{entity_name}_pii"',
            f'    table_comment: "Restricted PII customer identity dimension"',
            f'    unique_keys: ["{primary_key}"]',
            f'    foreign_key_expr: "xxhash64(cast({primary_key} as string))"',
            f'    foreign_key_name: "subscriber_account_id"',
            f'    cluster_by: ["{primary_key}"]',
            f'    fields:'
        ])
        for f in pii_fields:
            lines.append(f'      - name: "{f["name"]}"')
            lines.append(f'        type: "{f["type"]}"')
            lines.append(f'        expr: "{f["expr"]}"')
            lines.append(f'        description: "{f["description"]}"')

    return "\n".join(lines) + "\n"
