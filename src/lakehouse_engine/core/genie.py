import logging
from typing import List
from lakehouse_engine.models.config import EntityPipelineConfig

logger = logging.getLogger(__name__)

def generate_unity_catalog_constraints(config: EntityPipelineConfig) -> List[str]:
    """
    Generates Unity Catalog PK and FK constraint DDLs (NOT ENFORCED).
    Databricks Genie and the Photon Optimizer use these constraints to discover
    relationships and construct accurate multi-table joins.
    """
    ddl_statements = []
    table_fqdn = config.gold.fqdn

    # 1. Primary Key Constraint
    pk_cols = [f.name for f in config.gold.fields if f.is_primary_key] or config.gold.unique_keys
    if pk_cols:
        pk_cols_str = ", ".join(pk_cols)
        pk_name = f"pk_{config.gold.table}_{'_'.join(pk_cols)}"
        ddl_statements.append(
            f"ALTER TABLE {table_fqdn} ADD CONSTRAINT {pk_name} PRIMARY KEY ({pk_cols_str}) NOT ENFORCED;"
        )

    # 2. Foreign Key Constraints (Kimball Star Schema Links)
    for fk in config.gold.foreign_keys:
        fk_name = f"fk_{config.gold.table}_{fk.column}"
        ddl_statements.append(
            f"ALTER TABLE {table_fqdn} ADD CONSTRAINT {fk_name} "
            f"FOREIGN KEY ({fk.column}) REFERENCES {fk.referenced_table}({fk.referenced_column}) NOT ENFORCED;"
        )

    return ddl_statements

def generate_genie_space_documentation(configs: List[EntityPipelineConfig]) -> str:
    """
    Compiles declarative entity configurations into a complete Databricks Genie Space
    system prompt and curated instructions document.
    """
    doc_lines = [
        "# Databricks Genie Space System Instructions & Context",
        "",
        "## 1. Domain & Semantic Data Model",
        "This Genie Space provides conversational analytics over curated Kimball dimensional models and conformed lakehouse entities.",
        "",
        "### Available Tables & Relationships",
    ]

    for cfg in configs:
        t = cfg.gold
        doc_lines.extend([
            f"#### `{t.fqdn}` ({t.table_type.upper()})",
            f"- **Description**: {t.table_comment or cfg.description or 'No description provided.'}",
            f"- **Grain**: One row per `{', '.join(t.unique_keys)}`.",
        ])

        if t.foreign_keys:
            doc_lines.append("- **Relationships (Joins)**:")
            for fk in t.foreign_keys:
                rel_desc = f" ({fk.relationship_description})" if fk.relationship_description else ""
                doc_lines.append(f"  - Join `{t.fqdn}.{fk.column}` to `{fk.referenced_table}.{fk.referenced_column}`{rel_desc}")

        if t.metrics:
            doc_lines.append("- **Official Business Metrics & Formulas**:")
            for m in t.metrics:
                syns = f" (Synonyms: {', '.join(m.synonyms)})" if m.synonyms else ""
                doc_lines.append(f"  - **{m.name}**{syns}: `{m.sql_formula}` — *{m.description}*")

        doc_lines.append("")

    doc_lines.extend([
        "## 2. SQL Querying Best Practices for Genie",
        "1. **Always use official metrics**: When users ask for business KPIs, use the exact SQL formulas declared above.",
        "2. **Join on Primary/Foreign Keys**: Use the documented relationships to join fact and dimension tables.",
        "3. **Never query raw PII**: If a user asks for personal details (email, phone), query the anonymized/masked fields or inform the user that PII is restricted.",
        "4. **Handle Active Filters**: When querying status dimensions, ensure you filter on active lifecycle states unless historical trends are requested.",
        "",
        "## 3. Recommended Unity Catalog DDL Constraints",
        "Run these commands in Databricks SQL to register the entity relationships in Unity Catalog:",
        "```sql"
    ])

    for cfg in configs:
        for stmt in generate_unity_catalog_constraints(cfg):
            doc_lines.append(stmt)

    doc_lines.extend([
        "```",
        ""
    ])

    return "\n".join(doc_lines)
