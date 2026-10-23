# Standard Domain Schema (Analytics / BI)
resource "databricks_schema" "standard_schema" {
  catalog_name = var.catalog_name
  name         = var.schema_name
  comment      = "Standard curated analytics schema (Non-PII)"
}

# Restricted PII Schema (Restricted access)
resource "databricks_schema" "pii_schema" {
  catalog_name = var.catalog_name
  name         = var.pii_schema_name
  comment      = "Restricted PII data layer with fine-grained access control"
}

# Unity Catalog Volumes for Engine Libraries, Configs, and Checkpoints
resource "databricks_volume" "library_volume" {
  name         = "library"
  catalog_name = var.catalog_name
  schema_name  = databricks_schema.standard_schema.name
  volume_type  = "MANAGED"
  comment      = "Volume for engine wheels and libraries"
}

resource "databricks_volume" "configs_volume" {
  name         = "configs"
  catalog_name = var.catalog_name
  schema_name  = databricks_schema.standard_schema.name
  volume_type  = "MANAGED"
  comment      = "Volume for entity pipeline YAML definitions"
}

resource "databricks_volume" "checkpoints_volume" {
  name         = "checkpoints"
  catalog_name = var.catalog_name
  schema_name  = databricks_schema.standard_schema.name
  volume_type  = "MANAGED"
  comment      = "Volume for streaming checkpoints"
}

# Optional: Grant Analysts read-only access to standard non-PII schema
resource "databricks_grants" "standard_schema_grants" {
  count  = var.manage_grants ? 1 : 0
  schema = "${var.catalog_name}.${var.schema_name}"

  grant {
    principal  = var.analysts_group
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}

# Optional: Grant Privileged users access to PII schema
resource "databricks_grants" "pii_schema_grants" {
  count  = var.manage_grants ? 1 : 0
  schema = "${var.catalog_name}.${var.pii_schema_name}"

  grant {
    principal  = var.privileged_pii_group
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}

# Unity Catalog Column Masking Function for Email
resource "databricks_sql_function" "mask_email" {
  name               = "${var.catalog_name}.${var.pii_schema_name}.mask_email"
  routine_definition = "CASE WHEN is_account_group_member('${var.privileged_pii_group}') THEN email ELSE CONCAT(LEFT(email, 2), '****@', SPLIT(email, '@')[1]) END"
  data_type          = "STRING"
  parameter {
    name      = "email"
    data_type = "STRING"
  }
}
