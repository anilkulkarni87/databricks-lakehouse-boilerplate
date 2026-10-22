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

# Grant Analysts read-only access to standard non-PII schema
resource "databricks_grants" "standard_schema_grants" {
  schema = "${var.catalog_name}.${var.schema_name}"

  grant {
    principal  = var.analysts_group
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}

# Grant Privileged users access to PII schema
resource "databricks_grants" "pii_schema_grants" {
  schema = "${var.catalog_name}.${var.pii_schema_name}"

  grant {
    principal  = var.privileged_pii_group
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}

# Unity Catalog Column Masking Function for Email
resource "databricks_sql_function" "mask_email" {
  name     = "${var.catalog_name}.${var.pii_schema_name}.mask_email"
  routine_definition = "CASE WHEN is_account_group_member('${var.privileged_pii_group}') THEN email ELSE CONCAT(LEFT(email, 2), '****@', SPLIT(email, '@')[1]) END"
  data_type = "STRING"
  parameter {
    name      = "email"
    data_type = "STRING"
  }
}
