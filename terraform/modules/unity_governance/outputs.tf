output "standard_schema_id" {
  value = databricks_schema.standard_schema.id
}

output "pii_schema_id" {
  value = databricks_schema.pii_schema.id
}

output "mask_email_function" {
  value = databricks_sql_function.mask_email.id
}
