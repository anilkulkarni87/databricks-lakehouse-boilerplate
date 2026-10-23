variable "catalog_name" {
  type        = string
  description = "Target Unity Catalog name"
}

variable "schema_name" {
  type        = string
  description = "Standard domain schema name"
}

variable "pii_schema_name" {
  type        = string
  description = "Restricted PII domain schema name"
}

variable "manage_grants" {
  type        = bool
  description = "Whether to manage access grants for groups (set to false if groups do not exist yet)"
  default     = false
}

variable "analysts_group" {
  type        = string
  description = "Databricks group for standard analytics users"
  default     = "data_analysts"
}

variable "privileged_pii_group" {
  type        = string
  description = "Databricks group allowed to access unmasked PII"
  default     = "pii_privileged_users"
}
