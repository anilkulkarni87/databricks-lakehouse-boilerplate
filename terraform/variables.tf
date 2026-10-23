variable "databricks_host" {
  type        = string
  description = "Databricks workspace URL (e.g. https://adb-<id>.<suffix>.azuredatabricks.net)"
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev, stage, prod)"
  default     = "dev"
}

variable "cloud_provider" {
  type        = string
  description = "Cloud provider: 'azure' or 'aws'"
  default     = "azure"
}

variable "catalog_name" {
  type        = string
  description = "Unity Catalog name in Azure Databricks"
}

variable "domain_name" {
  type        = string
  description = "Default domain name for schemas (e.g. ecommerce, finance, subscriptions)"
  default     = "ecommerce"
}

variable "node_type_id" {
  type        = string
  description = "VM node type (e.g. Standard_D4s_v5 for Azure)"
  default     = "Standard_D4s_v5"
}

variable "manage_grants" {
  type        = bool
  description = "Whether to manage access grants for groups"
  default     = false
}

variable "service_principal_id" {
  type        = string
  description = "Optional Azure Service Principal Application ID to run jobs as"
  default     = null
}
