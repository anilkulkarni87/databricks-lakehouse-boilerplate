variable "databricks_host" {
  type        = string
  description = "Databricks workspace URL"
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev, stage, prod)"
  default     = "dev"
}

variable "catalog_name" {
  type        = string
  description = "Unity Catalog name"
}

variable "domain_name" {
  type        = string
  description = "Domain name for schemas"
  default     = "operations"
}

variable "wheel_file_path" {
  type        = string
  description = "Path to wheel artifact in DBFS or UC Volume"
}
