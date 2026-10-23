variable "job_name" {
  type        = string
  description = "Name of the Databricks workflow job"
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev, stage, prod)"
}

variable "cloud_provider" {
  type        = string
  description = "Cloud provider hosting Databricks: 'azure' or 'aws'"
  default     = "azure"
}

variable "domain" {
  type        = string
  description = "Business domain for this job (e.g. ecommerce, finance, subscriptions)"
  default     = "ecommerce"
}

variable "catalog_name" {
  type        = string
  description = "Unity Catalog name"
  default     = null
}

variable "schedule_cron" {
  type        = string
  description = "Quartz cron expression for job schedule"
  default     = "0 0 * * * ?" # Hourly default
}

variable "spark_version" {
  type        = string
  description = "Databricks Runtime (DBR) version"
  default     = "15.4.x-scala2.12"
}

variable "node_type_id" {
  type        = string
  description = "Single-node VM instance type (e.g. Standard_D4s_v5 for Azure, rgd-fleet.xlarge for AWS)"
  default     = "Standard_D4s_v5"
}

variable "use_spot" {
  type        = bool
  description = "Whether to use Spot instances with fallback for cost savings"
  default     = true
}

variable "spot_bid_price_percent" {
  type        = number
  description = "Spot bid price percent for AWS"
  default     = 100
}

variable "wheel_file_path" {
  type        = string
  description = "Unity Catalog Volume path to the uploaded Python wheel artifact"
}

variable "config_file_path" {
  type        = string
  description = "Explicit Unity Catalog Volume path to the entity YAML config file"
  default     = null
}

variable "config_file_name" {
  type        = string
  description = "Config filename inside the domain configs volume (e.g. subscriptions.yml)"
  default     = null
}

variable "service_principal_id" {
  type        = string
  description = "Service principal client ID / application ID to run the job as"
  default     = null
}

variable "data_security_mode" {
  type        = string
  description = "Unity Catalog data security mode (e.g. SINGLE_USER, USER_ISOLATION, NONE)"
  default     = "SINGLE_USER"
}

variable "max_retries" {
  type        = number
  description = "Max retry attempts on failure"
  default     = 3
}

variable "custom_tags" {
  type        = map(string)
  description = "Additional tags to merge with default job tags"
  default     = {}
}
