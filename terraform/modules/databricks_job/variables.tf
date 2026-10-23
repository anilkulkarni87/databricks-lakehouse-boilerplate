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
  description = "Single-node VM instance type (e.g. Standard_D4s_v5 for Azure, i3.xlarge for AWS)"
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
  description = "Unity Catalog Volume path to the entity YAML config file"
}

variable "service_principal_id" {
  type        = string
  description = "Service principal client ID to run the job as"
  default     = null
}
