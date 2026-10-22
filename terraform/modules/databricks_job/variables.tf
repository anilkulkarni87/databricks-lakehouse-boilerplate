variable "job_name" {
  type        = string
  description = "Name of the Databricks workflow job"
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev, stage, prod)"
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
  description = "Single-node VM instance type"
  default     = "i3.xlarge"
}

variable "spot_bid_price_percent" {
  type        = number
  description = "Spot bid price percent for cost optimization"
  default     = 100
}

variable "wheel_file_path" {
  type        = string
  description = "DBFS or Volume path to the uploaded Python wheel artifact"
}

variable "config_file_path" {
  type        = string
  description = "DBFS or Volume path to the entity YAML config file"
}

variable "service_principal_id" {
  type        = string
  description = "Service principal client ID to run the job as"
  default     = null
}
