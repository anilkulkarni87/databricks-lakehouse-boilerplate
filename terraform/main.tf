terraform {
  required_version = ">= 1.5.0"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50.0"
    }
  }
}

provider "databricks" {
  host = var.databricks_host
}

# 1. Provision Unity Catalog Schemas, Volumes (library, configs, checkpoints) & Masking
module "governance" {
  source          = "./modules/unity_governance"
  catalog_name    = var.catalog_name
  schema_name     = var.domain_name
  pii_schema_name = "${var.domain_name}_pii"
  manage_grants   = var.manage_grants
}

# 2. Production Databricks Workflow Job for Subscriptions
module "subscriptions_pipeline_job" {
  source           = "./modules/databricks_job"
  job_name         = "lakehouse-subscriptions"
  environment      = var.environment
  cloud_provider   = var.cloud_provider
  node_type_id     = var.node_type_id
  wheel_file_path  = "/Volumes/${var.catalog_name}/${var.domain_name}/library/lakehouse_engine-0.1.0-py3-none-any.whl"
  config_file_path = "/Volumes/${var.catalog_name}/${var.domain_name}/configs/subscriptions.yml"
  schedule_cron    = "0 15 * * * ?" # Runs at 15 mins past every hour
}
