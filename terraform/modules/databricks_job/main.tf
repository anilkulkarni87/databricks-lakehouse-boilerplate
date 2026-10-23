locals {
  resolved_config_path = var.config_file_path != null ? var.config_file_path : (
    var.catalog_name != null && var.config_file_name != null ? "/Volumes/${var.catalog_name}/${var.domain}/configs/${var.config_file_name}" : ""
  )
}

resource "databricks_job" "pipeline_job" {
  name                = "${var.job_name}-${var.environment}"
  max_concurrent_runs = 1
  timeout_seconds     = 14400 # 4 hours

  dynamic "run_as" {
    for_each = var.service_principal_id != null ? [var.service_principal_id] : []
    content {
      service_principal_name = run_as.value
    }
  }

  tags = merge(
    {
      "Environment"   = var.environment
      "ManagedBy"     = "Terraform"
      "Framework"     = "LakehouseEngine"
      "CloudProvider" = var.cloud_provider
      "Domain"        = var.domain
    },
    var.custom_tags
  )

  schedule {
    quartz_cron_expression = var.schedule_cron
    timezone_id            = "UTC"
  }

  job_cluster {
    job_cluster_key = "single_node_worker"
    new_cluster {
      num_workers             = 0
      spark_version           = var.spark_version
      node_type_id            = var.node_type_id
      driver_node_type_id     = var.node_type_id
      runtime_engine          = "PHOTON"
      data_security_mode      = var.data_security_mode
      single_user_name        = var.data_security_mode == "SINGLE_USER" ? var.service_principal_id : null

      # Single-node cluster optimization
      custom_tags = {
        "ResourceClass" = "SingleNode"
      }

      spark_conf = {
        "spark.master"                                  = "local[*,4]"
        "spark.databricks.cluster.profile"              = "singleNode"
        "spark.databricks.delta.optimizeWrites.enabled"  = "true"
        "spark.databricks.dataLineage.enabled"          = "true"
        "spark.sql.files.ignoreMissingFiles"            = "true"
        "spark.scheduler.mode"                          = "FIFO"
      }

      # Azure Spot Configuration
      dynamic "azure_attributes" {
        for_each = var.cloud_provider == "azure" ? [1] : []
        content {
          availability    = var.use_spot ? "SPOT_WITH_FALLBACK_AZURE" : "ON_DEMAND_AZURE"
          first_on_demand = 1
        }
      }

      # AWS Spot Configuration
      dynamic "aws_attributes" {
        for_each = var.cloud_provider == "aws" ? [1] : []
        content {
          first_on_demand        = 1
          zone_id                = "auto"
          spot_bid_price_percent = var.spot_bid_price_percent
        }
      }
    }
  }

  task {
    task_key        = "run_entity_pipeline"
    job_cluster_key = "single_node_worker"
    max_retries     = var.max_retries

    library {
      whl = var.wheel_file_path
    }

    python_wheel_task {
      package_name = "lakehouse_engine"
      entry_point  = "cli"
      parameters = [
        "run",
        "--config-path", local.resolved_config_path,
        "--env", var.environment
      ]
    }
  }
}
