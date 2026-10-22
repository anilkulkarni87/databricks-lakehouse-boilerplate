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

  tags = {
    "Environment" = var.environment
    "ManagedBy"   = "Terraform"
    "Framework"   = "LakehouseEngine"
  }

  schedule {
    quartz_cron_expression = var.schedule_cron
    timezone_id            = "UTC"
  }

  job_cluster {
    job_cluster_key = "single_node_worker"
    new_cluster {
      spark_version  = var.spark_version
      node_type_id   = var.node_type_id
      runtime_engine = "PHOTON"

      # Single-node spot cluster optimization
      custom_tags = {
        "ResourceClass" = "SingleNode"
      }
      spark_conf = {
        "spark.master"                      = "local[*]"
        "spark.databricks.cluster.profile"  = "singleNode"
        "spark.databricks.delta.optimizeWrites.enabled" = "true"
      }
      aws_attributes {
        first_on_demand        = 1
        zone_id                = "auto"
        spot_bid_price_percent = var.spot_bid_price_percent
      }
    }
  }

  task {
    task_key        = "run_entity_pipeline"
    job_cluster_key = "single_node_worker"

    library {
      whl = var.wheel_file_path
    }

    spark_python_task {
      python_file = "lakehouse_engine/cli.py"
      parameters = [
        "--config-path", var.config_file_path,
        "--env", var.environment
      ]
    }
  }
}
