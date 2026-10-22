output "job_id" {
  value       = databricks_job.pipeline_job.id
  description = "Databricks Job ID"
}

output "job_url" {
  value       = databricks_job.pipeline_job.url
  description = "Databricks Job URL"
}
