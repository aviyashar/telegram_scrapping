output "cloud_run_service_uri" {
  description = "The URL of the Cloud Run service."
  value       = google_cloud_run_v2_service.main.uri
}

output "cloud_scheduler_job_name" {
  description = "The name of the Cloud Scheduler job."
  value       = google_cloud_scheduler_job.job.name
}
