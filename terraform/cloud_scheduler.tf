resource "google_cloud_scheduler_job" "job" {
  name        = "${var.service_name}-trigger"
  description = "Triggers the ${var.service_name} Cloud Run service daily"
  schedule    = "0 3 * * *" # Runs every day at 3:00 AM UTC

  http_target {
    uri         = "${google_cloud_run_v2_service.main.uri}/run"
    http_method = "POST"

    oidc_token {
      service_account_email = "telegram-scraper-sa@${var.project_id}.iam.gserviceaccount.com"
    }
  }

  retry_config {
    retry_count = 1
  }
}
