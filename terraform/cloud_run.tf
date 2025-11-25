resource "google_cloud_run_v2_service" "main" {
  name     = var.service_name
  location = var.region

  template {
    service_account = "telegram-scraper-sa@${var.project_id}.iam.gserviceaccount.com"
    timeout         = "3600s"

    containers {
      image = var.image_url

      ports {
        container_port = 8080
      }

      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }

      env {
        name  = "BQ_PROJECT_ID"
        value = var.project_id
      }

      env {
        name  = "BQ_DATASET"
        value = "telegram"
      }

      env {
        name  = "BQ_TABLE"
        value = "telegram_messages"
      }

      env {
        name  = "BQ_METADATA_TABLE"
        value = "telegram_last_ingestion"
      }

      env {
        name  = "BQ_GROUPS_TABLE"
        value = "groups"
      }

      env {
        name = "TELEGRAM_API_ID"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.telegram_api_id.secret_id
            version = "latest"
          }
        }
      }

      env {
        name = "TELEGRAM_API_HASH"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.telegram_api_hash.secret_id
            version = "latest"
          }
        }
      }

      resources {
        limits = {
          cpu    = "4"
          memory = "8Gi"
        }
      }
    }
  }

  depends_on = [
    google_secret_manager_secret.telegram_api_id,
    google_secret_manager_secret.telegram_api_hash
  ]
}

# Allow unauthenticated access is not needed - we use Cloud Scheduler with OIDC
