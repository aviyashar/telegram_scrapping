resource "google_secret_manager_secret" "telegram_api_id" {
  secret_id = "telegram-scraper-api-id"

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret" "telegram_api_hash" {
  secret_id = "telegram-scraper-api-hash"

  replication {
    auto {}
  }
}

# Grant the Cloud Run service account access to secrets
resource "google_secret_manager_secret_iam_member" "telegram_api_id_access" {
  secret_id = google_secret_manager_secret.telegram_api_id.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:telegram-scraper-sa@${var.project_id}.iam.gserviceaccount.com"
}

resource "google_secret_manager_secret_iam_member" "telegram_api_hash_access" {
  secret_id = google_secret_manager_secret.telegram_api_hash.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:telegram-scraper-sa@${var.project_id}.iam.gserviceaccount.com"
}
