terraform {
  backend "gcs" {
    bucket = "pwcnext-sandbox01-terraform-state"
    prefix = "telegram-scraper"
  }

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
