terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

locals {
  envs = { for tuple in regexall("(.*)=(.*)", file(".env")) : tuple[0] => sensitive(tuple[1]) }
}

provider "google" {
  credentials = file(var.credentials)
  project     = local.envs["GCP_PROJECT_ID"]
  region      = var.region
}


resource "google_storage_bucket" "financial-transaction-bucket" {
  name          = local.envs["GCP_GCS_BUCKET"]
  location      = var.location
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}


resource "google_bigquery_dataset" "financial_transaction_dataset" {
  dataset_id                 = local.envs["GCP_BIGQUERY_DATASET"]
  location                   = var.location
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "financial_transaction_dataset_dbt" {
  dataset_id                 = "financial_transaction_transformed_data"
  location                   = var.location
  delete_contents_on_destroy = true
}

# resource "google_project_service" "cloud_run_api" {
#   project = local.envs["GCP_PROJECT_ID"]
#   service = "compute.googleapis.com"
# }

# resource "google_project_service" "cloud_run_api" {
#   project = local.envs["GCP_PROJECT_ID"]
#   service = "run.googleapis.com"
# }

# resource "google_cloud_run_service" "renderer" {
#   name     = "renderer"
#   location = var.location

#   template {
#     spec {
#       containers {
#         image = "gcr.io/${var.project}/renderer:latest"
#       }
#     }
#   }

#   depends_on = [
#     google_project_service.cloud_run_api
#   ]
# }

# resource "google_compute_instance" "financa_transaction_vm" {
#   name         = var.vm_instance
#   machine_type = var.machine_type
#   zone         = var.region

#   service_account {
#     email  = "${var.project_service_account_id}@${local.envs["GCP_PROJECT_ID"]}.iam.gserviceaccount.com"
#     scopes = [
#       "userinfo-email",
#       "compute-ro",
#       "storage-full",
#       "https://www.googleapis.com/auth/iam",
#       "https://www.googleapis.com/auth/cloud-platform"
#     ]
#   }

#   boot_disk {
#     initialize_params {
#       image = "ubuntu-2004-focal-v20250213"
#     }
#   }

#   network_interface {
#     network = "default"
#     access_config {}
#   }
# }