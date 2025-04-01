variable "GCP_PROJECT_ID" {
  description = "GCP Project ID"
  type        = string
}

variable "GCP_GCS_BUCKET" {
  description = "GCS Bucket Name"
  type        = string
}

variable "GCP_BIGQUERY_DATASET" {
  description = "BigQuery Dataset"
  type        = string
}



variable "credentials" {
  description = "My Credentials"
  default     = "./airflow-etl/.keys/google_cloud/google_creds.json"
}


variable "project_service_account_id" {
  description = "value"
  default     = "finance-trans-service-account"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default = "europe-west2-a"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default = "EU"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "vm_instance" {
  description = "Name of VM Instance"
  default     = "finance-transaction-vm"
}

variable "machine_type" {
  description = "VM machine type"
  default     = "e2-standard-4"
}