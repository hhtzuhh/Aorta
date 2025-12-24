# ============================================================
# Aorta - Terraform Variables
# ============================================================

variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API Key (create at https://confluent.cloud/settings/api-keys)"
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API Secret"
  type        = string
  sensitive   = true
}

variable "gcp_region" {
  description = "GCP region for Confluent Cloud cluster deployment"
  type        = string
  default     = "us-central1"

  validation {
    condition = contains([
      "us-central1",
      "us-east1",
      "us-west1",
      "us-west2",
      "us-east4",
      "asia-east1",
      "asia-northeast1",
      "asia-southeast1",
      "europe-west1",
      "europe-west2",
      "europe-west3"
    ], var.gcp_region)
    error_message = "Must be a valid GCP region supported by Confluent Cloud."
  }
}

variable "gcp_project_id" {
  description = "GCP Project ID (for future GCP resource integration)"
  type        = string
  default     = ""
}

variable "owner_email" {
  description = "Email address of resource owner (for tagging)"
  type        = string
  default     = ""
}
