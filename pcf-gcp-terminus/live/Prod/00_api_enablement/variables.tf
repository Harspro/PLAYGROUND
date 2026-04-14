variable "project_id" {
  type        = string
  default     = "pcb-prod-terraform-seed"
  description = "project to enable APIs in"
}

variable "project_to_api_mapping" {
  default = {
    pcb-prod-processing = [
      "pubsub.googleapis.com",
      "monitoring.googleapis.com",
      "dataproc.googleapis.com",
      "websecurityscanner.googleapis.com",
      "cloudkms.googleapis.com",
      "composer.googleapis.com",
      "container.googleapis.com",
      "bigquery.googleapis.com",
      "networkmanagement.googleapis.com"
    ]
    pcb-prod-curated = [
      "websecurityscanner.googleapis.com",
      "bigquery.googleapis.com",
      "bigquerystorage.googleapis.com",
      "networkmanagement.googleapis.com"
    ]
    pcb-prod-landing = [
      "dataproc.googleapis.com",
      "websecurityscanner.googleapis.com",
      "composer.googleapis.com",
      "bigquery.googleapis.com",
      "iap.googleapis.com",
      "pubsub.googleapis.com",
      "networkmanagement.googleapis.com"
    ]
    pcb-prod-svpc = [
      "composer.googleapis.com",
      "container.googleapis.com",
      "networkmanagement.googleapis.com"
    ]
  }
  type = map(any)
}