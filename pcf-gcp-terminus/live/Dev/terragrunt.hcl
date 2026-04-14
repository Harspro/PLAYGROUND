inputs = {
  business_unit                   = "pcf"
  subscription_type               = "nonprod"
  environment                     = "dev"
  deployment_environment_name     = "dev"
  pcb_compute_env_type            = "dev"
  deployment_environment_number   = "002"
  gcp_multiregion                 = "US"
  gcp_region                      = "northamerica-northeast1"
  gcp_zone                        = "northamerica-northeast1-b"
  vault_addr                      = "https://web-vault.dolphin.azure.nonprod.pcfcloud.io/"
  deploy_env_storage_suffix       = "-dev"
  money_movement_composer_name    = "money-movement"
  data_management_sa_name         = "service-411515563548@gcp-sa-dataplex.iam.gserviceaccount.com"
  data_management_conn_sa_name    = "bqcx-411515563548-qhbs@gcp-sa-bigquery-condel.iam.gserviceaccount.com"
  document_generation_count       = 0
  terraform_gcp_terminus_revision = "1.54.3"
}

remote_state {
  backend = "gcs"
  generate = {
    path      = "backend.tf"
    if_exists = "overwrite_terragrunt"
  }
  config = {
    bucket               = "pcb-dev-002--tfstate"
    skip_bucket_creation = true
    prefix               = "${path_relative_to_include()}"
  }
}

generate "provider" {
  path      = "provider.tf"
  if_exists = "overwrite_terragrunt"
  contents  = <<EOF
provider "google" {
    project = var.project_id
}
provider "google-beta" {
    project = var.project_id
}
EOF
}

generate "versions" {
  path      = "versions.tf"
  if_exists = "overwrite_terragrunt"
  contents  = <<EOF
terraform {
  required_version = ">= 1.14.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">=6.1.0, <8.0.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = ">=6.1.0, <8.0.0"
    }

    random = {
      source = "hashicorp/random"
      version = "3.1.0"
    }
    archive = {
      source = "hashicorp/archive"
      version = "2.2.0"
    }
    external = {
      source = "hashicorp/external"
      version = "2.1.0"
    }
    vault = {
      source  = "hashicorp/vault"
      version = "3.23.0"
    }
    venafi = {
      source = "venafi/venafi"
    }
  }
}

provider "vault" {
  address = "https://web-vault.dolphin.azure.nonprod.pcfcloud.io"
  skip_tls_verify = true
  auth_login_jwt {
    role  = "dp-gcp-terminus-terraformer"
  }
}

EOF
}
