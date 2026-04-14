include {
  path = find_in_parent_folders()
}

inputs = {
  cloud             = "gcp"
  common_name       = "scheduler-prod.gcp-prod.pcfcloud.io"
  san_dns           = ["scheduler-prod.gcp-prod.pcfcloud.io"]
  algorithm         = "RSA"
  rsa_bits          = "2048"
  url               = "https://cwhevtapr007.ngco.com"
  zone              = "Certificates\\PCF\\DevOps\\Terraform-GCP\\PROD"
}

terraform {
  source = "git::https://bitbucket.org/pc-technology/pcf-terraform-modules.git//common/venafi_certs_gcp?ref=release/main-165"
}