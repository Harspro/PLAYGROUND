include {
  path = find_in_parent_folders()
}

inputs = {
  cloud             = "gcp"
  common_name       = "scheduler-dev.gcp-nonprod.pcfcloud.io"
  san_dns           = ["scheduler-dev.gcp-nonprod.pcfcloud.io"]
  algorithm         = "RSA"
  rsa_bits          = "2048"
  cost_centre       = "248157"
  url               = "https://cwhevtapr007.ngco.com"
  zone              = "Certificates\\PCF\\DevOps\\Terraform-GCP\\NonPROD"
}

terraform {
  source = "git::https://bitbucket.org/pc-technology/pcf-terraform-modules.git//common/venafi_certs_gcp?ref=release/main-165"
}