include {
  path = find_in_parent_folders()
}

inputs = {
  project_id                              = "pcb-prod-processing"
  project_zone                            = "processing"
  tf_state_data_foundation_prefix         = "tf-data-foundation"
  cloudstorage_name                       = "processing-zone"
  enable_pdf_afp_processing_bucket_access = true
  bq_dataset_ids = {
    acct_master_dataset             = "AccountMasterDataset"
    acct_scores_dataset             = "AccountScoresDataset"
    triad_report_dataset            = "TriadReportDataset"
    pco_scores_dataset              = "PCOScoresDataset"
    dbtran_dataset                  = "DbtranAuthDataset"
    acct_misc_dataset               = "AccountMiscDataset"
    cust_master_dataset             = "CustomerMasterDataset"
    dns_dataset                     = "DNSDataset"
    ods_dataset                     = "ODSDataset"
    domain_account_management       = "domain_account_management"
    domain_customer_management      = "domain_customer_management"
    domain_ledger                   = "domain_ledger"
    domain_retail                   = "domain_retail"
    domain_audit                    = "domain_audit"
    domain_fraud                    = "domain_fraud"
    domain_dispute                  = "domain_dispute"
    domain_aml                      = "domain_aml"
    domain_communication            = "domain_communication"
    domain_customer_acquisition     = "domain_customer_acquisition"
    domain_customer_service         = "domain_customer_service"
    domain_loyalty                  = "domain_loyalty"
    domain_marketing                = "domain_marketing"
    domain_payments                 = "domain_payments"
    domain_scoring                  = "domain_scoring"
    domain_securitization           = "domain_securitization"
    domain_technical                = "domain_technical"
    domain_treasury                 = "domain_treasury"
    domain_validation_verification  = "domain_validation_verification"
    cots_customer_service_ctt       = "cots_customer_service_ctt"
    domain_talent_acquisiton        = "domain_talent_acquisiton"
    cots_aml_sas                    = "cots_aml_sas"
    cots_alm_tbsm                   = "cots_alm_tbsm"
    snapshot_archive                = "snapshot_archive"
    cots_aml_verafin                = "cots_aml_verafin"
    domain_consent                  = "domain_consent"
    domain_iam                      = "domain_iam"
    domain_dns                      = "domain_dns"
    cots_customer_service_blockworx = "cots_customer_service_blockworx"
    cots_fraud_ffm                  = "cots_fraud_ffm"
    domain_payments_ops             = "domain_payments_ops"
    domain_tax_slips                = "domain_tax_slips"
    domain_cdic                     = "domain_cdic"
    cots_cdic                       = "cots_cdic"
    cots_scms_tkm                   = "cots_scms_tkm"
    cots_scms_andis_se              = "cots_scms_andis_se"
    domain_card_management          = "domain_card_management"
    sql_server_tables               = "sql_server_tables"
    domain_ops_adhoc                = "domain_ops_adhoc"
    domain_fraud_risk_intelligence  =  "domain_fraud_risk_intelligence"
    domain_cri_compliance           = "domain_cri_compliance"
  }
  delete_contents_on_destroy = true
  landing_project_id         = "pcb-prod-landing"
  processing_sa              = "infra-admin-sa@pcb-prod-processing.iam.gserviceaccount.com"
  siem_pubsub_topic          = "pubsub.googleapis.com/projects/pcb-prod-dataflow/topics/pcb-prod-siem-logs"
  environment                = "prod"
  pdf_generation_cf_version  = "0.42.3"
  model_inference_cf_version = "1.0.0"

  vpc_connectors = {
    name               = "pcb-prod-proc-connector"
    egress_setting     = "PRIVATE_RANGES_ONLY"
    egress_setting_all = "ALL_TRAFFIC"
  }

  data_management_poc_flag = 0
}


locals {
  common_config = read_terragrunt_config(find_in_parent_folders("terragrunt.hcl"))
  terraform_gcp_terminus_revision = local.common_config.inputs.terraform_gcp_terminus_revision
}

terraform {
  source = "git::https://gitlab.lblw.ca/pcf-engineering/shared/pcf-terraform-modules/pcf-gcp-terraform-modules/terraform-gcp-terminus.git//processingzone?ref=${local.terraform_gcp_terminus_revision}"
}

