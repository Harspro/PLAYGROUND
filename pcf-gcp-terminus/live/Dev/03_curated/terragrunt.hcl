include {
  path = find_in_parent_folders()
}

dependency "processing_zone" {
  config_path = "../02_processing"
}

inputs = {
  project_id                     = "pcb-dev-curated"
  project_id_landing             = "pcb-dev-landing"
  cots_mft_sterling              = "cots_mft_sterling"
  project_zone                   = "curated"
  cloudstorage_name              = "curated-zone"
  creditrisk_business_project_id = "pcb-nonprod-creditrisk-scoring"
  creditrisk_business_dataset_id = "Dan_Tran"
  vpc_connectors = {
    name = "pcb-dev-curated-connector"
    egress_setting = "ALL_TRAFFIC"
  }
  bq_dataset_ids              = {
    acct_scores_dataset               = "AccountScoresDataset"
    triad_report_dataset              = "TriadReportDataset"
    ods_dataset                       = "ODSDataset"
    ledger_dataset                    = "LedgerDataset"
    pco_scores_dataset                = "PCOScoresDataset"
    job_control_dataset               = "JobControlDataset"
    experian_dataset                  = "ExperianDataset"
    stores_list_dataset               = "StoresListDataset"
    idvmgmt_dataset                   = "IDVMGMTDataset"
    uwork_dataset                     = "UWORKDataset"
    cardinal_notif_dataset            = "CardinalNotifDataset"
    acct_misc_dataset                 = "AccountMiscDataset"
    cust_master_dataset               = "CustomerMasterDataset"
    dns_dataset                       = "DNSDataset"
    cots_fraud_ffm_vw                 = "cots_fraud_ffm_vw"
    domain_audit_vw                   = "domain_audit_vw"
    domain_aml                        = "domain_aml"
    domain_payments                   = "domain_payments"
    domain_account_management         = "domain_account_management"
    domain_customer_management        = "domain_customer_management"
    domain_retail                     = "domain_retail"
    cots_customer_service_ctt         = "cots_customer_service_ctt"
    domain_customer_acquisition       = "domain_customer_acquisition"
    cots_loyalty_teradata             = "cots_loyalty_teradata"
    domain_fraud                      = "domain_fraud"
    domain_validation_verification    = "domain_validation_verification"
    domain_communication              = "domain_communication"
    domain_customer_service           = "domain_customer_service"
    domain_ledger                     = "domain_ledger"
    domain_scoring                    = "domain_scoring"
    domain_technical                  = "domain_technical"
    domain_project_artifacts          = "domain_project_artifacts"
    cots_hr                           = "cots_hr"
    cots_mft_sterling_vw              = "cots_mft_sterling_vw"
    domain_loyalty                    = "domain_loyalty"
    domain_talent_acquisiton          = "domain_talent_acquisiton"
    domain_treasury                   = "domain_treasury"
    domain_dispute                    = "domain_dispute"
    cots_aml_sas                      = "cots_aml_sas"
    cots_alm_tbsm                     = "cots_alm_tbsm"
    snapshot_archive                  = "snapshot_archive"
    cots_aml_verafin                  = "cots_aml_verafin"
    domain_marketing                  = "domain_marketing"
    domain_iam                        = "domain_iam"
    domain_consent                    = "domain_consent"
    domain_dns                        = "domain_dns"
    cots_customer_service_blockworx   = "cots_customer_service_blockworx"
    domain_securitization             = "domain_securitization"
    domain_payments_ops               = "domain_payments_ops"
    domain_tax_slips                  = "domain_tax_slips"
    domain_cdic                       = "domain_cdic"
    cots_cdic                         = "cots_cdic"
    cots_scms_tkm                     = "cots_scms_tkm"
    cots_scms_andis_se                = "cots_scms_andis_se"
    domain_card_management            = "domain_card_management"
    domain_data_management            = "domain_data_management"
    cots_digital_marketing            = "cots_digital_marketing"
    domain_ops_adhoc                  = "domain_ops_adhoc"
    domain_data_logs                  = "domain_data_logs"
    domain_fraud_risk_intelligence    = "domain_fraud_risk_intelligence"
    domain_approval                   = "domain_approval"
    domain_customer_contact_centre    = "domain_customer_contact_centre"
    domain_cri_compliance             = "domain_cri_compliance"
  }
  bq_monitoring_ids = {
    cots_customer_service_ctt         =  "cots_customer_service_ctt"
    domain_customer_acquisition       = "domain_customer_acquisition"
    domain_communication              = "domain_communication"
    domain_loyalty                    = "domain_loyalty"
  }
  bq_serving_dataset_ids      = {
    pco_scores_serving_dataset  = "PCOScoresServingDataset"
  }
  bq_authorized_datasets = {
    PCOScoresDataset = "PCOScoresServingDataset"
  }
  bq_switch_dataset_ids = {
    cots_digital_marketing = "cots_digital_marketing"
  }
  bq_tables = {
    merchant = {
      bq_dataset_id       = "domain_loyalty"
      bq_table_id         = "MERCHANT"
      schema_file         = "/bigquery_table_schemas/merchant.json"
      deletion_protection = false
      clustering          = ["MRCH_ID"]
      time_partitioning   = {
        type                     = "MONTH",
        expiration_ms            = null,
        field                    = null,
        require_partition_filter = false
      }
    },
    interac_recon_exceptions = {
      bq_dataset_id       = "domain_payments"
      bq_table_id         = "INTERAC_RECON_EXCEPTIONS"
      schema_file         = "/bigquery_table_schemas/interac_recon_exceptions.json"
      deletion_protection = false
      clustering          = ["INTERAC_NETWORK_FILE_ID"]
      time_partitioning   = {
        type                     = "MONTH",
        expiration_ms            = null,
        field                    = "BUSINESS_DATE",
        require_partition_filter = true
      }
    },
    pcma_cdic_0500 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0500"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0500.json"
      deletion_protection = false
      clustering          = ["DEPOSITOR_UNIQUE_ID", "ACCOUNT_UNIQUE_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0100 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0100"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0100.json"
      deletion_protection = false
      clustering          = ["DEPOSITOR_UNIQUE_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0110 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0110"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0110.json"
      deletion_protection = false
      clustering          = ["DEPOSITOR_UNIQUE_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0120 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0120"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0120.json"
      deletion_protection = false
      clustering          = ["DEPOSITOR_UNIQUE_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0152 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0152"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0152.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0153 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0153"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0153.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0400 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0400"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0400.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0600 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0600"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0600.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0800 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0800"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0800.json"
      deletion_protection = false
      clustering          = ["ACCOUNT_UNIQUE_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0900 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0900"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0900.json"
      deletion_protection = false
      clustering          = ["ACCOUNT_UNIQUE_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0121 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0121"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0121.json"
      deletion_protection = false
      clustering          = ["DEPOSITOR_UNIQUE_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0130 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0130"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0130.json"
      deletion_protection = false
      clustering          = ["ACCOUNT_UNIQUE_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0140 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0140"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0140.json"
      deletion_protection = false
      clustering          = ["ACCOUNT_UNIQUE_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    loyality_manual_transactions = {
      bq_dataset_id       = "domain_loyalty"
      bq_table_id         = "LOYALTY_MANUAL_TRANSACTIONS"
      schema_file         = "/bigquery_table_schemas/loyalty/manual_campaigns_transactions/manual_campaigns_transactions.json"
      deletion_protection = false
      clustering          = null
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "TRANSACTION_DATE",
        require_partition_filter = false
      }
    },
    pcma_cdic_0700 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0700"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0700.json"
      deletion_protection = false
      clustering          = ["ACCOUNT_UNIQUE_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0212 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0212"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0212.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0231 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0231"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0231.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0232 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0232"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0232.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0233 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0233"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0233.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0234 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0234"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0234.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0235 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0235"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0235.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0201 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0201"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0201.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0202 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0202"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0202.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0211 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0211"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0211.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0221 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0221"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0221.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0501 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0501"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0501.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0999 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0999"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0999.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
 pcma_cdic_0236 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0236"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0236.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0237 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0237"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0237.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0238= {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0238"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0238.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0239 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0239"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0239.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0240 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0240"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0240.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0241 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0241"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0241.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0242 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0242"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0242.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0401 = {
      bq_dataset_id       = "cots_cdic"
      bq_table_id         = "PCMA_CDIC_0401"
      schema_file         = "/bigquery_table_schemas/risk_management/cdic/pcma/pcma_cdic_0401.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    }
  }
  bq_table_ids                = {
    pco_scores                         = "PCO_SCORES"
    pco_scores_view                    = "PCO_SCORES_VIEW"
    application_affiliate_daily_report = "APPLICATION_AFFILIATE_DAILY_REPORT_DATA"
  }

  cots_mft_sterling_bq_views = [
    {
      table_name = "COMPLETED_TRANSACTIONS_VIEW"
      query_file = "Completed_Transactions_Query.txt"
    },
    {
      table_name = "FAILED_TRANSACTIONS_VIEW"
      query_file = "Failed_Transactions_Query.txt"
    },
    {
      table_name = "ALL_TRANSACTIONS_VIEW"
      query_file = "All_Transactions_Query.txt"
    }
  ]

  trs_bq_views = [
    {
      table_name = "trs_nonprod_table",
      query_file = "trs_daily_input_table.sql"
    }
  ]

  # private_cloudbuild_uri      = "projects/pcb-terraform-seed/locations/northamerica-northeast1/workerPools/pcb-cloud-build-worker-pool"
  delete_contents_on_destroy  = true
  data_foundation_composer_sa = dependency.processing_zone.outputs.data_foundation_composer_sa
  money_movement_composer_sa  = dependency.processing_zone.outputs.money_movement_composer_sa
  dataproc_sa                 = dependency.processing_zone.outputs.dataproc_sa
  batch_sa                    = dependency.processing_zone.outputs.batch_sa
  switch_sa                   = "pcb-switch-sa@pcb-dev-processing.iam.gserviceaccount.com"
  siem_pubsub_topic = "pubsub.googleapis.com/projects/pcb-nonprod-dataflow/topics/pcb-nonprod-siem-logs"
  looker_bq_aml_dashboard_sa = "looker-bq-aml-dashboard-sa"
  cots_mft_sterling_views_bool = 0
}

locals {
  common_config = read_terragrunt_config(find_in_parent_folders("terragrunt.hcl"))
  terraform_gcp_terminus_revision = local.common_config.inputs.terraform_gcp_terminus_revision
}

terraform {
  source = "git::https://gitlab.lblw.ca/pcf-engineering/shared/pcf-terraform-modules/pcf-gcp-terraform-modules/terraform-gcp-terminus.git//curatedzone?ref=${local.terraform_gcp_terminus_revision}"
}
