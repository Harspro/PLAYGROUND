include {
  path = find_in_parent_folders()
}

dependency "processing_zone" {
  config_path = "../02_processing"
}

dependency "curated_zone" {
  config_path = "../03_curated"
}

inputs = {
  project_id                             = "pcb-prod-landing"
  project_zone                           = "landing"
  cloudstorage_name                      = "landing-zone"
  retention_days                         = 7
  project_id_curated                     = "pcb-prod-curated"
  landing_sa                             = "infra-admin-sa@pcb-prod-landing.iam.gserviceaccount.com"
  monitoring-bq-sa                       = "monitoring-bq-sa@pcb-prod-curated.iam.gserviceaccount.com"
  monitoring_sa                          = "monitoring-prometheus@pcb-prod-processing.iam.gserviceaccount.com"
  dataproc_sa                            = dependency.processing_zone.outputs.dataproc_sa
  processing_project                     = dependency.processing_zone.outputs.project_id
  curated_project                        = dependency.curated_zone.outputs.project_id
  processing_domain_audit_sa             = dependency.processing_zone.outputs.domain_audit_sa
  siem_pubsub_topic                      = "pubsub.googleapis.com/projects/pcb-prod-dataflow/topics/pcb-prod-siem-logs"
  data_foundation_composer_sa            = dependency.processing_zone.outputs.data_foundation_composer_sa
  data_foundation_composer_webserver_uri = dependency.processing_zone.outputs.data_foundation_composer_web_uri
  money_movement_composer_sa             = dependency.processing_zone.outputs.money_movement_composer_sa
  money_movement_composer_webserver_uri  = "https://money-movement-dot-northamerica-northeast1.composer.googleusercontent.com/"
  enable_pdf_afp_landing_bucket_access   = true
  all_score_sa                           = "pcb-all-score-sa@pcb-prod-processing.iam.gserviceaccount.com"
  switch_sa                              = "pcb-switch-sa@pcb-prod-processing.iam.gserviceaccount.com"

  data_management_poc_flag = 0

  vpc_connectors = {
    name               = "pcb-pr-landing-connector"
    egress_setting     = "PRIVATE_RANGES_ONLY"
    egress_setting_all = "ALL_TRAFFIC"
  }
  bq_dataset_ids     = {
    ffm_app_reporting_dataset         =  "FfmAppReportingDataset"
    cots_fraud_ffm                    =  "cots_fraud_ffm"
    domain_audit                      =  "domain_audit"
    cots_customer_service_blockworx   =  "cots_customer_service_blockworx"
    cots_customer_service_ctt         =  "cots_customer_service_ctt"
    cots_security_ciam                =  "cots_security_ciam"
    domain_account_management         =  "domain_account_management"
    domain_aml                        =  "domain_aml"
    domain_communication              =  "domain_communication"
    domain_customer_acquisition       =  "domain_customer_acquisition"
    domain_customer_management        =  "domain_customer_management"
    domain_customer_service           =  "domain_customer_service"
    domain_fraud                      =  "domain_fraud"
    domain_ledger                     =  "domain_ledger"
    domain_loyalty                    =  "domain_loyalty"
    domain_marketing                  =  "domain_marketing"
    domain_payments                   =  "domain_payments"
    domain_project_artifacts          =  "domain_project_artifacts"
    domain_retail                     =  "domain_retail"
    domain_scoring                    =  "domain_scoring"
    domain_securitization             =  "domain_securitization"
    cots_hr                           =  "cots_hr"
    domain_technical                  =  "domain_technical"
    domain_treasury                   =  "domain_treasury"
    domain_validation_verification    =  "domain_validation_verification"
    cardinal_staging_dataset          =  "cardinal_staging_dataset"
    cots_mft_sterling                 =  "cots_mft_sterling"
    domain_talent_acquisiton          =  "domain_talent_acquisiton"
    domain_dispute                    =  "domain_dispute"
    cots_aml_sas                      =  "cots_aml_sas"
    cots_alm_tbsm                     =  "cots_alm_tbsm"
    snapshot_archive                  =  "snapshot_archive"
    cots_aml_verafin                  =  "cots_aml_verafin"
    domain_movemoney_technical        =  "domain_movemoney_technical"
    domain_consent                    = "domain_consent"
    domain_iam                        =  "domain_iam"
    domain_dns                        = "domain_dns"
    domain_payments_ops               =  "domain_payments_ops"
    domain_tax_slips                  =  "domain_tax_slips"
    domain_cdic                       =  "domain_cdic"
    cots_cdic                         =  "cots_cdic"
    cots_scms_tkm                     =  "cots_scms_tkm"
    cots_scms_andis_se                =  "cots_scms_andis_se"
    domain_card_management            =  "domain_card_management"
    cots_digital_marketing            =  "cots_digital_marketing"
    domain_ops_adhoc                  =  "domain_ops_adhoc"
    domain_data_logs                  =  "domain_data_logs"
    domain_fraud_risk_intelligence    =  "domain_fraud_risk_intelligence"
    domain_approval                   =  "domain_approval"
    cots_product_insights             =  "cots_product_insights"
    domain_cri_compliance             = "domain_cri_compliance"
    domain_customer_contact_centre    = "domain_customer_contact_centre"
  }
  bq_monitoring_ids = {
    cots_mft_sterling                 =  "cots_mft_sterling"
    domain_customer_acquisition       = "domain_customer_acquisition"
  }
  pubsub_topic_sub  = [
    {
      name         = "pcb-data-ctt-custom-contact-detail-record"
      dlq_name     = "pcb-data-ctt-custom-contact-detail-record-dlt"
      subscription = "pcb-data-ctt-custom-contact-detail-record-sub"
      schema_name  = "pcb-data-ctt-custom-contact-detail-record-schema"
      schema_file  = "CustomContactDetailRecord.json"
      bq_table_name = "CUSTOM_CONTACT_DETAIL_RECORDS"
    },
    {
      name         = "pcb-data-ctt-touch-point"
      dlq_name     = "pcb-data-ctt-touch-point-dlt"
      subscription = "pcb-data-ctt-touch-point-sub"
      schema_name  = "pcb-data-ctt-touch-point-schema"
      schema_file  = "TouchPoint.json"
      bq_table_name = "TOUCH_POINT"
    },
    {
      name          = "pcb-data-ctt-stat-adr"
      dlq_name      = "pcb-data-ctt-stat-adr-dlt"
      subscription  = "pcb-data-ctt-stat-adr-sub"
      schema_name   = "pcb-data-ctc-stat-adr-schema"
      schema_file   = "StatAdr.json"
      bq_table_name = "STAT_ADR"
    },
    {
      name         = "pcb-data-ctt-stat-agent-not-ready-breakdown-d"
      dlq_name     = "pcb-data-ctt-stat-agent-not-ready-breakdown-d-dlt"
      subscription = "pcb-data-ctt-stat-agent-not-ready-breakdown-d-sub"
      schema_name  = "pcb-data-ctt-stat-agent-not-ready-breakdown-d-schema"
      schema_file  = "StatAgentNotReadyBreakdownD.json"
      bq_table_name = "STAT_AGENT_NOT_READY_BREAKDOWN_D"
    },
    {
      name         = "pcb-data-ctt-stat-cdr"
      dlq_name     = "pcb-data-ctt-stat-cdr-dlt"
      subscription = "pcb-data-ctt-stat-cdr-sub"
      schema_name  = "pcb-data-ctc-stat-cdr-schema"
      schema_file  = "StatCdr.json"
      bq_table_name = "STAT_CDR"
    },
    {
      name         = "pcb-data-ctt-stat-agent-activity-d"
      dlq_name     = "pcb-data-ctt-stat-agent-activity-d-dlt"
      subscription = "pcb-data-ctt-stat-agent-activity-d-sub"
      schema_name  = "pcb-data-ctt-stat-agent-activity-d-schema"
      schema_file  = "StatAgentActivityD.json"
      bq_table_name = "STAT_AGENT_ACTIVITY_D"
    },
    {
      name         = "pcb-data-ctt-stat-queue-activity-d"
      dlq_name     = "pcb-data-ctt-stat-queue-activity-d-dlt"
      subscription = "pcb-data-ctt-stat-queue-activity-d-sub"
      schema_name  = "pcb-data-ctt-stat-queue-activity-d-schema"
      schema_file  = "StatQueueActivityD.json"
      bq_table_name = "STAT_QUEUEACTIVITY_D"
    },
    {
      name         = "pcb-data-ctt-stat-agent-activity-by-queue-d"
      dlq_name     = "pcb-data-ctt-stat-agent-activity-by-queue-d-dlt"
      subscription = "pcb-data-ctt-stat-agent-activity-by-queue-d-sub"
      schema_name  = "pcb-data-ctt-stat-agent-activity-by-queue-d-schema"
      schema_file  = "StatAgentActivityByQueueD.json"
      bq_table_name = "STAT_AGENTACTIVITYBYQUEUE_D"
    }
  ]

  acct_mgmt_pubsub_to_bq = {
    pcb-customer-alert-preference-update = {
      topic_name      = "pcb-customer-alert-preference-update"
      schema_file     = "customer-alert-preference-update-history.json"
      bq_dataset_id   = "domain_account_management"
      bq_table_id     = "CUSTOMER_ALERT_PREFERENCE_UPDATE_HISTORY"
    }
  }

  bq_authorized_datasets = {
    domain_account_management         = "domain_account_management"
    domain_retail                     = "domain_retail"
    domain_aml                        = "domain_aml"
    domain_payments                   = "domain_payments"
    domain_customer_management        = "domain_customer_management"
    domain_fraud                      = "domain_fraud"
    domain_validation_verification    =  "domain_validation_verification"
    domain_communication              =  "domain_communication"
    domain_customer_service           =  "domain_customer_service"
    cots_hr                           =  "cots_hr"
    domain_technical                  =  "domain_technical"
    domain_customer_acquisition       =  "domain_customer_acquisition"
    domain_project_artifacts          =  "domain_project_artifacts"
    cots_customer_service_ctt         =  "cots_customer_service_ctt"
    cots_mft_sterling                 =  "cots_mft_sterling_vw"
    domain_talent_acquisiton          =  "domain_talent_acquisiton"
    domain_dispute                    =  "domain_dispute"
    domain_loyalty                    =  "domain_loyalty"
    domain_audit                      =  "domain_audit_vw"
    domain_scoring                    =  "domain_scoring"
    domain_ledger                     =  "domain_ledger"
    domain_marketing                  =  "domain_marketing"
    domain_consent                    = "domain_consent"
    domain_dns                        =  "domain_dns"
    cots_customer_service_blockworx   =  "cots_customer_service_blockworx"
    domain_securitization             =  "domain_securitization"
    domain_iam                        =  "domain_iam"
    cots_alm_tbsm                     =  "cots_alm_tbsm"
    cots_aml_sas                      =  "cots_aml_sas"
    domain_tax_slips                  =  "domain_tax_slips"
    domain_cdic                       =  "domain_cdic"
    cots_cdic                         =  "cots_cdic"
    cots_scms_tkm                     =  "cots_scms_tkm"
    cots_scms_andis_se                =  "cots_scms_andis_se"
    domain_card_management            =  "domain_card_management"
    cots_digital_marketing            =  "cots_digital_marketing"
    domain_ops_adhoc                  =  "domain_ops_adhoc"
    domain_data_logs                  =  "domain_data_logs"
    domain_fraud_risk_intelligence    =  "domain_fraud_risk_intelligence"
    domain_customer_contact_centre    =  "domain_customer_contact_centre"
  }
  bq_all_score_dataset_ids = {
    domain_scoring = "domain_scoring"
  }
  bq_switch_dataset_ids = {
    cots_product_insights = "cots_product_insights"
  }

  pubsub_to_bq = [
    {
      topic_name      = "pcb-data-application-affiliate-report"
      schema_file     = "pcb_data_application_affiliate_report.json"
      service_account = "app-affiliate-report-sa"
      bq_dataset_id   = "domain_customer_acquisition"
      bq_table_id     = "APPLICATION_AFFILIATE_REPORT_DATA"
    }
  ]

  comms_pubsub_to_bq = {
    pcb-communication-request = {
      topic_name      = "pcb-communication-request"
      schema_file     = "communication-request.json"
      bq_dataset_id   = "domain_communication"
      bq_table_id     = "COMMUNICATION_REQUEST"
    },
    pcb-communication-event = {
      topic_name      = "pcb-communication-event"
      schema_file     = "communication-event.json"
      bq_dataset_id   = "domain_communication"
      bq_table_id     = "COMMUNICATION_EVENT"
    },
    pcb-tsys-comms-event = {
      topic_name      = "pcb-tsys-comms-event"
      schema_file     = "tsys-comms-event.json"
      bq_dataset_id   = "domain_communication"
      bq_table_id     = "TSYS_COMMS_EVENT"
    }
  }

  // BQ table resource creation.
  bq_tables = {
    application_affiliate_daily_report = {
      bq_dataset_id       = "domain_customer_acquisition"
      bq_table_id         = "APPLICATION_AFFILIATE_REPORT_DATA"
      schema_file         = "/bq_table_schemas/application_affiliate_report_data.json"
      deletion_protection = true
      clustering          = null
      time_partitioning   = null
    },
    pc_loyalty_subscriptions = {
      bq_dataset_id       = "domain_loyalty"
      bq_table_id         = "PC_LOYALTY_SUBSCRIPTIONS"
      schema_file         = "/bq_table_schemas/pc_loyalty_subscriptions.json"
      deletion_protection = false
      clustering          = ["customerId"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "eventDate",
        require_partition_filter = false
      }
    },
    pc_optimum_wallet_event = {
      bq_dataset_id       = "domain_loyalty"
      bq_table_id         = "PC_OPTIMUM_WALLET_EVENT"
      schema_file         = "/bq_table_schemas/pc_optimum_wallet_event.json"
      deletion_protection = false
      clustering          = ["customerId"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pc_optimum_identities_event = {
      bq_dataset_id       = "domain_loyalty"
      bq_table_id         = "PC_OPTIMUM_IDENTITIES_EVENT"
      schema_file         = "/bq_table_schemas/loyalty/account_ref_service/pc_optimum_identities_event.json"
      deletion_protection = false
      clustering          = ["customerId"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pc_points_awarded = {
      bq_dataset_id       = "domain_loyalty"
      bq_table_id         = "PC_POINTS_AWARDED"
      schema_file         = "/bq_table_schemas/pc_points_awarded.json"
      deletion_protection = false
      clustering          = ["pcfCustomerIdCluster"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "transactionDatePartition",
        require_partition_filter = false
      }
    },
    pc_points_reprocess = {
      bq_dataset_id       = "domain_loyalty"
      bq_table_id         = "PC_POINTS_REPROCESS"
      schema_file         = "/bq_table_schemas/pc_points_reprocess.json"
      deletion_protection = false
      clustering          = ["pcfCustomerIdCluster"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "transactionDatePartition",
        require_partition_filter = false
      }
    },
    loyalty_points_earned = {
      bq_dataset_id       = "domain_loyalty"
      bq_table_id         = "LOYALTY_POINTS_EARNED"
      schema_file         = "/bq_table_schemas/loyalty/points_comprehension/loyalty_points_earned.json"
      deletion_protection = false
      clustering          = ["PCF_CUSTOMER_ID","EARNING_CATEGORY"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "CREATE_DT",
        require_partition_filter = false
      }
    },
    loyalty_points_earned_lifetime = {
      bq_dataset_id       = "domain_loyalty"
      bq_table_id         = "LOYALTY_POINTS_EARNED_LIFETIME"
      schema_file         = "/bq_table_schemas/loyalty/points_comprehension/loyalty_points_earned_lifetime.json"
      deletion_protection = false
      clustering          = ["PCF_CUSTOMER_ID","EARNING_CATEGORY"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "CREATE_DT",
        require_partition_filter = false
      }
    },
    loyalty_subscription_usage = {
      bq_dataset_id       = "domain_loyalty"
      bq_table_id         = "LOYALTY_SUBSCRIPTION_USAGE"
      schema_file         = "/bq_table_schemas/loyalty/points_comprehension/loyalty_subscription_usage.json"
      deletion_protection = false
      clustering          = ["PCF_CUSTOMER_ID","SUBSCRIPTION_TYPE"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "CREATE_DT",
        require_partition_filter = false
      }
    },
    loyalty_subscription_usage_lifetime = {
      bq_dataset_id       = "domain_loyalty"
      bq_table_id         = "LOYALTY_SUBSCRIPTION_USAGE_LIFETIME"
      schema_file         = "/bq_table_schemas/loyalty/points_comprehension/loyalty_subscription_usage_lifetime.json"
      deletion_protection = false
      clustering          = ["PCF_CUSTOMER_ID","SUBSCRIPTION_TYPE"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "CREATE_DT",
        require_partition_filter = false
      }
    },
    esso_fuel_transactions = {
      bq_dataset_id       = "domain_loyalty"
      bq_table_id         = "ESSO_FUEL_TRANSACTIONS"
      schema_file         = "/bq_table_schemas/loyalty/esso/esso_fuel_transactions.json"
      deletion_protection = false
      clustering          = ["MERCHANT_NUMBER"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    tcs_alerts = {
      bq_dataset_id       = "domain_dispute"
      bq_table_id         = "TCS_ALERTS"
      schema_file         = "/bq_table_schemas/tcs_alerts.json"
      deletion_protection = true
      clustering          = ["PZINSKEY"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "FILE_HDR_DT",
        require_partition_filter = false
      }
    },
    tcs_disposition_rpt = {
      bq_dataset_id       = "domain_dispute"
      bq_table_id         = "TCS_DISPOSITION_RPT"
      schema_file         = "/bq_table_schemas/tcs_disposition_rpt.json"
      deletion_protection = true
      clustering          = ["INTERACTIONID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "FILE_HDR_DT",
        require_partition_filter = false
      }
    },
    tcs_dispute_claim = {
      bq_dataset_id       = "domain_dispute"
      bq_table_id         = "TCS_DISPUTE_CLAIM"
      schema_file         = "/bq_table_schemas/tcs_dispute_claim.json"
      deletion_protection = true
      clustering          = ["PZINSKEY"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "FILE_HDR_DT",
        require_partition_filter = false
      }
    },
    tcs_dispute_dtl = {
      bq_dataset_id       = "domain_dispute"
      bq_table_id         = "TCS_DISPUTE_DTL"
      schema_file         = "/bq_table_schemas/tcs_dispute_dtl.json"
      deletion_protection = true
      clustering          = ["PZINSKEY"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "FILE_HDR_DT",
        require_partition_filter = false
      }
    },
    tcs_dispute_fraud_common = {
      bq_dataset_id       = "domain_dispute"
      bq_table_id         = "TCS_DISPUTE_FRAUD_COMMON"
      schema_file         = "/bq_table_schemas/tcs_dispute_fraud_common.json"
      deletion_protection = true
      clustering          = ["PZINSKEY"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "FILE_HDR_DT",
        require_partition_filter = false
      }
    },
    tcs_disputes_ltr = {
      bq_dataset_id       = "domain_dispute"
      bq_table_id         = "TCS_DISPUTES_LTR"
      schema_file         = "/bq_table_schemas/tcs_disputes_ltr.json"
      deletion_protection = true
      clustering          = ["PZINSKEY"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "FILE_HDR_DT",
        require_partition_filter = false
      }
    },
    tcs_fraud_dtl = {
      bq_dataset_id       = "domain_dispute"
      bq_table_id         = "TCS_FRAUD_DTL"
      schema_file         = "/bq_table_schemas/tcs_fraud_dtl.json"
      deletion_protection = true
      clustering          = ["PZINSKEY"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "FILE_HDR_DT",
        require_partition_filter = false
      }
    },
    tcs_fraud_monitoring = {
      bq_dataset_id       = "domain_dispute"
      bq_table_id         = "TCS_FRAUD_MONITORING"
      schema_file         = "/bq_table_schemas/tcs_fraud_monitoring.json"
      deletion_protection = true
      clustering          = ["PZINSKEY"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "FILE_HDR_DT",
        require_partition_filter = false
      }
    },
    tcs_fraud_reporting = {
      bq_dataset_id       = "domain_dispute"
      bq_table_id         = "TCS_FRAUD_REPORTING"
      schema_file         = "/bq_table_schemas/tcs_fraud_reporting.json"
      deletion_protection = true
      clustering          = ["PZINSKEY"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "FILE_HDR_DT",
        require_partition_filter = false
      }
    },
    tcs_lost_stolen = {
      bq_dataset_id       = "domain_dispute"
      bq_table_id         = "TCS_LOST_STOLEN"
      schema_file         = "/bq_table_schemas/tcs_lost_stolen.json"
      deletion_protection = true
      clustering          = ["PZINSKEY"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "FILE_HDR_DT",
        require_partition_filter = false
      }
    },
    tcs_work_basket = {
      bq_dataset_id       = "domain_dispute"
      bq_table_id         = "TCS_WORK_BASKET"
      schema_file         = "/bq_table_schemas/tcs_work_basket.json"
      deletion_protection = true
      clustering          = ["PXASSIGNEDOPERATORID", "PXREFOBJECTINSNAME"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "FILE_HDR_DT",
        require_partition_filter = false
      }
    },
    pcb_interac_funds_move = {
       bq_dataset_id       = "domain_payments"
       bq_table_id         = "PCB_FUNDS_MOVE_CREATED"
       schema_file         = "/bq_table_schemas/pcb_funds_move_created.json"
       deletion_protection = false
       clustering          = ["PAYMENT_RAIL"]
       time_partitioning   = {
         type                     = "DAY",
         expiration_ms            = null,
         field                    = "INGESTION_TIMESTAMP",
         require_partition_filter = false
       }
    },
    pcb_interac_funds_move_failed = {
      bq_dataset_id       = "domain_payments"
      bq_table_id         = "PCB_FUNDS_MOVE_FAILED"
      schema_file         = "/bq_table_schemas/pcb_funds_move_failed.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    eft_external_account_change = {
      bq_dataset_id       = "domain_payments"
      bq_table_id         = "EFT_EXTERNAL_ACCOUNT_CHANGED"
      schema_file         = "/bq_table_schemas/eft_account_changed_event.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    eft_link_request_verification = {
      bq_dataset_id       = "domain_payments"
      bq_table_id         = "EFT_LINK_REQUEST_VERIFICATION"
      schema_file         = "/bq_table_schemas/accounts/eft_link_request.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    eft_linkreq_account_transactions = {
        bq_dataset_id       = "domain_payments"
        bq_table_id         = "EFT_LINK_REQUEST_ACCOUNT_TRANSACTIONS"
        schema_file         = "/bq_table_schemas/eft_link_request_transactions.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
          type                     = "DAY",
          expiration_ms            = null,
          field                    = "INGESTION_TIMESTAMP",
          require_partition_filter = false
      }
    },
    tsys_comms_event = {
      bq_dataset_id       = "domain_communication"
      bq_table_id         = "TSYS_COMMS_EVENT"
      schema_file         = "/bq_table_schemas/tsys-comms/tsys_comms_event.json"
      deletion_protection = true
      clustering          = ["alertType"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "publish_time",
        require_partition_filter = false
      }
    },
    communication_request = {
      bq_dataset_id       = "domain_communication"
      bq_table_id         = "COMMUNICATION_REQUEST"
      schema_file         = "/bq_table_schemas/communications/communication_request.json"
      deletion_protection = true
      clustering          = ["notificationCode", "customerId"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "publish_time",
        require_partition_filter = true
      }
    },
    communication_event = {
      bq_dataset_id       = "domain_communication"
      bq_table_id         = "COMMUNICATION_EVENT"
      schema_file         = "/bq_table_schemas/communications/communication_event.json"
      deletion_protection = true
      clustering          = ["notificationCode", "customerId", "statusCode", "statusChannel"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "publish_time",
        require_partition_filter = true
      }
    },
    email_status = {
      bq_dataset_id       = "domain_communication"
      bq_table_id         = "EMAIL_STATUS"
      schema_file         = "/bq_table_schemas/communications/email_status.json"
      deletion_protection = true
      clustering          = ["statusCode"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "timestamp",
        require_partition_filter = false
      }
    },
    email_unsubscribed_event = {
      bq_dataset_id       = "domain_communication"
      bq_table_id         = "EMAIL_UNSUBSCRIBED_EVENT"
      schema_file         = "/bq_table_schemas/communications/email_unsubscribed_event.json"
      deletion_protection = false
      clustering          = []
      time_partitioning   = {
        type                     = "MONTH",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    dns_updated = {
      bq_dataset_id       = "domain_communication"
      bq_table_id         = "DNS_UPDATED"
      schema_file         = "/bq_table_schemas/communications/dns_updated.json"
      deletion_protection = true
      clustering          = []
      time_partitioning   = {
        type                     = "MONTH",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    sms_inbound_message = {
      bq_dataset_id       = "domain_communication"
      bq_table_id         = "SMS_INBOUND_MESSAGE"
      schema_file         = "/bq_table_schemas/communications/sms_inbound_message.json"
      deletion_protection = false
      clustering          = ["fromNumber"]
      time_partitioning   = {
        type                     = "MONTH",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    ada_chatbot_conversation = {
      bq_dataset_id       = "domain_communication"
      bq_table_id         = "ADA_CHATBOT_CONVERSATION"
      schema_file         = "/bq_table_schemas/communications/ada_chatbot_conversation.json"
      deletion_protection = true
      clustering          = ["pcfcustid"],
      time_partitioning   = {
        type                     = "MONTH",
        expiration_ms            = null,
        field                    = "date_created",
        require_partition_filter = false
      }
    },
    ada_chatbot_message = {
      bq_dataset_id       = "domain_communication"
      bq_table_id         = "ADA_CHATBOT_MESSAGE"
      schema_file         = "/bq_table_schemas/communications/ada_chatbot_message.json"
      deletion_protection = true
      clustering          = ["conversation_id"]
      time_partitioning   = {
        type                     = "MONTH",
        expiration_ms            = null,
        field                    = "date_created",
        require_partition_filter = false
      }
    },
    mail_print = {
      bq_dataset_id       = "domain_communication"
      bq_table_id         = "MAIL_PRINT"
      schema_file         = "/bq_table_schemas/communications/mail_print.json"
      deletion_protection = true
      clustering          = ["printingMode", "mailType"]
      time_partitioning   = {
        type                     = "MONTH",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    mail_print_status = {
      bq_dataset_id       = "domain_communication"
      bq_table_id         = "MAIL_PRINT_STATUS"
      schema_file         = "/bq_table_schemas/communications/mail_print_status.json"
      deletion_protection = true
      clustering          = ["outboundFileName", "status"]
      time_partitioning = {
        type                     = "MONTH",
        expiration_ms            = null,
        field                    = "REC_CREATE_TIMESTAMP",
        require_partition_filter = false
      }
    },
    generic_batch_communication_request = {
      bq_dataset_id       = "domain_communication"
      bq_table_id         = "GENERIC_BATCH_COMMUNICATION_REQUEST"
      schema_file         = "/bq_table_schemas/digital_adoption/generic_notification/generic_batch_communication_request.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    tsys_triad_authbase_reporting = {
      bq_dataset_id       = "domain_account_management"
      bq_table_id         = "TSYS_AUTHORIZATION_BASE_TRIAD_RPT"
      schema_file         = "/bq_table_schemas/tsys_triad_report/tsys_authorization_base.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "FILE_CREATE_DT",
        require_partition_filter = false
      }
    },
    tsys_triad_auth_reporting = {
      bq_dataset_id       = "domain_account_management"
      bq_table_id         = "TSYS_AUTHORIZATION_OUTCOME_TRIAD_RPT"
      schema_file         = "/bq_table_schemas/tsys_triad_report/tsys_authorization_outcome.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "FILE_CREATE_DT",
        require_partition_filter = false
      }
    },
    tsys_triad_cda_reporting = {
      bq_dataset_id       = "domain_account_management"
      bq_table_id         = "TSYS_CDA_OUTCOMES_TRIAD_RPT"
      schema_file         = "/bq_table_schemas/tsys_triad_report/tsys_cda_outcomes.json"
      deletion_protection = false
      clustering          = ["RD_HD_ACCOUNT_ID", "RD_FO_STGY_ID", "RD_FO_SCEN_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "FILE_CREATE_DT",
        require_partition_filter = false
      }
    },
    tsys_triad_collection_reporting = {
      bq_dataset_id       = "domain_account_management"
      bq_table_id         = "TSYS_COLLECTION_OUTCOMES_TRIAD_RPT"
      schema_file         = "/bq_table_schemas/tsys_triad_report/tsys_collection_outcomes.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "FILE_CREATE_DT",
        require_partition_filter = false
      }
    },
    tsys_triad_crdtfacilit_reporting = {
      bq_dataset_id       = "domain_account_management"
      bq_table_id         = "TSYS_CRDT_FACILIT_OUTCOMES_TRIAD_RPT"
      schema_file         = "/bq_table_schemas/tsys_triad_report/tsys_crdt_facilit_outcomes.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "FILE_CREATE_DT",
        require_partition_filter = false
      }
    },
    tsys_triad_scoring_reporting = {
      bq_dataset_id       = "domain_account_management"
      bq_table_id         = "TSYS_SCORING_OUTCOMES_TRIAD_RPT"
      schema_file         = "/bq_table_schemas/tsys_triad_report/tsys_scoring_outcomes.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "FILE_CREATE_DT",
        require_partition_filter = false
      }
    },
    tsys_triad_strategy_reporting = {
      bq_dataset_id       = "domain_account_management"
      bq_table_id         = "TSYS_STRATEGY_OUTCOMES_TRIAD_RPT"
      schema_file         = "/bq_table_schemas/tsys_triad_report/tsys_strategy_outcomes.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "FILE_CREATE_DT",
        require_partition_filter = false
      }
    },
    tsys_triad_decision_keys = {
      bq_dataset_id       = "domain_account_management"
      bq_table_id         = "TSYS_TRIAD_DECISION_KEYS_RPT"
      schema_file         = "/bq_table_schemas/tsys_triad_report/tsys_triad_decision_keys.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "FILE_CREATE_DT",
        require_partition_filter = false
      }
    },
    provenir_response = {
      bq_dataset_id       = "domain_customer_acquisition"
      bq_table_id         = "PROVENIR_RESPONSE"
      schema_file         = "/bq_table_schemas/applications/provenir_response.json"
      deletion_protection = false
      clustering          = ["clientTransactionID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    application_publish_response = {
      bq_dataset_id       = "domain_customer_acquisition"
      bq_table_id         = "APPLICATION_PUBLISH_RESPONSE"
      schema_file         = "/bq_table_schemas/applications/application_publish_response.json"
      deletion_protection = false
      clustering          = ["tsysApplicationReferenceNumber"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    isa_recording_number = {
      bq_dataset_id       = "domain_customer_acquisition"
      bq_table_id         = "ISA_RECORDING_NUMBER"
      schema_file         = "/bq_table_schemas/applications/isa_recording_number.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    product_onboarding_statistics = {
      bq_dataset_id       = "domain_customer_acquisition"
      bq_table_id         = "PRODUCT_ONBOARDING_STATISTICS"
      schema_file         = "/bq_table_schemas/applications/product_onboarding_statistics.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    account_relationship = {
      bq_dataset_id       = "domain_account_management"
      bq_table_id         = "ACCOUNT_RELATIONSHIP"
      schema_file         = "/bq_table_schemas/accounts/account_relationship.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    application_submission_payload = {
      bq_dataset_id       = "domain_customer_acquisition"
      bq_table_id         = "APPLICATION_SUBMISSION_PAYLOAD"
      schema_file         = "/bq_table_schemas/applications/application_submission_payload.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
          type                     = "DAY",
          expiration_ms            = null,
          field                    = "INGESTION_TIMESTAMP",
          require_partition_filter = false
          }
     },
    declined_switch_survey = {
      bq_dataset_id       = "domain_customer_acquisition"
      bq_table_id         = "SURVEY_DECLINED_SWITCH"
      schema_file         = "/bq_table_schemas/applications/declined_switch_survey.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    tcs_ethoca_dispute_reject_dtl = {
      bq_dataset_id       = "domain_movemoney_technical"
      bq_table_id         = "TCS_ETHOCA_DISPUTE_REJECT_DTL"
      schema_file         = "/bq_table_schemas/tcs_ethoca_reject_dtl.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "AUTHORISATION_DATE_TIME",
        require_partition_filter = false
      }
    },
    iam_authentication = {
      bq_dataset_id       = "domain_iam"
      bq_table_id         = "IAM_AUTHENTICATION"
      schema_file         = "/bq_table_schemas/iam_authentication.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "eventDateTime",
        require_partition_filter = false
      }
    },
    iam_customer_verification = {
      bq_dataset_id       = "domain_iam"
      bq_table_id         = "IAM_CUSTOMER_VERIFICATION"
      schema_file         = "/bq_table_schemas/iam_customer_verification.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "eventDateTime",
        require_partition_filter = false
      }
    },
    cnst_events_analytics = {
      bq_dataset_id       = "domain_consent"
      bq_table_id         = "CNST_EVENTS_ANALYTICS"
      schema_file         = "/bq_table_schemas/cnst_events_analytics.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    voice_intent_history = {
      bq_dataset_id       = "domain_customer_service"
      bq_table_id         = "VOICE_INTENT_HISTORY"
      schema_file         = "/bq_table_schemas/customer_service/voice_intent_history.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    transact_account_balance = {
      bq_dataset_id       = "domain_account_management"
      bq_table_id         = "TRANSACT_ACCOUNT_BALANCE"
      schema_file         = "/bq_table_schemas/accounts/transact_account_balance.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    transact_account_interest = {
      bq_dataset_id       = "domain_account_management"
      bq_table_id         = "TRANSACT_ACCOUNT_INTEREST"
      schema_file         = "/bq_table_schemas/accounts/transact_account_interest.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    transact_account = {
      bq_dataset_id       = "domain_account_management"
      bq_table_id         = "TRANSACT_ACCOUNT"
      schema_file         = "/bq_table_schemas/accounts/transact_account_event.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    transact_product = {
      bq_dataset_id       = "domain_account_management"
      bq_table_id         = "PRODUCT_TRANSACTION"
      schema_file         = "/bq_table_schemas/accounts/product_transaction_event.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    transact_account_interest_details = {
      bq_dataset_id       = "domain_account_management"
      bq_table_id         = "TRANSACT_ACCOUNT_INTEREST_DETAILS"
      schema_file         = "/bq_table_schemas/accounts/transact_account_interest_details.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    temporary_wallet = {
           bq_dataset_id       = "domain_account_management"
           bq_table_id         = "TEMPORARY_WALLET"
           schema_file         = "/bq_table_schemas/accounts/temporary_wallet.json"
           deletion_protection = false
           clustering          = null
           time_partitioning   = {
             type                     = "DAY",
             expiration_ms            = null,
             field                    = "INGESTION_TIMESTAMP",
             require_partition_filter = false
      }
    },
    t5_tax_slip = {
      bq_dataset_id       = "domain_tax_slips"
      bq_table_id         = "T5_TAX_SLIP"
      schema_file         = "/bq_table_schemas/digital_adoption/temenos_tax/t5_temenos_tax_slip.json"
      deletion_protection = true
      clustering          = null
      time_partitioning   = null
    },
    t5_tax_trailer = {
      bq_dataset_id       = "domain_tax_slips"
      bq_table_id         = "T5_TAX_TRAILER"
      schema_file         = "/bq_table_schemas/digital_adoption/temenos_tax/t5_temenos_tax_trailer.json"
      deletion_protection = true
      clustering          = null
      time_partitioning   = null
    },
    t5_tax_header = {
      bq_dataset_id       = "domain_tax_slips"
      bq_table_id         = "T5_TAX_HEADER"
      schema_file         = "/bq_table_schemas/digital_adoption/temenos_tax/t5_temenos_tax_header.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = null
    },
    t5_tax_validation_fail_records = {
      bq_dataset_id       = "domain_tax_slips"
      bq_table_id         = "T5_TAX_VALIDATION_FAIL_RECORDS"
      schema_file         = "/bq_table_schemas/digital_adoption/temenos_tax/t5_tax_null_data.json"
      deletion_protection = true
      clustering          = null
      time_partitioning   = null
    },
    rl3_reference = {
      bq_dataset_id       = "domain_tax_slips"
      bq_table_id         = "RL3_REFERENCE"
      schema_file         = "/bq_table_schemas/digital_adoption/temenos_tax/rl3_reference.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = null
    },
    rl3_tax_header = {
      bq_dataset_id       = "domain_tax_slips"
      bq_table_id         = "RL3_TAX_HEADER"
      schema_file         = "/bq_table_schemas/digital_adoption/temenos_tax/rl3_tax_header.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = null
    },
    rl3_tax_trailer = {
      bq_dataset_id       = "domain_tax_slips"
      bq_table_id         = "RL3_TAX_TRAILER"
      schema_file         = "/bq_table_schemas/digital_adoption/temenos_tax/rl3_tax_trailer.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = null
    },
    rl3_tax_slip = {
      bq_dataset_id       = "domain_tax_slips"
      bq_table_id         = "RL3_TAX_SLIP"
      schema_file         = "/bq_table_schemas/digital_adoption/temenos_tax/rl3_tax_slip.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = null
    },
    customer_statement = {
      bq_dataset_id       = "domain_customer_management"
      bq_table_id         = "CUSTOMER_STATEMENT"
      schema_file         = "/bq_table_schemas/digital_adoption/temenos_tax/customer_statement.json"
      deletion_protection = true
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "startDate",
        require_partition_filter = false
      }
    },
    customer_merge_split = {
      bq_dataset_id       = "domain_customer_management"
      bq_table_id         = "CUSTOMER_MERGE_SPLIT"
      schema_file         = "/bq_table_schemas/digital_adoption/temenos_tax/merge_split.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = null
    },
    card_embossing_request_status = {
      bq_dataset_id       = "domain_card_management"
      bq_table_id         = "CARD_EMBOSSING_REQUEST_STATUS"
      schema_file         = "/bq_table_schemas/card_embossing_request_status.json"
      deletion_protection = false
      clustering          = ["requestId"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    credit_limit_offer_status = {
      bq_dataset_id       = "domain_account_management"
      bq_table_id         = "CREDIT_LIMIT_OFFER_STATUS"
      schema_file         = "/bq_table_schemas/digital_adoption/credit_limit/credit_limit_offer_status.json"
      deletion_protection = false
      clustering          = ["offerId","accountId"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "offerCreatedDate",
        require_partition_filter = false
      }
    },
    embossing_reprocessing_event = {
      bq_dataset_id       = "domain_card_management"
      bq_table_id         = "EMBOSSING_REPROCESSING_EVENT"
      schema_file         = "/bq_table_schemas/embossing_reprocessing_event.json"
      deletion_protection = false
      clustering          = ["reprocessingEventId"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    instant_issuance_requested_card_details = {
      bq_dataset_id       = "domain_card_management"
      bq_table_id         = "INSTANT_ISSUANCE_REQUESTED_CARD_DETAILS"
      schema_file         = "/bq_table_schemas/instant_issuance/instant_issuance_audit.json"
      deletion_protection = false
      clustering          = []
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    instant_issuance_customer_info_for_requested_card = {
      bq_dataset_id       = "domain_card_management"
      bq_table_id         = "INSTANT_ISSUANCE_CUSTOMER_INFO_FOR_REQUESTED_CARD"
      schema_file         = "/bq_table_schemas/instant_issuance/instant_issuance_audit.json"
      deletion_protection = false
      clustering          = []
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    instant_issuance_orchestration = {
      bq_dataset_id       = "domain_card_management"
      bq_table_id         = "INSTANT_ISSUANCE_ORCHESTRATION"
      schema_file         = "/bq_table_schemas/instant_issuance/instant_issuance_orchestration.json"
      deletion_protection = false
      clustering          = ["orchestrationId"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "createDate",
        require_partition_filter = false
      }
    },
    embossing_terminus_mapping = {
      bq_dataset_id       = "domain_card_management"
      bq_table_id         = "EMBOSSING_TERMINUS_MAPPING"
      schema_file         = "/bq_table_schemas/card_embossing_terminus_mapping.json"
      deletion_protection = false
      clustering          = ["cardEmbossingRequestId"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    tsys_streaming_health_events = {
      bq_dataset_id       = "domain_account_management"
      bq_table_id         = "TSYS_STREAMING_HEALTH_EVENTS"
      schema_file         = "/bq_table_schemas/accounts/tsys_streaming_health_events.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    tsys_events = {
      bq_dataset_id       = "domain_account_management"
      bq_table_id         = "TSYS_EVENTS"
      schema_file         = "/bq_table_schemas/accounts/tsys_events.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = 31536000000,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    reprocessing_event = {
      bq_dataset_id       = "domain_technical"
      bq_table_id         = "REPROCESSING_EVENT"
      schema_file         = "/bq_table_schemas/reprocessing_event.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0201 = {
      bq_dataset_id       = "domain_cdic"
      bq_table_id         = "PCMA_CDIC_0201"
      schema_file         = "/bq_table_schemas/risk_management/cdic/pcma/pcma_cdic_0201.json"
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
      bq_dataset_id       = "domain_cdic"
      bq_table_id         = "PCMA_CDIC_0202"
      schema_file         = "/bq_table_schemas/risk_management/cdic/pcma/pcma_cdic_0202.json"
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
      bq_dataset_id       = "domain_cdic"
      bq_table_id         = "PCMA_CDIC_0211"
      schema_file         = "/bq_table_schemas/risk_management/cdic/pcma/pcma_cdic_0211.json"
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
      bq_dataset_id       = "domain_cdic"
      bq_table_id         = "PCMA_CDIC_0221"
      schema_file         = "/bq_table_schemas/risk_management/cdic/pcma/pcma_cdic_0221.json"
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
      bq_dataset_id       = "domain_cdic"
      bq_table_id         = "PCMA_CDIC_0501"
      schema_file         = "/bq_table_schemas/risk_management/cdic/pcma/pcma_cdic_0501.json"
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
      bq_dataset_id       = "domain_cdic"
      bq_table_id         = "PCMA_CDIC_0999"
      schema_file         = "/bq_table_schemas/risk_management/cdic/pcma/pcma_cdic_0999.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pcma_cdic_0212 = {
      bq_dataset_id       = "domain_cdic"
      bq_table_id         = "PCMA_CDIC_0212"
      schema_file         = "/bq_table_schemas/risk_management/cdic/pcma/pcma_cdic_0212.json"
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
      bq_dataset_id       = "domain_cdic"
      bq_table_id         = "PCMA_CDIC_0231"
      schema_file         = "/bq_table_schemas/risk_management/cdic/pcma/pcma_cdic_0231.json"
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
      bq_dataset_id       = "domain_cdic"
      bq_table_id         = "PCMA_CDIC_0232"
      schema_file         = "/bq_table_schemas/risk_management/cdic/pcma/pcma_cdic_0232.json"
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
      bq_dataset_id       = "domain_cdic"
      bq_table_id         = "PCMA_CDIC_0233"
      schema_file         = "/bq_table_schemas/risk_management/cdic/pcma/pcma_cdic_0233.json"
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
      bq_dataset_id       = "domain_cdic"
      bq_table_id         = "PCMA_CDIC_0234"
      schema_file         = "/bq_table_schemas/risk_management/cdic/pcma/pcma_cdic_0234.json"
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
      bq_dataset_id       = "domain_cdic"
      bq_table_id         = "PCMA_CDIC_0235"
      schema_file         = "/bq_table_schemas/risk_management/cdic/pcma/pcma_cdic_0235.json"
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
      bq_dataset_id       = "domain_cdic"
      bq_table_id         = "PCMA_CDIC_0236"
      schema_file         = "/bq_table_schemas/risk_management/cdic/pcma/pcma_cdic_0236.json"
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
      bq_dataset_id       = "domain_cdic"
      bq_table_id         = "PCMA_CDIC_0237"
      schema_file         = "/bq_table_schemas/risk_management/cdic/pcma/pcma_cdic_0237.json"
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
      bq_dataset_id       = "domain_cdic"
      bq_table_id         = "PCMA_CDIC_0238"
      schema_file         = "/bq_table_schemas/risk_management/cdic/pcma/pcma_cdic_0238.json"
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
      bq_dataset_id       = "domain_cdic"
      bq_table_id         = "PCMA_CDIC_0239"
      schema_file         = "/bq_table_schemas/risk_management/cdic/pcma/pcma_cdic_0239.json"
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
      bq_dataset_id       = "domain_cdic"
      bq_table_id         = "PCMA_CDIC_0240"
      schema_file         = "/bq_table_schemas/risk_management/cdic/pcma/pcma_cdic_0240.json"
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
      bq_dataset_id       = "domain_cdic"
      bq_table_id         = "PCMA_CDIC_0241"
      schema_file         = "/bq_table_schemas/risk_management/cdic/pcma/pcma_cdic_0241.json"
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
      bq_dataset_id       = "domain_cdic"
      bq_table_id         = "PCMA_CDIC_0242"
      schema_file         = "/bq_table_schemas/risk_management/cdic/pcma/pcma_cdic_0242.json"
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
      bq_dataset_id       = "domain_cdic"
      bq_table_id         = "PCMA_CDIC_0401"
      schema_file         = "/bq_table_schemas/risk_management/cdic/pcma/pcma_cdic_0401.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    re_idv_required_customer = {
        bq_dataset_id       = "domain_customer_acquisition"
        bq_table_id         = "RE_IDV_REQUIRED_CUSTOMER"
        schema_file         = "/bq_table_schemas/applications/re_idv_required_customer.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "INGESTION_TIMESTAMP",
            require_partition_filter = false
        }
    },
    pc_targeted_offer = {
      bq_dataset_id       = "domain_loyalty"
      bq_table_id         = "PC_TARGETED_OFFER"
      schema_file         = "/bq_table_schemas/loyalty/TOS/targeted_offer.json"
      deletion_protection = false
      clustering          = null
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "createTimestamp",
        require_partition_filter = false
      }
    },
    pc_targeted_offer_fulfillment = {
      bq_dataset_id       = "domain_loyalty"
      bq_table_id         = "PC_TARGETED_OFFER_FULFILLMENT"
      schema_file         = "/bq_table_schemas/loyalty/TOS/offer_fulfillment.json"
      deletion_protection = false
      clustering          = null
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "createTimestamp",
        require_partition_filter = false
      }
    },
    product_upgrade_offer = {
        bq_dataset_id       = "domain_account_management"
        bq_table_id         = "PRODUCT_UPGRADE_OFFER"
        schema_file         = "/bq_table_schemas/digital_adoption/product_upgrade_offer/product_upgrade_offer.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = 31556926000,
            field                    = "OFFER_CREATED_DT",
            require_partition_filter = false
        }
    },
    product_upgrade_offer_status = {
        bq_dataset_id       = "domain_account_management"
        bq_table_id         = "PRODUCT_UPGRADE_OFFER_STATUS"
        schema_file         = "/bq_table_schemas/digital_adoption/product_upgrade_offer/product_upgrade_offer_status.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "INGESTION_TIMESTAMP",
            require_partition_filter = false
        }
    },
    ref_customer_offer_detail = {
        bq_dataset_id       = "domain_account_management"
        bq_table_id         = "REF_CUSTOMER_OFFER_DETAIL"
        schema_file         = "/bq_table_schemas/digital_adoption/product_upgrade_offer/ref_customer_offer_detail.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = null
    },
    ref_loyalty_offer_detail = {
      bq_dataset_id       = "domain_account_management"
      bq_table_id         = "REF_LOYALTY_OFFER_DETAIL"
      schema_file         = "/bq_table_schemas/digital_adoption/eve_loyalty_offer/ref_loyalty_offer_detail.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = null
    },
    loyalty_offer_eligible_account = {
      bq_dataset_id       = "domain_account_management"
      bq_table_id         = "LOYALTY_OFFER_ELIGIBLE_ACCOUNT"
      schema_file         = "/bq_table_schemas/digital_adoption/eve_loyalty_offer/loyalty_offer_eligible_account.json"
      deletion_protection = false
      clustering          = ["PCMA_ACCOUNT_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = 2629800000,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    loyalty_offer_account = {
      bq_dataset_id       = "domain_account_management"
      bq_table_id         = "LOYALTY_OFFER_ACCOUNT"
      schema_file         = "/bq_table_schemas/digital_adoption/eve_loyalty_offer/loyalty_offer_account.json"
      deletion_protection = false
      clustering          = ["pcmaAccountId"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    tsys_tcs_purge_case_details = {
      bq_dataset_id       = "domain_dispute"
      bq_table_id         = "TSYS_TCS_PURGE_CASE_DETAILS"
      schema_file         = "/bq_table_schemas/risk_management/tcs_purge/tsys_tcs_purge_case_details.json"
      deletion_protection = false
      clustering          = ["PARENT_CASE_ID","CASE_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    tsys_tcs_purge_case_image_metadata = {
      bq_dataset_id       = "domain_dispute"
      bq_table_id         = "TSYS_TCS_PURGE_CASE_IMAGE_METADATA"
      schema_file         = "/bq_table_schemas/risk_management/tcs_purge/tsys_tcs_purge_case_image_metadata.json"
      deletion_protection = false
      clustering          = ["PARENT_CASE_ID","CASE_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pc_account_ref_service_par_mapping = {
      bq_dataset_id       = "domain_loyalty"
      bq_table_id         = "PC_ACCOUNT_REF_SERVICE_PAR_MAPPING"
      schema_file         = "/bq_table_schemas/loyalty/account_ref_service/account_ref_service_par_mapping.json"
      deletion_protection = false
      clustering          = null
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "createDate",
        require_partition_filter = false
        }
     },
    tcs_purge_case_details_status = {
        bq_dataset_id       = "domain_dispute"
        bq_table_id         = "TCS_PURGE_CASE_DETAILS_STATUS"
        schema_file         = "/bq_table_schemas/risk_management/tcs_purge/tcs_purge_case_details_status.json"
        deletion_protection = true
        clustering          = ["parentCaseId","caseId"]
        time_partitioning   = {
          type                     = "DAY",
          expiration_ms            = null,
          field                    = "INGESTION_TIMESTAMP",
          require_partition_filter = false
        }
      },
    tcs_purge_case_image_metadata_status = {
        bq_dataset_id       = "domain_dispute"
        bq_table_id         = "TCS_PURGE_CASE_IMAGE_METADATA_STATUS"
        schema_file         = "/bq_table_schemas/risk_management/tcs_purge/tcs_purge_case_image_metadata_status.json"
        deletion_protection = true
        clustering          = ["parentCaseId","caseId"]
        time_partitioning   = {
          type                     = "DAY",
          expiration_ms            = null,
          field                    = "INGESTION_TIMESTAMP",
          require_partition_filter = false
      }
    },
    pc_declined_app_offer_match = {
    bq_dataset_id       = "domain_loyalty"
    bq_table_id         = "PC_DECLINED_APPLICATION_OFFER_ASSIGNMENT"
    schema_file         = "/bq_table_schemas/loyalty/targeted_app_offers/declined_app_offer_assignment.json"

    deletion_protection = false
    clustering          = null
    time_partitioning   = null
    },
    scoring_prep_prizm_unique = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_PRIZM_UNIQUE"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_prizm_unique.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_mth_acquisition_eq = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_MTH_ACQUISITION_EQ"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_mth_acquisition_eq.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_pcmc_data_depot = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_PCMC_DATA_DEPOT"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_pcmc_data_depot.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_am00_pre = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_AM00_PRE"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_am00_pre.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    batch_approvals_status = {
      bq_dataset_id       = "domain_approval"
      bq_table_id         = "BATCH_APPROVALS_STATUS"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/batch_approvals_status.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    batch_run_log = {
       bq_dataset_id       = "domain_payments_ops"
       bq_table_id         = "BATCH_RUN_LOG"
       schema_file         = "/bq_table_schemas/batch_run_log.json"
       deletion_protection = false
       clustering          = null
       time_partitioning   = null
    },
    fraud_risk_assessment_payload = {
        bq_dataset_id       = "domain_fraud_risk_intelligence"
        bq_table_id         = "FRAUD_RISK_ASSESSMENT_PAYLOAD"
        schema_file         = "/bq_table_schemas/applications/fraud_risk_assessment_payload.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "INGESTION_TIMESTAMP",
            require_partition_filter = false
        }
    },
    fraud_risk_assessment_response = {
      bq_dataset_id       = "domain_fraud_risk_intelligence"
      bq_table_id         = "FRAUD_RISK_ASSESSMENT_RESPONSE"
      schema_file         = "/bq_table_schemas/applications/fraud_risk_assessment_response.json"
      deletion_protection = false
      clustering          = null
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    pc_failed_transaction = {
      bq_dataset_id       = "domain_loyalty"
      bq_table_id         = "PC_FAILED_TRANSACTION"
      schema_file         = "/bq_table_schemas/loyalty/failed_transaction/failed_transaction_table_schema.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "postedDatePartition",
        require_partition_filter = false
      }
    },
    click_to_pay_customer_status = {
      bq_dataset_id       = "domain_customer_management"
      bq_table_id         = "CLICK_TO_PAY_CUSTOMER_STATUS"
      schema_file         = "/bq_table_schemas/digital_adoption/click_to_pay/click_to_pay_customer_status.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    click_to_pay_event_status = {
      bq_dataset_id       = "domain_customer_management"
      bq_table_id         = "CLICK_TO_PAY_EVENT_STATUS"
      schema_file         = "/bq_table_schemas/digital_adoption/click_to_pay/click_to_pay_event_status.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    fraud_risk_truth_data_inbound = {
        bq_dataset_id       = "domain_fraud_risk_intelligence"
        bq_table_id         = "FRAUD_RISK_TRUTH_DATA_INBOUND"
        schema_file         = "/bq_table_schemas/applications/fraud_risk_truth_data_inbound.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "INGESTION_TIMESTAMP",
            require_partition_filter = false
        }
    },
    fraud_risk_update_response = {
            bq_dataset_id       = "domain_fraud_risk_intelligence"
            bq_table_id         = "FRAUD_RISK_UPDATE_RESPONSE"
            schema_file         = "/bq_table_schemas/applications/fraud_risk_update_response.json"
            deletion_protection = false
            clustering          = null
            time_partitioning   = {
                type                     = "DAY",
                expiration_ms            = null,
                field                    = "INGESTION_TIMESTAMP",
                require_partition_filter = false
         }
    },
    scoring_prep_wk_tsys_cycle = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_WK_TSYS_CYCLE"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_tsys_cycle.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_wk_tsys_cycle_trans = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_WK_TSYS_CYCLE_TRANS"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_tsys_cycle_trans.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_wk_tsys_chgoff_full = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_WK_TSYS_CHGOFF_FULL"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_tsys_chgoff_full.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_wk_tsys_chgoff_fr = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_WK_TSYS_CHGOFF_FR"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_tsys_chgoff_fr.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_wk_tsys_chgoff_suffix = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_WK_TSYS_CHGOFF_SUFFIX"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_tsys_chgoff_suffix.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_wk_ace_final = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_WK_ACE_FINAL"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_ace_final.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
          type                     = "DAY",
          expiration_ms            = null,
          field                    = "REC_LOAD_TIMESTAMP",
          require_partition_filter = false
        }
    },
    scoring_prep_wk_score_lnk = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_WK_SCORE_LNK"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_score_lnk.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
          type                     = "DAY",
          expiration_ms            = null,
          field                    = "REC_LOAD_TIMESTAMP",
          require_partition_filter = false
        }
    },
    scoring_prep_wk_cli_offer_data = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_WK_CLI_OFFER_DATA"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_cli_offer_data.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
          type                     = "DAY",
          expiration_ms            = null,
          field                    = "REC_LOAD_TIMESTAMP",
          require_partition_filter = false
        }
    },
    scoring_prep_wk_cli_events = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_WK_CLI_EVENTS"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_cli_events.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
          type                     = "DAY",
          expiration_ms            = null,
          field                    = "REC_LOAD_TIMESTAMP",
          require_partition_filter = false
        }
    },
    scoring_prep_wk_raw_score = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_WK_RAW_SCORE"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_raw_score.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
          type                     = "DAY",
          expiration_ms            = null,
          field                    = "REC_LOAD_TIMESTAMP",
          require_partition_filter = false
        }
    },
    scoring_prep_wk_acquisition = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_WK_ACQUISITION"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_acquisition.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
          type                     = "DAY",
          expiration_ms            = null,
          field                    = "REC_LOAD_TIMESTAMP",
          require_partition_filter = false
        }
    },
    scoring_prep_wk_mmm = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_WK_MMM"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_mmm.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_wk_tsys_demgfx = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_WK_TSYS_DEMGFX"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_tsys_demgfx.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_wk_test_digit = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_WK_TEST_DIGIT"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_test_digit.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_mth_current_scoring = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_MTH_CURRENT_SCORING"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_mth_current_scoring.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_wk_mth6 = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_WK_MTH6"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_mth6.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_wk_hist_6mth = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_WK_HIST_6MTH"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_hist_6mth.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_wk_hist_3mth = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_WK_HIST_3MTH"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_hist_3mth.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_wk_tsys_trans = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_WK_TSYS_TRANS"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_tsys_trans.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_wk_tsys_am00 = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_WK_TSYS_AM00"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_tsys_am00.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_wk_tsys_am01 = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_WK_TSYS_AM01"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_tsys_am01.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_wk_tsys_am02 = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_WK_TSYS_AM02"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_tsys_am02.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_wk_tsys_bni_score = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_WK_TSYS_BNI_SCORE"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_tsys_bni_score.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_wk_tsys_cb_score = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_WK_TSYS_CB_SCORE"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_tsys_cb_score.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_wk_tsys_mthend_full = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_WK_TSYS_MTHEND_FULL"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_tsys_mthend_full.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_wk_tsys_mthend_suffix = {
        bq_dataset_id       = "domain_scoring"
        bq_table_id         = "SCORING_PREP_WK_TSYS_MTHEND_SUFFIX"
        schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_wk_tsys_mthend_suffix.json"
        deletion_protection = false
        clustering          = null
        time_partitioning   = {
            type                     = "DAY",
            expiration_ms            = null,
            field                    = "REC_LOAD_TIMESTAMP",
            require_partition_filter = false
        }
    },
    scoring_prep_customer_list_daily = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_CUSTOMER_LIST_DAILY"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_customer_list_daily.json"
      deletion_protection = false
      clustering          = null
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_score_pop_daily = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_SCORE_POP_DAILY"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_score_pop_daily.json"
      deletion_protection = false
      clustering          = null
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_lcl_store_list = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_LCL_STORE_LIST"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_lcl_store_list.json"
      deletion_protection = false
      clustering          = null
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_lcl_store_spend_daily = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_LCL_STORE_SPEND_DAILY"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_lcl_store_spend_daily.json"
      deletion_protection = false
      clustering          = null
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_all_trans_vars_daily = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_ALL_TRANS_VARS_DAILY"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_all_trans_vars_daily.json"
      deletion_protection = false
      clustering          = null
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_trans_grocery_cash_daily = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_TRANS_GROCERY_CASH_DAILY"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_trans_grocery_cash_daily.json"
      deletion_protection = false
      clustering          = null
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_trans_hist_base = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_TRANS_HIST_BASE"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_trans_hist_base.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY"
        expiration_ms            = null
        field                    = "REC_LOAD_TIMESTAMP"
        require_partition_filter = false
      }
    },
    scoring_prep_cust_trans_full_daily = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_CUST_TRANS_FULL_DAILY"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_cust_trans_full_daily.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY"
        expiration_ms            = null
        field                    = "REC_LOAD_TIMESTAMP"
        require_partition_filter = false
      }
    },
    scoring_prep_trans_summary_daily = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_TRANS_SUMMARY_DAILY"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_trans_summary_daily.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY"
        expiration_ms            = null
        field                    = "REC_LOAD_TIMESTAMP"
        require_partition_filter = false
      }
    },
    scoring_prep_grocery_for_lcl_nonlcl_daily = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_GROCERY_FOR_LCL_NONLCL_DAILY"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_grocery_for_lcl_nonlcl_daily.json"
      deletion_protection = false
      clustering          = null
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_lcl_nonlcl_grocery_daily = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_LCL_NONLCL_GROCERY_DAILY"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_lcl_nonlcl_grocery_daily.json"
      deletion_protection = false
      clustering          = null
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_non_grocery_mcc_daily = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_NON_GROCERY_MCC_DAILY"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_non_grocery_mcc_daily.json"
      deletion_protection = false
      clustering          = null
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_combined_features_daily = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_COMBINED_FEATURES_DAILY"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_combined_features_daily.json"
      deletion_protection = false
      clustering          = null
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_segmented_input_daily = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_SEGMENTED_INPUT_DAILY"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_segmented_input_daily.json"
      deletion_protection = false
      clustering          = null
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_non_lcl_store_spend_daily = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_NON_LCL_STORE_SPEND_DAILY"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_non_lcl_store_spend_daily.json"
      deletion_protection = false
      clustering          = null
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_cash_advances_daily = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_CASH_ADVANCES_DAILY"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_cash_advances_daily.json"
      deletion_protection = false
      clustering          = null
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_grocery_trans_daily = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_GROCERY_TRANS_DAILY"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/scoring_prep_grocery_trans_daily.json"
      deletion_protection = false
      clustering          = null
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_credscore_calculated_metrics = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_CREDSCORE_CALCULATED_METRICS"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/scoring_prep_credscore_calculated_metrics.json"
      deletion_protection = false
      clustering          = ["MAST_ACCOUNT_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_demostats = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_DEMOSTATS"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/scoring_prep_demostats.json"
      deletion_protection = false
      clustering          = ["CODE"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_postal_code_ri_v4_risk_ranking = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_POSTAL_CODE_RI_V4_RISK_RANKING"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/scoring_prep_postal_code_ri_v4_risk_ranking.json"
      deletion_protection = false
      clustering          = ["CIFP_ZIP_CODE"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_transaction_summary_datamart = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_TRANSACTION_SUMMARY_DATAMART"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/scoring_prep_transaction_summary_datamart.json"
      deletion_protection = false
      clustering          = ["MAST_ACCOUNT_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_area_risk_lookup = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_AREA_RISK_LOOKUP"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/scoring_prep_area_risk_lookup.json"
      deletion_protection = false
      clustering = ["AREA_CD"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_cif_snapshot = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_CIF_SNAPSHOT"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/scoring_prep_cif_snapshot.json"
      deletion_protection = false
      clustering = ["MAST_ACCOUNT_ID"]
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_existing_pcmc_pcma = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_EXISTING_PCMC_PCMA"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/scoring_prep_existing_pcmc_pcma.json"
      deletion_protection = false
      clustering = ["MAST_ACCOUNT_ID", "WALLET_ID"]
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_decision_keys = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_DECISION_KEYS"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/scoring_prep_decision_keys.json"
      deletion_protection = false
      clustering = ["MAST_ACCOUNT_ID"]
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_pco_score = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_PCO_SCORE"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/scoring_prep_pco_score.json"
      deletion_protection = false
      clustering = ["WALLET_ID"]
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_cred_das_history_base_01 = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_CRED_DAS_HISTORY_BASE_01"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/scoring_prep_cred_das_history_base_01.json"
      deletion_protection = false
      clustering          = ["MAST_ACCOUNT_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_cred_das_history_base_02 = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_CRED_DAS_HISTORY_BASE_02"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/scoring_prep_cred_das_history_base_02.json"
      deletion_protection = false
      clustering          = ["MAST_ACCOUNT_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_cred_das_history_base_03 = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_CRED_DAS_HISTORY_BASE_03"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/scoring_prep_cred_das_history_base_03.json"
      deletion_protection = false
      clustering          = ["MAST_ACCOUNT_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_cred_das_history_base_04 = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_CRED_DAS_HISTORY_BASE_04"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/scoring_prep_cred_das_history_base_04.json"
      deletion_protection = false
      clustering          = ["MAST_ACCOUNT_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_cred_das_history_base_05 = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_CRED_DAS_HISTORY_BASE_05"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/scoring_prep_cred_das_history_base_05.json"
      deletion_protection = false
      clustering          = ["MAST_ACCOUNT_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_cred_das_history_base_06 = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_CRED_DAS_HISTORY_BASE_06"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/scoring_prep_cred_das_history_base_06.json"
      deletion_protection = false
      clustering          = ["MAST_ACCOUNT_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_cred_das_history_base_07 = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_CRED_DAS_HISTORY_BASE_07"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/scoring_prep_cred_das_history_base_07.json"
      deletion_protection = false
      clustering          = ["MAST_ACCOUNT_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_cred_das_history_base_08 = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_CRED_DAS_HISTORY_BASE_08"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/scoring_prep_cred_das_history_base_08.json"
      deletion_protection = false
      clustering          = ["MAST_ACCOUNT_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_cred_das_history_base_09 = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_CRED_DAS_HISTORY_BASE_09"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/scoring_prep_cred_das_history_base_09.json"
      deletion_protection = false
      clustering          = ["MAST_ACCOUNT_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_cred_das_history_base_10 = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_CRED_DAS_HISTORY_BASE_10"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/scoring_prep_cred_das_history_base_10.json"
      deletion_protection = false
      clustering          = ["MAST_ACCOUNT_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_account_ids_randomized = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_ACCOUNT_IDS_RANDOMIZED"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/scoring_prep_account_ids_randomized.json"
      deletion_protection = false
      clustering = ["MAST_ACCOUNT_ID"]
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_credscore_engineered_features = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_CREDSCORE_ENGINEERED_FEATURES"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/scoring_prep_credscore_engineered_features.json"
      deletion_protection = false
      clustering          = ["MAST_ACCOUNT_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    scoring_prep_pre_app_score = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "SCORING_PREP_PRE_APP_SCORE"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/scoring_prep_pre_app_score.json"
      deletion_protection = false
      clustering          = ["wallet_id"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    output_trs_t0090_unnested = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "OUTPUT_TRS_T0090_UNNESTED"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/output_trs_t0090_unnested.json"
      deletion_protection = false
      clustering          = ["MAST_ACCOUNT_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    output_index4_t0040_unnested_comparison = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "OUTPUT_INDEX4_T0040_UNNESTED_COMPARISON"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/output_index4_t0040_unnested_comparison.json"
      deletion_protection = false
      clustering          = ["MAST_ACCOUNT_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    input_index4_t0040_features = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "INPUT_INDEX4_T0040_FEATURES"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/input_index4_t0040_features.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    ouput_index4_t0040 = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "OUTPUT_INDEX4_T0040"
      schema_file         = "/bq_table_schemas/risk_management/all_score/index4_t0040/output_index4_t0040.json"
      deletion_protection = false
      clustering          = ["EXECUTION_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    input_trs_t0090_features = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "INPUT_TRS_T0090_FEATURES"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/input_trs_t0090_features.json"
      deletion_protection = false
      clustering          = null
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    output_trs_t0090 = {
      bq_dataset_id       = "domain_scoring"
      bq_table_id         = "OUTPUT_TRS_T0090"
      schema_file         = "/bq_table_schemas/risk_management/all_score/trs_t0090/output_trs_t0090.json"
      deletion_protection = false
      clustering          = ["EXECUTION_ID"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "REC_LOAD_TIMESTAMP",
        require_partition_filter = false
      }
    },
    cri_wrk_transactions = {
      bq_dataset_id       = "domain_cri_compliance"
      bq_table_id         = "CRI_WRK_TRANSACTIONS"
      schema_file         = "/bq_table_schemas/risk_management/cri/cri_wrk_transactions.json"
      deletion_protection = false
      clustering          = ["AccountId", "ModelTradeDate"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "RecLoadTimestamp",
        require_partition_filter = false
      }
    },
    cri_wrk_model_transactions = {
      bq_dataset_id       = "domain_cri_compliance"
      bq_table_id         = "CRI_WRK_MODEL_TRANSACTIONS"
      schema_file         = "/bq_table_schemas/risk_management/cri/cri_wrk_model_transactions.json"
      deletion_protection = false
      clustering          = ["AccountId", "ModelTradeDate"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "RecLoadTimestamp",
        require_partition_filter = false
      }
    },
    cri_stg_account_master = {
      bq_dataset_id       = "domain_cri_compliance"
      bq_table_id         = "CRI_STG_ACCOUNT_MASTER"
      schema_file         = "/bq_table_schemas/risk_management/cri/cri_stg_account_master.json"
      deletion_protection = false
      clustering          = ["AccountId"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "RecLoadTimestamp",
        require_partition_filter = false
      }
    },
    account_cri_refund = {
      bq_dataset_id       = "domain_cri_compliance"
      bq_table_id         = "ACCOUNT_CRI_REFUND"
      schema_file         = "/bq_table_schemas/risk_management/cri/account_cri_refund.json"
      deletion_protection = false
      clustering          = ["InternalAccountId","LedgerAccountId","CriEvaluationDate"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "RecLoadTimestamp",
        require_partition_filter = false
      }
    },
    account_cri_evaluation = {
      bq_dataset_id       = "domain_cri_compliance"
      bq_table_id         = "ACCOUNT_CRI_EVALUATION"
      schema_file         = "/bq_table_schemas/risk_management/cri/account_cri_evaluation.json"
      deletion_protection = false
      clustering          = ["InternalAccountId","LedgerAccountId","InternalCustomerId","ExternalCustomerId"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "RecLoadTimestamp",
        require_partition_filter = false
      }
    },
    cri_stg_transactions = {
      bq_dataset_id       = "domain_cri_compliance"
      bq_table_id         = "CRI_STG_TRANSACTIONS"
      schema_file         = "/bq_table_schemas/risk_management/cri/cri_stg_transactions.json"
      deletion_protection = false
      clustering          = ["AccountId","PostDate"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "RecLoadTimestamp",
        require_partition_filter = false
      }
    },
    cri_wrk_lookup_tcat_tcode_fees_interest = {
      bq_dataset_id       = "domain_cri_compliance"
      bq_table_id         = "CRI_WRK_LOOKUP_TCAT_TCODE_FEES_INTEREST"
      schema_file         = "/bq_table_schemas/risk_management/cri/cri_wrk_lookup_tcat_tcode_fees_interest.json"
      deletion_protection = false
      clustering          = ["TCat","TCode"]
      time_partitioning   = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "RecLoadTimestamp",
        require_partition_filter = false
      }
    },
    cri_wrk_account_master = {
      bq_dataset_id       = "domain_cri_compliance"
      bq_table_id         = "CRI_WRK_ACCOUNT_MASTER"
      schema_file         = "/bq_table_schemas/risk_management/cri/wrk_account_master.json"
      deletion_protection = false
      clustering          = ["AccountId"]
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "RecLoadTimestamp",
        require_partition_filter = false
      }
    },
    cri_wrk_lookup_tcat_tcode_master = {
      bq_dataset_id       = "domain_cri_compliance"
      bq_table_id         = "CRI_WRK_LOOKUP_TCAT_TCODE_MASTER"
      schema_file         = "/bq_table_schemas/risk_management/cri/wrk_lookup_tcat_tcode_master.json"
      deletion_protection = false
      clustering          = ["TCat", "TCode"]
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "RecLoadTimestamp",
        require_partition_filter = false
      }
    },
    reprocessing_credit_limit_offer = {
      bq_dataset_id       = "domain_account_management"
      bq_table_id         = "REPROCESSING_CREDIT_LIMIT_OFFER"
      schema_file         = "/bq_table_schemas/digital_adoption/credit_limit/reprocessing_credit_limit_offer.json"
      deletion_protection = false
      clustering          = null
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    cri_wrk_lookup_tcat_tcode_classification = {
      bq_dataset_id       = "domain_cri_compliance"
      bq_table_id         = "CRI_WRK_LOOKUP_TCAT_TCODE_CLASSIFICATION"
      schema_file         = "/bq_table_schemas/risk_management/cri/cri_wrk_lookup_tcat_tcode_classification.json"
      deletion_protection = false
      clustering          = ["TCat", "TCode"]
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "INGESTION_TIMESTAMP",
        require_partition_filter = false
      }
    },
    cri_refund_request_status = {
      bq_dataset_id       = "domain_cri_compliance"
      bq_table_id         = "CRI_REFUND_REQUEST_STATUS"
      schema_file         = "/bq_table_schemas/risk_management/cri/cri_refund_request_status.json"
      deletion_protection = false
      clustering          = ["accountId", "customerId"]
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "RecLoadTimestamp",
        require_partition_filter = false
      }
    },
    interac_aml_report = {
      bq_dataset_id       = "domain_aml"
      bq_table_id         = "INTERAC_AML_REPORT"
      schema_file         = "/bigquery_table_schemas/InteracAml.json"
      deletion_protection = false
      clustering          = null
      time_partitioning = {
          type                     = "DAY",
          expiration_ms            = null,
          field                    = "RecLoadTimestamp",
          require_partition_filter = false
      }
    },
    cri_audit_data_issues = {
      bq_dataset_id       = "domain_cri_compliance"
      bq_table_id         = "CRI_AUDIT_DATA_ISSUES"
      schema_file         = "/bq_table_schemas/risk_management/cri/cri_audit_data_issues.json"
      deletion_protection = false
      clustering          = ["EntityType", "EntityId"]
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "RecLoadTimestamp",
        require_partition_filter = false
      }
    },
    cri_audit_new_transaction_types = {
      bq_dataset_id       = "domain_cri_compliance"
      bq_table_id         = "CRI_AUDIT_NEW_TRANSACTION_TYPES"
      schema_file         = "/bq_table_schemas/risk_management/cri/cri_audit_new_transaction_types.json"
      deletion_protection = false
      clustering          = ["Tcat", "Tcode"]
      time_partitioning = {
        type                     = "DAY",
        expiration_ms            = null,
        field                    = "RecLoadTimestamp",
        require_partition_filter = false
      }
    }
  },

  ctt_bq_tables = [
    {
      table_name   = "CUSTOM_CONTACT_DETAIL_RECORDS"
      table_schema = "CustomContactDetailRecords.json"
    },
    {
      table_name   = "STAT_ADR"
      table_schema = "StatAdr.json"
    },
    {
      table_name   = "STAT_AGENT_ACTIVITY_D"
      table_schema = "StatAgentActivityD.json"
    },
    {
      table_name   = "STAT_AGENT_NOT_READY_BREAKDOWN_D"
      table_schema = "StatAgentNotReadyBreakdownD.json"
    },
    {
      table_name   = "STAT_CDR"
      table_schema = "StatCdr.json"
    },
    {
      table_name   = "TOUCH_POINT"
      table_schema = "TouchPoint.json"
    },
    {
      table_name   = "CUSTOM_LOB"
      table_schema = "CustomLob.json"
    },
    {
      table_name   = "NOT_READY_REASON_NAME"
      table_schema = "NotReadyReasonName.json"
    },
    {
      table_name   = "LOB_NAME"
      table_schema = "LobName.json"
    },
    {
      table_name   = "STAT_QUEUEACTIVITY_D"
      table_schema = "StatQueueActivityD.json"
    },
    {
      table_name   = "STAT_AGENTACTIVITYBYQUEUE_D"
      table_schema = "StatAgentActivityByQueueD.json"
    }
  ]

  acct_mgmt_bq_tables = {
    customer_alert_preference_update_history = {
      bq_dataset_id       = "domain_account_management"
      bq_table_id         = "CUSTOMER_ALERT_PREFERENCE_UPDATE_HISTORY"
      schema_file         = "customer_alert_preference_update_history.json"
      deletion_protection = true
    }
  }

  # private_cloudbuild_uri = "projects/pcb-prod-terraform-seed/locations/northamerica-northeast1/workerPools/pcb-cloud-build-worker-pool"

  # Business variables
  business_projects_composer_sa = [
    "creditrisk-airflow@pcb-prod-creditrisk.iam.gserviceaccount.com",
    "creditriskecl-airflow@pcb-prod-creditrisk-ecl.iam.gserviceaccount.com",
    "creditriskscoring-airflow@pcb-prod-creditrisk-scoring.iam.gserviceaccount.com",
    "fraud-airflow@pcb-prod-fraud.iam.gserviceaccount.com",
    "pds-airflow@pcb-prod-pds.iam.gserviceaccount.com",
    "portfolio-mgmt@pcb-prod-portfolio-mgmt.iam.gserviceaccount.com",
    "pcb-erm@pcb-prod-erm.iam.gserviceaccount.com"
  ]

  cross_domain_authorized_datasets = {
    data_freshness_view_dependency = {
      landing_dataset_id = "domain_technical"
      curated_dataset_id = "domain_data_logs"
    }
  }
}

locals {
  common_config = read_terragrunt_config(find_in_parent_folders("terragrunt.hcl"))
  terraform_gcp_terminus_revision = local.common_config.inputs.terraform_gcp_terminus_revision
}

terraform {
  source = "git::https://gitlab.lblw.ca/pcf-engineering/shared/pcf-terraform-modules/pcf-gcp-terraform-modules/terraform-gcp-terminus.git//landingzone?ref=${local.terraform_gcp_terminus_revision}"
}
