SELECT DISTINCT * FROM (SELECT
    IFNULL(INTERAC.FINANCIAL_INSTITUTION_ID,'CA000320') AS FINANCIAL_INSTITUTION_ID,
    IFNULL(INTERAC.RECONCILIATION_TYPE,'FULL' ) AS RECONCILIATION_TYPE,
    IFNULL(INTERAC.INTERAC_NETWORK_FILE_ID,'NOT_FOUND') AS INTERAC_NETWORK_FILE_ID,
    CURRENT_DATETIME() as RUN_DATE,
    DATE(IFNULL(PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ',INTERAC.FILE_FROM_TIMESTAMP), PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ',<<<from_timestamp_goes_here>>>))) AS BUSINESS_DATE,
    IFNULL(PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ',INTERAC.FILE_FROM_TIMESTAMP), PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ',<<<from_timestamp_goes_here>>>)) AS FILE_FROM_TIMESTAMP,
    IFNULL(PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ',INTERAC.FILE_TO_TIMESTAMP), PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ',<<<to_timestamp_goes_here>>>)) AS FILE_TO_TIMESTAMP,
    CASE
       WHEN INTERAC.INTERAC_TRANSFER_REFERENCE_NUMBER IS NULL AND FI.FI_TRANSFER_REFERENCE_NUMBER IS NOT NULL THEN "MISSING_AT_INTERAC"
       WHEN INTERAC.INTERAC_TRANSFER_REFERENCE_NUMBER IS NOT NULL AND FI.FI_TRANSFER_REFERENCE_NUMBER IS NULL THEN "MISSING_AT_FI"
       WHEN INTERAC.INTERAC_TRANSFER_REFERENCE_NUMBER IS NOT NULL AND FI.FI_TRANSFER_REFERENCE_NUMBER IS NOT NULL AND CAST(FI.FI_TRANSFER_AMOUNT AS NUMERIC)<>CAST(INTERAC.INTERAC_TRANSFER_AMOUNT AS NUMERIC) THEN "DISCREPANCY_DETECTED"
       WHEN COALESCE(INTERAC.INTERAC_FIREQNO,FI.FI_FIREQNO) <> FI.FI_FIREQNO then "FIREQNO_MISMATCH"
       ELSE 'N/A'
    END as EXCEPTION_TYPE,
    IFNULL(INTERAC.INITIATED_BY,'FI') AS INITIATED_BY,
    IFNULL(INTERAC.INTERAC_TRANSFER_REFERENCE_NUMBER,FI.FI_TRANSFER_REFERENCE_NUMBER) AS TRANSFER_REFERENCE_NUMBER,
    FI.FI_ACCESS_MEDIUM_NUMBER as ACCESS_MEDIUM_NUMBER,
    FI.FI_GIVEN_NAME as GIVEN_NAME,
    FI.FI_SURNAME as SURNAME,
    FI.FI_CURRENCY_CODE,
    FI.FI_FIREQNO,
    CAST(FI.FI_TRANSACTION_DT AS DATETIME) AS FI_TRANSACTION_DT,
    CAST(FI.FI_LOCAL_TRANSACTION_DT AS DATETIME) AS FI_LOCAL_TRANSACTION_DT,
    CAST(FI.FI_TRANSFER_AMOUNT AS NUMERIC) AS FI_TRANSFER_AMOUNT,
    IFNULL(FI.FI_PARTICIPANT_USER_ID_CRDT,FI.FI_PARTICIPANT_USER_ID_DBIT) AS FI_PARTICIPANT_USER_ID,
    FI.FI_TRANSACTION_TYPE,
    FI.FI_CHANNEL_INDICATOR,
    FI.FI_SENDER_PARTICIPANT_ID,
    FI.FI_RECIPIENT_PARTICIPANT_USERID,
    INTERAC.INTERAC_CURRENCY_CODE,
    INTERAC.INTERAC_FIREQNO,
    CAST(PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ',INTERAC.INTERAC_TRANSACTION_DT) AS DATETIME) AS INTERAC_TRANSACTION_DT,
    CAST(PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ',INTERAC.INTERAC_LOCAL_TRANSACTION_DT) AS DATETIME) AS INTERAC_LOCAL_TRANSACTION_DT,
    CAST(INTERAC.INTERAC_TRANSFER_AMOUNT AS NUMERIC) AS INTERAC_TRANSFER_AMOUNT,
    INTERAC.INTERAC_TRANSACTION_TYPE,
    INTERAC.INTERAC_CHANNEL_INDICATOR,
    INTERAC.INTERAC_SENDER_PARTICIPANT_ID,
    INTERAC.INTERAC_RECIPIENT_PARTICIPANT_USERID,
    'N' AS INVALID_IND
FROM
(SELECT ctti.paymentIdentification.clearingSystemReference as FI_TRANSFER_REFERENCE_NUMBER,
          FI_ACM.ACCESS_MEDIUM_NO as FI_ACCESS_MEDIUM_NUMBER,
          FI_CST.GIVEN_NAME as FI_GIVEN_NAME,
          FI_CST.SURNAME as FI_SURNAME,
          ctti.paymentTypeInformation.categoryPurpose.proprietary as FI_TRANSACTION_TYPE,
          ctti.interbankSettlementAmount.currency as FI_CURRENCY_CODE,
          ctti.paymentIdentification.instructionIdentification as FI_FIREQNO,
          ctti.acceptanceDateTime as FI_TRANSACTION_DT,
          ctti.acceptanceDateTime as FI_LOCAL_TRANSACTION_DT,
          ctti.interbankSettlementAmount.amount as FI_TRANSFER_AMOUNT,
          (SELECT identification FROM UNNEST(ctti.creditor.identification.privateIdentification.other) WHERE schemeName.code='BANK') as FI_PARTICIPANT_USER_ID_CRDT,
          (SELECT identification FROM UNNEST(ctti.creditor.identification.privateIdentification.other) WHERE schemeName.proprietary='participantUserType') as FI_PARTICIPANT_USER_ID_TYPE_CRDT,
          (SELECT identification FROM UNNEST(ctti.debtor.identification.privateIdentification.other) WHERE schemeName.code='BANK') as FI_PARTICIPANT_USER_ID_DBIT,
          (SELECT identification FROM UNNEST(ctti.debtor.identification.privateIdentification.other) WHERE schemeName.proprietary='participantUserType') as FI_PARTICIPANT_USER_ID_TYPE_DBIT,
          FI_EVENT.payment_rail as FI_CHANNEL_INDICATOR,
          ctti.debtorAgent.financialInstitutionIdentification.clearingSystemMemberIdentification.memberIdentification as FI_SENDER_PARTICIPANT_ID,
          ctti.creditorAgent.financialInstitutionIdentification.clearingSystemMemberIdentification.memberIdentification as FI_RECIPIENT_PARTICIPANT_USERID,
          case when ctti.paymentTypeInformation.categoryPurpose.proprietary in ('INAD','INAR','INCA','INRC','INRR')  then 'CRDT'
               when ctti.paymentTypeInformation.categoryPurpose.proprietary in ('INSE','INMR') then 'DBIT'
               else null
          end as FI_CREDIT_DEBIT_IND
   FROM `pcb-{env}-landing.domain_payments.PCB_FUNDS_MOVE_CREATED` as FI_EVENT,
       unnest(FI_EVENT.fiToFiCustomerCreditTransfer.creditTransferTransactionInformation) as ctti
   left join pcb-{env}-landing.domain_account_management.ACCOUNT_CUSTOMER as FI_AC
       on FI_AC.ACCOUNT_UID=
       CAST(case when ctti.paymentTypeInformation.categoryPurpose.proprietary in ('INAD','INAR','INCA','INRC','INRR')
   then (SELECT identification FROM UNNEST(FI_EVENT.fiToFiCustomerCreditTransfer.creditTransferTransactionInformation[SAFE_OFFSET(0)].creditor.identification.privateIdentification.other) WHERE schemeName.proprietary='accountId')
      when ctti.paymentTypeInformation.categoryPurpose.proprietary in ('INSE','INMR')
   then ((SELECT identification FROM UNNEST(FI_EVENT.fiToFiCustomerCreditTransfer.creditTransferTransactionInformation[SAFE_OFFSET(0)].debtor.identification.privateIdentification.other) WHERE schemeName.proprietary='accountId'))
   else null
end as INT) and FI_AC.CUSTOMER_UID=
      CAST(case when ctti.paymentTypeInformation.categoryPurpose.proprietary in ('INAD','INAR','INCA','INRC','INRR')
  then (SELECT identification FROM UNNEST(FI_EVENT.fiToFiCustomerCreditTransfer.creditTransferTransactionInformation[SAFE_OFFSET(0)].creditor.identification.privateIdentification.other) WHERE schemeName.code='CUST')
     when ctti.paymentTypeInformation.categoryPurpose.proprietary in ('INSE','INMR')
  then (SELECT identification FROM UNNEST(FI_EVENT.fiToFiCustomerCreditTransfer.creditTransferTransactionInformation[SAFE_OFFSET(0)].debtor.identification.privateIdentification.other) WHERE schemeName.code='CUST')
  else null
  end as INT)
  left join `pcb-{env}-landing.domain_account_management.ACCOUNT_CUSTOMER_ROLE` FI_ACR
       on FI_ACR.code='PRIMARY-CARD-HOLDER'
      and FI_AC.ACCOUNT_CUSTOMER_ROLE_UID=FI_ACR.ACCOUNT_CUSTOMER_ROLE_UID
   left join `pcb-{env}-landing.domain_account_management.ACCESS_MEDIUM` FI_ACM
       on FI_AC.ACCOUNT_CUSTOMER_UID = FI_ACM.ACCOUNT_CUSTOMER_UID
      and FI_ACM.DEACTIVATED_DT is null
   left join `pcb-{env}-landing.domain_customer_management.CUSTOMER` FI_CST
       on FI_AC.CUSTOMER_UID = FI_CST.CUSTOMER_UID
   where FI_EVENT.payment_rail in ( 'INAD','INAR','INCA','INRC','INRR','INSE','INMR')
   AND ctti.acceptanceDateTime BETWEEN CAST(<<<from_timestamp_goes_here>>> AS TIMESTAMP) AND CAST(<<<to_timestamp_goes_here>>> AS TIMESTAMP)
   ) FI
LEFT JOIN
(SELECT
       bank_to_customer_account_report.group_header.additional_information as RECONCILIATION_TYPE,
       bank_to_customer_account_report.group_header.message_identification as INTERAC_NETWORK_FILE_ID,
           bank_to_customer_account_report.report.from_to_date.from_date_time as FILE_FROM_TIMESTAMP,
           bank_to_customer_account_report.report.from_to_date.to_date_time  as FILE_TO_TIMESTAMP,
           IF(bank_to_customer_account_report.report.account.servicer.financial_institution_identification.clearing_system_member_identification.member_identification ='CA000320','FI','INTERAC') as INITIATED_BY,
       INTERAC_EVENT_ENTRY.entry_details.transaction_details.references.clearing_system_reference as INTERAC_TRANSFER_REFERENCE_NUMBER,
       INTERAC_EVENT_ENTRY.amount.currency as INTERAC_CURRENCY_CODE,
       CASE WHEN INTERAC_EVENT_ENTRY.credit_debit_indicator = 'CRDT'THEN INTERAC_EVENT_ENTRY.entry_details.transaction_details.references.account_servicer_reference
            WHEN INTERAC_EVENT_ENTRY.credit_debit_indicator = 'DBIT' THEN INTERAC_EVENT_ENTRY.entry_details.transaction_details.references.transaction_identification
       END as INTERAC_FIREQNO,
       INTERAC_EVENT_ENTRY.entry_details.transaction_details.related_dates.transaction_date_time as INTERAC_TRANSACTION_DT,
       INTERAC_EVENT_ENTRY.entry_details.transaction_details.related_dates.transaction_date_time as INTERAC_LOCAL_TRANSACTION_DT,
       INTERAC_EVENT_ENTRY.amount.amount as INTERAC_TRANSFER_AMOUNT,
       IF(INTERAC_EVENT_ENTRY.credit_debit_indicator = 'CRDT',INTERAC_EVENT_ENTRY.entry_details.transaction_details.related_parties.creditor.party.identification.organisation_identification.other.identification,null) as INTERAC_PARTICIPANT_USER_ID_CRDT,
       IF(INTERAC_EVENT_ENTRY.credit_debit_indicator = 'DBIT',INTERAC_EVENT_ENTRY.entry_details.transaction_details.related_parties.debtor.party.identification.organisation_identification.other.identification,null) as INTERAC_PARTICIPANT_USER_ID_DBIT,
           INTERAC_EVENT_ENTRY.entry_details.transaction_details.related_agents.debtor_agent.financial_institution_identification.clearing_system_member_identification.member_identification as INTERAC_SENDER_PARTICIPANT_ID,
           INTERAC_EVENT_ENTRY.entry_details.transaction_details.related_agents.creditor_agent.financial_institution_identification.clearing_system_member_identification.member_identification as INTERAC_RECIPIENT_PARTICIPANT_USERID,
bank_to_customer_account_report.report.account.servicer.financial_institution_identification.clearing_system_member_identification.member_identification as FINANCIAL_INSTITUTION_ID,
           INTERAC_EVENT_ENTRY.entry_details.transaction_details.local_instrument.proprietary as INTERAC_TRANSACTION_TYPE,
           INTERAC_EVENT_ENTRY.technical_input_channel.proprietary as INTERAC_CHANNEL_INDICATOR,
           INTERAC_EVENT_ENTRY.credit_debit_indicator as INTERAC_CREDIT_DEBIT_IND
   FROM `pcb-{env}-landing.domain_payments.INTERAC_RECON_FILE_HEADERS` AS INTERAC_EVENT
        inner join `pcb-{env}-landing.domain_payments.INTERAC_RECON_TRANSACTIONS` AS INTERAC_EVENT_ENTRY
        on INTERAC_EVENT_ENTRY.message_id = INTERAC_EVENT.bank_to_customer_account_report.group_header.message_identification
      where CAST(PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ',INTERAC_EVENT.bank_to_customer_account_report.report.from_to_date.from_date_time) AS DATETIME)
            BETWEEN DATETIME_SUB(PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ',<<<from_timestamp_goes_here>>>), INTERVAL <<<time_window_days>>> DAY)
                AND DATETIME_ADD(PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ',<<<to_timestamp_goes_here>>>), INTERVAL <<<time_window_days>>> DAY)
   ) INTERAC
     ON FI.FI_TRANSFER_REFERENCE_NUMBER=INTERAC.INTERAC_TRANSFER_REFERENCE_NUMBER
    AND FI.FI_CREDIT_DEBIT_IND=INTERAC.INTERAC_CREDIT_DEBIT_IND
UNION ALL
(SELECT
    IFNULL(INTERAC_MISS_FI.FINANCIAL_INSTITUTION_ID,'CA000320') AS FINANCIAL_INSTITUTION_ID,
    IFNULL(INTERAC_MISS_FI.RECONCILIATION_TYPE,'FULL' ) AS RECONCILIATION_TYPE,
    IFNULL(INTERAC_MISS_FI.INTERAC_NETWORK_FILE_ID,'NOT_FOUND') AS INTERAC_NETWORK_FILE_ID,
    CURRENT_DATETIME() as RUN_DATE,
    DATE(IFNULL(PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ',INTERAC_MISS_FI.FILE_FROM_TIMESTAMP) , PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ',<<<from_timestamp_goes_here>>>))) AS BUSINESS_DATE,
    IFNULL(PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ',INTERAC_MISS_FI.FILE_FROM_TIMESTAMP) , PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ',<<<from_timestamp_goes_here>>>)) AS FILE_FROM_TIMESTAMP,
    IFNULL(PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ',INTERAC_MISS_FI.FILE_TO_TIMESTAMP) , PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ',<<<to_timestamp_goes_here>>>)) AS FILE_TO_TIMESTAMP,
    CASE
        WHEN INTERAC_MISS_FI.INTERAC_TRANSFER_REFERENCE_NUMBER IS NULL AND FI_MISS_FI.FI_TRANSFER_REFERENCE_NUMBER IS NOT NULL THEN "MISSING_AT_INTERAC"
        WHEN INTERAC_MISS_FI.INTERAC_TRANSFER_REFERENCE_NUMBER IS NOT NULL AND FI_MISS_FI.FI_TRANSFER_REFERENCE_NUMBER IS NULL THEN "MISSING_AT_FI"
        WHEN INTERAC_MISS_FI.INTERAC_TRANSFER_REFERENCE_NUMBER IS NOT NULL AND FI_MISS_FI.FI_TRANSFER_REFERENCE_NUMBER IS NOT NULL AND CAST(FI_MISS_FI.FI_TRANSFER_AMOUNT AS NUMERIC)<>CAST(INTERAC_MISS_FI.INTERAC_TRANSFER_AMOUNT AS NUMERIC) THEN "DISCREPANCY_DETECTED"
        when COALESCE(INTERAC_MISS_FI.INTERAC_FIREQNO,FI_MISS_FI.FI_FIREQNO) <> FI_MISS_FI.FI_FIREQNO then "FIREQNO_MISMATCH"
        ELSE 'N/A'
    END as EXCEPTION_TYPE,
    IFNULL(INTERAC_MISS_FI.INITIATED_BY,'FI') AS INITIATED_BY,
    IFNULL(INTERAC_MISS_FI.INTERAC_TRANSFER_REFERENCE_NUMBER,FI_MISS_FI.FI_TRANSFER_REFERENCE_NUMBER) AS TRANSFER_REFERENCE_NUMBER,
    FI_MISS_FI.FI_ACCESS_MEDIUM_NUMBER as ACCESS_MEDIUM_NUMBER,
    FI_MISS_FI.FI_GIVEN_NAME as GIVEN_NAME,
    FI_MISS_FI.FI_SURNAME as SURNAME,
    FI_MISS_FI.FI_CURRENCY_CODE,
    FI_MISS_FI.FI_FIREQNO,
    CAST(FI_MISS_FI.FI_TRANSACTION_DT AS DATETIME) AS FI_TRANSACTION_DT,
    CAST(FI_MISS_FI.FI_LOCAL_TRANSACTION_DT AS DATETIME) AS FI_LOCAL_TRANSACTION_DT,
    CAST(FI_MISS_FI.FI_TRANSFER_AMOUNT AS NUMERIC) AS FI_TRANSFER_AMOUNT,
    IFNULL(FI_MISS_FI.FI_PARTICIPANT_USER_ID_CRDT,FI_MISS_FI.FI_PARTICIPANT_USER_ID_DBIT) AS FI_PARTICIPANT_USER_ID,
    FI_MISS_FI.FI_TRANSACTION_TYPE,
    FI_MISS_FI.FI_CHANNEL_INDICATOR,
    FI_MISS_FI.FI_SENDER_PARTICIPANT_ID,
    FI_MISS_FI.FI_RECIPIENT_PARTICIPANT_USERID,
    INTERAC_MISS_FI.INTERAC_CURRENCY_CODE,
    INTERAC_MISS_FI.INTERAC_FIREQNO,
    CAST(PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ',INTERAC_MISS_FI.INTERAC_TRANSACTION_DT) AS DATETIME) AS INTERAC_TRANSACTION_DT,
    CAST(PARSE_DATETIME('%Y-%m-%dT%H:%M:%E*SZ',INTERAC_MISS_FI.INTERAC_LOCAL_TRANSACTION_DT) AS DATETIME) AS INTERAC_LOCAL_TRANSACTION_DT,
    CAST(INTERAC_MISS_FI.INTERAC_TRANSFER_AMOUNT AS NUMERIC) AS INTERAC_TRANSFER_AMOUNT,
    INTERAC_MISS_FI.INTERAC_TRANSACTION_TYPE,
    INTERAC_MISS_FI.INTERAC_CHANNEL_INDICATOR,
    INTERAC_MISS_FI.INTERAC_SENDER_PARTICIPANT_ID,
    INTERAC_MISS_FI.INTERAC_RECIPIENT_PARTICIPANT_USERID,
    'N' AS INVALID_IND
FROM
   (SELECT
       bank_to_customer_account_report.group_header.additional_information as RECONCILIATION_TYPE,
       bank_to_customer_account_report.group_header.message_identification as INTERAC_NETWORK_FILE_ID,
           bank_to_customer_account_report.report.from_to_date.from_date_time as FILE_FROM_TIMESTAMP,
           bank_to_customer_account_report.report.from_to_date.to_date_time  as FILE_TO_TIMESTAMP,
           IF(bank_to_customer_account_report.report.account.servicer.financial_institution_identification.clearing_system_member_identification.member_identification ='CA000320','FI','INTERAC') as INITIATED_BY,
       INTERAC_EVENT_ENTRY.entry_details.transaction_details.references.clearing_system_reference as INTERAC_TRANSFER_REFERENCE_NUMBER,
       INTERAC_EVENT_ENTRY.amount.currency as INTERAC_CURRENCY_CODE,
       CASE WHEN INTERAC_EVENT_ENTRY.credit_debit_indicator = 'CRDT'THEN INTERAC_EVENT_ENTRY.entry_details.transaction_details.references.account_servicer_reference
            WHEN INTERAC_EVENT_ENTRY.credit_debit_indicator = 'DBIT' THEN INTERAC_EVENT_ENTRY.entry_details.transaction_details.references.transaction_identification
       END as INTERAC_FIREQNO,
       INTERAC_EVENT_ENTRY.entry_details.transaction_details.related_dates.transaction_date_time as INTERAC_TRANSACTION_DT,
       INTERAC_EVENT_ENTRY.entry_details.transaction_details.related_dates.transaction_date_time as INTERAC_LOCAL_TRANSACTION_DT,
       INTERAC_EVENT_ENTRY.amount.amount as INTERAC_TRANSFER_AMOUNT,
       IF(INTERAC_EVENT_ENTRY.credit_debit_indicator = 'CRDT',INTERAC_EVENT_ENTRY.entry_details.transaction_details.related_parties.creditor.party.identification.organisation_identification.other.identification,null) as INTERAC_PARTICIPANT_USER_ID_CRDT,
       IF(INTERAC_EVENT_ENTRY.credit_debit_indicator = 'DBIT',INTERAC_EVENT_ENTRY.entry_details.transaction_details.related_parties.debtor.party.identification.organisation_identification.other.identification,null) as INTERAC_PARTICIPANT_USER_ID_DBIT,
           INTERAC_EVENT_ENTRY.entry_details.transaction_details.related_agents.debtor_agent.financial_institution_identification.clearing_system_member_identification.member_identification as INTERAC_SENDER_PARTICIPANT_ID,
           INTERAC_EVENT_ENTRY.entry_details.transaction_details.related_agents.creditor_agent.financial_institution_identification.clearing_system_member_identification.member_identification as INTERAC_RECIPIENT_PARTICIPANT_USERID,
bank_to_customer_account_report.report.account.servicer.financial_institution_identification.clearing_system_member_identification.member_identification as FINANCIAL_INSTITUTION_ID,
           INTERAC_EVENT_ENTRY.entry_details.transaction_details.local_instrument.proprietary as INTERAC_TRANSACTION_TYPE,
           INTERAC_EVENT_ENTRY.technical_input_channel.proprietary as INTERAC_CHANNEL_INDICATOR,
           INTERAC_EVENT_ENTRY.credit_debit_indicator as INTERAC_CREDIT_DEBIT_IND
   FROM `pcb-{env}-landing.domain_payments.INTERAC_RECON_FILE_HEADERS` AS INTERAC_EVENT
         inner join `pcb-{env}-landing.domain_payments.INTERAC_RECON_TRANSACTIONS` AS INTERAC_EVENT_ENTRY
                 on INTERAC_EVENT_ENTRY.message_id = INTERAC_EVENT.bank_to_customer_account_report.group_header.message_identification
         where INTERAC_EVENT.bank_to_customer_account_report.group_header.message_identification=<<<file_id_goes_here>>>
         and  INTERAC_EVENT_ENTRY.status.code not in ('PDNG')
   ) INTERAC_MISS_FI
LEFT JOIN
(SELECT ctti.paymentIdentification.clearingSystemReference as FI_TRANSFER_REFERENCE_NUMBER,
          FI_ACM.ACCESS_MEDIUM_NO as FI_ACCESS_MEDIUM_NUMBER,
          FI_CST.GIVEN_NAME as FI_GIVEN_NAME,
          FI_CST.SURNAME as FI_SURNAME,
          ctti.paymentTypeInformation.categoryPurpose.proprietary as FI_TRANSACTION_TYPE,
          ctti.interbankSettlementAmount.currency as FI_CURRENCY_CODE,
          ctti.paymentIdentification.instructionIdentification as FI_FIREQNO,
          ctti.acceptanceDateTime as FI_TRANSACTION_DT,
          ctti.acceptanceDateTime as FI_LOCAL_TRANSACTION_DT,
          ctti.interbankSettlementAmount.amount as FI_TRANSFER_AMOUNT,
          (SELECT identification FROM UNNEST(ctti.creditor.identification.privateIdentification.other) WHERE schemeName.code='BANK') as FI_PARTICIPANT_USER_ID_CRDT,
          (SELECT identification FROM UNNEST(ctti.creditor.identification.privateIdentification.other) WHERE schemeName.proprietary='participantUserType') as FI_PARTICIPANT_USER_ID_TYPE_CRDT,
          (SELECT identification FROM UNNEST(ctti.debtor.identification.privateIdentification.other) WHERE schemeName.code='BANK') as FI_PARTICIPANT_USER_ID_DBIT,
          (SELECT identification FROM UNNEST(ctti.debtor.identification.privateIdentification.other) WHERE schemeName.proprietary='participantUserType') as FI_PARTICIPANT_USER_ID_TYPE_DBIT,
          FI_EVENT.payment_rail as FI_CHANNEL_INDICATOR,
          ctti.debtorAgent.financialInstitutionIdentification.clearingSystemMemberIdentification.memberIdentification as FI_SENDER_PARTICIPANT_ID,
          ctti.creditorAgent.financialInstitutionIdentification.clearingSystemMemberIdentification.memberIdentification as FI_RECIPIENT_PARTICIPANT_USERID,
          case when ctti.paymentTypeInformation.categoryPurpose.proprietary in ('INAD','INAR','INCA','INRC','INRR')  then 'CRDT'
               when ctti.paymentTypeInformation.categoryPurpose.proprietary in ('INSE','INMR') then 'DBIT'
               else null
          end as FI_CREDIT_DEBIT_IND
   FROM `pcb-{env}-landing.domain_payments.PCB_FUNDS_MOVE_CREATED` as FI_EVENT,
       unnest(FI_EVENT.fiToFiCustomerCreditTransfer.creditTransferTransactionInformation) as ctti
   left join pcb-{env}-landing.domain_account_management.ACCOUNT_CUSTOMER as FI_AC
       on FI_AC.ACCOUNT_UID=
       CAST(case when ctti.paymentTypeInformation.categoryPurpose.proprietary in ('INAD','INAR','INCA','INRC','INRR')
   then (SELECT identification FROM UNNEST(FI_EVENT.fiToFiCustomerCreditTransfer.creditTransferTransactionInformation[SAFE_OFFSET(0)].creditor.identification.privateIdentification.other) WHERE schemeName.proprietary='accountId')
      when ctti.paymentTypeInformation.categoryPurpose.proprietary in ('INSE','INMR')
   then ((SELECT identification FROM UNNEST(FI_EVENT.fiToFiCustomerCreditTransfer.creditTransferTransactionInformation[SAFE_OFFSET(0)].debtor.identification.privateIdentification.other) WHERE schemeName.proprietary='accountId'))
   else null
end as INT) and FI_AC.CUSTOMER_UID=
      CAST(case when ctti.paymentTypeInformation.categoryPurpose.proprietary in ('INAD','INAR','INCA','INRC','INRR')
  then (SELECT identification FROM UNNEST(FI_EVENT.fiToFiCustomerCreditTransfer.creditTransferTransactionInformation[SAFE_OFFSET(0)].creditor.identification.privateIdentification.other) WHERE schemeName.code='CUST')
     when ctti.paymentTypeInformation.categoryPurpose.proprietary in ('INSE','INMR')
  then (SELECT identification FROM UNNEST(FI_EVENT.fiToFiCustomerCreditTransfer.creditTransferTransactionInformation[SAFE_OFFSET(0)].debtor.identification.privateIdentification.other) WHERE schemeName.code='CUST')
  else null
  end as INT)
  left join `pcb-{env}-landing.domain_account_management.ACCOUNT_CUSTOMER_ROLE` FI_ACR
       on FI_ACR.code='PRIMARY-CARD-HOLDER'
      and FI_AC.ACCOUNT_CUSTOMER_ROLE_UID=FI_ACR.ACCOUNT_CUSTOMER_ROLE_UID
   left join `pcb-{env}-landing.domain_account_management.ACCESS_MEDIUM` FI_ACM
       on FI_AC.ACCOUNT_CUSTOMER_UID = FI_ACM.ACCOUNT_CUSTOMER_UID
      and FI_ACM.DEACTIVATED_DT is null
   left join `pcb-{env}-landing.domain_customer_management.CUSTOMER` FI_CST
       on FI_AC.CUSTOMER_UID = FI_CST.CUSTOMER_UID
   where FI_EVENT.payment_rail in ( 'INAD','INAR','INCA','INRC','INRR','INSE','INMR')
   AND ctti.acceptanceDateTime >= TIMESTAMP_SUB(CAST(<<<from_timestamp_goes_here>>> AS TIMESTAMP), INTERVAL 90 DAY)
   ) FI_MISS_FI
     ON FI_MISS_FI.FI_TRANSFER_REFERENCE_NUMBER=INTERAC_MISS_FI.INTERAC_TRANSFER_REFERENCE_NUMBER
    AND FI_MISS_FI.FI_CREDIT_DEBIT_IND=INTERAC_MISS_FI.INTERAC_CREDIT_DEBIT_IND
    )
)
WHERE
         FI_TRANSFER_AMOUNT IS NULL
      OR INTERAC_TRANSFER_AMOUNT IS NULL
      OR FI_TRANSFER_AMOUNT <> INTERAC_TRANSFER_AMOUNT
      OR FI_FIREQNO <> INTERAC_FIREQNO