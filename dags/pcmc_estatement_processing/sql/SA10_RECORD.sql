CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
WITH
  sa10_record AS (
  SELECT
    SA10_ACCT_ID AS ACCT_ID,
    SA10_CLIENT_NUM AS CLIENT_NUM,
    SA10_SEQ_NUM,
    SA10_RECORD_ID,
    SA10_KEY_STMT_FORM,
    SA10_KEY_INSRT_GRP,
    SA10_TSYS_PROD_CODE,
    SA10_CLIENT_PROD_CODE,
    SA10_UNEDITED_ACCT_NBR_1,
    SA10_AUTO_PAY_METHOD,
    SA10_STMT_HOLD_CODE,
    SA10_CASH_AVAIL * 100 AS SA10_CASH_AVAIL,
    SA10_CASH_LIMIT,
    SA10_BILLING_CYCLE,
    SA10_CRED_LIMIT,
    SA10_CRED_AVAIL * 100 AS SA10_CRED_AVAIL,
    SA10_BAL_CUR * 100 AS SA10_BAL_CUR,
    SA10_MISC_DEBITS * 100 AS SA10_MISC_DEBITS,
    SA10_BAL_PREV * 100 AS SA10_BAL_PREV,
    SA10_AMT_PAYMENTS_CTD * 100 AS SA10_AMT_PAYMENTS_CTD,
    SA10_ARCHIVAL_ONLY,
    SA10_STMT_START_DATE,
    SA10_TOTAL_PASTDUE * 100 AS SA10_TOTAL_PASTDUE,
    SA10_TOTAL_OVERLIMIT * 100 AS SA10_TOTAL_OVERLIMIT,
    SA10_STMT_MINPAY * 100 AS SA10_STMT_MINPAY,
    SA10_CLOSING_DATE,
    SA10_PAYMENT_DUE_DATE,
    SA10_CUST_DATA_12,
    SA10_DATE_ACCT_OPEN,
    SA10_NUM_STMT_LTD,
    SA10_STC_SEC_FRAUD,
    SA10_STC_CLOSED,
    SA10_STC_CHARGEOFF,
    SA10_STC_CUR_OL,
    SA10_STC_CUR_PD,
    SA10_STC_TRANSFER,
    SA10_STC_LEGAL,
    SA10_STC_WATCH,
    SA10_STC_RETURN_MAIL,
    SA10_STC_SKIP_A_PAY,
    SA10_STC_STOP_PAY,
    SA10_STC_FIRST_USE,
    SA10_STC_FORECLOSURE,
    SA10_STC_FIXED_PAY,
    SA10_STC_CRED_REVOKED,
    SA10_STC_CIT_ACCEPTED,
    SA10_STC_DISPUTE,
    SA10_STC_CLIENT_DEF,
    SA10_STC_HIGHLY_ACTIVE,
    SA10_STC_MISC_TRANSFER,
    SA10_STC_ICS,
    SA10_STC_CO_1099_RPT,
    SA10_STC_DLQ,
    SA10_STC_REAGE_AUTO,
    SA10_STC_REAGE_MAN,
    SA10_STC_APPLICATION,
    SA10_STC_BUREAU_DISP,
    SA10_STC_MOC,
    SA10_STC_RECENCY,
    SA10_STC_TIERED_AUTH,
    SA10_STC_AUTH_OVERRIDE,
    SA10_STC_DORMANT,
    SA10_STF_CC,
    SA10_LANG_CODE_BEG_CY,
    SA10_MONEY_AVAILABLE,
    SA10_EBPP_STMT_IND,
    FILE_CREATE_DT,
    PARENT_CARD_NUM
  FROM
    `pcb-{env}-landing.domain_account_management.SA10` sa10
  WHERE
    sa10.FILE_CREATE_DT='{file_create_dt}'
    AND sa10.FILE_NAME='{file_name}')
SELECT
  (((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((LPAD(CAST(sa10_record.CLIENT_NUM AS STRING), 4, '0') || LPAD(CAST (sa10_record.ACCT_ID AS STRING), 12, '0')) || sa10_record.sa10_RECORD_ID) || LPAD(CAST(sa10_record.SA10_SEQ_NUM AS STRING), 5, '0')) || ' ') || LPAD(IFNULL(sa10_record.SA10_KEY_STMT_FORM, ' '), 4, ' ')) || LPAD(IFNULL(sa10_record.SA10_KEY_INSRT_GRP, ' '), 3, ' ')) || ' ') || LPAD(sa10_record.SA10_TSYS_PROD_CODE, 2, ' ')) || LPAD(sa10_record.SA10_CLIENT_PROD_CODE, 3, ' ')) || LPAD(sa10_record.SA10_UNEDITED_ACCT_NBR_1, 19, ' ')) ||
                                                                                                                    CASE
                                                                                                                      WHEN sa10_record.SA10_AUTO_PAY_METHOD IS NULL THEN ' '
                                                                                                                      WHEN sa10_record.SA10_AUTO_PAY_METHOD='' THEN ' '
                                                                                                                      ELSE sa10_record.SA10_AUTO_PAY_METHOD
                                                                                                                  END
                                                                                                                    ) || LPAD(CAST(sa10_record.SA10_STMT_HOLD_CODE AS STRING), 3, '0')) || LPAD(CAST(sa10_record.SA10_BILLING_CYCLE AS STRING), 2, '0')) ||
                                                                                                            IF
                                                                                                              ((sa10_record.SA10_CASH_AVAIL < 0), (LPAD(REPLACE(CAST((sa10_record.SA10_CASH_AVAIL * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(sa10_record.SA10_CASH_AVAIL AS STRING), '.', ''), 13, '0') || '+'))) || LPAD((CAST(sa10_record.SA10_CASH_LIMIT AS STRING) || '+'), 12, '0')) || LPAD((CAST(sa10_record.SA10_CRED_LIMIT AS STRING) || '+'), 12, '0')) ||
                                                                                                      IF
                                                                                                        ((sa10_record.SA10_CRED_AVAIL < 0), (LPAD(REPLACE(CAST((sa10_record.SA10_CRED_AVAIL * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(sa10_record.SA10_CRED_AVAIL AS STRING), '.', ''), 13, '0') || '+'))) ||
                                                                                                    IF
                                                                                                      ((sa10_record.SA10_BAL_CUR < 0), (LPAD(REPLACE(CAST((sa10_record.SA10_BAL_CUR * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(sa10_record.SA10_BAL_CUR AS STRING), '.', ''), 13, '0') || '+'))) ||
                                                                                                  IF
                                                                                                    ((sa10_record.SA10_MISC_DEBITS < 0), (LPAD(REPLACE(CAST((sa10_record.SA10_MISC_DEBITS * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(sa10_record.SA10_MISC_DEBITS AS STRING), '.', ''), 13, '0') || '+'))) ||
                                                                                                IF
                                                                                                  ((sa10_record.SA10_BAL_PREV < 0), (LPAD(REPLACE(CAST((sa10_record.SA10_BAL_PREV * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(sa10_record.SA10_BAL_PREV AS STRING), '.', ''), 13, '0') || '+'))) ||
                                                                                              IF
                                                                                                ((sa10_record.SA10_AMT_PAYMENTS_CTD < 0), (LPAD(REPLACE(CAST((sa10_record.SA10_AMT_PAYMENTS_CTD * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(sa10_record.SA10_AMT_PAYMENTS_CTD AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(IFNULL(sa10_record.SA10_ARCHIVAL_ONLY, ' '), 1,' ')) || LPAD(IFNULL(sa10_record.SA10_STMT_START_DATE, ' '), 8, ' ')) ||
                                                                                        IF
                                                                                          ((sa10_record.SA10_TOTAL_PASTDUE < 0), (LPAD(REPLACE(CAST((sa10_record.SA10_TOTAL_PASTDUE * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(sa10_record.SA10_TOTAL_PASTDUE AS STRING), '.', ''), 13, '0') || '+'))) ||
                                                                                      IF
                                                                                        ((sa10_record.SA10_TOTAL_OVERLIMIT < 0), ( LPAD(REPLACE(CAST((sa10_record.SA10_TOTAL_OVERLIMIT * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(sa10_record.SA10_TOTAL_OVERLIMIT AS STRING), '.', ''), 13, '0') || '+'))) ||
                                                                                    IF
                                                                                      ((sa10_record.SA10_STMT_MINPAY < 0), (LPAD(REPLACE(CAST((sa10_record.SA10_STMT_MINPAY * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(sa10_record.SA10_STMT_MINPAY AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(IFNULL(sa10_record.SA10_CLOSING_DATE, ' '), 8, ' ')) || LPAD(IFNULL(sa10_record.SA10_PAYMENT_DUE_DATE, ' '), 8, ' ')) || LPAD(IFNULL(sa10_record.SA10_CUST_DATA_12, ''),1,' ')) || LPAD(IFNULL(sa10_record.SA10_DATE_ACCT_OPEN, ' '), 8, ' ')) || LPAD(CAST(sa10_record.SA10_NUM_STMT_LTD AS STRING), 3, '0')) || LPAD(IFNULL(sa10_record.SA10_STC_SEC_FRAUD, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_CLOSED, ' '), 2, ' ')) || LPAD( IFNULL(sa10_record.SA10_STC_CHARGEOFF, ' '), 2, ' ')) || LPAD(CAST(sa10_record.SA10_STC_CUR_OL AS STRING), 2, '0')) || LPAD(CAST(sa10_record.SA10_STC_CUR_PD AS STRING), 2, '0')) || LPAD(IFNULL(sa10_record.SA10_STC_TRANSFER, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_LEGAL, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_WATCH, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_RETURN_MAIL, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_SKIP_A_PAY, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_STOP_PAY, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_FIRST_USE, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_FORECLOSURE, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_FIXED_PAY, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_CRED_REVOKED, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_CIT_ACCEPTED, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_DISPUTE, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_CLIENT_DEF, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_HIGHLY_ACTIVE, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_MISC_TRANSFER, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_ICS, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_CO_1099_RPT, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_DLQ, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_REAGE_AUTO, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_REAGE_MAN, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_APPLICATION, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_BUREAU_DISP, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_MOC, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_RECENCY, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_TIERED_AUTH, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STC_AUTH_OVERRIDE, ' '), 1, ' ')) || LPAD( IFNULL(sa10_record.SA10_STC_DORMANT, ' '), 2, ' ')) || LPAD(IFNULL(sa10_record.SA10_STF_CC, ' '), 1, ' ')) || LPAD(IFNULL(sa10_record.SA10_LANG_CODE_BEG_CY, ' '), 3, ' ')) || LPAD((CAST(sa10_record.SA10_MONEY_AVAILABLE AS STRING) || '+'), 12, '0')) || LPAD(IFNULL(sa10_record.SA10_EBPP_STMT_IND, ' '), 1, ' ') || LPAD(' ', 169, ' ')) AS record,
  sa10_record.ACCT_ID,
  sa10_record.SA10_SEQ_NUM AS SEQ_NUM,
  sa10_record.FILE_CREATE_DT,
  sa10_record.SA10_RECORD_ID AS RECORD_ID,
  SUBSTR(sa10_record.PARENT_CARD_NUM,11) AS PARENT_NUM,
  1 AS RECORD_SEQ
FROM
  sa10_record