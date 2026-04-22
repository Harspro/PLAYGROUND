CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
WITH
  sa88_record AS (
  SELECT
    SA88_CLIENT_NUM AS client_num,
    SA88_ACCT_ID AS ACCT_ID,
    SA88_SEQ_NUM AS SEQ_NUM,
    sa88.*
  FROM
    `pcb-{env}-processing.domain_account_management.SA88_DBEXT` sa88
  WHERE
    sa88.FILE_CREATE_DT='{file_create_dt}')
SELECT
  (((((((((((((((((((((((((((((((((((((((((((( LPAD(CAST(SA88_RECORD.SA88_CLIENT_NUM AS STRING), 4, '0') || LPAD(CAST(SA88_RECORD.SA88_ACCT_ID AS STRING), 12, '0')) || SA88_RECORD.SA88_RECORD_ID) || LPAD(CAST(SA88_RECORD.SA88_SEQ_NUM AS STRING), 5, '0')) || ' ') || LPAD(IFNULL(SA88_RECORD.KEY_STMT_FORM,' '), 4, ' ')) || LPAD(IFNULL(SA88_RECORD.KEY_INSRT_GRP,' '), 3, ' ')) || ' ') ||
                                                                          IF
                                                                            ((SA88_RECORD.SA88_PURCHASES < 0), (LPAD(REPLACE(CAST((SA88_RECORD.SA88_PURCHASES * -100) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(SA88_RECORD.SA88_PURCHASES * 100 AS STRING), '.', ''), 13, '0') || '+'))) ||
                                                                        IF
                                                                          ((SA88_RECORD.SA88_CASH_ADVANCES < 0), (LPAD(REPLACE(CAST((SA88_RECORD.SA88_CASH_ADVANCES * -100) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(SA88_RECORD.SA88_CASH_ADVANCES * 100 AS STRING), '.', ''), 13, '0') || '+'))) ||
                                                                      IF
                                                                        ((SA88_RECORD.SA88_CONV_CHEQUES < 0), (LPAD(REPLACE(CAST((SA88_RECORD.SA88_CONV_CHEQUES * -100) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(SA88_RECORD.SA88_CONV_CHEQUES * 100 AS STRING), '.', ''), 13, '0') || '+'))) ||
                                                                    IF
                                                                      ((SA88_RECORD.SA88_PROM_PURCHASES < 0), (LPAD(REPLACE(CAST((SA88_RECORD.SA88_PROM_PURCHASES * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(SA88_RECORD.SA88_PROM_PURCHASES AS STRING), '.', ''), 13, '0') || '+'))) ||
                                                                  IF
                                                                    ((SA88_RECORD.SA88_FEES < 0), (LPAD(REPLACE(CAST((SA88_RECORD.SA88_FEES * -100) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(SA88_RECORD.SA88_FEES *100 AS STRING), '.', ''), 13, '0') || '+'))) ||
                                                                IF
                                                                  ((SA88_RECORD.SA88_OTHER_CHARGES < 0), (LPAD(REPLACE(CAST((SA88_RECORD.SA88_OTHER_CHARGES * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(SA88_RECORD.SA88_OTHER_CHARGES AS STRING), '.', ''), 13, '0') || '+'))) ||
                                                              IF
                                                                ((SA88_RECORD.SA88_PC_POINTS < 0), (LPAD(REPLACE(CAST((SA88_RECORD.SA88_PC_POINTS * -100) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(SA88_RECORD.SA88_PC_POINTS *100 AS STRING), '.', ''), 13, '0') || '+'))) ||
                                                            IF
                                                              ((SA88_RECORD.SA88_PC_BONUS_POINTS < 0), (LPAD(REPLACE(CAST((SA88_RECORD.SA88_PC_BONUS_POINTS * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(SA88_RECORD.SA88_PC_BONUS_POINTS AS STRING), '.', ''), 13, '0') || '+'))) ||
                                                          IF
                                                            ((SA88_RECORD.SA88_PC_TOTAL_POINTS < 0), (LPAD(REPLACE(CAST((SA88_RECORD.SA88_PC_TOTAL_POINTS * -100) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(SA88_RECORD.SA88_PC_TOTAL_POINTS * 100 AS STRING), '.', ''), 13, '0') || '+'))) ||
                                                        IF
                                                          ((SA88_RECORD.SA88_POINTS_BALANCE < 0), (LPAD(REPLACE(CAST((SA88_RECORD.SA88_POINTS_BALANCE * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(SA88_RECORD.SA88_POINTS_BALANCE AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(IFNULL(CAST(SA88_RECORD.SA88_SALUTATION AS string),' '), 4, ' ')) || LPAD(IFNULL(CAST(SA88_RECORD.SA88_GENDER AS string), ' '), 1, ' ')) || LPAD(IFNULL(SA88_RECORD.SA88_FIRST_NAME,' '), 36, ' ')) || LPAD(IFNULL(TRIM(SA88_RECORD.SA88_MIDDLE_INIT), ''), 1,' ')) || LPAD(IFNULL(SA88_RECORD.SA88_LAST_NAME,' '), 36, ' ')) ||
                                            IF
                                              ((SA88_RECORD.SA88_CRED_AVAIL < 0), (LPAD(REPLACE(CAST((SA88_RECORD.SA88_CRED_AVAIL * -100) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(SA88_RECORD.SA88_CRED_AVAIL *100 AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(IFNULL(SA88_RECORD.SA88_LYLTY_PLUS_CODE, ' '), 1, ' ')) || LPAD(IFNULL(SA88_RECORD.SA88_DNS_CODE, ' '), 1, ' ')) || LPAD(IFNULL(SA88_RECORD.SA88_AUTH_USER_CODE, ' '), 1, ' ')) ||
                                    IF
                                      ((SA88_RECORD.SA88_IN_STORE_PURCHASE_AMT < 0), (LPAD(REPLACE(CAST((SA88_RECORD.SA88_IN_STORE_PURCHASE_AMT * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(SA88_RECORD.SA88_IN_STORE_PURCHASE_AMT AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(IFNULL(SA88_RECORD.SA88_CREDIT_CARE_ENRLL, ' '), 1, ' ')) ||
                                IF
                                  ((SA88_RECORD.SA88_SPEND_BUCKET < 0), (LPAD(REPLACE(CAST((SA88_RECORD.SA88_SPEND_BUCKET * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(SA88_RECORD.SA88_SPEND_BUCKET AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(IFNULL(SA88_RECORD.SA88_PAYMENT_METHOD,' '), 10, ' ')) || LPAD(IFNULL(SA88_RECORD.SA88_PREV_STMT_DATE,' '), 8, ' ')) || LPAD(IFNULL(SA88_RECORD.SA88_SUB_TOTAL_IND, ' '), 1, ' ')) || LPAD(IFNULL(CAST(SA88_RECORD.SA88_NOTIFICATION_AREA AS string),' '), 75, ' ')) || LPAD(IFNULL(SA88_RECORD.SA88_PRSNT_CRTRIA,' '), 25, ' ')) || LPAD(IFNULL(SA88_RECORD.SA88_SOURCE,' '), 15, ' ')) || LPAD(IFNULL(CAST(SA88_RECORD.SA88_INSERTS AS string),' '), 12, ' ')) || LPAD(IFNULL(SA88_RECORD.SA88_STMT_CAT, ' '),1,' ')) || LPAD(IFNULL(SA88_RECORD.SA88_REPRICE_DATE,' '), 8, ' ')) || LPAD(IFNULL(SA88_RECORD.SA88_COUNTRY_NAME,' '), 25, ' ')) || LPAD(IFNULL(CAST(SA88_RECORD.SA88_TTP_IND AS STRING),'0'),1,'0')) || LPAD(IFNULL(SA88_RECORD.SA88_COUNTRY_CODE,' '), 3, ' ')) || LPAD(IFNULL(SA88_RECORD.SA88_CDF_80_1,' '),1,' ') || LPAD(IFNULL(SA88_RECORD.SA88_CDF_80_2,' '),1,' ') || LPAD(IFNULL(SA88_RECORD.SA88_CDF_80_3,' '), 1, ' ')|| LPAD(IFNULL(SA88_RECORD.SA88_CDF_80_4,' '),1,' ') || LPAD(IFNULL(SA88_RECORD.SA88_CDF_80_5,' '),1,' ') || LPAD(IFNULL(SA88_RECORD.SA88_CDF_80_6,' '),1,' ') || LPAD(IFNULL(SA88_RECORD.SA88_CDF_80_7,' '),1,' ') || LPAD(IFNULL(SA88_RECORD.SA88_CDF_80_8,' '),1,' ') || LPAD(REPLACE(CAST(SA88_RECORD.SA88_TOTAL_DEPOSITS * 100 AS STRING), '.', ''), 13, '0') || '-' ) || LPAD(REPLACE(CAST(SA88_RECORD.SA88_TOTAL_WITHDRAWALS * 100 AS STRING), '.', ''), 13, '0') || '+' ) || LPAD(' ', 182, ' ')) AS record,
  SA88_record.sa88_ACCT_ID,
  SA88_RECORD.SA88_SEQ_NUM AS SEQ_NUM,
  SA88_RECORD.file_create_dt,
  SA88_RECORD.SA88_RECORD_ID,
  SUBSTR(SA88_record.PARENT_CARD_NUM,11) AS PARENT_NUM,
  12 AS RECORD_SEQ
FROM
  SA88_RECORD