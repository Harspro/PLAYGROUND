CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) ) AS
WITH
  sa30_record AS (
  SELECT
    FILE_CREATE_DT,
    SA30_ACCT_ID AS ACCT_ID,
    SA30_CLIENT_NUM AS CLIENT_NUM,
    sa30_KEY_STMT_FORM,
    SA30_KEY_INSRT_GRP,
    SA30_RECORD_ID,
    SA30_SEQ_NUM,
    SA30_AMT_FC_CTD * 100 AS SA30_AMT_FC_CTD,
    SA30_AMT_CREDITS_CTD * 100 AS SA30_AMT_CREDITS_CTD,
    SA30_AMT_FEE_AS_FC * 100 AS SA30_AMT_FEE_AS_FC,
    SA30_NUM_TCATS,
    SA30_TCAT_1,
    SA30_TCAT_2,
    SA30_TCAT_3,
    SA30_TCAT_4,
    SA30_TCAT_5,
    SA30_TCAT_6,
    SA30_TCAT_7,
    SA30_TCAT_8,
    SA30_TCAT_9,
    SA30_TCAT_10,
    SA30_TCAT_11,
    SA30_TCAT_12,
    SA30_TCAT_13,
    SA30_TCAT_14,
    SA30_TCAT_15,
    SA30_TCAT_16,
    SA30_TCAT_17,
    SA30_TCAT_18,
    SA30_TCAT_19,
    SA30_TCAT_20,
    SA30_TCAT_21,
    SA30_TCAT_22,
    SA30_TCAT_23,
    SA30_TCAT_24,
    SA30_TCAT_25,
    SA30_TCAT_26,
    SA30_TCAT_27,
    SA30_TCAT_28,
    SA30_TCAT_29,
    SA30_TCAT_30,
    SA30_TCAT_31,
    SA30_TCAT_32,
    SA30_TCAT_33,
    SA30_TCAT_34,
    SA30_TCAT_35,
    SA30_TCAT_36,
    SA30_AMT_CTD_1 * 100 AS SA30_AMT_CTD_1,
    SA30_AMT_CTD_2 * 100 AS SA30_AMT_CTD_2,
    SA30_AMT_CTD_3 * 100 AS SA30_AMT_CTD_3,
    SA30_AMT_CTD_4 * 100 AS SA30_AMT_CTD_4,
    SA30_AMT_CTD_5 * 100 AS SA30_AMT_CTD_5,
    SA30_AMT_CTD_6 * 100 AS SA30_AMT_CTD_6,
    SA30_AMT_CTD_7 * 100 AS SA30_AMT_CTD_7,
    SA30_AMT_CTD_8 * 100 AS SA30_AMT_CTD_8,
    SA30_AMT_CTD_9 * 100 AS SA30_AMT_CTD_9,
    SA30_AMT_CTD_10 * 100 AS SA30_AMT_CTD_10,
    SA30_AMT_CTD_11 * 100 AS SA30_AMT_CTD_11,
    SA30_AMT_CTD_12 * 100 AS SA30_AMT_CTD_12,
    SA30_AMT_CTD_13 * 100 AS SA30_AMT_CTD_13,
    SA30_AMT_CTD_14 * 100 AS SA30_AMT_CTD_14,
    SA30_AMT_CTD_15 * 100 AS SA30_AMT_CTD_15,
    SA30_AMT_CTD_16 * 100 AS SA30_AMT_CTD_16,
    SA30_AMT_CTD_17 * 100 AS SA30_AMT_CTD_17,
    SA30_AMT_CTD_18 * 100 AS SA30_AMT_CTD_18,
    SA30_AMT_CTD_19 * 100 AS SA30_AMT_CTD_19,
    SA30_AMT_CTD_20 * 100 AS SA30_AMT_CTD_20,
    SA30_AMT_CTD_21 * 100 AS SA30_AMT_CTD_21,
    SA30_AMT_CTD_22 * 100 AS SA30_AMT_CTD_22,
    SA30_AMT_CTD_23 * 100 AS SA30_AMT_CTD_23,
    SA30_AMT_CTD_24 * 100 AS SA30_AMT_CTD_24,
    SA30_AMT_CTD_25 * 100 AS SA30_AMT_CTD_25,
    SA30_AMT_CTD_26 * 100 AS SA30_AMT_CTD_26,
    SA30_AMT_CTD_27 * 100 AS SA30_AMT_CTD_27,
    SA30_AMT_CTD_28 * 100 AS SA30_AMT_CTD_28,
    SA30_AMT_CTD_29 * 100 AS SA30_AMT_CTD_29,
    SA30_AMT_CTD_30 * 100 AS SA30_AMT_CTD_30,
    SA30_AMT_CTD_31 * 100 AS SA30_AMT_CTD_31,
    SA30_AMT_CTD_32 * 100 AS SA30_AMT_CTD_32,
    SA30_AMT_CTD_33 * 100 AS SA30_AMT_CTD_33,
    SA30_AMT_CTD_34 * 100 AS SA30_AMT_CTD_34,
    SA30_AMT_CTD_35 * 100 AS SA30_AMT_CTD_35,
    SA30_AMT_CTD_36 * 100 AS SA30_AMT_CTD_36,
    PARENT_CARD_NUM
  FROM
    `pcb-{env}-landing.domain_account_management.SA30` sa30
  WHERE
    sa30.FILE_CREATE_DT='{file_create_dt}'
  AND sa30.FILE_NAME='{file_name}')
SELECT
  (((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((LPAD(CAST(sa30_record.CLIENT_NUM AS STRING), 4, '0') || LPAD(CAST(sa30_record.ACCT_ID AS STRING), 12, '0')) || sa30_record.sa30_RECORD_ID) || LPAD(CAST(sa30_record.SA30_SEQ_NUM AS STRING), 5, '0')) || ' ') || LPAD(IFNULL(sa30_record.sa30_KEY_STMT_FORM, ' '), 4, ' ')) || LPAD(IFNULL(sa30_record.sa30_KEY_INSRT_GRP, ' '), 3, ' ')) || ' ') ||
                                                                                                                                            IF
                                                                                                                                              ((sa30_record.SA30_AMT_FC_CTD < 0), (LPAD( REPLACE(CAST((sa30_record.SA30_AMT_FC_CTD * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(sa30_record.SA30_AMT_FC_CTD AS STRING), '.', ''), 13, '0') || '+'))) ||
                                                                                                                                          IF
                                                                                                                                            ((sa30_record.SA30_AMT_CREDITS_CTD < 0), (LPAD(REPLACE(CAST((sa30_record.SA30_AMT_CREDITS_CTD * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(sa30_record.SA30_AMT_CREDITS_CTD AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(' ', 7, ' ')) ||
                                                                                                                                      IF
                                                                                                                                        ((sa30_record.SA30_AMT_FEE_AS_FC < 0), (LPAD(REPLACE(CAST((sa30_record.SA30_AMT_FEE_AS_FC * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(sa30_record.SA30_AMT_FEE_AS_FC AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(' ', 300, ' ')) || LPAD(CAST(sa30_record.SA30_NUM_TCATS AS STRING), 3, '0')) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_1, 0) AS STRING), 4, '0')) ||
                                                                                                                              IF
                                                                                                                                ((IFNULL(sa30_record.SA30_AMT_CTD_1,0) < 0), ( LPAD(REPLACE(CAST((sa30_record.SA30_AMT_CTD_1 * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_1,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_2,0) AS STRING), 4, '0')) ||
                                                                                                                          IF
                                                                                                                            ((IFNULL(sa30_record.SA30_AMT_CTD_2,0) < 0), (LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_2,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_2,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_3,0) AS STRING), 4, '0')) ||
                                                                                                                      IF
                                                                                                                        ((IFNULL(sa30_record.SA30_AMT_CTD_3,0) < 0), (LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_3,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_3,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_4,0) AS STRING), 4, '0')) ||
                                                                                                                  IF
                                                                                                                    ((IFNULL(sa30_record.SA30_AMT_CTD_4,0) < 0), ( LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_4,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_4,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(IFNULL(sa30_record.SA30_TCAT_5,0),0) AS STRING), 4, '0')) ||
                                                                                                              IF
                                                                                                                ((IFNULL(sa30_record.SA30_AMT_CTD_5,0) < 0), (LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_5,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_5,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_6,0) AS STRING), 4, '0')) ||
                                                                                                          IF
                                                                                                            ((IFNULL(sa30_record.SA30_AMT_CTD_6,0) < 0), (LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_6,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_6,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_7,0) AS STRING), 4, '0')) ||
                                                                                                      IF
                                                                                                        ((IFNULL(sa30_record.SA30_AMT_CTD_7,0) < 0), (LPAD( REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_7,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_7,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_8,0) AS STRING), 4, '0')) ||
                                                                                                  IF
                                                                                                    ((IFNULL(sa30_record.SA30_AMT_CTD_8,0) < 0), (LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_8,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_8,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_9,0) AS STRING), 4, '0')) ||
                                                                                              IF
                                                                                                ((IFNULL(sa30_record.SA30_AMT_CTD_9,0) < 0), (LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_9,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_9,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_10,0) AS STRING), 4, '0')) ||
                                                                                          IF
                                                                                            ((IFNULL(sa30_record.SA30_AMT_CTD_10,0) < 0), (LPAD(REPLACE( CAST((IFNULL(sa30_record.SA30_AMT_CTD_10,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_10,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_11,0) AS STRING), 4, '0')) ||
                                                                                      IF
                                                                                        ((IFNULL(sa30_record.SA30_AMT_CTD_11,0) < 0), (LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_11,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_11,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_12,0) AS STRING), 4, '0')) ||
                                                                                  IF
                                                                                    ((IFNULL(sa30_record.SA30_AMT_CTD_12,0) < 0), (LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_12,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_12,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_13,0) AS STRING), 4, '0')) ||
                                                                              IF
                                                                                ((IFNULL(sa30_record.SA30_AMT_CTD_13,0) < 0), (LPAD( REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_13,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_13,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_14,0) AS STRING), 4, '0')) ||
                                                                          IF
                                                                            ((IFNULL(sa30_record.SA30_AMT_CTD_14,0) < 0), (LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_14,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_14,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_15,0) AS STRING), 4, '0')) ||
                                                                      IF
                                                                        ((IFNULL(sa30_record.SA30_AMT_CTD_15,0) < 0), (LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_15,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_15,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_16,0) AS STRING), 4, '0')) ||
                                                                  IF
                                                                    ((IFNULL(sa30_record.SA30_AMT_CTD_16,0) < 0), ( LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_16,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_16,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_17,0) AS STRING), 4, '0')) ||
                                                              IF
                                                                ((IFNULL(sa30_record.SA30_AMT_CTD_17,0) < 0), (LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_17,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_17,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_18,0) AS STRING), 4, '0')) ||
                                                          IF
                                                            ((IFNULL(sa30_record.SA30_AMT_CTD_18,0) < 0), (LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_18,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_18,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_19,0) AS STRING), 4, '0')) ||
                                                      IF
                                                        ((IFNULL(sa30_record.SA30_AMT_CTD_19,0) < 0), ( LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_19,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_19,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_20,0) AS STRING), 4, '0')) ||
                                                  IF
                                                    ((IFNULL(sa30_record.SA30_AMT_CTD_20,0) < 0), (LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_20,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_20,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_21,0) AS STRING), 4, '0')) ||
                                              IF
                                                ((IFNULL(sa30_record.SA30_AMT_CTD_21,0) < 0), (LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_21,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_21,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_22,0) AS STRING), 4, '0')) ||
                                          IF
                                            ((IFNULL(sa30_record.SA30_AMT_CTD_22,0) < 0), ( LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_22,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_22,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_23,0) AS STRING), 4, '0')) ||
                                      IF
                                        ((IFNULL(sa30_record.SA30_AMT_CTD_23,0) < 0), (LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_23,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_23,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_24,0) AS STRING), 4, '0')) ||
                                  IF
                                    ((IFNULL(sa30_record.SA30_AMT_CTD_24,0) < 0), (LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_24,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_24,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_25,0) AS STRING), 4, '0')) ||
                              IF
                                ((IFNULL(sa30_record.SA30_AMT_CTD_25,0) < 0), ( LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_25,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_25,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_26,0) AS STRING), 4, '0')) ||
                          IF
                            ((IFNULL(sa30_record.SA30_AMT_CTD_26,0) < 0), (LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_26,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_26,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_27,0) AS STRING), 4, '0')) ||
                      IF
                        ((IFNULL(sa30_record.SA30_AMT_CTD_27,0) < 0), (LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_27,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_27,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_28,0) AS STRING), 4, '0')) ||
                  IF
                    ((IFNULL(sa30_record.SA30_AMT_CTD_28,0) < 0), ( LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_28,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_28,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_29,0) AS STRING), 4, '0')) ||
              IF
                ((IFNULL(sa30_record.SA30_AMT_CTD_29,0) < 0), (LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_29,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_29,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_30,0) AS STRING), 4, '0')) ||
          IF
            ((IFNULL(sa30_record.SA30_AMT_CTD_30,0) < 0), (LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_30,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_30,0) AS STRING), '.', ''), 13, '0') || '+'))) || LPAD(CAST(IFNULL(sa30_record.SA30_TCAT_31,0) AS STRING), 4, '0')) ||
      IF
        ((IFNULL(sa30_record.SA30_AMT_CTD_31,0) < 0), ( LPAD(REPLACE(CAST((IFNULL(sa30_record.SA30_AMT_CTD_31,0) * -1) AS STRING), '.', ''), 13, '0') || '-'), (LPAD(REPLACE(CAST(IFNULL(sa30_record.SA30_AMT_CTD_31,0) AS STRING), '.', ''), 13, '0') || '+')))) || CONCAT( LPAD(SAFE_CAST(IFNULL(sa30_record.SA30_TCAT_32, 0) AS STRING), 4, '0'),
      CASE
        WHEN IFNULL(sa30_record.SA30_AMT_CTD_32, 0) < 0 THEN LPAD(REPLACE(SAFE_CAST((IFNULL(sa30_record.SA30_AMT_CTD_32, 0) * -1) AS STRING), '.', ''), 13, '0') || '-'
        ELSE LPAD(REPLACE(SAFE_CAST(IFNULL(sa30_record.SA30_AMT_CTD_32, 0) AS STRING), '.', ''), 13, '0') || '+'
    END
      ) || CONCAT( LPAD(SAFE_CAST(IFNULL(sa30_record.SA30_TCAT_33, 0) AS STRING), 4, '0'),
      CASE
        WHEN IFNULL(sa30_record.SA30_AMT_CTD_33, 0) < 0 THEN LPAD(REPLACE(SAFE_CAST((IFNULL(sa30_record.SA30_AMT_CTD_33, 0) * -1) AS STRING), '.', ''), 13, '0') || '-'
        ELSE LPAD(REPLACE(SAFE_CAST(IFNULL(sa30_record.SA30_AMT_CTD_33, 0) AS STRING), '.', ''), 13, '0') || '+'
    END
      ) || CONCAT( LPAD(SAFE_CAST(IFNULL(sa30_record.SA30_TCAT_34, 0) AS STRING), 4, '0'),
      CASE
        WHEN IFNULL(sa30_record.SA30_AMT_CTD_34, 0) < 0 THEN LPAD(REPLACE(SAFE_CAST((IFNULL(sa30_record.SA30_AMT_CTD_34, 0) * -1) AS STRING), '.', ''), 13, '0') || '-'
        ELSE LPAD(REPLACE(SAFE_CAST(IFNULL(sa30_record.SA30_AMT_CTD_34, 0) AS STRING), '.', ''), 13, '0') || '+'
    END
      ) || CONCAT( LPAD(SAFE_CAST(IFNULL(sa30_record.SA30_TCAT_35, 0) AS STRING), 4, '0'),
      CASE
        WHEN IFNULL(sa30_record.SA30_AMT_CTD_35, 0) < 0 THEN LPAD(REPLACE(SAFE_CAST((IFNULL(sa30_record.SA30_AMT_CTD_35, 0) * -1) AS STRING), '.', ''), 13, '0') || '-'
        ELSE LPAD(REPLACE(SAFE_CAST(IFNULL(sa30_record.SA30_AMT_CTD_35, 0) AS STRING), '.', ''), 13, '0') || '+'
    END
      ) || CONCAT( LPAD(SAFE_CAST(IFNULL(sa30_record.SA30_TCAT_36, 0) AS STRING), 4, '0'),
      CASE
        WHEN IFNULL(sa30_record.SA30_AMT_CTD_36, 0) < 0 THEN LPAD(REPLACE(SAFE_CAST((IFNULL(sa30_record.SA30_AMT_CTD_36, 0) * -1) AS STRING), '.', ''), 13, '0') || '-'
        ELSE LPAD(REPLACE(SAFE_CAST(IFNULL(sa30_record.SA30_AMT_CTD_36, 0) AS STRING), '.', ''), 13, '0') || '+'
    END
      ) || LPAD(' ', 166, ' ')) AS record,
  sa30_record.ACCT_ID,
  sa30_record.sa30_SEQ_NUM AS SEQ_NUM,
  sa30_record.FILE_CREATE_DT,
  sa30_record.SA30_RECORD_ID AS RECORD_ID,
  SUBSTR(sa30_record.PARENT_CARD_NUM,11) AS PARENT_NUM,
  5 AS RECORD_SEQ
FROM
  sa30_record