WITH max_seq_num as(
  SELECT
    max(SA00_FILE_SEQ_NBR) as value
  FROM `pcb-{env}-landing.domain_account_management.SA00` ),
  next_seq_num as(
    SELECT
      CASE
        WHEN value > 0
          THEN value + 1
        ELSE
          (SELECT
            max(FILE_SEQ_NBR)
          FROM pcb-{env}-landing.domain_account_management.STMT_DAILY ) + 1
      END AS FILE_SEQ_NUM
    FROM max_seq_num
  )
SELECT
  * REPLACE((SELECT CAST(FILE_SEQ_NUM AS INTEGER) FROM next_seq_num ) AS SA00_FILE_SEQ_NBR)
FROM
  pcb-{env}-processing.domain_account_management.SA00{file_type}_DBEXT_COLUMN_TF