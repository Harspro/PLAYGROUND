CREATE OR REPLACE TABLE
  pcb-{env}-processing.domain_account_management.{output_table_name}
  OPTIONS ( expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR) )
  AS
SELECT * FROM pcb-{env}-processing.domain_account_management.SA10_RECORD_DBEXT

 UNION ALL

SELECT * FROM pcb-{env}-processing.domain_account_management.SA14_RECORD_DBEXT

 UNION ALL

SELECT * FROM pcb-{env}-processing.domain_account_management.SA15_RECORD_DBEXT

 UNION ALL

SELECT * FROM pcb-{env}-processing.domain_account_management.SA20_RECORD_DBEXT

 UNION ALL

SELECT * FROM pcb-{env}-processing.domain_account_management.SA30_RECORD_DBEXT

 UNION ALL

SELECT * FROM pcb-{env}-processing.domain_account_management.SA40_RECORD_DBEXT

 UNION ALL

SELECT * FROM pcb-{env}-processing.domain_account_management.SA59_RECORD_DBEXT

 UNION ALL

SELECT * FROM pcb-{env}-processing.domain_account_management.SA60_RECORD_DBEXT

 UNION ALL

SELECT * FROM pcb-{env}-processing.domain_account_management.SA85_RECORD_DBEXT

 UNION ALL

SELECT * FROM pcb-{env}-processing.domain_account_management.SA88_RECORD_DBEXT
;