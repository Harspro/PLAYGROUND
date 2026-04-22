CREATE OR REPLACE VIEW
  pcb-{env}-processing.domain_account_management.NEW_APPLICATION
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS
SELECT
  DISTINCT apa_app_num,
  MAX(execution_id) AS max_execution_id
FROM
  `pcb-{env}-curated.domain_customer_acquisition.ADM_STD_DISP_APP_DATA`
WHERE
  UPPER(apa_status) = 'NEWACCOUNT'
  AND UPPER(apa_queue_id) = 'APPROVE'
  AND (apa_test_account_flag IS NULL
    OR apa_test_account_flag = '')
GROUP BY
  apa_app_num;
