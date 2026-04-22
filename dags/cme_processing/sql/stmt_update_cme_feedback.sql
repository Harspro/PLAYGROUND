INSERT INTO
  `pcb-{env}-landing.domain_marketing.STMT_CME_FEEDBACK`
SELECT
  * EXCEPT(CUSTOMER_UPDATE_ID)
FROM
  pcb-{env}-processing.domain_customer_management.STMT_CME_FEEDBACK_Y
WHERE
  CUSTOMER_UPDATE_ID = 'Y'