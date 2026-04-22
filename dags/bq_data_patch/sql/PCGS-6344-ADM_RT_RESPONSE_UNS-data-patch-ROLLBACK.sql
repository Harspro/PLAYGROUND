DELETE
FROM
  pcb-{env}-landing.domain_customer_acquisition.ADM_RT_RESPONSE_UNS
WHERE
  DATE(REC_CREATE_TMS) BETWEEN '2024-09-19' AND '2025-06-22'
  AND SOURCE_CD IN ('KOG', 'LNQ');

INSERT INTO
  pcb-{env}-landing.domain_customer_acquisition.ADM_RT_RESPONSE_UNS
SELECT
  *
FROM
  pcb-{env}-processing.domain_customer_acquisition.ADM_RT_RESPONSE_UNS_BACKUP
WHERE
  DATE(REC_CREATE_TMS) BETWEEN '2024-09-19' AND '2025-06-22'
  AND SOURCE_CD IN ('KOG', 'LNQ');
