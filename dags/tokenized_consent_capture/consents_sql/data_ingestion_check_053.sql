DELETE
FROM
  pcb-{env}-landing.domain_consent.CNST_EVENTS_ANALYTICS
WHERE
  legacyDataSource = {db_053}
  AND 1 = (
    SELECT 
        1
    FROM
        pcb-{env}-landing.domain_consent.CNST_EVENTS_ANALYTICS
    WHERE
        legacydataSource = {db_053}
    LIMIT 1
    );