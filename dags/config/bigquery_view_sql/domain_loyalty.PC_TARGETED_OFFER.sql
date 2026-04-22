SELECT * FROM(
  SELECT * EXCEPT(offerTriggers),
  ARRAY(
    SELECT AS STRUCT
      ot.* REPLACE(SAFE_DIVIDE(CAST(ot.pointsAwarded AS FLOAT64), 1000) AS pointsAwarded)
    FROM UNNEST(offerTriggers) AS ot
  ) AS offerTriggers,
  ROW_NUMBER() OVER(PARTITION BY targetedOfferId ORDER BY INGESTION_TIMESTAMP DESC) AS latest_record
  FROM `pcb-{env}-landing.domain_loyalty.PC_TARGETED_OFFER`
  )
WHERE latest_record = 1
AND (UPPER(status) IN ('PENDING', 'ACTIVE', 'CANCELLED'));