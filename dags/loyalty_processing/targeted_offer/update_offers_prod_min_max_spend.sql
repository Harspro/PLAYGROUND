UPDATE
  `pcb-{env}-landing.domain_loyalty.PC_TARGETED_OFFER` AS t
SET
  offerTriggers = ARRAY(
  SELECT
    AS STRUCT ot.* REPLACE(
    IF
      ( ot.offerTriggerId IN UNNEST([34, 46])
        AND (ot.maxTotalAmount IS NULL), '200.00', ot.maxTotalAmount ) AS maxTotalAmount,
    IF
      ( ot.offerTriggerId IN UNNEST([34, 46])
        AND SAFE_CAST(ot.minAmount AS NUMERIC) = 200.00, NULL, ot.minAmount ) AS minAmount )
  FROM
    UNNEST(t.offerTriggers) AS ot )
WHERE
  t.targetedOfferId IN UNNEST([18, 26])
  AND t.offerCode IN UNNEST(["PDSB-6724-P50K", "PDSB-6724-P50K_S"])
  AND t.Status IN UNNEST(["ACTIVE"]);