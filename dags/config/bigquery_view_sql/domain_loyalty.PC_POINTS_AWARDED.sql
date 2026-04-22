WITH
  PC_POINTS_AWARDED AS (
  SELECT
    pcPoints.id,
    pcfCustomerIdCluster AS pcfCustomerId,
    pcPoints.customerId,
    pcPoints.campaignId,
    pcPoints.externalCampaignId,
    pcPoints.points,
    DATETIME( transactionDatePartition,
      CASE
        WHEN LENGTH(pcPoints.transactionDate) >= 20 AND REGEXP_CONTAINS(pcPoints.transactionDate, r'([+-]\d{2}:\d{2})') THEN SUBSTR(pcPoints.transactionDate, -6, 6)
        ELSE 'America/Toronto'
    END
      ) AS transactionDate,
    pcPoints.description,
    pcPoints.externalId,
    pcPoints.transactionOrigin,
    pcPoints.cardHash,
    pcPoints.cardBin,
    pcPoints.transactionCode,
    memberId,
    pointEventId,
    pointEventReference,
    ingestion_timestamp
  FROM
    `pcb-{env}-landing.domain_loyalty.PC_POINTS_AWARDED`)
SELECT
  * EXCEPT(transactionDate),
  DATETIME_TRUNC(transactionDate, SECOND) AS transactionDate
FROM
  PC_POINTS_AWARDED