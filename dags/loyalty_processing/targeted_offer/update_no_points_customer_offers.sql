UPDATE `pcb-{env}-landing.domain_loyalty.PC_TARGETED_OFFER`
SET offerTriggers = ARRAY(
  SELECT AS STRUCT
    t.* REPLACE (
      'Earn X points when you set up a minimum $X direct deposit within 2 months of opening and maintain recurring deposits for 3 consecutive months' AS triggerDescription,
      'DIR_DEPO' AS triggerType,
      'AMA_PYRLL2' AS categoryCode,
      60 AS fulfillmentPeriodDays,
      '1500' AS minAmount,
      100000 AS pointsAwarded
    )
  FROM UNNEST(offerTriggers) AS t
)
WHERE targetedOfferId = 76;