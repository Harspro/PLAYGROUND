SELECT DISTINCT
    TOS.targetedOfferId,
    TOF.tsysAccountId,
    TOS.offerCode,
    TOS.billC86,
    TOS.billC86Days
FROM
    `pcb-{env}-curated.domain_loyalty.PC_TARGETED_OFFER` TOS,
    UNNEST(TOS.offerTriggers) as OT
JOIN
    `pcb-{env}-curated.domain_loyalty.PC_TARGETED_OFFER_FULFILLMENT` TOF
ON
    TOF.offerTriggerId = OT.offerTriggerId
ORDER BY
  TOF.tsysAccountId