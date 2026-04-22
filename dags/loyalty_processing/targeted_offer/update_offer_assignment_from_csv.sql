UPDATE `pcb-{env}-landing.domain_loyalty.PC_TARGETED_OFFER_FULFILLMENT` T
SET offerTriggerId = S.offerTriggerId
FROM `pcb-{env}-processing.domain_loyalty.PC_TARGETED_OFFER_FULFILLMENT_EXT` S
WHERE T.tsysAccountId = S.tsysAccountId
AND UPPER(S.updateType) = 'UPDATE'