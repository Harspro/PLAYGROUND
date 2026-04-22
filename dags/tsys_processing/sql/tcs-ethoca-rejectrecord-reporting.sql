INSERT INTO pcb-{env}-landing.domain_movemoney_technical.TCS_ETHOCA_DISPUTE_REJECT_DTL
SELECT
 *
FROM
 pcb-{env}-processing.domain_dispute.TCS_DISPUTE_DTL_ETHOCA_STGNG
WHERE
 CARD_NUMBER = '';