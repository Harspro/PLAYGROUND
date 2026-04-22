CREATE OR REPLACE TABLE pcb-{env}-processing.domain_dispute.TCS_DISPUTE_DTL_ETHOCA
  AS
    (SELECT *
    FROM pcb-{env}-processing.domain_dispute.TCS_DISPUTE_DTL_ETHOCA_STGNG
    WHERE CARD_NUMBER !='');