BEGIN
DELETE
  FROM pcb-{env}-processing.domain_dispute.TCS_FRAUD_DTL_STGNG
WHERE
  REC_CHNG_ID =  "<<file_name>>";
DELETE
  FROM pcb-{env}-processing.domain_dispute.TCS_DISPUTE_CLAIM_STGNG
WHERE
  REC_CHNG_ID =  "<<file_name>>";
DELETE
  FROM pcb-{env}-processing.domain_dispute.TCS_ALERTS_STGNG
WHERE
  REC_CHNG_ID =  "<<file_name>>";
DELETE
  FROM pcb-{env}-processing.domain_dispute.TCS_FRAUD_MONITORING_STGNG
WHERE
  REC_CHNG_ID =  "<<file_name>>";
DELETE
  FROM pcb-{env}-processing.domain_dispute.TCS_DISPUTE_DTL_STGNG
WHERE
  REC_CHNG_ID =  "<<file_name>>";
DELETE
  FROM pcb-{env}-processing.domain_dispute.TCS_DISPUTES_LTR_STGNG
WHERE
  REC_CHNG_ID =  "<<file_name>>";
DELETE
  FROM pcb-{env}-processing.domain_dispute.TCS_DISPUTE_FRAUD_COMMON_STGNG
WHERE
  REC_CHNG_ID =  "<<file_name>>";
DELETE
  FROM pcb-{env}-processing.domain_dispute.TCS_FRAUD_REPORTING_STGNG
WHERE
  REC_CHNG_ID =  "<<file_name>>";
DELETE
  FROM pcb-{env}-processing.domain_dispute.TCS_LOST_STOLEN_STGNG
WHERE
  REC_CHNG_ID =  "<<file_name>>";
END;
