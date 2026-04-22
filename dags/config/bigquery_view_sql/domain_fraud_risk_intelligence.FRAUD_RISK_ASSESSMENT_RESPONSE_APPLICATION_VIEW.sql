SELECT *
FROM
  `pcb-{env}-landing.domain_fraud_risk_intelligence.FRAUD_RISK_ASSESSMENT_RESPONSE`
WHERE UPPER(OriginatedFlow) LIKE 'APPLICATION%'
