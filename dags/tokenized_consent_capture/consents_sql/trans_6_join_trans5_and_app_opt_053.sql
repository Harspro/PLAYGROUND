CREATE OR REPLACE VIEW `pcb-{env}-processing.domain_consent.cnst_events_analytics_v2_aop_join` AS
SELECT
  v2.*,
  aop.prod_desc as eventtype
FROM
  `pcb-{env}-processing.domain_consent.cnst_events_analytics_v1_aopx_join` v2
LEFT JOIN
  `pcb-{env}-landing.domain_customer_acquisition.APP_OPTIONAL_PROD` aop
ON
  v2.prod_id = aop.prod_id