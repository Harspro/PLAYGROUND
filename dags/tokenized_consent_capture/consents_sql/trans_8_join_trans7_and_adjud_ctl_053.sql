CREATE OR REPLACE VIEW `pcb-{env}-processing.domain_consent.cnst_events_analytics_v4_adj_join` AS
SELECT
  v4.*,
  CASE
  WHEN aac.vendor_desc IS NOT NULL AND aac.channel_desc IS NOT NULL THEN CONCAT(aac.vendor_desc, " --> ", aac.CHANNEL_DESC)
  WHEN aac.channel_desc IS NULL AND aac.vendor_desc IS NOT NULL THEN aac.vendor_desc
  WHEN aac.channel_desc IS NULL AND aac.vendor_desc IS NULL THEN CONCAT(aac.source_cd, " --> ", aac.APP_TYPE)
  ELSE NULL
  END AS channel
FROM
  `pcb-{env}-processing.domain_consent.cnst_events_analytics_v3_reg_join` v4
LEFT JOIN
  `pcb-{env}-landing.domain_customer_acquisition.APP_ADJUDICATION_CTL` aac
ON
  v4.source_cd = aac.source_cd AND v4.app_type = aac.app_type