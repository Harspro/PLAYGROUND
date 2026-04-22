CREATE OR REPLACE VIEW `pcb-{env}-processing.domain_consent.cnst_events_analytics_v6_tdc_join` AS
SELECT
  v6.*,
  tdc.ftf_ind AS facetofaceind
FROM
  `pcb-{env}-processing.domain_consent.cnst_events_analytics_v5_sig_join` v6
LEFT JOIN
  `pcb-{env}-landing.domain_customer_acquisition.TU_DIRECTORY_CHECK` tdc 
ON 
  tdc.source_cd = v6.source_cd AND tdc.app_type = v6.app_type