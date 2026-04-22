EXPORT DATA OPTIONS (
  uri = (
    FORMAT(
      'gs://communications-inbound-pcb{deploy_env_storage_suffix}/c2p_privacy_update_notification/pcb_int_comms_c2p_privacy_update_notification_%s-*.csv',
      FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP())
    )
  ),
    format = 'CSV',
    field_delimiter = '|',
    overwrite = true,
    header = true
  ) AS
SELECT
  'CLICK-TO-PAY-PRIVACY-POLICY-UPDATE' AS communicationCode,
  GENERATE_UUID() AS uniqueId,
  'CLICK-TO-PAY' AS source,
  '' AS sourceIdempotencyKey,
  UPPER(CUSTOMER_PREFERENCE_PARAM.value) AS language,
  '' AS contactEmail,
  '' AS contactPhone,
  '' AS alternatePhone,
  '' AS pushToken,
  '' AS channels,
  '' AS domainCode,
  '' AS tenant,
  CURRENT_DATE('America/Toronto') AS createdDt,
  '' AS pcfCustomerId,
  '' AS accountId,
  C2P_ELIGIBLE_CUSTOMERS.customer_uid AS customerId,
  '' AS accountNumber,
  '' AS customerNumber,
  '' AS productCode,
  '' AS lastDigits,
  '' AS recipientTimezone,
  '' AS customerRole,
  TO_JSON_STRING([ STRUCT( 'dpEnrolledInd' AS paramName,
      CAST(dp_enrolled_ind AS STRING) AS paramValue,
      'STRING' AS paramType )]) AS parameters
FROM
  pcb-{env}-curated.domain_account_management.C2P_ELIGIBLE_CUSTOMERS
LEFT JOIN
  pcb-{env}-curated.domain_customer_management.CUSTOMER_PREFERENCE
ON
  C2P_ELIGIBLE_CUSTOMERS.customer_uid = CUSTOMER_PREFERENCE.customer_uid
  AND UPPER(CUSTOMER_PREFERENCE.code)='LANGUAGE'
LEFT JOIN
  pcb-{env}-curated.domain_customer_management.CUSTOMER_PREFERENCE_PARAM
ON
  CUSTOMER_PREFERENCE_PARAM.CUSTOMER_PREFERENCE_UID = CUSTOMER_PREFERENCE.CUSTOMER_PREFERENCE_UID
WHERE
  seq_no BETWEEN {start_seq_no}
  AND {end_seq_no};