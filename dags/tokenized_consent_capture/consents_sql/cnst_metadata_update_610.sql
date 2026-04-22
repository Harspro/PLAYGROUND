CREATE OR REPLACE TABLE `{staging_table_id}`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
) AS
SELECT
  signatureFile AS signature_id,
  documentStorageLink AS document_id,
  cnstEventId As cnst_event_id
FROM `pcb-{env}-landing.domain_consent.CNST_EVENTS_ANALYTICS` AS enc
LIMIT 0;

INSERT INTO `{staging_table_id}`
(signature_id,cnst_event_id, document_id)
SELECT
  enc.signatureId,
  evt.cnstEventId,
  evt.documentStorageLink
FROM  `pcb-{env}-landing.domain_consent.CNST_EVENTS_ANALYTICS` AS evt
JOIN `pcb-{env}-landing.domain_consent.CNST_ENCRYPTED_SIGNATURE` AS enc
  ON evt.signatureFile = enc.signatureId
WHERE enc.source = '610'and evt.signatureFile is not null and evt.cnstEventId is not null and evt.documentStorageLink is not null;