SELECT
  correlationId,
  alertType,
  requestType,
  identifiers,
  publish_time
FROM
  pcb-{env}-landing.domain_communication.TSYS_COMMS_EVENT
WHERE cast(publish_time as date) > '1900-01-01'