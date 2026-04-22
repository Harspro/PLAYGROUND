SELECT
  id,
  email,
  timestamp,
  statusCode,
  message,
  attempt,
  bounceType,
  bounceClassification,
  requester,
  identifiers.code as code,
  identifiers.system as system,
  identifiers.uuid as uuid,
  externalEventId,
  externalMessageId,
  externalEventCode,
  externalTemplateVersion,
  other
FROM
  pcb-{env}-landing.domain_communication.EMAIL_STATUS