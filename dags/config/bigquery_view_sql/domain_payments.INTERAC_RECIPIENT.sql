SELECT
  customerId AS CustomerId,
  interacRecipientId AS InteracRecipientId,
  recipientId AS InteracIssuedContactId,
  name AS InteracRegistrationName,
  phoneNumber AS PhoneNumber,
  emailAddress AS EmailAddress,
  language AS Language,
  active AS Active,
  oneTimeContact AS OneTimeContact,
  DATETIME(TIMESTAMP(INGESTION_TIMESTAMP), 'America/Toronto') AS RECORD_LOAD_TIMESTAMP,
FROM
  pcb-{env}-landing.domain_payments.INTERAC_RECIPIENT_CHANGED
