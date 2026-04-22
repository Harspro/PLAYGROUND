SELECT 
  `group` as marketingGroup, 
  channel, 
  contactType, 
  contact, 
  dns,
  INGESTION_TIMESTAMP
FROM 
  pcb-{env}-landing.domain_communication.DNS_UPDATED