UPDATE pcb-{env}-landing.domain_consent.CNST_EVENTS_ANALYTICS evt_antys
SET documentStorageLink = enc_sig.UUID
FROM pcb-{env}-landing.domain_consent.CNST_ENCRYPTED_SIGNATURE enc_sig
WHERE evt_antys.cnstEventId=enc_sig.eventId