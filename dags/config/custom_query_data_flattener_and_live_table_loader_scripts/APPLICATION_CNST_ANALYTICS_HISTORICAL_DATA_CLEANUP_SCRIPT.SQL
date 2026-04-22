delete from pcb-{env}-landing.domain_consent.CNST_EVENTS_ANALYTICS where cnstEventId in (select SAFE_CAST(cnstEventId as STRING) from pcb-{env}-processing.domain_consent.CNST_EVENTS_ANALYTICS_DBEXT);


     