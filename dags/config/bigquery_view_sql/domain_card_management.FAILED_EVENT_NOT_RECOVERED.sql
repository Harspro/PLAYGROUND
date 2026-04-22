--LIST ALL FAILED EVENTS NOT YET RECOVERED
WITH ALL_FAILED_EVENTS_NOT_YET_RECOVERED as (
    select reprocessingEventId
    from pcb-{env}-landing.domain_card_management.EMBOSSING_REPROCESSING_EVENT PAR_FAIL
    where not exists (
        select reprocessingEventId
        from pcb-{env}-landing.domain_card_management.EMBOSSING_REPROCESSING_EVENT RECOVERY
        where PAR_FAIL.reprocessingEventId=RECOVERY.reprocessingEventId and RECOVERY.status='SUCCESSFUL'
    )
),
     RANKED_FAILED_EVENT as (
         select EVT.reprocessingEventId,
                ARRAY_REVERSE(SPLIT(avroSchema, '.'))[OFFSET(0)] AS avroSchema,
                ARRAY_REVERSE(SPLIT(exceptionClass, '.'))[OFFSET(0)] AS exceptionClass,
                EVT.traceabilityId,
                DATETIME(EVT.INGESTION_TIMESTAMP, 'America/New_York') as OCCURED_ON, status, requestId,
--                 SUBSTR(MAP.cardNumber, 0, 4) || '********' || SUBSTR(MAP.cardNumber, 13,16) as MASKED_CARD_NUMBER,
                MAP.cardNumber as CARD_NUMBER,
                DATETIME(MAP.INGESTION_TIMESTAMP, 'America/New_York') as CARD_CREATED_DT,
                MAP.embossingRequestFlow,
                EVT.failureReason,
                ROW_NUMBER() over (partition by EVT.reprocessingEventId ORDER BY EVT.INGESTION_TIMESTAMP desc) as rn
         from pcb-{env}-landing.domain_card_management.EMBOSSING_REPROCESSING_EVENT EVT
                  left join pcb-{env}-landing.domain_card_management.EMBOSSING_TERMINUS_MAPPING MAP on MAP.cardEmbossingRequestId=requestId
         where exists (
             select reprocessingEventId from ALL_FAILED_EVENTS_NOT_YET_RECOVERED FAIL where FAIL.reprocessingEventId=EVT.reprocessingEventId
         )
         order by 1, 5
     )
select *
from RANKED_FAILED_EVENT
where rn=1
;