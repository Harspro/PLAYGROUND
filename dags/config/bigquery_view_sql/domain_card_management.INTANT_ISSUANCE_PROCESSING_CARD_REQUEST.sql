-- ALL INSTANT ISSUANCE REQUEST WITH STATUS
with MAPPING as (
    select MAP.cardEmbossingRequestId, MAP.cardNumber, MAP.panSeqNumber, MAP.embossingRequestFlow, MAP.instantIssuanceOrchestrationId
    from pcb-{env}-landing.domain_card_management.EMBOSSING_TERMINUS_MAPPING MAP
    where MAP.embossingRequestFlow='INSTANT_ISSUANCE'
),
     INSTANT_ISSUANCE as (
         select
             MAP.*,
             DATETIME(II_XML1.createDate, 'America/New_York') as XML1_CREATE_DT,
             DATETIME(II_XML2.createDate, 'America/New_York') as XML2_CREATE_DT,
             DATETIME(II_REPLACEMENT.createDate, 'America/New_York') as REPLACEMENT_DT
         from MAPPING MAP
                  left join pcb-{env}-landing.domain_card_management.INSTANT_ISSUANCE_ORCHESTRATION II_XML1 on MAP.instantIssuanceOrchestrationId=II_XML1.orchestrationId and II_XML1.orchestrationEventName='REQUESTED_CARD_DETAILS'
                  left join pcb-{env}-landing.domain_card_management.INSTANT_ISSUANCE_ORCHESTRATION II_XML2 on MAP.instantIssuanceOrchestrationId=II_XML2.orchestrationId and II_XML2.orchestrationEventName='CUSTOMER_INFO_FOR_REQUESTED_CARD'
                  left join pcb-{env}-landing.domain_card_management.INSTANT_ISSUANCE_ORCHESTRATION II_REPLACEMENT on MAP.instantIssuanceOrchestrationId=II_REPLACEMENT.orchestrationId and II_REPLACEMENT.orchestrationEventName='REPLACEMENT'
     ),
     EMBOSSING_REQUEST_CREATE_DT as (
         select eventId, transactionId, SAFE_CAST(requestId as int64) as cardEmbossingRequestId, domain, title,
                case
                    when domain='pcb' then DATETIME(ST.occurredOn, 'America/New_York')
                    else TIMESTAMP_ADD(DATETIME(ST.occurredOn, 'America/New_York'), INTERVAL 4 HOUR) --GND NOTIFICATION TIMESTAMP NOT UTC FIX
                    end as OCCURED_ON,
                ROW_NUMBER() over (partition by requestId order by occurredOn desc) as rn
         from pcb-{env}-landing.domain_card_management.CARD_EMBOSSING_REQUEST_STATUS ST
         where ST.title='PENDING'
     ),
     SUBMITTED_TO_GND as (
         select eventId, transactionId, SAFE_CAST(requestId as int64) as cardEmbossingRequestId, domain, title,
                case
                    when domain='pcb' then DATETIME(ST.occurredOn, 'America/New_York')
                    else TIMESTAMP_ADD(DATETIME(ST.occurredOn, 'America/New_York'), INTERVAL 4 HOUR) --GND NOTIFICATION TIMESTAMP NOT UTC FIX
                    end as OCCURED_ON,
                ROW_NUMBER() over (partition by requestId order by occurredOn desc) as rn
         from pcb-{env}-landing.domain_card_management.CARD_EMBOSSING_REQUEST_STATUS ST
         where ST.title='card_submitted'
     ),
     PROCESSED_GND as (
         select eventId, transactionId, SAFE_CAST(requestId as int64) as cardEmbossingRequestId, domain, title,
                case
                    when domain='pcb' then DATETIME(ST.occurredOn, 'America/New_York')
                    else TIMESTAMP_ADD(DATETIME(ST.occurredOn, 'America/New_York'), INTERVAL 4 HOUR) --GND NOTIFICATION TIMESTAMP NOT UTC FIX
                    end as OCCURED_ON,
                ROW_NUMBER() over (partition by requestId order by occurredOn desc) as rn
         from pcb-{env}-landing.domain_card_management.CARD_EMBOSSING_REQUEST_STATUS ST
         where ST.title='proc.processed'
     ),
     SHIPPED_GND as (
         select eventId, transactionId, SAFE_CAST(requestId as int64) as cardEmbossingRequestId, domain, title,
                case
                    when domain='pcb' then DATETIME(ST.occurredOn, 'America/New_York')
                    else TIMESTAMP_ADD(DATETIME(ST.occurredOn, 'America/New_York'), INTERVAL 4 HOUR) --GND NOTIFICATION TIMESTAMP NOT UTC FIX
                    end as OCCURED_ON,
                ROW_NUMBER() over (partition by requestId order by occurredOn desc) as rn
         from pcb-{env}-landing.domain_card_management.CARD_EMBOSSING_REQUEST_STATUS ST
         where ST.title='ops.shipped'
     ),
     REJECTED_GND as (
         select eventId, transactionId, SAFE_CAST(requestId as int64) as cardEmbossingRequestId, domain, title,
                case
                    when domain='pcb' then DATETIME(ST.occurredOn, 'America/New_York')
                    else TIMESTAMP_ADD(DATETIME(ST.occurredOn, 'America/New_York'), INTERVAL 4 HOUR) --GND NOTIFICATION TIMESTAMP NOT UTC FIX
                    end as OCCURED_ON,
                ROW_NUMBER() over (partition by requestId order by occurredOn desc) as rn
         from pcb-{env}-landing.domain_card_management.CARD_EMBOSSING_REQUEST_STATUS ST
         where ST.title='proc.rejected'
     ),
     REPORT as (
         select
             II.embossingRequestFlow,
             II.cardEmbossingRequestId,
             II.instantIssuanceOrchestrationId,
             II.XML1_CREATE_DT, II.XML2_CREATE_DT, II.REPLACEMENT_DT,
             II.cardNumber as CARD_NUMBER,
             II.panSeqNumber                                                             as PSN,
             PEND.title                                                                   as pending_status,
             PEND.OCCURED_ON                                                              as pending_dt,
             SUB.title                                                                    as submitted_status,
             SUB.OCCURED_ON                                                               as SUBMITTED_GND_DT,
             PROC.title                                                                   as processed_status,
             PROC.OCCURED_ON                                                              as PROCESSED_GND_DT,
             REJ.title                                                                    as rejected_status,
             REJ.OCCURED_ON                                                               as REJECTED_GND_DT,
             SHIPPED.title                                                                as shipped_status,
             SHIPPED.OCCURED_ON                                                           as SHIPPED_GND_DT
         from INSTANT_ISSUANCE II
          left join EMBOSSING_REQUEST_CREATE_DT PEND on PEND.cardEmbossingRequestId=II.cardEmbossingRequestId and PEND.rn=1
          left join SUBMITTED_TO_GND SUB on SUB.cardEmbossingRequestId = II.cardEmbossingRequestId and SUB.rn = 1
          left join PROCESSED_GND PROC on PROC.cardEmbossingRequestId = II.cardEmbossingRequestId and PROC.rn = 1
          left join REJECTED_GND REJ on REJ.cardEmbossingRequestId = II.cardEmbossingRequestId and REJ.rn = 1
          left join SHIPPED_GND SHIPPED on SHIPPED.cardEmbossingRequestId=II.cardEmbossingRequestId and SHIPPED.rn=1
     )
select *
from REPORT
order by pending_dt desc;