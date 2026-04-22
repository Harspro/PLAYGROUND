--LIST ALL STATUS FOR REQUEST ID
WITH ALL_STATUSES_FOR_REQUEST_ID as (
    select SAFE_CAST(requestId as int64) as REQUEST_ID,
           ST.domain, ST.title,
           ST.eventId,
           ST.transactionId,
           case
               when domain='pcb' then ST.occurredOn
               else TIMESTAMP_ADD(ST.occurredOn, INTERVAL 4 HOUR) --GND NOTIFICATION TIMESTAMP NOT UTC FIX
               end as OCCURED_ON
    from pcb-{env}-landing.domain_card_management.CARD_EMBOSSING_REQUEST_STATUS ST
)
select * from ALL_STATUSES_FOR_REQUEST_ID
order by REQUEST_ID desc, OCCURED_ON;