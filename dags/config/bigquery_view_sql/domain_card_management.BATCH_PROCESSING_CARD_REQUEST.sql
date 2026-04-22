with MAPPING as (
    select MAP.cardEmbossingRequestId, MAP.cardNumber, MAP.panSeqNumber, MAP.embossingRequestFlow, MAP.fileUuid as FILE_UUID
    from pcb-{env}-landing.domain_card_management.EMBOSSING_TERMINUS_MAPPING MAP
),
     FILE_AUD as (
         select FILE_UUID, FILE_NAME, JOB_NAME, TRLR_CNT as TOTAL_FILE_RECORDS_COUNT, STATUS, REC_LOAD_TIMESTAMP
         from pcb-{env}-landing.domain_card_management.CARD_EMBOSSING_FILE_AUDIT_LOG
         where FILE_UUID<>'8a72eef3-1992-4f9f-bbed-54f6627d3af1' --Exclude the corrupted file due to PIN BLOCK on July 8th GO LIVE DATE
     ),
     ALL_BATCH_RECORDS as (
         select GEN_ACCT_IDENTIFIER, GEN_ACCT_NBR_ON_CARD as CARD_NUMBER, GEN_PAN_SEQ_NBR, FILE_UUID, RECORD_NUM, ISSUE_TYPE, REC_LOAD_TIMESTAMP, GEN_REQUEST_TYPE
         from pcb-{env}-landing.domain_card_management.EMBOSSING
         union all
         select GEN_ACCT_IDENTIFIER, GEN_ACCT_NBR_ON_CARD as CARD_NUMBER, GEN_PAN_SEQ_NBR, FILE_UUID, RECORD_NUM, ISSUE_TYPE, REC_LOAD_TIMESTAMP, GEN_REQUEST_TYPE
         from pcb-{env}-landing.domain_card_management.EMBOSSING_RUSH
         union all
         select GEN_ACCT_IDENTIFIER, GEN_ACCT_NBR_ON_CARD as CARD_NUMBER, GEN_PAN_SEQ_NBR, FILE_UUID, RECORD_NUM, ISSUE_TYPE, REC_LOAD_TIMESTAMP, GEN_REQUEST_TYPE
         from pcb-{env}-landing.domain_card_management.EMBOSSING_REISSUE
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

     REPORT as (
         select AUD.FILE_UUID,
                AUD.REC_LOAD_TIMESTAMP,
                AUD.JOB_NAME,
                AUD.TOTAL_FILE_RECORDS_COUNT,
                MAP.cardEmbossingRequestId,
                MAP.cardNumber as CARD_NUMBER,
                MAP.panSeqNumber                                                             as PSN,
                PEND.title                                                                   as pending_status,
                PEND.OCCURED_ON                                                              as pending_dt,
                SUB.title                                                                    as submitted_status,
                SUB.OCCURED_ON                                                               as SUBMITTED_GND_DT,
                PROC.title                                                                   as processed_status,
                PROC.OCCURED_ON                                                              as PROCESSED_GND_DT,
                SHIPPED.title                                                                as shipped_status,
                SHIPPED.OCCURED_ON                                                           as SHIPPED_GND_DT,
                CARDS.GEN_ACCT_IDENTIFIER,
                CARDS.ISSUE_TYPE,
                CARDS.GEN_REQUEST_TYPE
         from FILE_AUD AUD
          left join ALL_BATCH_RECORDS CARDS on CARDS.FILE_UUID = AUD.FILE_UUID
          left join MAPPING MAP on MAP.cardNumber = CARDS.CARD_NUMBER and MAP.panSeqNumber = CARDS.GEN_PAN_SEQ_NBR
          left join EMBOSSING_REQUEST_CREATE_DT PEND on PEND.cardEmbossingRequestId=MAP.cardEmbossingRequestId and PEND.rn=1
          left join SUBMITTED_TO_GND SUB on SUB.cardEmbossingRequestId = MAP.cardEmbossingRequestId and SUB.rn = 1
          left join PROCESSED_GND PROC on PROC.cardEmbossingRequestId = MAP.cardEmbossingRequestId and PROC.rn = 1
          left join SHIPPED_GND SHIPPED on SHIPPED.cardEmbossingRequestId=MAP.cardEmbossingRequestId and SHIPPED.rn=1
     )
select *
from REPORT
order by REC_LOAD_TIMESTAMP desc;