with CDA as (
  select
    RD_HD_ACCOUNT_ID,
    SUBSTR(RD_HD_ACCOUNT_ID, 5,11) AS ACCOUNT_NO,
    RD_FO_ACTION_6,
    IF(SAFE_CAST(RD_FO_ACTION_6 as NUMERIC) < 0, NULL, SAFE_CAST(RD_FO_ACTION_6 as NUMERIC)) as INT_ACTION_6,
    RD_FO_ACTION_7,
    IF(SAFE_CAST(RD_FO_ACTION_7 as NUMERIC) < 0, NULL, SAFE_CAST(RD_FO_ACTION_7 as NUMERIC)) as INT_ACTION_7,
  from `{cda_table_name}`
  where FILE_CREATE_DT = CAST('{file_create_dt}' AS DATE)
    and (LENGTH(RD_FO_ACTION_6) > 0 or LENGTH(RD_FO_ACTION_7) > 0)
),
ACC as (
  select
    ACCOUNT_NO,
    ACCOUNT_UID,
    PRODUCT_UID
  from
    `pcb-{env}-landing.domain_account_management.ACCOUNT`
),
PROD as (
  select
    PRODUCT_UID,
    CASE TYPE
      WHEN 'CREDIT-CARD' THEN 'PCMC'
      ELSE 'PCMA'
    END AS TYPE
  from `pcb-{env}-landing.domain_account_management.PRODUCT`
),
PCMA as (
  select
    CDA.* ,
    ACC.ACCOUNT_UID,
    ACC.PRODUCT_UID
  from (
    CDA
    INNER JOIN ACC ON CDA.ACCOUNT_NO = ACC.ACCOUNT_NO
    INNER JOIN PROD ON ACC.PRODUCT_UID = PROD.PRODUCT_UID
  )
  where
    PROD.TYPE = 'PCMA'
),
ACUST as (
  select
    ACCOUNT_UID,
    CUSTOMER_UID
  from `pcb-{env}-landing.domain_account_management.ACCOUNT_CUSTOMER`
),
FINAL_JOINED as (
  select
    PCMA.*,
    ACUST.CUSTOMER_UID
  from (
    PCMA
    INNER JOIN ACUST ON PCMA.ACCOUNT_UID = ACUST.ACCOUNT_UID
  )
),
MAX_LIMITS_CUST as (
  select
    CUSTOMER_UID,
    MAX(INT_ACTION_6) AS DAILY,
    MAX(INT_ACTION_6) AS PER_REQUEST,
    MAX(INT_ACTION_7) AS MONTHLY
  from FINAL_JOINED
  group by CUSTOMER_UID
),
MAX_LIMITS_MELT as (
  select *
  from MAX_LIMITS_CUST
  UNPIVOT(maxLimit for periodType in (DAILY, PER_REQUEST, MONTHLY))
)
select 
  GENERATE_UUID() as eventId,
  SAFE_CAST(CUSTOMER_UID as STRING) as customerId,
  'EPUL' as fundsMoveType,
  IF(periodType = 'PER_REQUEST', 'PER-REQUEST', periodType) as periodType,
  SAFE_CAST(NULL as STRING) as minLimit,
  SAFE_CAST(maxLimit as STRING) as maxLimit,
  FORMAT_DATETIME("%Y-%m-%dT%H:%M:%E6S",CURRENT_DATETIME("America/Toronto")) as requestDate,
  SAFE_CAST(NULL as BOOL) as removeLimit,
  'TRIAD_LIMIT_SET' as reason
from
  MAX_LIMITS_MELT
  order by CUSTOMER_UID
