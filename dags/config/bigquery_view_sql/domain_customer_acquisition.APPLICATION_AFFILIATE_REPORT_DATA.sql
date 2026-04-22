SELECT
  clickId,
  affiliate,
  admApplicationNumber,
  isApplication,
  applicationDate,
  isApproved,
  approvalDate,
  isActivated,
  activatedDate,
  revenueAmount,
  productID,
  message_id,
  subscription_name,
  attributes,
  DATETIME(publish_time, 'America/Toronto') AS publish_time,
  data,
  promoCode,
  INGESTION_TIMESTAMP
FROM
  `pcb-{env}-landing.domain_customer_acquisition.APPLICATION_AFFILIATE_REPORT_DATA`
QUALIFY
  ROW_NUMBER()
    OVER (
      PARTITION BY clickId, affiliate, admApplicationNumber
      ORDER BY INGESTION_TIMESTAMP DESC
    )
  = 1
