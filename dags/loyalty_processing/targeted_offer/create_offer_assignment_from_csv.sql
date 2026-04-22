CREATE OR REPLACE EXTERNAL TABLE `pcb-{env}-processing.domain_loyalty.PC_TARGETED_OFFER_FULFILLMENT_EXT`
(
  tsysAccountId STRING,
  updateType STRING,
  offerFulfillmentId INT64,
  accountId INT64,
  offerTriggerId INT64,
  periodStartTimestamp STRING,
  periodEndTimestamp STRING,
  trackerType STRING,
  status STRING,
  currentValue STRING,
  targetValue STRING,
  points INT64,
  fulfilledTimestamp STRING,
  fulfilledTransactionId STRING,
  createTimestamp STRING,
  createUserId STRING,
  updateTimestamp STRING,
  updateUserId STRING,
  eventType STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://pcb-{env}-staging-loyalty/Offer_assignment.csv'],
  skip_leading_rows = 1,
  field_delimiter = ','
)