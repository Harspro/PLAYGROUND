INSERT INTO `pcb-{env}-landing.domain_loyalty.PC_TARGETED_OFFER`  (
  targetedOfferId, offerCode, offerStartDate, offerEndDate, ticket, lifecycleEvent,
  status, promoCodes, sourceCodes, storeIds, productType, billC86, billC86Days, 
  extCampaignId, isAlwaysOn, offerDescription, createTimestamp, createUserId, 
  updateTimestamp, updateUserId, offerTriggers, eventType
)
VALUES 
(
  1,                                                                          -- targetedOfferId (INTEGER)
  'PDSB-5815-N150K',                                                          -- offerCode (STRING)
  DATE '2025-01-16',                                                          -- offerStartDate (DATE)
  DATE '2025-12-31',                                                          -- offerEndDate (DATE)
  'PDSB-5815',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  '90,977,910,291,152,500,000,000,000,000,000,000,000,000,000,000,000',       -- promoCodes (STRING or NULL)
  'INT,DPL',                                                                  -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMA',                                                                     -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  TRUE,                                                                       -- isAlwaysOn (BOOLEAN)
  'PDSB-5815-N150K',                                                          -- offerDescription (STRING)
  '2025-05-26 18:50:43.058372+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      13,
      'PDSB-5815',
      'FTP',
      'DIR_DEPO',
      'AMA_PYRLL2',
      TRUE,
      NULL,
      NULL,
      '1500.00',
      150000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  2,                                                                          -- targetedOfferId (INTEGER)
  'PDSB-5813-N100K',                                                          -- offerCode (STRING)
  DATE '2025-01-23',                                                          -- offerStartDate (DATE)
  DATE '2025-12-31',                                                          -- offerEndDate (DATE)
  'PDSB-5813',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  NULL,                                                                       -- promoCodes (STRING or NULL)
  NULL,                                                                       -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMA',                                                                     -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  TRUE,                                                                       -- isAlwaysOn (BOOLEAN)
  'PDSB-5813-N100K',                                                          -- offerDescription (STRING)
  '2025-05-26 18:50:43.058372+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      12,
      'PDSB-5813',
      'FTP',
      'DIR_DEPO',
      'AMA_PYRLL2',
      TRUE,
      NULL,
      NULL,
      '1500.00',
      100000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  3,                                                                          -- targetedOfferId (INTEGER)
  'PDSB-3417-N100K',                                                          -- offerCode (STRING)
  DATE '2024-12-31',                                                          -- offerStartDate (DATE)
  DATE '2025-12-31',                                                          -- offerEndDate (DATE)
  'PDSB-3417',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  '11528',                                                                    -- promoCodes (STRING or NULL)
  'INT,DPL',                                                                  -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCNF',                                                                   -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  TRUE,                                                                       -- isAlwaysOn (BOOLEAN)
  'PDSB-3417-N100K',                                                          -- offerDescription (STRING)
  '2025-05-26 18:50:43.058372+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      1,
      'PDSB-3417',
      'FTP',
      'ESSO_170_L',
      'ESSO_170_L',
      FALSE,
      NULL,
      90,
      NULL,
      0,
      NULL,
      NULL,
      NULL,
      '400.00',
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  4,                                                                          -- targetedOfferId (INTEGER)
  'PDSB-4353-N125K',                                                          -- offerCode (STRING)
  DATE '2025-01-01',                                                          -- offerStartDate (DATE)
  DATE '2025-12-31',                                                          -- offerEndDate (DATE)
  'PDSB-4353',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  '90949',                                                                    -- promoCodes (STRING or NULL)
  'INT,DPL',                                                                  -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCNF',                                                                   -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  TRUE,                                                                       -- isAlwaysOn (BOOLEAN)
  'PDSB-4353-N125K',                                                          -- offerDescription (STRING)
  '2025-05-26 18:50:43.058372+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      2,
      'PDSB-4353',
      'FTP',
      'SDM_SPND',
      'SDM_SPND',
      TRUE,
      NULL,
      NULL,
      '250.00',
      125000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  5,                                                                          -- targetedOfferId (INTEGER)
  'PDSB-5349-N150K',                                                          -- offerCode (STRING)
  DATE '2025-01-01',                                                          -- offerStartDate (DATE)
  DATE '2025-12-31',                                                          -- offerEndDate (DATE)
  'PDSB-5349',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  '11,533,911,539,115,600,000',                                               -- promoCodes (STRING or NULL)
  'INT,DPL',                                                                  -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCNF',                                                                   -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  TRUE,                                                                       -- isAlwaysOn (BOOLEAN)
  'PDSB-5349-N150K',                                                          -- offerDescription (STRING)
  '2025-05-26 18:50:43.058372+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      9,
      'PDSB-5349',
      'FTP',
      'SDM_SPND',
      'SDM_SPND',
      TRUE,
      NULL,
      NULL,
      '250.00',
      150000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  6,                                                                          -- targetedOfferId (INTEGER)
  'PDSB-6000-N125K',                                                          -- offerCode (STRING)
  DATE '2025-05-13',                                                          -- offerStartDate (DATE)
  DATE '2025-12-31',                                                          -- offerEndDate (DATE)
  'PDSB-6000',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  '11,599,908,759,112,700,000',                                               -- promoCodes (STRING or NULL)
  'INT,DPL',                                                                  -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCNF',                                                                   -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  FALSE,                                                                      -- isAlwaysOn (BOOLEAN)
  'PDSB-6000-N125K',                                                          -- offerDescription (STRING)
  '2025-05-26 18:50:43.058372+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      10,
      'PDSB-6000',
      'FTP',
      'LCL_FP',
      'ACQ_LCLFP',
      FALSE,
      NULL,
      NULL,
      NULL,
      105000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  7,                                                                          -- targetedOfferId (INTEGER)
  'PDSB-6455-P40K',                                                           -- offerCode (STRING)
  DATE '2025-02-24',                                                          -- offerStartDate (DATE)
  DATE '2025-06-15',                                                          -- offerEndDate (DATE)
  'PDSB-6455',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  NULL,                                                                       -- promoCodes (STRING or NULL)
  'LNC,LNQ',                                                                  -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCNF',                                                                   -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  FALSE,                                                                      -- isAlwaysOn (BOOLEAN)
  'PDSB-6455-P40K',                                                           -- offerDescription (STRING)
  '2025-05-26 18:50:43.058372+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      15,
      'PDSB-6455',
      'FTP',
      'ESLCL_SPND',
      'ACQ_LCL_M',
      TRUE,
      NULL,
      NULL,
      '40.00',
      40000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
        NULL,
        NULL,
        'ADD'
      )
    ],
  'ADD'                             -- eventType (STRING)
),
(
  8,                                                                          -- targetedOfferId (INTEGER)
  'PDSB-5181-N300K',                                                          -- offerCode (STRING)
  DATE '2025-01-01',                                                          -- offerStartDate (DATE)
  DATE '2025-12-31',                                                          -- offerEndDate (DATE)
  'PDSB-5181',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  '91148',                                                                    -- promoCodes (STRING or NULL)
  'INT,DPL',                                                                  -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCF',                                                                    -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  TRUE,                                                                       -- isAlwaysOn (BOOLEAN)
  'PDSB-5181-N300K',                                                          -- offerDescription (STRING)
  '2025-05-26 18:50:43.058372+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      3,
      'PDSB-5181',
      'FTP',
      'IWE_SP150',
      'IWE_SP150',
      TRUE,
      NULL,
      NULL,
      NULL,
      150000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    ),
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      4,
      'PDSB-5181',
      'FTP',
      'IWE_SP50',
      'IWE_SP50',
      TRUE,
      NULL,
      NULL,
      NULL,
      150000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  9,                                                                          -- targetedOfferId (INTEGER)
  'PDSB-5182-N300K',                                                          -- offerCode (STRING)
  DATE '2025-01-01',                                                          -- offerStartDate (DATE)
  DATE '2025-12-31',                                                          -- offerEndDate (DATE)
  'PDSB-5182',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  '91150',                                                                    -- promoCodes (STRING or NULL)
  'INT,DPL',                                                                  -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCF',                                                                    -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  TRUE,                                                                       -- isAlwaysOn (BOOLEAN)
  'PDSB-5182-N300K',                                                          -- offerDescription (STRING)
  '2025-05-26 18:50:43.058372+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      5,
      'PDSB-5182',
      'FTP',
      'IWE_SP150',
      'IWE_SP150',
      TRUE,
      NULL,
      NULL,
      NULL,
      150000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    ),
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      6,
      'PDSB-5182',
      'FTP',
      'IWE_SP50',
      'IWE_SP50',
      TRUE,
      NULL,
      NULL,
      NULL,
      150000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  10,                                                                         -- targetedOfferId (INTEGER)
  'PDSB-5183-N300K',                                                          -- offerCode (STRING)
  DATE '2024-12-31',                                                          -- offerStartDate (DATE)
  DATE '2025-12-31',                                                          -- offerEndDate (DATE)
  'PDSB-5183',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  '91067',                                                                    -- promoCodes (STRING or NULL)
  'INT,DPL',                                                                  -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCF',                                                                    -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  TRUE,                                                                       -- isAlwaysOn (BOOLEAN)
  'PDSB-5183-N300K',                                                          -- offerDescription (STRING)
  '2025-05-26 18:50:43.058372+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      7,
      'PDSB-5183',
      'FTP',
      'IWE_SP150',
      'IWE_SP150',
      TRUE,
      NULL,
      NULL,
      NULL,
      150000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    ),
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      8,
      'PDSB-5183',
      'FTP',
      'IWE_SP50',
      'IWE_SP50',
      TRUE,
      NULL,
      NULL,
      NULL,
      150000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  11,                                                                         -- targetedOfferId (INTEGER)
  'PDSB-5638-N150K',                                                          -- offerCode (STRING)
  DATE '2025-02-15',                                                          -- offerStartDate (DATE)
  DATE '2025-12-31',                                                          -- offerEndDate (DATE)
  'PDSB-5638',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  '7,004,070,043',                                                            -- promoCodes (STRING or NULL)
  'INT,DPL',                                                                  -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCNF',                                                                   -- productType (STRING)
  TRUE,                                                                       -- billC86 (BOOLEAN)
  365,                                                                        -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  FALSE,                                                                      -- isAlwaysOn (BOOLEAN)
  'PDSB-5638-N150K',                                                          -- offerDescription (STRING)
  '2025-05-26 18:50:43.058372+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      11,
      'PDSB-5638',
      'FTP',
      'IWE_SP150',
      'IWE_SP150',
      TRUE,
      NULL,
      NULL,
      NULL,
      150000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  12,                                                                         -- targetedOfferId (INTEGER)
  'PDSB-6272-P25K',                                                           -- offerCode (STRING)
  DATE '2025-04-01',                                                          -- offerStartDate (DATE)
  DATE '2025-05-31',                                                          -- offerEndDate (DATE)
  'PDSB-6272',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  NULL,                                                                       -- promoCodes (STRING or NULL)
  'KOG',                                                                      -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCNF',                                                                   -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  FALSE,                                                                      -- isAlwaysOn (BOOLEAN)
  'PDSB-6272-P25K',                                                           -- offerDescription (STRING)
  '2025-05-26 18:50:43.058372+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      14,
      'PDSB-6272',
      'FTP',
      'ESLCL_SPND',
      'ACQ_LCL_M',
      TRUE,
      NULL,
      NULL,
      '25.00',
      25000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  13,                                                                         -- targetedOfferId (INTEGER)
  'PDSB-6465-N100K',                                                          -- offerCode (STRING)
  DATE '2025-05-03',                                                          -- offerStartDate (DATE)
  DATE '2025-05-16',                                                          -- offerEndDate (DATE)
  'PDSB-6465',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  '91027',                                                                    -- promoCodes (STRING or NULL)
  'INT,DPL',                                                                  -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCNF',                                                                   -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  FALSE,                                                                      -- isAlwaysOn (BOOLEAN)
  'PDSB-6465-N100K',                                                          -- offerDescription (STRING)
  '2025-05-26 18:50:43.058372+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      16,
      'PDSB-6465',
      'FTP',
      'LCL_FP',
      'ACQ_LCLFP',
      FALSE,
      NULL,
      NULL,
      NULL,
      80000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  14,                                                                         -- targetedOfferId (INTEGER)
  'PDSB-6484-P100K',                                                          -- offerCode (STRING)
  DATE '2025-04-01',                                                          -- offerStartDate (DATE)
  DATE '2025-12-31',                                                          -- offerEndDate (DATE)
  'PDSB-5183',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  NULL,                                                                       -- promoCodes (STRING or NULL)
  'KOG,LNQ,LNC',                                                              -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCF',                                                                    -- productType (STRING)
  TRUE,                                                                       -- billC86 (BOOLEAN)
  365,                                                                        -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  TRUE,                                                                       -- isAlwaysOn (BOOLEAN)
  'PDSB-6484-P100K',                                                          -- offerDescription (STRING)
  '2025-05-26 18:50:43.058372+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      17,
      'PDSB-6484',
      'FTP',
      'IWE_AU3M',
      'IWE_AU3M',
      FALSE,
      NULL,
      NULL,
      NULL,
      10000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    ),
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      18,
      'PDSB-6484',
      'FTP',
      'IWE_MW2M',
      'IWE_MW2M',
      FALSE,
      NULL,
      NULL,
      NULL,
      20000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    ),
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      19,
      'PDSB-6484',
      'FTP',
      'IWE_SP3M',
      'IWE_SP3M',
      FALSE,
      NULL,
      NULL,
      NULL,
      70000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  15,                                                                         -- targetedOfferId (INTEGER)
  'PDSB-6485-N100K',                                                          -- offerCode (STRING)
  DATE '2025-04-01',                                                          -- offerStartDate (DATE)
  DATE '2025-06-02',                                                          -- offerEndDate (DATE)
  'PDSB-6485',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  NULL,                                                                       -- promoCodes (STRING or NULL)
  'INT,DPL',                                                                  -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCF',                                                                    -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  TRUE,                                                                       -- isAlwaysOn (BOOLEAN)
  'PDSB-6485-N100K',                                                          -- offerDescription (STRING)
  '2025-05-26 18:50:43.058372+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      20,
      'PDSB-6485',
      'FTP',
      'IWE_SP3M',
      'IWE_SP3M',
      FALSE,
      NULL,
      NULL,
      NULL,
      70000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    ),
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      21,
      'PDSB-6485',
      'FTP',
      'IWE_SPAF3M',
      'IWE_SPAF3M',
      FALSE,
      NULL,
      NULL,
      NULL,
      30000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  16,                                                                         -- targetedOfferId (INTEGER)
  'PDSB-6534-N100K',                                                          -- offerCode (STRING)
  DATE '2025-04-01',                                                          -- offerStartDate (DATE)
  DATE '2025-12-31',                                                          -- offerEndDate (DATE)
  'PDSB-6534',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  '70074',                                                                    -- promoCodes (STRING or NULL)
  'INT,DPL',                                                                  -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCF',                                                                    -- productType (STRING)
  TRUE,                                                                       -- billC86 (BOOLEAN)
  365,                                                                        -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  TRUE,                                                                       -- isAlwaysOn (BOOLEAN)
  'PDSB-6534-N100K',                                                          -- offerDescription (STRING)
  '2025-05-26 18:50:43.058372+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      22,
      'PDSB-6534',
      'FTP',
      'IWE_SP3M',
      'IWE_SP3M',
      FALSE,
      NULL,
      NULL,
      NULL,
      70000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    ),
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      23,
      'PDSB-6534',
      'FTP',
      'IWE_SPAF3M',
      'IWE_SPAF3M',
      FALSE,
      NULL,
      NULL,
      NULL,
      30000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  17,                                                                         -- targetedOfferId (INTEGER)
  'WELM1_210301001',                                                          -- offerCode (STRING)
  DATE '2000-01-01',                                                          -- offerStartDate (DATE)
  DATE '2999-12-31',                                                          -- offerEndDate (DATE)
  'PCMCNF-WELCOME-000',                                                       -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  NULL,                                                                       -- promoCodes (STRING or NULL)
  NULL,                                                                       -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCNF',                                                                   -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  TRUE,                                                                       -- isAlwaysOn (BOOLEAN)
  'WELM1_210301001',                                                          -- offerDescription (STRING)
  '2025-05-26 18:50:43.058372+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      24,
      'PCMCNF-WELCOME-000',
      'FTP',
      'LCL_FP',
      'ACQ_LCLFP',
      TRUE,
      NULL,
      NULL,
      NULL,
      20000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-05-26 18:51:17.133389+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  18,                                                                         -- targetedOfferId (INTEGER)
  'PDSB-6724-P50K',                                                           -- offerCode (STRING)
  DATE '2025-05-21',                                                          -- offerStartDate (DATE)
  DATE '2025-12-31',                                                          -- offerEndDate (DATE)
  'PDSB-6724',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  NULL,                                                                       -- promoCodes (STRING or NULL)
  'KOG',                                                                      -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCNF',                                                                   -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  TRUE,                                                                       -- isAlwaysOn (BOOLEAN)
  'PDSB-6724-P50K',                                                           -- offerDescription (STRING)
  '2025-06-11 15:47:44.342121+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      34,
      'PDSB-6724',
      'FTP',
      'ACQ_SPD',
      'ACQ_SPD_L',
      TRUE,
      NULL,
      60,
      '200.00',
      20000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 15:50:41.001615+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    ),
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      35,
      'PDSB-6724',
      'FTP',
      'PCMCAUTH',
      'PCMCAUTH',
      TRUE,
      NULL,
      NULL,
      NULL,
      10000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 15:50:41.001615+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    ),
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      36,
      'PDSB-6724',
      'FTP',
      'PCMCMOBW',
      'PCMCMOBW',
      TRUE,
      NULL,
      60,
      NULL,
      20000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 15:50:41.001615+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  19,                                                                         -- targetedOfferId (INTEGER)
  'PDSB-6486-N125K',                                                          -- offerCode (STRING)
  DATE '2025-04-01',                                                          -- offerStartDate (DATE)
  DATE '2025-12-31',                                                          -- offerEndDate (DATE)
  'PDSB-6486',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  '7,000,770,034,700,350,000,000,000,000,000,000,000,000,' ||
  '000,000,000,000,000,000,000,000,000,000,000,000,000,000,' ||
  '000,000,000,000,000,000,000,000,000,000,000,000,000,000,' ||
  '000,000,000,000,000,000,000',                                              -- promoCodes (STRING or NULL)
  'INT,DPL',                                                                  -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCF',                                                                    -- productType (STRING)
  TRUE,                                                                       -- billC86 (BOOLEAN)
  365,                                                                        -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  TRUE,                                                                       -- isAlwaysOn (BOOLEAN)
  'PDSB-6486-N125K',                                                          -- offerDescription (STRING)
  '2025-06-11 15:47:44.342121+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      25,
      'PDSB-6486',
      'FTP',
      'IWE_MW2M',
      'IWE_MW2M',
      TRUE,
      NULL,
      NULL,
      NULL,
      10000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 15:50:41.001615+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    ),
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      26,
      'PDSB-6486',
      'FTP',
      'IWE_SP3M',
      'IWE_SP3M',
      TRUE,
      NULL,
      NULL,
      NULL,
      70000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 15:50:41.001615+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    ),
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      27,
      'PDSB-6486',
      'FTP',
      'IWE_SPAF3M',
      'IWE_SPAF3M',
      TRUE,
      NULL,
      NULL,
      NULL,
      45000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 15:50:41.001615+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  20,                                                                         -- targetedOfferId (INTEGER)
  'PDSB-6602-N100K',                                                          -- offerCode (STRING)
  DATE '2025-04-15',                                                          -- offerStartDate (DATE)
  DATE '2025-06-10',                                                          -- offerEndDate (DATE)
  'PDSB-6534',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  '9,115,211,534,993,240,000,000,000,000,000,000,000,' ||
  '000,000,000,000,000,000,000,000,000,000,000,000,000,' ||
  '000,000,000,000,000,000,000,000,000,000,000,000,000,' ||
  '000,000,000,000,000,000,000,000,000,000,000,000,000,000,000',              -- promoCodes (STRING or NULL)
  'INT,DPL',                                                                  -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCNF',                                                                   -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  FALSE,                                                                      -- isAlwaysOn (BOOLEAN)
  'PDSB-6602-N100K',                                                          -- offerDescription (STRING)
  '2025-06-11 15:47:44.342121+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      37,
      'PDSB-6602',
      'FTP',
      'ACQ_EN_FP',
      'ACQ_LCLFP',
      FALSE,
      NULL,
      NULL,
      '50.00',
      75000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 15:50:41.001615+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    ),
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      38,
      'PDSB-6602',
      'FTP',
      'ACQ_ES_FP',
      'ACQ_ES_FP',
      FALSE,
      NULL,
      NULL,
      '50.00',
      25000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 15:50:41.001615+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  21,                                                                         -- targetedOfferId (INTEGER)
  'PDSB-6819-N125K',                                                          -- offerCode (STRING)
  DATE '2025-06-11',                                                          -- offerStartDate (DATE)
  DATE '2025-06-10',                                                          -- offerEndDate (DATE)
  'PDSB-6819',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  NULL,                                                                       -- promoCodes (STRING or NULL)
  'INT,DPL',                                                                  -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCF',                                                                    -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  TRUE,                                                                       -- isAlwaysOn (BOOLEAN)
  'PDSB-6819-N125K',                                                          -- offerDescription (STRING)
  '2025-06-11 15:47:44.342121+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      39,
      'PDSB-6819',
      'FTP',
      'IWE_SP3M',
      'IWE_SP3M',
      FALSE,
      NULL,
      NULL,
      NULL,
      80000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 15:50:41.001615+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    ),
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      40,
      'PDSB-6819',
      'FTP',
      'IWE_SPAF3M',
      'IWE_SPAF3M',
      TRUE,
      NULL,
      NULL,
      NULL,
      150000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 15:50:41.001615+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  22,                                                                         -- targetedOfferId (INTEGER)
  'PDSB-6726-N100K',                                                          -- offerCode (STRING)
  DATE '2025-06-18',                                                          -- offerStartDate (DATE)
  DATE '2025-12-31',                                                          -- offerEndDate (DATE)
  'PDSB-6726',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  '90949',                                                                    -- promoCodes (STRING or NULL)
  'INT,DPL',                                                                  -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCNF',                                                                   -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  FALSE,                                                                      -- isAlwaysOn (BOOLEAN)
  'PDSB-6726-N100K',                                                          -- offerDescription (STRING)
  '2025-06-11 15:47:44.342121+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      41,
      'PDSB-6726',
      'FTP',
      'PCMCMOBW',
      'PCMCMOBW',
      TRUE,
      NULL,
      60,
      NULL,
      50000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 15:50:41.001615+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    ),
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      42,
      'PDSB-6726',
      'FTP',
      'SDM_20P',
      'ACQ_20PSDM',
      TRUE,
      NULL,
      60,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '250.00',
      NULL,
      NULL,
      NULL,
      '2025-06-11 15:50:41.001615+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  23,                                                                         -- targetedOfferId (INTEGER)
  'PDSB-6483-N150K',                                                          -- offerCode (STRING)
  DATE '2025-06-26',                                                          -- offerStartDate (DATE)
  DATE '2025-07-02',                                                          -- offerEndDate (DATE)
  'PDSB-6483',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  '7,000,970,006',                                                            -- promoCodes (STRING or NULL)
  'INT,DPL',                                                                  -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCF',                                                                    -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  FALSE,                                                                      -- isAlwaysOn (BOOLEAN)
  'PDSB-6483-N150K',                                                          -- offerDescription (STRING)
  '2025-06-11 15:47:44.342121+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      43,
      'PDSB-6483',
      'FTP',
      'IWE_SP3M',
      'IWE_SP3M',
      FALSE,
      NULL,
      NULL,
      NULL,
      75000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 15:50:41.001615+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    ),
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      44,
      'PDSB-6483',
      'FTP',
      'IWE_SPAF3M',
      'IWE_SPAF3M',
      TRUE,
      NULL,
      NULL,
      NULL,
      20000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 15:50:41.001615+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    ),
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      45,
      'PDSB-6483',
      'FTP',
      'IWE_MW2M',
      'IWE_MW2M',
      TRUE,
      NULL,
      NULL,
      NULL,
      15000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 15:50:41.001615+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  24,                                                                         -- targetedOfferId (INTEGER)
  'PDSB-6600-P50K',                                                           -- offerCode (STRING)
  DATE '2025-04-01',                                                          -- offerStartDate (DATE)
  DATE '2025-12-31',                                                          -- offerEndDate (DATE)
  'PDSB-6600',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  NULL,                                                                       -- promoCodes (STRING or NULL)
  'KOG,EDO',                                                                  -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCNF',                                                                   -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  TRUE,                                                                       -- isAlwaysOn (BOOLEAN)
  'PDSB-6600-P50K',                                                           -- offerDescription (STRING)
  '2025-06-11 15:49:22.208596+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      31,
      'PDSB-6600',
      'FTP',
      'ESSO_170_L',
      'ESSO_170_L',
      FALSE,
      NULL,
      90,
      NULL,
      0,
      NULL,
      NULL,
      NULL,
      '100.00',
      NULL,
      NULL,
      NULL,
      '2025-06-11 15:50:41.001615+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    ),
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      32,
      'PDSB-6600',
      'FTP',
      'IWE_AU3M',
      'IWE_AU3M',
      TRUE,
      NULL,
      NULL,
      NULL,
      10000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 15:50:41.001615+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    ),
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      33,
      'PDSB-6600',
      'FTP',
      'IWE_MW2M',
      'IWE_MW2M',
      TRUE,
      NULL,
      NULL,
      NULL,
      20000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 15:50:41.001615+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  25,                                                                         -- targetedOfferId (INTEGER)
  'PDSB-6597-P100K',                                                          -- offerCode (STRING)
  DATE '2025-04-01',                                                          -- offerStartDate (DATE)
  DATE '2025-12-31',                                                          -- offerEndDate (DATE)
  'PDSB-6597',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  NULL,                                                                       -- promoCodes (STRING or NULL)
  'KOG,LNQ,LNC',                                                              -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCF',                                                                    -- productType (STRING)
  TRUE,                                                                       -- billC86 (BOOLEAN)
  365,                                                                        -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  TRUE,                                                                       -- isAlwaysOn (BOOLEAN)
  'PDSB-6597-P100K',                                                          -- offerDescription (STRING)
  '2025-06-11 15:49:22.208596+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      28,
      'PDSB-6597',
      'FTP',
      'ESSO_170_L',
      'ESSO_170_L',
      FALSE,
      NULL,
      90,
      NULL,
      0,
      NULL,
      NULL,
      NULL,
      '350.00',
      NULL,
      NULL,
      NULL,
      '2025-06-11 15:50:41.001615+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    ),
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      29,
      'PDSB-6597',
      'FTP',
      'IWE_AU3M',
      'IWE_AU3M',
      TRUE,
      NULL,
      NULL,
      NULL,
      10000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 15:50:41.001615+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    ),
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      30,
      'PDSB-6597',
      'FTP',
      'IWE_MW2M',
      'IWE_MW2M',
      TRUE,
      NULL,
      NULL,
      NULL,
      20000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 15:50:41.001615+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  26,                                                                         -- targetedOfferId (INTEGER)
  'PDSB-6724-P50K_S',                                                         -- offerCode (STRING)
  DATE '2025-06-18',                                                          -- offerStartDate (DATE)
  DATE '2025-12-31',                                                          -- offerEndDate (DATE)
  'PDSB-6724',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  NULL,                                                                       -- promoCodes (STRING or NULL)
  'LNQ,LNC',                                                                  -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCNF',                                                                   -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  TRUE,                                                                       -- isAlwaysOn (BOOLEAN)
  'PDSB-6724-P50K_S',                                                         -- offerDescription (STRING)
  '2025-06-11 17:14:07.942151+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      46,
      'PDSB-6724',
      'FTP',
      'ACQ_SPD',
      'ACQ_SPD_L',
      TRUE,
      NULL,
      60,
      '200.00',
      20000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 17:14:37.600176+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    ),
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      47,
      'PDSB-6724',
      'FTP',
      'PCMCAUTH',
      'PCMCAUTH',
      TRUE,
      NULL,
      NULL,
      NULL,
      10000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 17:14:37.600176+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    ),
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      48,
      'PDSB-6724',
      'FTP',
      'PCMCMOBW',
      'PCMCMOBW',
      TRUE,
      NULL,
      60,
      NULL,
      20000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 17:14:37.600176+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  27,                                                                         -- targetedOfferId (INTEGER)
  'PDSB-6469-P75K',                                                           -- offerCode (STRING)
  DATE '2025-06-13',                                                          -- offerStartDate (DATE)
  DATE '2025-06-19',                                                          -- offerEndDate (DATE)
  'PDSB-6469',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  NULL,                                                                       -- promoCodes (STRING or NULL)
  'KOG',                                                                      -- sourceCodes (STRING or NULL)
  '9537',                                                                     -- storeIds (STRING or NULL)
  'PCMCNF',                                                                   -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  FALSE,                                                                      -- isAlwaysOn (BOOLEAN)
  'PDSB-6469-P75K',                                                           -- offerDescription (STRING)
  '2025-06-11 22:37:00.794896+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      49,
      'PDSB-6469',
      'FTP',
      'LCL_FP',
      'ACQ_LCLFP',
      FALSE,
      NULL,
      NULL,
      NULL,
      55000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 22:38:23.325004+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  28,                                                                         -- targetedOfferId (INTEGER)
  'PDSB-6469-P75K_S',                                                         -- offerCode (STRING)
  DATE '2025-06-20',                                                          -- offerStartDate (DATE)
  DATE '2025-07-26',                                                          -- offerEndDate (DATE)
  'PDSB-6469',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  NULL,                                                                       -- promoCodes (STRING or NULL)
  'KOG',                                                                      -- sourceCodes (STRING or NULL)
  '67,917,651',                                                               -- storeIds (STRING or NULL)
  'PCMCNF',                                                                   -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  FALSE,                                                                      -- isAlwaysOn (BOOLEAN)
  'PDSB-6469-P75K_S',                                                         -- offerDescription (STRING)
  '2025-06-11 22:37:00.794896+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      50,
      'PDSB-6469',
      'FTP',
      'LCL_FP',
      'ACQ_LCLFP',
      FALSE,
      NULL,
      NULL,
      NULL,
      55000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 22:38:23.325004+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  29,                                                                         -- targetedOfferId (INTEGER)
  'PDSB-6469-P75K_T',                                                         -- offerCode (STRING)
  DATE '2025-06-27',                                                          -- offerStartDate (DATE)
  DATE '2025-07-03',                                                          -- offerEndDate (DATE)
  'PDSB-6469',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  NULL,                                                                       -- promoCodes (STRING or NULL)
  'KOG',                                                                      -- sourceCodes (STRING or NULL)
  '9796',                                                                     -- storeIds (STRING or NULL)
  'PCMCNF',                                                                   -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  FALSE,                                                                      -- isAlwaysOn (BOOLEAN)
  'PDSB-6469-P75K_T',                                                         -- offerDescription (STRING)
  '2025-06-11 22:37:00.794896+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      51,
      'PDSB-6469',
      'FTP',
      'LCL_FP',
      'ACQ_LCLFP',
      FALSE,
      NULL,
      NULL,
      NULL,
      55000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 22:38:23.325004+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
),
(
  30,                                                                         -- targetedOfferId (INTEGER)
  'PDSB-6000-N125K_S',                                                        -- offerCode (STRING)
  DATE '2025-06-19',                                                          -- offerStartDate (DATE)
  DATE '2025-06-25',                                                          -- offerEndDate (DATE)
  'PDSB-6000',                                                                -- ticket (STRING or NULL)
  'PON',                                                                      -- lifecycleEvent (STRING)
  'A',                                                                        -- status (STRING ENUM)
  '1,159,811,549',                                                            -- promoCodes (STRING or NULL)
  'INT,DPL',                                                                  -- sourceCodes (STRING or NULL)
  NULL,                                                                       -- storeIds (STRING or NULL)
  'PCMCNF',                                                                   -- productType (STRING)
  FALSE,                                                                      -- billC86 (BOOLEAN)
  NULL,                                                                       -- billC86Days (INTEGER or NULL)
  NULL,                                                                       -- extCampaignId (STRING or NULL)
  FALSE,                                                                      -- isAlwaysOn (BOOLEAN)
  'PDSB-6000-N125K_S',                                                        -- offerDescription (STRING)
  '2025-06-11 22:39:08.135404+00',                                            -- createTimestamp (TIMESTAMP)
  'sysUser',                                                                  -- createUserId (STRING)
  NULL,                                                                       -- updateTimestamp (TIMESTAMP or NULL)
  NULL,                                                                       -- updateUserId (STRING or NULL)
  [                                                                           -- offerTriggers
    STRUCT
    <
      offerTriggerId INT64,
      triggerDescription STRING,
      offerTriggerType STRING,
      triggerType STRING,
      categoryCode STRING,
      noFpPts BOOL,
      merchantGroups STRING,
      fulfillmentPeriodDays INT64,
      minAmount STRING,
      pointsAwarded INT64,
      percentRewarded STRING,
      targetTransactionCount INT64,
      targetTotalAmount STRING,
      maxTotalAmount STRING,
      maxTransactionCount INT64,
      numberOfPeriods INT64,
      campaignId INT64,
      createTimestamp TIMESTAMP,
      createUserId STRING,
      updateTimestamp TIMESTAMP,
      updateUserId STRING,
      eventType STRING
    >
    (
      52,
      'PDSB-6000',
      'FTP',
      'LCL_FP',
      'ACQ_LCLFP',
      FALSE,
      NULL,
      NULL,
      NULL,
      105000,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      '2025-06-11 22:39:39.729873+00',
      'sysUser',
      NULL,
      NULL,
      'ADD'
    )
  ],
  'ADD'                             -- eventType (STRING)
)
;
