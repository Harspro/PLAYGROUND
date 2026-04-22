CREATE OR REPLACE TABLE
  `pcb-{env}-curated.cots_cdic.CDIC_0501`
AS
  SELECT
    *
  FROM pcb-{env}-curated.domain_cdic.CDIC_0501_REFERENCE;