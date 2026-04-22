CREATE OR REPLACE TABLE
  `pcb-{env}-curated.cots_cdic.CDIC_0401`
AS
  SELECT
    *
  FROM pcb-{env}-curated.domain_cdic.CDIC_0401_REFERENCE;