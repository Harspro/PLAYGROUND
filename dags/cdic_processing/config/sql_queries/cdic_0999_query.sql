CREATE OR REPLACE TABLE
  `pcb-{env}-curated.cots_cdic.CDIC_0999`
AS
  SELECT
    *
  FROM pcb-{env}-curated.domain_cdic.CDIC_0999_REFERENCE;