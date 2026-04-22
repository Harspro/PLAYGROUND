CREATE OR REPLACE TABLE
  `pcb-{env}-curated.cots_cdic.CDIC_0233`
AS
  SELECT
    *
  FROM pcb-{env}-curated.domain_cdic.CDIC_0233_REFERENCE
  LIMIT 0;

INSERT INTO
  `pcb-{env}-curated.cots_cdic.CDIC_0233`
SELECT
  *
FROM pcb-{env}-curated.domain_cdic.CDIC_0233_REFERENCE;