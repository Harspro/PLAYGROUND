CREATE OR REPLACE TABLE
  `pcb-{env}-curated.cots_cdic.CDIC_0211`
AS
  SELECT
    *
  FROM pcb-{env}-curated.domain_cdic.CDIC_0211_REFERENCE
  LIMIT 0;

INSERT INTO
  `pcb-{env}-curated.cots_cdic.CDIC_0211`
SELECT
  *
FROM pcb-{env}-curated.domain_cdic.CDIC_0211_REFERENCE;