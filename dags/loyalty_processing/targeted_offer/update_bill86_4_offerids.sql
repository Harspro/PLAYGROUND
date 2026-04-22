--This query updates billC86 fields for specific offer ids records in (40, 45, 53, 54)
UPDATE
  `pcb-{env}-landing.domain_loyalty.PC_TARGETED_OFFER`
SET
  billC86 = TRUE,
  billC86Days =
  CASE
    WHEN billC86Days != 365 THEN 365
    ELSE billC86Days
END
WHERE
  targetedOfferId IN (40,
    45,
    53,
    54)
  AND billC86 = FALSE;