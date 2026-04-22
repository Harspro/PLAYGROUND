SELECT COUNT(*) AS count
FROM (
    SELECT sin, COUNT(*) AS count
    FROM `pcb-{env}-processing.domain_tax_slips.T5_ENRICHED_TAX_SLIP`
    WHERE sin != '000000000'
    GROUP BY sin
    HAVING COUNT(*) > 1
) AS SIN;