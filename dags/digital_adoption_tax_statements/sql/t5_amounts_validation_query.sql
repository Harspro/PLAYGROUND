    SELECT
        CASE
            WHEN (table1_count.total_count = table2.slp_cnt)
             AND (table1_sum.total_amount = table2.tot_cdn_int_amt)
            THEN 'Match'
            ELSE 'No Match'
        END AS validation_status
    FROM
        (SELECT COUNT(*) AS total_count FROM `pcb-{env}-processing.domain_tax_slips.T5_ENRICHED_TAX_SLIP`) AS table1_count
    JOIN
        (SELECT SUM(CAST(cdn_int_amt AS NUMERIC)) AS total_amount
         FROM `pcb-{env}-processing.domain_tax_slips.T5_ENRICHED_TAX_SLIP`) AS table1_sum ON 1=1
    JOIN
        (SELECT CAST(slp_cnt AS NUMERIC) AS slp_cnt,
                CAST(tot_cdn_int_amt AS NUMERIC) AS tot_cdn_int_amt
         FROM `pcb-{env}-landing.domain_tax_slips.T5_TAX_RAW_TRAILER`) AS table2 ON 1=1;