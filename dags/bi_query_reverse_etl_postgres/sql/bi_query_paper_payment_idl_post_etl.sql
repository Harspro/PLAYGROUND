DROP TABLE IF EXISTS `pcb-{env}-landing.domain_payments.BI_QUERY_PAPER_PAYMENT_RECORDS`;

CREATE
OR REPLACE TABLE `pcb-{env}-landing.domain_payments.BI_QUERY_PAPER_PAYMENT_RECORDS` PARTITION BY DATE_TRUNC(UPDATE_DT, MONTH) AS
SELECT
    *
FROM
    `pcb-{env}-processing.domain_payments.BI_QUERY_PAPER_PAYMENT`;