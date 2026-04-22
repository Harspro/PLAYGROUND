DROP TABLE IF EXISTS `pcb-{env}-landing.domain_payments.BI_QUERY_TELPAY_RECORDS_053`;

CREATE
OR REPLACE TABLE `pcb-{env}-landing.domain_payments.BI_QUERY_TELPAY_RECORDS_053` PARTITION BY DATE_TRUNC(UPDATE_DT, MONTH) AS
SELECT
    *
FROM
    `pcb-{env}-processing.domain_payments.BI_QUERY_TELPAY_053`;