CREATE TABLE `pcb-{env}-landing.domain_retail.GLOBAL_PAYMENT_MERCHANT`
AS
SELECT * FROM `pcb-{env}-processing.domain_retail.GLOBAL_PAYMENT_MERCHANT_STAGING`
LIMIT 0;