INSERT INTO `pcb-{env}-landing.domain_retail.GLOBAL_PAYMENT_MERCHANT`
SELECT * FROM `pcb-{env}-processing.domain_retail.GLOBAL_PAYMENT_MERCHANT_STAGING`
WHERE MERCHANT_ID NOT IN (SELECT MERCHANT_ID FROM `pcb-{env}-landing.domain_retail.GLOBAL_PAYMENT_MERCHANT`);