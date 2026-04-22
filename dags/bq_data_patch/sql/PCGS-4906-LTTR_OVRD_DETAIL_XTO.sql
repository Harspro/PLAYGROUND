UPDATE pcb-{env}-landing.domain_customer_acquisition.LTTR_OVRD_DETAIL_XTO SET CUSTOM_DATA_85 = 'SAVINGS' WHERE APP_NUM IN (
SELECT DISTINCT CAST(tsys_reference_number AS INT64)
FROM pcb-{env}-landing.domain_customer_acquisition.APPLICATION_PRODUCT
WHERE product_type = 'INDIVIDUAL'
AND create_dt BETWEEN CAST('2025-01-21 00:00:00' AS datetime) AND CAST('2025-01-30 00:00:00' AS datetime)
AND application_submission_uid IN
(SELECT ap.application_submission_uid FROM pcb-{env}-landing.domain_customer_acquisition.APPLICATION_PRODUCT ap WHERE ap.product_type = 'SAVINGS')
);