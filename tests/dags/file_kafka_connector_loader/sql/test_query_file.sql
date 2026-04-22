EXPORT DATA OPTIONS
(
uri='gs://pcb-{env}-staging-extract/test-folder-name/test-file-name-*.parquet',
format = 'PARQUET',
overwrite = true
) AS (SELECT * FROM `pcb-{env}-landing.domain_test.test_table`);