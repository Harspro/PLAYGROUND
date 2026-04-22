-- {env}_reverse_etl.bi_query_telpay_053 definition
-- Drop table
DROP table if exists {env}_reverse_etl.bi_query_telpay CASCADE;

DROP table if exists {env}_reverse_etl.bi_query_telpay_053 CASCADE;

DROP table if exists {env}_reverse_etl.bi_query_telpay_053_default CASCADE;

CREATE table if not exists {env}_reverse_etl.bi_query_telpay_053 (
	pcf_cust_id text NULL,
	account_no text NULL,
	card_number text NULL,
	payer_name text NULL,
	amount numeric(38, 9) NULL,
	orig_fi_dt timestamp NULL,
	eff_pymt_dt timestamp NULL,
	trace_id text NULL,
	report_number text NULL,
	run_number text NULL,
	telpay_pymt_uid numeric(38, 9) NULL,
	card_deposit_fulfillment_uid numeric(38, 9) NULL,
	hdr_id text NULL,
	run_timestamp timestamp NULL,
	version text NULL,
	biller_cd text NULL,
	create_dt timestamp NULL,
	create_user_id text NULL,
	create_function_name text NULL,
	update_dt timestamp NULL,
	update_month INT null,
	update_year INT null,
	update_user_id text NULL,
	update_function_name text NULL,
	bi_query_telpay_uid text NULL,
	telpay_source text NULL,
	bi_query_telpay_date date NULL,
	bi_query_dag_run_id text null,
	constraint PK_TELPAY_053 primary KEY(
		card_number,
		trace_id,
		hdr_id,
		update_year,
		update_month
	)
) PARTITION BY RANGE (update_year, update_month);

-- Default Partition (Catches Unhandled Data)
CREATE TABLE IF NOT EXISTS {env}_reverse_etl.bi_query_telpay_053_default PARTITION OF {env}_reverse_etl.bi_query_telpay_053 DEFAULT;