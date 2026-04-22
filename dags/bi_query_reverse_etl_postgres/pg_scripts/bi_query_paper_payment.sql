-- {env}_reverse_etl.bi_query_paper_payment definition
-- Drop table
DROP table if exists {env}_reverse_etl.bi_query_paper_payment CASCADE;

DROP table if exists {env}_reverse_etl.bi_query_paper_payment_default CASCADE;

CREATE table if not exists {env}_reverse_etl.bi_query_paper_payment (
	pcf_cust_id text NULL,
	account_no text NULL,
	card_number text NULL,
	amount numeric(38, 9) NULL,
	seq_num text NULL,
	cheque_num text NULL,
	cheque_date date NULL,
	process_date timestamp NULL,
	transit_number text NULL,
	institution_id text NULL,
	fdr_flag text NULL,
	fdr_date timestamp NULL,
	create_dt timestamp NULL,
	update_user_id text NULL,
	update_dt timestamp NULL,
	update_month INT null,
	update_year INT null,
	st_cd text NULL,
	st_dt timestamp NULL,
	csh_ind text NULL,
	create_user_id text NULL,
	create_function_name text NULL,
	trace_id text NULL,
	update_function_name text NULL,
	bi_query_paper_payment_uid text NULL,
	paper_payment_source text NULL,
	bi_query_paper_payment_date date NULL,
	bi_query_dag_run_id text null,
	constraint PK_PAPER_PAYMENT primary KEY(
		card_number,
		seq_num,
		update_year,
        update_month
	)
) partition by range(update_year, update_month);

-- Default Partition (Catches Unhandled Data)
CREATE TABLE IF NOT EXISTS {env}_reverse_etl.bi_query_paper_payment_default PARTITION OF {env}_reverse_etl.bi_query_paper_payment DEFAULT;