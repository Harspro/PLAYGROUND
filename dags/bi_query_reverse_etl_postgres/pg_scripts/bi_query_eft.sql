-- {env}_reverse_etl.bi_query_eft definition
-- Drop table
DROP table if EXISTS {env}_reverse_etl.bi_query_eft CASCADE;

DROP table if EXISTS {env}_reverse_etl.bi_query_eft_default CASCADE;

CREATE table if not EXISTS {env}_reverse_etl.bi_query_eft (
	pcf_cust_id text NULL,
	account_no text NULL,
	card_number text NULL,
	payer_name text NULL,
	amount numeric(38, 9) NULL,
	eft_req_uid numeric(38, 9) NULL,
	eft_fulfillment_uid numeric(38, 9) NULL,
	eft_reqtype_cd text NULL,
	fundsmove_instruction_id int8 NULL,
	fundsmove_gl_req_uid int8 NULL,
	fundsmove_processing_dt timestamp NULL,
	institution_id text NULL,
	transit_number text NULL,
	eft_account_no text NULL,
	originator_name text NULL,
	originator_id text NULL,
	settle_on timestamp NULL,
	pcb_assigned_opacct_nickname text NULL,
	opacct_institution_no text NULL,
	opacct_transit_number text NULL,
	opacct_number text NULL,
	trace_id text NULL,
	executed_on timestamp NULL,
	request_on timestamp NULL,
	create_dt timestamp NULL,
	create_user_id text NULL,
	create_function_name text NULL,
	update_dt timestamp NULL,
	update_month INT null,
	update_year INT null,
	update_user_id text NULL,
	update_function_name text NULL,
	transfer_type text NULL,
	cpa_transaction_type text NULL,
	pcb_institution_id text NULL,
	pcb_transit_number text NULL,
	pcb_customer_account_number text NULL,
	pcb_customer_name text NULL,
	origin_cross_reference_number text NULL,
	origin_institution_id text NULL,
	origin_transit_number text NULL,
	origin_account_number text NULL,
	origin_trace_number text NULL,
	bi_query_eft_uid text NULL,
	eft_source text null,
	bi_query_eft_date date NULL,
	bi_query_dag_run_id text null,
	constraint PK_EFT primary KEY(
		eft_req_uid,
		update_year,
		update_month
	)
) partition by range(update_year, update_month);

-- Default Partition (Catches Unhandled Data)
CREATE TABLE IF NOT EXISTS {env}_reverse_etl.bi_query_eft_default PARTITION OF {env}_reverse_etl.bi_query_eft DEFAULT;