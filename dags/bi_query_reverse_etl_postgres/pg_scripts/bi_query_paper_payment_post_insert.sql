-- Paper Payment Indexes
CREATE INDEX IF NOT EXISTS bi_query_paper_payment_account_no_idx ON {env}_reverse_etl.bi_query_paper_payment USING btree (account_no);

CREATE INDEX IF NOT EXISTS bi_query_paper_payment_amount_idx ON {env}_reverse_etl.bi_query_paper_payment USING btree (amount);

CREATE INDEX IF NOT EXISTS bi_query_paper_payment_card_number_idx ON {env}_reverse_etl.bi_query_paper_payment USING btree (card_number);

CREATE INDEX IF NOT EXISTS bi_query_paper_payment_pcf_cust_id_idx ON {env}_reverse_etl.bi_query_paper_payment USING btree (pcf_cust_id);

CREATE INDEX IF NOT EXISTS bi_query_paper_payment_process_date_idx ON {env}_reverse_etl.bi_query_paper_payment USING btree (process_date);

CREATE INDEX IF NOT EXISTS bi_query_paper_payment_update_dt_idx ON {env}_reverse_etl.bi_query_paper_payment USING btree (update_dt);

-- Paper Payment default Indexes
CREATE INDEX IF NOT EXISTS bi_query_paper_payment_default_account_no_idx ON {env}_reverse_etl.bi_query_paper_payment_default USING btree (account_no);

CREATE INDEX IF NOT EXISTS bi_query_paper_payment_default_amount_idx ON {env}_reverse_etl.bi_query_paper_payment_default USING btree (amount);

CREATE INDEX IF NOT EXISTS bi_query_paper_payment_default_card_number_idx ON {env}_reverse_etl.bi_query_paper_payment_default USING btree (card_number);

CREATE INDEX IF NOT EXISTS bi_query_paper_payment_default_pcf_cust_id_idx ON {env}_reverse_etl.bi_query_paper_payment_default USING btree (pcf_cust_id);

CREATE INDEX IF NOT EXISTS bi_query_paper_payment_default_process_date_idx ON {env}_reverse_etl.bi_query_paper_payment_default USING btree (process_date);

CREATE INDEX IF NOT EXISTS bi_query_paper_payment_default_update_dt_idx ON {env}_reverse_etl.bi_query_paper_payment_default USING btree (update_dt);