-- Telpay ODS Indexes
CREATE INDEX IF NOT EXISTS bi_query_telpay_ods_account_no_idx ON {env}_reverse_etl.bi_query_telpay_ods USING btree (account_no);

CREATE INDEX IF NOT EXISTS bi_query_telpay_ods_amount_idx ON {env}_reverse_etl.bi_query_telpay_ods USING btree (amount);

CREATE INDEX IF NOT EXISTS bi_query_telpay_ods_card_number_idx ON {env}_reverse_etl.bi_query_telpay_ods USING btree (card_number);

CREATE INDEX IF NOT EXISTS bi_query_telpay_ods_orig_fi_dt_idx ON {env}_reverse_etl.bi_query_telpay_ods USING btree (orig_fi_dt);

CREATE INDEX IF NOT EXISTS bi_query_telpay_ods_payer_name_idx ON {env}_reverse_etl.bi_query_telpay_ods USING btree (payer_name);

CREATE INDEX IF NOT EXISTS bi_query_telpay_ods_pcf_cust_id_idx ON {env}_reverse_etl.bi_query_telpay_ods USING btree (pcf_cust_id);

CREATE INDEX IF NOT EXISTS bi_query_telpay_ods_update_dt_idx ON {env}_reverse_etl.bi_query_telpay_ods USING btree (update_dt);

-- Telpay ODS default Indexes
CREATE INDEX IF NOT EXISTS bi_query_telpay_ods_default_account_no_idx ON {env}_reverse_etl.bi_query_telpay_ods_default USING btree (account_no);

CREATE INDEX IF NOT EXISTS bi_query_telpay_ods_default_amount_idx ON {env}_reverse_etl.bi_query_telpay_ods_default USING btree (amount);

CREATE INDEX IF NOT EXISTS bi_query_telpay_ods_default_card_number_idx ON {env}_reverse_etl.bi_query_telpay_ods_default USING btree (card_number);

CREATE INDEX IF NOT EXISTS bi_query_telpay_ods_default_orig_fi_dt_idx ON {env}_reverse_etl.bi_query_telpay_ods_default USING btree (orig_fi_dt);

CREATE INDEX IF NOT EXISTS bi_query_telpay_ods_default_payer_name_idx ON {env}_reverse_etl.bi_query_telpay_ods_default USING btree (payer_name);

CREATE INDEX IF NOT EXISTS bi_query_telpay_ods_default_pcf_cust_id_idx ON {env}_reverse_etl.bi_query_telpay_ods_default USING btree (pcf_cust_id);

CREATE INDEX IF NOT EXISTS bi_query_telpay_ods_default_update_dt_idx ON {env}_reverse_etl.bi_query_telpay_ods_default USING btree (update_dt);