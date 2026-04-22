-- Telpay 053 Indexes
CREATE INDEX IF NOT EXISTS bi_query_telpay_053_account_no_idx ON {env}_reverse_etl.bi_query_telpay_053 USING btree (account_no);

CREATE INDEX IF NOT EXISTS bi_query_telpay_053_amount_idx ON {env}_reverse_etl.bi_query_telpay_053 USING btree (amount);

CREATE INDEX IF NOT EXISTS bi_query_telpay_053_card_number_idx ON {env}_reverse_etl.bi_query_telpay_053 USING btree (card_number);

CREATE INDEX IF NOT EXISTS bi_query_telpay_053_orig_fi_dt_idx ON {env}_reverse_etl.bi_query_telpay_053 USING btree (orig_fi_dt);

CREATE INDEX IF NOT EXISTS bi_query_telpay_053_payer_name_idx ON {env}_reverse_etl.bi_query_telpay_053 USING btree (payer_name);

CREATE INDEX IF NOT EXISTS bi_query_telpay_053_pcf_cust_id_idx ON {env}_reverse_etl.bi_query_telpay_053 USING btree (pcf_cust_id);

CREATE INDEX IF NOT EXISTS bi_query_telpay_053_update_dt_idx ON {env}_reverse_etl.bi_query_telpay_053 USING btree (update_dt);

-- Telpay 053 default Indexes
CREATE INDEX IF NOT EXISTS bi_query_telpay_053_default_account_no_idx ON {env}_reverse_etl.bi_query_telpay_053_default USING btree (account_no);

CREATE INDEX IF NOT EXISTS bi_query_telpay_053_default_amount_idx ON {env}_reverse_etl.bi_query_telpay_053_default USING btree (amount);

CREATE INDEX IF NOT EXISTS bi_query_telpay_053_default_card_number_idx ON {env}_reverse_etl.bi_query_telpay_053_default USING btree (card_number);

CREATE INDEX IF NOT EXISTS bi_query_telpay_053_default_orig_fi_dt_idx ON {env}_reverse_etl.bi_query_telpay_053_default USING btree (orig_fi_dt);

CREATE INDEX IF NOT EXISTS bi_query_telpay_053_default_payer_name_idx ON {env}_reverse_etl.bi_query_telpay_053_default USING btree (payer_name);

CREATE INDEX IF NOT EXISTS bi_query_telpay_053_default_pcf_cust_id_idx ON {env}_reverse_etl.bi_query_telpay_053_default USING btree (pcf_cust_id);

CREATE INDEX IF NOT EXISTS bi_query_telpay_053_default_update_dt_idx ON {env}_reverse_etl.bi_query_telpay_053_default USING btree (update_dt);