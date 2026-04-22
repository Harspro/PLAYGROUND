-- EFT Indexes
CREATE INDEX IF NOT EXISTS bi_query_eft_account_no_idx ON {env}_reverse_etl.bi_query_eft USING btree (account_no);

CREATE INDEX IF NOT EXISTS bi_query_eft_amount_idx ON {env}_reverse_etl.bi_query_eft USING btree (amount);

CREATE INDEX IF NOT EXISTS bi_query_eft_card_number_idx ON {env}_reverse_etl.bi_query_eft USING btree (card_number);

CREATE INDEX IF NOT EXISTS bi_query_eft_fundsmove_processing_dt_idx ON {env}_reverse_etl.bi_query_eft USING btree (fundsmove_processing_dt);

CREATE INDEX IF NOT EXISTS bi_query_eft_payer_name_idx ON {env}_reverse_etl.bi_query_eft USING btree (payer_name);

CREATE INDEX IF NOT EXISTS bi_query_eft_pcf_cust_id_idx ON {env}_reverse_etl.bi_query_eft USING btree (pcf_cust_id);

CREATE INDEX IF NOT EXISTS bi_query_eft_update_dt_idx ON {env}_reverse_etl.bi_query_eft USING btree (update_dt);

-- EFT default Indexes
CREATE INDEX IF NOT EXISTS bi_query_eft_default_account_no_idx ON {env}_reverse_etl.bi_query_eft_default USING btree (account_no);

CREATE INDEX IF NOT EXISTS bi_query_eft_default_amount_idx ON {env}_reverse_etl.bi_query_eft_default USING btree (amount);

CREATE INDEX IF NOT EXISTS bi_query_eft_default_card_number_idx ON {env}_reverse_etl.bi_query_eft_default USING btree (card_number);

CREATE INDEX IF NOT EXISTS bi_query_eft_default_fundsmove_processing_dt_idx ON {env}_reverse_etl.bi_query_eft_default USING btree (fundsmove_processing_dt);

CREATE INDEX IF NOT EXISTS bi_query_eft_default_payer_name_idx ON {env}_reverse_etl.bi_query_eft_default USING btree (payer_name);

CREATE INDEX IF NOT EXISTS bi_query_eft_default_pcf_cust_id_idx ON {env}_reverse_etl.bi_query_eft_default USING btree (pcf_cust_id);

CREATE INDEX IF NOT EXISTS bi_query_eft_default_update_dt_idx ON {env}_reverse_etl.bi_query_eft_default USING btree (update_dt);