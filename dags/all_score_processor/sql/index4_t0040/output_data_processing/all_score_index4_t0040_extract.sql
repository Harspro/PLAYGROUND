-- Index4 T0040 Output Extract
-- Extracts model output from OUTPUT_INDEX4_T0040 table (REPEATED RECORD format)
-- and transforms into fixed-width extract format for downstream file generation

EXPORT DATA
  OPTIONS (
    uri = '{output_uri_processing}',
    format = 'CSV',
    header = TRUE,
    field_delimiter = '|',
    OVERWRITE = TRUE
  ) AS
SELECT DISTINCT
  '{client_id}' AS CLIENT_ID,
  '{record_type}' AS RECORD_TYPE,
  '{reject_code}' AS REJECT_CODE,
  '{account_number}' AS ACCOUNT_NUMBER,
  '{filler_1}' AS FILLER_1,
  -- Extract MAST_ACCOUNT_ID from OUTPUTS array
   MAST_ACCOUNT_ID AS ACCOUNT_IDENTIFIER,
  '{filler_2}' AS FILLER_2,
  '{primary_customer_id}' AS PRIMARY_CUSTOMER_ID,
  '{customer_type}' AS CUSTOMER_TYPE,
  '{filler_3}' AS FILLER_3,
  '{score_type}' AS SCORE_TYPE,
  '{score_calc_date}' AS SCORE_CALC_DATE,
  '{score_calc_time}' AS SCORE_CALC_TIME,
  '{score_raw_sign}' AS SCORE_RAW_SIGN,
  -- Extract SCORE_RAW from OUTPUTS array
   CRI_V4_SCORE AS SCORE_RAW,
  '{score_aligned_sign}' AS SCORE_ALIGNED_SIGN,
  -- Extract SCORECARD_POINTS from OUTPUTS array
   FINAL_INDEX_ALLOCATION AS SCORE_ALIGNED,
  '{score_source}' AS SCORE_SOURCE,
  '{rating_grade}' AS RATING_GRADE,
  '{bureau_id}' AS BUREAU_ID,
  '{market_address_code}' AS MARKET_ADDRESS_CODE,
  -- Extract score reason fields from OUTPUTS array
   CRI_V3_RISK_INDEX AS SCORE_REASON_1,
   CRI_V4_RISK_INDEX AS SCORE_REASON_2,
  '{score_reason_3}' AS SCORE_REASON_3,
  '{score_reason_4}' AS SCORE_REASON_4,
  -- Extract reason difference fields from OUTPUTS array
  '{reason_diff_1_sign}' AS REASON_DIFF_1_SIGN,
  '{reason_diff_1}' AS REASON_DIFF_1,
  '{reason_diff_2_sign}' AS REASON_DIFF_2_SIGN,
  '{reason_diff_2}' AS REASON_DIFF_2,
  '{reason_diff_3_sign}' AS REASON_DIFF_3_SIGN,
  '{reason_diff_3}' AS REASON_DIFF_3,
  '{reason_diff_4_sign}' AS REASON_DIFF_4_SIGN,
  '{reason_diff_4}' AS REASON_DIFF_4,
  '{acct_relation_code}' AS ACCT_RELATION_CODE,
  '{operator_id}' AS OPERATOR_ID,
  -- Extract user-defined fields from OUTPUTS array
  '{user_defined_1}' AS USER_DEFINED_1,
  '{user_defined_2}' AS USER_DEFINED_2,
  '{user_defined_3}' AS USER_DEFINED_3,
  '{user_defined_s1_sign}' AS USER_DEFINED_S1_SIGN,
  '{user_defined_s1}' AS USER_DEFINED_S1,
  '{user_defined_s2_sign}' AS USER_DEFINED_S2_SIGN,
  '{user_defined_s2}' AS USER_DEFINED_S2,
  '{tts_dollar_value}' AS TTS_DOLLAR_VALUE,
  '{inquiry_flag}' AS INQUIRY_FLAG,
  '{low_range_sign}' AS LOW_RANGE_SIGN,
  '{low_score_range}' AS LOW_SCORE_RANGE,
  '{high_range_sign}' AS HIGH_RANGE_SIGN,
  '{high_score_range}' AS HIGH_SCORE_RANGE,
  '{percentile_ranking}' AS PERCENTILE_RANKING,
  '{filler_4}' AS FILLER_4,
  '{score_type_2}' AS SCORE_TYPE_2
FROM
  `pcb-{env}-curated.domain_scoring.OUTPUT_INDEX4_T0040_UNNESTED_COMPARISON_LATEST`