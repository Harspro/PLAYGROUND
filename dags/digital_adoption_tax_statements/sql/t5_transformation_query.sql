CREATE OR REPLACE TABLE pcb-{env}-processing.domain_tax_slips.T5_ENRICHED_TAX_SLIP
AS
(
  WITH T5_CUSTOMER_UID AS 
  (
    SELECT
        T5Slip.*,
        SAFE_CAST(CI1.customerId AS INT64) AS CUSTOMER_UID
    FROM
        pcb-{env}-landing.domain_tax_slips.T5_TAX_RAW_SLIP T5Slip
    LEFT JOIN
    (
      -- Subquery to get the latest record for each ledgerCustomerId
      SELECT
          customerId,
          ledgerCustomerId,
          INGESTION_TIMESTAMP,
          ROW_NUMBER() OVER 
          (
            PARTITION BY ledgerCustomerId 
            ORDER BY 
              CASE 
                WHEN UPPER(accountEventType) IN ('MERGE', 'SPLIT') AND UPPER(activeInd) = 'Y' THEN 1
                WHEN UPPER(accountEventType) IN ('MERGE', 'SPLIT') AND UPPER(activeInd) = 'N' THEN 2
                ELSE 1
              END,
              INGESTION_TIMESTAMP DESC
          ) AS row_num
      FROM
          pcb-{env}-curated.domain_account_management.ACCOUNT_RELATIONSHIP
      where UPPER(ledgerAccountType) = 'TEMENOS'
    ) CI1
    ON T5Slip.rcpnt_fi_acct_nbr  = CI1.ledgerCustomerId
    AND CI1.row_num = 1
  ),

CUSTOMER_IDENTIFIER AS (
   SELECT
     ID.CUSTOMER_UID,
     ID.CUSTOMER_IDENTIFIER_NO AS sin,
     CASE
       WHEN LENGTH(ID.CUSTOMER_IDENTIFIER_NO) != 9 THEN 'Invalid'
       WHEN MOD(
         SUM(
           CASE
             WHEN MOD(position, 2) = 1 THEN CAST(SUBSTR(ID.CUSTOMER_IDENTIFIER_NO, position, 1) AS INT64) -- Odd positions
             ELSE
               -- Even positions: Double the digit, and subtract 9 if the result is greater than 9
               CASE
                 WHEN CAST(SUBSTR(ID.CUSTOMER_IDENTIFIER_NO, position, 1) AS INT64) * 2 > 9 THEN
                   CAST(SUBSTR(ID.CUSTOMER_IDENTIFIER_NO, position, 1) AS INT64) * 2 - 9
                 ELSE
                   CAST(SUBSTR(ID.CUSTOMER_IDENTIFIER_NO, position, 1) AS INT64) * 2
               END
           END
         ),
         10
       ) = 0 THEN 'Valid'
       ELSE 'Invalid'
     END AS mod10_status
   FROM
     `pcb-{env}-landing.domain_customer_management.CUSTOMER_IDENTIFIER` ID,
     UNNEST(GENERATE_ARRAY(1, LENGTH(ID.CUSTOMER_IDENTIFIER_NO))) AS position  -- Generate positions for each digit
   WHERE
     ID.TYPE = 'SIN'
     AND ID.DISABLED_IND = 'N'
     AND REGEXP_CONTAINS(SUBSTR(ID.CUSTOMER_IDENTIFIER_NO, position, 1), r'^\d$')  -- Ensure the character is numeric
   GROUP BY
     ID.CUSTOMER_UID,
     ID.CUSTOMER_IDENTIFIER_NO
 ),

CONTACT AS (
    SELECT
        CONTACT_UID,
        CUSTOMER_UID,
        TYPE,
        CONTEXT,
        ROW_NUMBER() OVER (PARTITION BY CUSTOMER_UID ORDER BY
            CASE CONTEXT
                WHEN 'MAILING' THEN 1
                WHEN 'PRIMARY' THEN 2
            END) AS OrderNumber
    FROM
        pcb-{env}-landing.domain_customer_management.CONTACT
    WHERE
        TYPE = 'ADDRESS'
        AND CONTEXT IN ('MAILING', 'PRIMARY')
)
SELECT
    LEFT(CU.SURNAME, 20) as snm,
    LEFT(CU.GIVEN_NAME, 12) as gvn_nm,
    LEFT(CU.MIDDLE_NAME, 1) as init,
    CASE
        WHEN CI.mod10_status = 'Valid' THEN CI.sin
        ELSE '000000000'
      END AS sin,
    TCU.slp_rcpnt_bn,
    TCU.rcpnt_tr_acct_nbr,
    TCU.l1_nm,
    TCU.l2_nm,
    LEFT(ACT.ADDRESS_LINE_1, 30) as addr_l1_txt,
    LEFT(ACT.ADDRESS_LINE_2, 30) as addr_l2_txt,
    LEFT(ACT.CITY, 28) as cty_nm,
    LEFT(ACT.PROVINCE_STATE, 2) prov_cd,
    LEFT(ACT.COUNTRY, 3) as cntry_cd,
    LEFT(ACT.POSTAL_ZIP_CODE, 10) as pstl_cd,
    TCU.bn,
    TCU.rcpnt_fi_br_nbr,
    RIGHT(CI2.CUSTOMER_IDENTIFIER_NO, 12) as rcpnt_fi_acct_nbr,
    TCU.rpt_tcd,
    TCU.rcpnt_tcd,
    TCU.fgn_crcy_ind,
    TCU.actl_elg_dvamt,
    TCU.actl_dvnd_amt,
    TCU.tx_elg_dvnd_pamt,
    TCU.tx_dvnd_amt,
    TCU.enhn_dvtc_amt,
    TCU.dvnd_tx_cr_amt,
    FORMAT("%.2f", CAST(TCU.cdn_int_amt AS FLOAT64)) as cdn_int_amt,
    TCU.fgn_incamt,
    TCU.fgn_tx_pay_amt,
    TCU.oth_cdn_incamt,
    TCU.cdn_royl_amt,
    TCU.cgain_dvnd_amt,
    TCU.acr_annty_amt,
    TCU.rsrc_alwnc_amt,
    TCU.cgain_dvnd_1_amt,
    TCU.cgain_dvnd_2_amt,
    TCU.lk_nt_acr_intamt,
    (select sbmt_ref_id from pcb-{env}-landing.domain_tax_slips.T5_TAX_RAW_HEADER) as sbmt_ref_id,
    current_datetime('America/Toronto') as create_date,
    cgain_dvnd_jan_to_jun_2024
FROM
    T5_CUSTOMER_UID TCU
    LEFT JOIN CUSTOMER_IDENTIFIER CI
        ON TCU.CUSTOMER_UID = CI.CUSTOMER_UID
    LEFT JOIN pcb-{env}-landing.domain_customer_management.CUSTOMER_IDENTIFIER CI2
            ON TCU.CUSTOMER_UID = CI2.CUSTOMER_UID
            AND CI2.type = 'PCF-CUSTOMER-ID'
            AND CI2.DISABLED_IND = 'N'
    LEFT JOIN pcb-{env}-landing.domain_customer_management.CUSTOMER CU
        ON TCU.CUSTOMER_UID = CU.CUSTOMER_UID
    LEFT JOIN CONTACT CT
        ON TCU.CUSTOMER_UID = CT.CUSTOMER_UID
        AND CT.OrderNumber = 1
    LEFT JOIN pcb-{env}-landing.domain_customer_management.ADDRESS_CONTACT ACT
        ON CT.CONTACT_UID = ACT.CONTACT_UID);