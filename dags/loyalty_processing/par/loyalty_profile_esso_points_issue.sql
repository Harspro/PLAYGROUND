EXPORT DATA
  OPTIONS (
    uri = 'gs://pcb-{env}-staging-extract/sync-loyalty-profile/esso-points-issue-*.parquet',
    format = 'PARQUET',
    overwrite = true
  )
AS
(
    WITH Esso_Missing_Points AS (
        SELECT DISTINCT
            pmt.wallettransactionid,
            pts.wallettransaction_reference,
            ity.wallet_id wallet_id,
            ity.identity_type,
            card_number_hash,
            pcf_tender,
            tender_type,
            pmt.trans_dt
        FROM
            `{table_ee_wallet_transaction_payment}` pmt
        LEFT JOIN
            `{table_cnsld_latest}` pts ON pmt.wallettransactionid = pts.wallettransactionid
        LEFT JOIN
            `{table_ee_identity}` ity ON pts.wallettransaction_identityid = ity.identity_id
        WHERE
            card_number_hash IS NULL
            AND pcf_tender IS TRUE
            AND pts.trans_dt BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            AND pmt.trans_dt BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            AND ity.business_effective_date > '2018-01-01'
    ),

     PCF_Customers AS (
        SELECT
        ee_identity_subset.identity_id,
        ee_identity_subset.business_effective_ts,
        ee_identity_subset.identity_state,
        ee_identity_subset.identity_status,
        ee_identity_subset.wallet_id as wallet_id,
        ee_identity_subset.identity_type,
        ee_identity_subset.value as CUSTOMER_IDENTIFIER_NO
        FROM (
          SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY identity_id ORDER BY business_effective_ts DESC, create_ts DESC, event_header_ees_event_id DESC) AS RANK_
          FROM
            `{table_ee_identity}`
          WHERE
            LOWER(event_header_client_id) NOT LIKE 'eestesting%'
            AND LOWER(identity_type) = 'cif'
            AND LOWER(identity_status) = 'active'
            AND business_effective_date >= DATE(2000,01,01)
        )ee_identity_subset
        WHERE
        ee_identity_subset.RANK_ = 1
    )

     SELECT DISTINCT
        SAFE_CAST(pcf.CUSTOMER_IDENTIFIER_NO AS STRING) AS CUSTOMER_IDENTIFIER_NO,
        SAFE_CAST(custIden.CUSTOMER_UID AS STRING) AS CUSTOMER_UID,
        SAFE_CAST('SYNC' AS STRING) AS SYNC_LOYALTY_EVENT_TYPE
     FROM
        PCF_CUSTOMERS pcf
     INNER JOIN
        Esso_Missing_Points esso ON pcf.wallet_id = esso.wallet_id
     INNER JOIN
        `pcb-{env}-landing.domain_customer_management.CUSTOMER_IDENTIFIER` custIden ON pcf.CUSTOMER_IDENTIFIER_NO = custIden.CUSTOMER_IDENTIFIER_NO
)