import logging
from util.miscutils import read_variable_or_file
from aml_processing.transaction.extraction.rail_type_mapping import RailTypeMapping, RailTypeMappingBase
from aml_processing.transaction.extraction.rail_type_mapping_BPAY_BTFR import RailTypeMappingBPAYBTFR
from aml_processing.transaction.extraction.rail_type_mapping_EPUL_EPSH import RailTypeMappingEPULEPSH
from aml_processing.transaction.extraction.rail_type_mapping_EDI import RailTypeMappingEDI
from aml_processing.transaction.extraction.rail_type_mapping_TLPY import RailTypeMappingTLPY
from aml_processing.transaction.extraction.rail_type_mapping_CCBR_ECBR import RailTypeMappingCCBRECBR
from aml_processing.transaction.extraction.rail_type_mapping_PAD import RailTypeMappingPAD
from aml_processing.transaction.extraction.rail_type_mapping_INAD_INAR_INRR import RailTypeMappingINADINARINRR
from aml_processing.transaction.extraction.rail_type_mapping_INSE_INMR import RailTypeMappingINSEINMR
from aml_processing.transaction.extraction.rail_type_mapping_NPFI_NPFO import RailTypeMappingNPFINPFO
from aml_processing.transaction.extraction.rail_type_mapping_INCA_INRC import RailTypeMappingINCAINRC


# Constants
DEPLOY_ENV = read_variable_or_file('gcp_config')['deployment_environment_name']

# Tables:
PROJECT_CURATED = f"pcb-{DEPLOY_ENV}-curated"
TRANSACTION_AGGREGATE_TABLE = f"{PROJECT_CURATED}.domain_aml.AGG_AT31"
REF_RAIL_TABLE = f"{PROJECT_CURATED}.domain_aml.REF_TRANSACTION_RAIL"

################################################
# Select fields
################################################

POSP_DESCRIPTION = """
    (CASE
        WHEN txn_agg.MERCHANT_CATEGORY_CD IN (5944 ,5094, 7631, 5933, 5972)
            THEN 'Funds Out - POS Purchase - Jewelry'
        WHEN txn_agg.MERCHANT_CATEGORY_CD IN (8398,8651,8661)
            THEN 'Funds Out - POS Purchase - Charitable'
        WHEN ((txn_agg.POS_ENTRY_MODE IN ("01", "81") AND txn_agg.MERCHANT_CATEGORY_CD IN (6012)) OR txn_agg.MERCHANT_CATEGORY_CD IN ( 6530, 6536, 6537, 6538 ))
            THEN 'Funds Out - POS Purchase - Virtual'
        ELSE  'Funds Out - POS Purchase'
    END)
    """

POSR_DESCRIPTION = """
    (CASE
        WHEN txn_agg.MERCHANT_CATEGORY_CD IN (5944 ,5094, 7631, 5933, 5972)
            THEN 'Funds In - Merchant Credits - Jewelry'
        WHEN txn_agg.MERCHANT_CATEGORY_CD IN (8398,8651,8661)
            THEN 'Funds In - Merchant Credits - Charitable'
        WHEN ((txn_agg.POS_ENTRY_MODE IN ("01", "81") AND txn_agg.MERCHANT_CATEGORY_CD in (6012)) OR txn_agg.MERCHANT_CATEGORY_CD IN ( 6530, 6536, 6537, 6538 ))
            THEN 'Funds In - Merchant Credits - Virtual'
        ELSE 'Funds In - Merchant Credits'
    END)
    """
NA_DESCRIPTION = """
                    CASE
                        WHEN txn_agg.RAIL_TYPE = 'NA'
                            AND txn_agg.CLASS_CD = 'PR'
                            AND txn_agg.TRANSACTION_DEBIT_CREDIT_IND = 'D'
                            AND ((txn_agg.MERCHANT_CATEGORY_CD IN (3551,3555,3561,3564,3582,3596,3597,3620,3624,3628,3662,3667,3669,3676,3679,3682,3708,3712,3726,3728,3730,3731,3737,3738,3761,3762,3764,3765,3766,3767,3768,3769,3771,3792,3794,3795,7800,7802,6050,9402,4829))
                                OR (txn_agg.MERCHANT_CATEGORY_CD IN (6051,7995) AND COALESCE(txn_agg.POS_ENTRY_MODE, '') NOT IN ('10','81')))
                            THEN 'Funds Out - Quasi Cash Advance'
                        WHEN txn_agg.RAIL_TYPE = 'NA'
                            AND txn_agg.CLASS_CD = 'PR'
                            AND ((txn_agg.pos_entry_mode IN ('10', '81') AND MERCHANT_CATEGORY_CD in (7995, 6051)) OR (MERCHANT_CATEGORY_CD = 7801))
                            THEN 'Funds Out - Purchase - Online Gambling'
                        WHEN txn_agg.RAIL_TYPE = 'NA'
                            AND ref_rail_with_dp.TRANSACTION_DESC IS NOT NULL
                            THEN ref_rail_with_dp.TRANSACTION_DESC
                        WHEN txn_agg.RAIL_TYPE = 'NA'
                            AND ref_rail_no_dp.TRANSACTION_DESC IS NOT NULL
                            THEN ref_rail_no_dp.TRANSACTION_DESC
                        ELSE NULL
                    END
                 """
NA_TRANSACTION_TYPE = """
                        CASE
                            WHEN txn_agg.RAIL_TYPE = 'NA'
                                AND txn_agg.CLASS_CD = 'PR'
                                AND txn_agg.TRANSACTION_DEBIT_CREDIT_IND = 'D'
                                AND ((txn_agg.MERCHANT_CATEGORY_CD IN (3551,3555,3561,3564,3582,3596,3597,3620,3624,3628,3662,3667,3669,3676,3679,3682,3708,3712,3726,3728,3730,3731,3737,3738,3761,3762,3764,3765,3766,3767,3768,3769,3771,3792,3794,3795,7800,7802,6050,9402,4829))
                                    OR (txn_agg.MERCHANT_CATEGORY_CD IN (6051,7995) AND COALESCE(txn_agg.POS_ENTRY_MODE, '') NOT IN ('10','81')))
                                THEN 'PURCHASE_DEBIT'
                            WHEN txn_agg.RAIL_TYPE = 'NA'
                                AND txn_agg.CLASS_CD = 'PR'
                                AND ((txn_agg.pos_entry_mode IN ('10', '81') AND MERCHANT_CATEGORY_CD in (7995, 6051)) OR (MERCHANT_CATEGORY_CD = 7801))
                                THEN 'PURCHASE_DEBIT'
                            WHEN txn_agg.RAIL_TYPE = 'NA'
                                AND ref_rail_with_dp.TRANSACTION_TYPE IS NOT NULL
                                THEN ref_rail_with_dp.TRANSACTION_TYPE
                            WHEN txn_agg.RAIL_TYPE = 'NA'
                                AND ref_rail_no_dp.TRANSACTION_TYPE IS NOT NULL
                                THEN ref_rail_no_dp.TRANSACTION_TYPE
                            ELSE NULL
                        END
                      """
NA_TRANSACTION_METHOD = """
                        CASE
                            WHEN txn_agg.RAIL_TYPE = 'NA'
                                AND txn_agg.CLASS_CD = 'PR'
                                AND txn_agg.TRANSACTION_DEBIT_CREDIT_IND = 'D'
                                AND ((txn_agg.MERCHANT_CATEGORY_CD IN (3551,3555,3561,3564,3582,3596,3597,3620,3624,3628,3662,3667,3669,3676,3679,3682,3708,3712,3726,3728,3730,3731,3737,3738,3761,3762,3764,3765,3766,3767,3768,3769,3771,3792,3794,3795,7800,7802,6050,9402,4829))
                                    OR (txn_agg.MERCHANT_CATEGORY_CD IN (6051,7995) AND COALESCE(txn_agg.POS_ENTRY_MODE, '') NOT IN ('10','81')))
                                THEN 'ONLINE'
                            WHEN txn_agg.RAIL_TYPE = 'NA'
                                AND txn_agg.CLASS_CD = 'PR'
                                AND ((txn_agg.pos_entry_mode IN ('10', '81') AND MERCHANT_CATEGORY_CD in (7995, 6051)) OR (MERCHANT_CATEGORY_CD = 7801))
                                THEN 'ONLINE'
                            WHEN txn_agg.RAIL_TYPE = 'NA'
                                AND ref_rail_with_dp.TRANSACTION_METHOD IS NOT NULL
                                THEN ref_rail_with_dp.TRANSACTION_METHOD
                            WHEN txn_agg.RAIL_TYPE = 'NA'
                                AND ref_rail_no_dp.TRANSACTION_METHOD IS NOT NULL
                                THEN ref_rail_no_dp.TRANSACTION_METHOD
                            ELSE NULL
                        END
                      """
TRANSACTION_DESCRIPTION = "ref_rail.TRANSACTION_DESC"
TRANSACTION_TYPE = "ref_rail.TRANSACTION_TYPE"
TRANSACTION_METHOD = "ref_rail.TRANSACTION_METHOD"


def getRailTypeMappping(rail_type: str) -> RailTypeMapping:
    rail_type_upper = rail_type.upper()
    logging.info(f"rail type:{rail_type_upper}")
    rail_type_mapping: RailTypeMapping = RailTypeMappingBase(
        rail_type, PROJECT_CURATED)
    if rail_type_upper in ['BPAY', 'BTFR']:
        rail_type_mapping = RailTypeMappingBPAYBTFR(rail_type, PROJECT_CURATED)
    elif rail_type_upper == 'EDI':
        rail_type_mapping = RailTypeMappingEDI(rail_type, PROJECT_CURATED)
    elif rail_type_upper in ['EPUL', 'EPSH']:
        rail_type_mapping = RailTypeMappingEPULEPSH(rail_type, PROJECT_CURATED)
    elif rail_type_upper == 'TLPY':
        rail_type_mapping = RailTypeMappingTLPY(rail_type, PROJECT_CURATED)
    elif rail_type_upper in ['CCBR', 'ECBR']:
        rail_type_mapping = RailTypeMappingCCBRECBR(rail_type, PROJECT_CURATED)
    elif rail_type_upper in ['PAD']:
        rail_type_mapping = RailTypeMappingPAD(rail_type, PROJECT_CURATED)
    elif rail_type_upper in ['NPFI', 'NPFO']:
        rail_type_mapping = RailTypeMappingNPFINPFO(rail_type, PROJECT_CURATED)
    elif rail_type_upper in ['INAD', 'INAR', 'INRR']:
        rail_type_mapping = RailTypeMappingINADINARINRR(rail_type, PROJECT_CURATED)
    elif rail_type_upper in ['INCA', 'INRC']:
        rail_type_mapping = RailTypeMappingINCAINRC(rail_type, PROJECT_CURATED)
    elif rail_type_upper in ['INSE', 'INMR']:
        rail_type_mapping = RailTypeMappingINSEINMR(rail_type, PROJECT_CURATED)
    else:
        logging.info("Mapping instance of RailTypeMappingBase")
    return rail_type_mapping


def process_transaction_feed(rail_type: str, start_time, end_time, dst_table) -> str:
    if (rail_type.upper() == 'POSP'):
        transaction_desc = POSP_DESCRIPTION
        transaction_type = TRANSACTION_TYPE
        transaction_method = TRANSACTION_METHOD
        join_condition = f"""
                        LEFT OUTER JOIN {REF_RAIL_TABLE} ref_rail
                            ON txn_agg.TRANSACTION_CATEGORY = ref_rail.TRANSACTION_CATEGORY
                            AND txn_agg.TRANSACTION_CD = ref_rail.TRANSACTION_CD
                            AND txn_agg.RAIL_TYPE = ref_rail.RAIL_TYPE
                         """
    elif (rail_type.upper() == 'POSR'):
        transaction_desc = POSR_DESCRIPTION
        transaction_type = TRANSACTION_TYPE
        transaction_method = TRANSACTION_METHOD
        join_condition = f"""
                        LEFT OUTER JOIN {REF_RAIL_TABLE} ref_rail
                            ON txn_agg.TRANSACTION_CATEGORY = ref_rail.TRANSACTION_CATEGORY
                            AND txn_agg.TRANSACTION_CD = ref_rail.TRANSACTION_CD
                            AND txn_agg.RAIL_TYPE = ref_rail.RAIL_TYPE
                         """
    elif ((rail_type.upper() == 'ABM') or (rail_type.upper() == 'CCBR') or (rail_type.upper() == 'PAD')):
        transaction_desc = TRANSACTION_DESCRIPTION
        transaction_type = TRANSACTION_TYPE
        transaction_method = TRANSACTION_METHOD
        join_condition = f"""
                        LEFT OUTER JOIN {REF_RAIL_TABLE} ref_rail
                            ON txn_agg.TRANSACTION_CATEGORY = ref_rail.TRANSACTION_CATEGORY
                            AND txn_agg.TRANSACTION_CD = ref_rail.TRANSACTION_CD
                            AND txn_agg.RAIL_TYPE = ref_rail.RAIL_TYPE
                         """
    elif (rail_type.upper() == 'NA'):
        transaction_desc = NA_DESCRIPTION
        transaction_type = NA_TRANSACTION_TYPE
        transaction_method = NA_TRANSACTION_METHOD
        join_condition = f"""
                        LEFT OUTER JOIN
                        (SELECT
         a.RAIL_TYPE,
         a.INITIATOR,
         a.TRANSACTION_CATEGORY,
         a.TRANSACTION_CD,
         a.TRANSACTION_METHOD,
         a.TRANSACTION_DESC,
         a.TRANSACTION_TYPE
       FROM
         {REF_RAIL_TABLE} a INNER JOIN (
                                              SELECT TRANSACTION_CATEGORY,
                                                     TRANSACTION_CD
                                              FROM {REF_RAIL_TABLE}
                                              WHERE DP_REF_NO IS NULL
                                              GROUP BY TRANSACTION_CATEGORY,TRANSACTION_CD
                                              HAVING COUNT(1) = 1
                                            ) b ON a.TRANSACTION_CATEGORY = b.TRANSACTION_CATEGORY
                                                 AND  a.TRANSACTION_CD = b.TRANSACTION_CD
       WHERE a.DP_REF_NO IS NULL)
        AS ref_rail_no_dp
        ON txn_agg.TRANSACTION_CATEGORY = ref_rail_no_dp.TRANSACTION_CATEGORY
            AND txn_agg.TRANSACTION_CD = ref_rail_no_dp.TRANSACTION_CD
    LEFT OUTER JOIN
        (SELECT
         a.RAIL_TYPE,
         a.INITIATOR,
         a.TRANSACTION_CATEGORY,
         a.TRANSACTION_CD,
         a.TRANSACTION_METHOD,
         a.TRANSACTION_DESC,
         a.TRANSACTION_TYPE,
         a.DP_REF_NO
       FROM
         {REF_RAIL_TABLE} a INNER JOIN (
                                              SELECT TRANSACTION_CATEGORY,
                                                     TRANSACTION_CD,
                                                     DP_REF_NO
                                              FROM {REF_RAIL_TABLE}
                                              WHERE   DP_REF_NO IS NOT NULL
                                              GROUP BY TRANSACTION_CATEGORY,TRANSACTION_CD,DP_REF_NO
                                              HAVING count(1) = 1
                                            ) b ON a.TRANSACTION_CATEGORY = b.TRANSACTION_CATEGORY
                                                 AND  a.TRANSACTION_CD = b.TRANSACTION_CD
                                                 AND  a.DP_REF_NO = b.DP_REF_NO
       WHERE a.DP_REF_NO IS NOT NULL) AS ref_rail_with_dp
        ON  txn_agg.TRANSACTION_CATEGORY = ref_rail_with_dp.TRANSACTION_CATEGORY
            AND txn_agg.TRANSACTION_CD = ref_rail_with_dp.TRANSACTION_CD
            AND ref_rail_with_dp.DP_REF_NO = SUBSTR(txn_agg.Ref_No , 12 , 2)
                          """
    else:
        transaction_desc = TRANSACTION_DESCRIPTION
        transaction_type = TRANSACTION_TYPE
        transaction_method = TRANSACTION_METHOD
        join_condition = f"""
                        LEFT OUTER JOIN {REF_RAIL_TABLE} ref_rail
                            ON txn_agg.TRANSACTION_CATEGORY = ref_rail.TRANSACTION_CATEGORY
                            AND txn_agg.TRANSACTION_CD = ref_rail.TRANSACTION_CD
                            AND ref_rail.DP_REF_NO = SUBSTR(txn_agg.Ref_No , 12 , 2)
                            AND txn_agg.RAIL_TYPE = ref_rail.RAIL_TYPE
                         """

    # funds move mapping
    rail_type_mapping: RailTypeMapping = getRailTypeMappping(rail_type)
    # NEW: Get CTE definitions
    cte_definitions = rail_type_mapping.get_cte_definitions()

    funds_move_join = rail_type_mapping.get_funds_move_join()
    conductor_external_cust_first_name = rail_type_mapping.get_conductor_external_cust_first_name()
    device_id = rail_type_mapping.get_device_id()
    ip_address = rail_type_mapping.get_ip_address()
    user_name = rail_type_mapping.get_user_name()
    session_date_time = rail_type_mapping.get_session_date_time()
    device_type = rail_type_mapping.get_device_type()
    conductor_external_cust_no = rail_type_mapping.get_conductor_external_cust_no()
    conductor_external_cust_last_name = rail_type_mapping.get_conductor_external_cust_last_name()
    external_bank_number = rail_type_mapping.get_external_bank_number()
    external_cust_name = rail_type_mapping.get_external_cust_name()
    interac_ref_number = rail_type_mapping.get_interac_ref_number()
    interac_email = rail_type_mapping.get_interac_email()
    interac_phone = rail_type_mapping.get_interac_phone()

    # Set value of conductor first name based on the rail type
    if rail_type.upper() != "INAD":
        conductor_external_cust_first_name = f"""
                                CASE
                                    WHEN txn_agg.INITIATOR = 'PCB'
                                        THEN txn_agg.CUSTOMER_FIRST_NAME
                                    WHEN txn_agg.INITIATOR = 'EXTERNAL'
                                        THEN {conductor_external_cust_first_name}
                                    ELSE NULL
                                END
                              """
    # Set value of payee first name and transfer account number based on the rail type
    payee_first_name = "NULL"
    if rail_type.upper() in ["INMR", "BPAY"]:
        payee_first_name = rail_type_mapping.get_payee_first_name()

    transfer_account_number = "NULL"
    if rail_type.upper() == "BPAY":
        transfer_account_number = rail_type_mapping.get_transfer_account_number()
    # end of funds move mapping
    # POS fields mapping
    atm_pos_terminal_id = "NULL"
    atm_pos_terminal_address = "NULL"
    atm_pos_terminal_city = "NULL"
    atm_pos_terminal_state = "NULL"
    atm_pos_terminal_country = "NULL"
    pos_entry_mode = "NULL"

    if rail_type.upper() in ["POSP", "POSR", "NA", "ABM"]:
        atm_pos_terminal_id = "txn_agg.MERCHANT_TERMINAL_ID"
        atm_pos_terminal_address = "txn_agg.MERCHANT_ZIP_CD"
        atm_pos_terminal_city = "txn_agg.MERCHANT_CITY"
        atm_pos_terminal_state = "txn_agg.MERCHANT_STATE"
        atm_pos_terminal_country = "txn_agg.MERCHANT_COUNTRY"
        pos_entry_mode = "txn_agg.POS_ENTRY_MODE"

    # end of POS fields mapping

    query = f"""
    CREATE OR REPLACE TABLE {dst_table} AS
    {cte_definitions}  -- NEW: FMC_UNNESTED CTE before CREATE TABLE (empty string if not needed)
    SELECT
        '320'                                                                 AS INSTITUTION_NUMBER,
        txn_agg.TRANSACTION_ID                                                AS TRANSACTION_ID,
        txn_agg.PRIMARY_CUSTOMER_NO                                           AS CUSTOMER_NUMBER,
        txn_agg.ACCOUNT_NO                                                    AS ACCOUNT_NUMBER,
        txn_agg.TRANSACTION_DT                                                AS TRANSACTION_DATE_TIME,
        CAST(txn_agg.POSTED_DT AS STRING FORMAT 'YYYY-MM-DD')                 AS POSTED_DATE,
        CASE
            WHEN txn_agg.REVERSE_IND = 'V'
                THEN SUBSTRING( txn_agg.TRANSACTION_DT, 1,10)
            ELSE
                NULL
        END                                                                   AS REVERSE_DATE,
        NULL                                                                  AS REVERSE_TRANSACTION_NUMBER,
        CASE
            WHEN txn_agg.TRANSACTION_DEBIT_CREDIT_IND = 'D'
                THEN
                FORMAT("%.2f",txn_agg.FINAL_AMT*-1)
            ELSE
                FORMAT("%.2f",txn_agg.FINAL_AMT)
        END                                                                   AS TOTAL_VALUE,
        CASE
            WHEN txn_agg.CURRENCY_CD <> 'CAD'
                THEN
                FORMAT("%.2f",txn_agg.ORIGINAL_AMT)
            ELSE
                NULL
        END                                                                   AS FOREIGN_TOTAL_VALUE,
        txn_agg.CURRENCY_CD                                                   AS TRANSACTION_CURRENCY,
        FORMAT("%.5f",txn_agg.FOREIGN_EXCHANGE_RATE)                          AS CURRENCY_RATE,
        txn_agg.TRANSACTION_CATEGORY || ' ' || txn_agg.TRANSACTION_CD         AS TRANSACTION_CODE,
        {transaction_method}                                                  AS TRANSACTION_METHOD,
        {transaction_type}                                                    AS TRANSACTION_TYPE,
        NULL                                                                  AS CARD_NUMBER,
        NULL                                                                  AS CHECK_RETURN_CODE,
        NULL                                                                  AS TIMES_RETURNED,
        {transaction_desc}                                                    AS TRANSACTION_DESCRIPTION_1,
        txn_agg.TRANSACTION_DESC                                              AS TRANSACTION_DESCRIPTION_2,
        CASE
            WHEN txn_agg.PRODUCT_TYPE = 'CREDIT-CARD'
                THEN 'PCMC'
            ELSE 'PCMA'
        END                                                                   AS TRANSACTION_DESCRIPTION_3,
        NULL                                                                  AS TRANSACTION_DESCRIPTION_4,
        txn_agg.MERCHANT_CATEGORY_CD_DESC                                     AS TRANSACTION_DESCRIPTION_5,
        NULL                                                                  AS TELLER_NUMBER,
        NULL                                                                  AS BRANCH_OF_TRANSACTION,
        CASE WHEN txn_agg.INITIATOR = 'PCB' THEN txn_agg.CUSTOMER_NO
          WHEN txn_agg.INITIATOR = 'EXTERNAL' THEN {conductor_external_cust_no}
           ELSE NULL
        END                                                                   AS CONDUCTOR_CUSTOMER_NUMBER,
        {conductor_external_cust_first_name}                                  AS CONDUCTOR_FIRST_NAME,
        CASE
            WHEN txn_agg.INITIATOR = 'PCB'
                THEN txn_agg.CUSTOMER_LAST_NAME
            WHEN txn_agg.INITIATOR = 'EXTERNAL'
                THEN {conductor_external_cust_last_name}
            ELSE NULL
        END                                                                   AS CONDUCTOR_LAST_NAME,
        NULL                                                                  AS CONDUCTOR_TIN,
        NULL                                                                  AS PHOTO_IDENTIFICATION_TYPE,
        NULL                                                                  AS PHOTO_ID_NUMBER,
        NULL                                                                  AS PHOTO_ID_STATE,
        NULL                                                                  AS PHOTO_ID_COUNTRY,
        {atm_pos_terminal_id}                                                 AS ATM_POS_TERMINAL_ID,
        {atm_pos_terminal_address}                                            AS ATM_POS_TERMINAL_ADDRESS,
        {atm_pos_terminal_city}                                               AS ATM_POS_TERMINAL_CITY,
        {atm_pos_terminal_state}                                              AS ATM_POS_TERMINAL_STATE,
        {atm_pos_terminal_country}                                            AS ATM_POS_TERMINAL_COUNTRY,
        CASE
            WHEN txn_agg.MERCHANT_CATEGORY_CD IS NOT NULL
                THEN LPAD(CAST(txn_agg.MERCHANT_CATEGORY_CD AS STRING), 4, '0')
            ELSE '5399'
        END                                                                   AS MERCHANT_CATEGORY_CODE,
        {pos_entry_mode}                                                      AS POS_ENTRY_MODE,
        NULL                                                                  AS POS_CARD_PRESENCE_INDICATOR,
        AUTHORIZATION_CD                                                      AS AUTHORIZATION_CODE,
        'ACCEPTED'                                                            AS RESPONSE_CODE,
        NULL                                                                  AS REQUEST_AUTHORIZATION_AMOUNT,
        NULL                                                                  AS REQUEST_COMPLETION_AMOUNT,
        NULL                                                                  AS ADDRESS_VERIFICATION_SERVICE_CODE,
        NULL                                                                  AS PROCESSOR_AUTHENTICATION_FIELD,
        MERCHANT_NAME                                                         AS MERCHANT_NAME,
        NULL                                                                  AS CHECK_ITEM_NUMBER,
        NULL                                                                  AS INSTITUTION_ITEM_SEQUENCE_NUMBER,
        NULL                                                                  AS CHECK_SEQUENCE_NUMBER,
        NULL                                                                  AS CHECK_ACCOUNT_NUMBER,
        NULL                                                                  AS CHECK_ROUTING_TRANSIT_NUMBER,
        {payee_first_name}                                                    AS PAYEE_FIRST_NAME,
        NULL                                                                  AS PAYEE_LAST_NAME,
        NULL                                                                  AS TRANSFER_BRANCH_NUMBER,
        NULL                                                                  AS TRANSFER_CUSTOMER_NUMBER,
        NULL                                                                  AS TRANSFER_ACCOUNT_BRANCH,
        {transfer_account_number}                                             AS TRANSFER_ACCOUNT_NUMBER,
        NULL                                                                  AS PRINCIPAL_AMOUNT,
        NULL                                                                  AS INTEREST_AMOUNT,
        NULL                                                                  AS OTHER_LOAN_AMOUNT,
        'GCP'                                                                 AS SYSTEM_ID,
        NULL                                                                  AS FINTECH_PROGRAM,
        {external_bank_number}                                                AS EXTERNAL_BANK_NUMBER,
        {external_cust_name}                                                  AS EXTERNAL_CUSTOMER_NAME,
        {device_id}                                                           AS DEVICE_ID,
        {ip_address}                                                          AS IP_ADDRESS,
        {user_name}                                                           AS USERNAME,
        {session_date_time}                                                   AS SESSION_DATE_TIME,
        {interac_ref_number}                                                  AS INTERAC_REF_NUMBER,
        {interac_email}                                                       AS INTERAC_EMAIL,
        {interac_phone}                                                       AS INTERAC_PHONE,
        NULL                                                                  AS INTERAC_BANK_ACCOUNT,
        NULL                                                                  AS PCF_MONEY_MOVE_REF_NUMBER,
        {device_type}                                                         AS DEVICE_TYPE
    FROM
        {TRANSACTION_AGGREGATE_TABLE} txn_agg
        {join_condition}
        {funds_move_join}
    WHERE
        txn_agg.RAIL_TYPE = '{rail_type}'
        AND txn_agg.REC_LOAD_TIMESTAMP BETWEEN '{start_time}' AND '{end_time}'
    """
    return query
