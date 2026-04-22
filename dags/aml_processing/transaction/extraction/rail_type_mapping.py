class RailTypeMapping():
    """An interface for specific rail types to return customized joining SQL statement fragment"""

    def __init__(self, rail_type, project_curated):
        self.rail_type = rail_type
        self.rail_type_upper = rail_type.upper()
        self.project_curated = project_curated
        self.funds_gl_join = RailTypeMappingBase._get_funds_gl_join(self.rail_type, self.project_curated)
        self.user_detail_join = RailTypeMappingBase._get_user_detail_join(self.project_curated)

    def get_funds_move_join(self) -> str:
        pass

    def get_interac_ref_number(self) -> str:
        pass

    def get_interac_email(self) -> str:
        pass

    def get_interac_phone(self) -> str:
        pass

    def get_external_cust_name(self) -> str:
        pass

    def get_conductor_external_cust_first_name(self) -> str:
        pass

    def get_device_id(self) -> str:
        pass

    def get_ip_address(self) -> str:
        pass

    def get_user_name(self) -> str:
        pass

    def get_session_date_time(self) -> str:
        pass

    def get_device_type(self) -> str:
        pass

    def get_conductor_external_cust_no(self) -> str:
        pass

    def get_conductor_external_cust_last_name(self) -> str:
        pass

    def get_external_bank_number(self) -> str:
        pass

    def get_payee_first_name(self) -> str:
        pass

    def get_transfer_account_number(self) -> str:
        pass

    def get_cte_definitions(self) -> str:
        pass


class RailTypeMappingBase(RailTypeMapping):

    def __init__(self, rail_type: str, project_curated: str):
        super().__init__(rail_type, project_curated)

    def get_funds_move_join(self) -> str:
        return ""

    def get_external_cust_name(self) -> str:
        return "NULL"

    def get_conductor_external_cust_first_name(self) -> str:
        return "NULL"

    def get_device_id(self) -> str:
        return "NULL"

    def get_ip_address(self) -> str:
        return "NULL"

    def get_user_name(self) -> str:
        return "NULL"

    def get_session_date_time(self) -> str:
        return "NULL"

    def get_device_type(self) -> str:
        return "NULL"

    def get_conductor_external_cust_no(self) -> str:
        return "NULL"

    def get_conductor_external_cust_last_name(self) -> str:
        return "NULL"

    def get_external_bank_number(self) -> str:
        return "NULL"

    def get_interac_ref_number(self) -> str:
        return "NULL"

    def get_interac_email(self) -> str:
        return "NULL"

    def get_interac_phone(self) -> str:
        return "NULL"

    def get_payee_first_name(self) -> str:
        return "NULL"

    def get_transfer_account_number(self) -> str:
        return "NULL"

    def get_cte_definitions(self) -> str:
        """
            Get CTE definitions for this rail type.
            Returns empty string if no CTEs are needed.
        """
        fmc_cte = RailTypeMappingBase._get_fmc_unnested_cte(self.rail_type, self.project_curated)

        if not fmc_cte:
            return ""  # No CTEs needed for curated view rails

        return fmc_cte

    @staticmethod
    def _get_fmc_unnested_cte(rail_type: str, project_curated: str) -> str:
        """
            Generate FMC_UNNESTED CTE for rails using PCB_FUNDS_MOVE_CREATED.
        """
        rail_type_upper = rail_type.upper()
        # Rails that do not need fmc_cte POSP,POSR,ABM,MCBR,MMOV,NA,MVMN
        if rail_type_upper in ['POSP', 'POSR', 'ABM', 'MCBR', 'MMOV', 'NA', 'MVMN']:
            return ""
        # Build field list based on rail type
        fields = [
            "FMC.PAYMENT_RAIL",
            "FMC.INGESTION_TIMESTAMP",
            "FMC.fiToFiCustomerCreditTransfer.supplementaryData",
            "FMC.fiToFiCustomerCreditTransfer.groupHeader.creationDateTime AS creationDateTime",
            # Base funds GL fields
            "FTFCCTI.paymentIdentification.clearingSystemReference AS clearingSystemReference",
            "FTFCCTI.interbankSettlementAmount.amount AS amount",
            "FTFCCTI.debtorAccount.identification.other.identification AS debtor_account_id",
        ]

        # Add rail-specific fields
        if rail_type_upper in ['BPAY', 'BTFR']:
            fields.extend([
                "FTFCCTI.creditorAccount.identification.other.identification AS creditor_account_id",
                "FTFCCTI.creditor.name AS creditor_name",
                "FTFCCTI.debtor.name AS debtor_name",
            ])
        elif rail_type_upper in ['NPFI', 'NPFO']:
            fields.extend([
                "FTFCCTI.debtor.name AS debtor_name",
            ])
        elif rail_type_upper in ['INSE', 'INMR']:
            fields.extend([
                "FTFCCTI.debtor.name AS debtor_name",
                "FTFCCTI.creditor.name AS creditor_name",
                "FTFCCTI.creditor.contactDetails.emailAddress AS creditor_email",
                "FTFCCTI.creditor.contactDetails.phoneNumber AS creditor_phone",
            ])
        elif rail_type_upper in ['INCA', 'INRC']:
            fields.extend([
                "FTFCCTI.debtor.name AS debtor_name",
            ])
        elif rail_type_upper in ['EPUL', 'EPSH']:
            fields.extend([
                "FTFCCTI.debtor.name AS debtor_name",
            ])
        elif rail_type_upper in ['INAD', 'INAR', 'INRR']:
            fields.extend([
                "FTFCCTI.debtor.name AS debtor_name",
                "FTFCCTI.creditor.name AS creditor_name",
                "FTFCCTI.creditorAccount.identification.other.identification AS creditor_account_id",
                "FTFCCTI.paymentIdentification.transactionIdentification AS transactionIdentification",
                "FTFCCTI.acceptanceDateTime AS acceptanceDateTime",
            ])

        # Add customer ID extraction (for user detail join)
        fields.extend([
            "(SELECT identification FROM UNNEST(IFNULL(FTFCCTI.debtor.identification.privateIdentification.other, [])) "
            "WHERE schemeName.code = 'CUST' LIMIT 1) AS debtor_customer_id",
            "(SELECT identification FROM UNNEST(IFNULL(FTFCCTI.creditor.identification.privateIdentification.other, [])) "
            "WHERE schemeName.code = 'CUST' LIMIT 1) AS creditor_customer_id",
            # Pre-compute instruction ID
            "CAST((SELECT envelope.any FROM UNNEST(FMC.fiToFiCustomerCreditTransfer.supplementaryData) "
            "WHERE placeAndName='InstructionData.instructionId') AS INT64) AS instruction_id"
        ])

        # Build WHERE clause
        if rail_type_upper in ['NPFI', 'NPFO']:
            where_clause = "FMC.PAYMENT_RAIL IN ('NPFI', 'NPFO')"
        elif rail_type_upper in ['INAD', 'INAR', 'INRR']:
            where_clause = "FMC.PAYMENT_RAIL IN ('INAD', 'INAR', 'INRR')"
        else:
            where_clause = f"FMC.PAYMENT_RAIL = '{rail_type_upper}'"

        fields_str = ',\n        '.join(fields)
        cte = f"""
            WITH FMC_UNNESTED AS (
            -- SINGLE UNNEST: Extract all fields needed across all joins
            SELECT
                {fields_str}
            FROM
                `{project_curated}.domain_payments.PCB_FUNDS_MOVE_CREATED` FMC,
                UNNEST(FMC.fiToFiCustomerCreditTransfer.creditTransferTransactionInformation) AS FTFCCTI
            WHERE
                {where_clause}
        )"""

        return cte

    # Static method for common fundsmove join
    @staticmethod
    def _get_funds_gl_join(rail_type: str, project_curated: str):
        """
        Generate funds GL join.
        - For rails using PCB_FUNDS_MOVE_CREATED: Uses FMC_UNNESTED CTE
        """
        rail_type_upper = rail_type.upper()
        # Rails using PCB_FUNDS_MOVE_CREATED (via FMC_UNNESTED CTE)
        if rail_type_upper not in ['NPFO', 'EDI', 'PAD', 'TLPY']:
            funds_gl_join = """
                LEFT JOIN (
                    SELECT
                        instruction_id AS FUNDSMOVE_INSTRUCTION_ID,
                        DATETIME(creationDateTime, 'America/Toronto') AS FUNDSMOVE_PROCESSING_DT,
                        clearingSystemReference AS REFERENCE_NUMBER,
                        debtor_account_id AS ACCESS_MEDIUM_NO,
                        amount AS AMOUNT,
                        NULL AS TRANS_CODE,
                        FORMAT_TIMESTAMP('%Y-%m-%d', DATETIME(creationDateTime, 'America/Toronto')) AS TRANS_DT,
                        ROW_NUMBER() OVER (PARTITION BY clearingSystemReference,
                                            debtor_account_id,
                                            amount,
                                            NULL,
                                            FORMAT_TIMESTAMP('%Y-%m-%d', DATETIME(creationDateTime, 'America/Toronto'))
                                    ORDER BY INGESTION_TIMESTAMP DESC NULLS LAST) REC_RANK
                    FROM
                        FMC_UNNESTED
                ) fm_gl_req ON 1 = 1
                AND substr(trim(txn_agg.REF_NO), 12, 11) = fm_gl_req.REFERENCE_NUMBER
                AND txn_agg.CARD_NO = fm_gl_req.ACCESS_MEDIUM_NO
                AND txn_agg.FINAL_AMT = fm_gl_req.AMOUNT
                AND (SUBSTR(txn_agg.TRANSACTION_DT, 0, 10) = fm_gl_req.TRANS_DT OR
                DATE_SUB(SAFE_CAST(SUBSTR(txn_agg.TRANSACTION_DT,0,10) AS DATE), INTERVAL 1 DAY) = DATE(fm_gl_req.TRANS_DT))
                AND fm_gl_req.REC_RANK = 1
            """
        else:
            funds_gl_join = f"""
                LEFT JOIN  (
                    SELECT
                        fmgr.FUNDSMOVE_INSTRUCTION_ID ,
                        fmgr.FUNDSMOVE_PROCESSING_DT,
                        fmgr.REFERENCE_NUMBER,
                        fmgr.ACCESS_MEDIUM_NO,
                        fmgr.AMOUNT,
                        fmgr.TRANS_CODE,
                        FORMAT_TIMESTAMP('%Y-%m-%d',fmgr.TRANS_DT) TRANS_DT,
                        ROW_NUMBER() OVER (PARTITION BY fmgr.REFERENCE_NUMBER,
                                                fmgr.ACCESS_MEDIUM_NO,
                                                fmgr.AMOUNT,
                                                fmgr.TRANS_CODE,
                                                FORMAT_TIMESTAMP('%Y-%m-%d',fmgr.TRANS_DT)
                                    ORDER BY fmgr.UPDATE_DT DESC NULLS LAST) REC_RANK
                    FROM(
                        SELECT
                            FUNDSMOVE_INSTRUCTION_ID,
                            FUNDSMOVE_PROCESSING_DT,
                            REFERENCE_NUMBER,
                            ACCESS_MEDIUM_NO,
                            AMOUNT,
                            TRANS_CODE,
                            TRANS_DT,
                            FUNDSMOVE_GL_REQ_UID,
                            UPDATE_DT
                        FROM
                            `{project_curated}.domain_payments.FUNDSMOVE_GL_REQ`
                        ) AS fmgr
                        INNER JOIN (
                            SELECT
                                INSTRUCTION_ID,
                                FUNDSMOVE_TYPE_UID
                            FROM
                                `{project_curated}.domain_payments.FUNDSMOVE_INSTRUCTION`
                        ) AS fmi
                        ON fmgr.FUNDSMOVE_INSTRUCTION_ID = fmi.INSTRUCTION_ID
                        INNER JOIN (
                            SELECT
                                FUNDSMOVE_TYPE_UID,
                                TYPE_ID
                            FROM
                                `{project_curated}.domain_payments.FUNDSMOVE_TYPE`
                            WHERE
                                TYPE_ID = '{rail_type_upper}') fmt
                                ON fmi.FUNDSMOVE_TYPE_UID = fmt.FUNDSMOVE_TYPE_UID
                        INNER JOIN  (
                            SELECT
                                FUNDSMOVE_GL_REQ_UID,
                                STATUS
                            FROM
                                `{project_curated}.domain_payments.FUNDSMOVE_GL_REQ_STATUS`
                            WHERE
                                STATUS = 'TRANSMITTED'
                            ) fmglstatus
                            ON fmgr.FUNDSMOVE_GL_REQ_UID = fmglstatus.FUNDSMOVE_GL_REQ_UID
                )  fm_gl_req ON 1 = 1
                AND substr(trim(txn_agg.REF_NO), 12, 11)    = fm_gl_req.REFERENCE_NUMBER
                AND txn_agg.CARD_NO                         = fm_gl_req.ACCESS_MEDIUM_NO
                AND txn_agg.FINAL_AMT                       = fm_gl_req.AMOUNT
                AND txn_agg.TRANSACTION_CD                  = fm_gl_req.TRANS_CODE
                AND (SUBSTR(txn_agg.TRANSACTION_DT, 0, 10)  = fm_gl_req.TRANS_DT OR
                DATE_SUB(SAFE_CAST(SUBSTR(txn_agg.TRANSACTION_DT,0,10) AS DATE) , INTERVAL 1 DAY) = DATE(fm_gl_req.TRANS_DT))
                AND fm_gl_req.REC_RANK = 1
        """

        return funds_gl_join

    # Static method for common user_detail code
    @staticmethod
    def _get_user_detail_join(project_curated: str) -> str:
        user_detail_join = f"""
            LEFT JOIN (
                        SELECT
                            NULL AS IP_ADD,  -- TODO: IP_ADDRESS not yet in PCB_FUNDS_MOVE_CREATED supplementaryData
                            NULL AS DEVICE_ID,  -- TODO: DEVICE_ID not yet in PCB_FUNDS_MOVE_CREATED supplementaryData
                            login_details.PLATFORM_USER_ID AS USER_ID,
                            FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S",
                            DATETIME(creationDateTime, 'America/Toronto')) AS SESSION_DT,
                            instruction_id AS FMI_INSTRUCTION_ID,
                            login_details.DEVICE_TYPE,
                            login_details.CUSTOMER_NO,
                            login_details.CUSTOMER_GIVEN_NAME,
                            login_details.CUSTOMER_SURNAME,
                            ROW_NUMBER() OVER (PARTITION BY instruction_id
                                    ORDER BY INGESTION_TIMESTAMP DESC NULLS LAST) REC_RANK
                        FROM
                            FMC_UNNESTED
            LEFT JOIN (
                        SELECT
                            pul.PLATFORM_USER_UID AS PLATFORM_USER_ID,
                            ci.CUSTOMER_IDENTIFIER_NO AS CUSTOMER_NO,
                            c.GIVEN_NAME AS CUSTOMER_GIVEN_NAME,
                            c.SURNAME AS CUSTOMER_SURNAME,
                            CASE
                                WHEN COALESCE(pul.WEB_LAST_LOGIN_DATE, DATETIME('1901-01-01 01:00:00')) >=
                                    COALESCE(pul.MOBILE_LAST_LOGIN_DATE, DATETIME('1901-01-01 01:00:00'))
                                THEN 'COMPUTER'
                                WHEN COALESCE(pul.WEB_LAST_LOGIN_DATE, DATETIME('1901-01-01 01:00:00')) <
                                    COALESCE(pul.MOBILE_LAST_LOGIN_DATE, DATETIME('1901-01-01 01:00:00'))
                                THEN 'MOBILE_PHONE'
                                ELSE 'OTHER'
                            END AS DEVICE_TYPE,
                            ROW_NUMBER() OVER (PARTITION BY ci.CUSTOMER_IDENTIFIER_NO
                                    ORDER BY ci.UPDATE_DT DESC NULLS LAST) REC_RANK
                        FROM
                            `{project_curated}.domain_customer_management.PLATFORM_USER` pu
                            INNER JOIN `{project_curated}.domain_customer_management.PLATFORM_USER_LOGIN` pul
                                ON pu.PLATFORM_USER_UID = pul.PLATFORM_USER_UID
                            INNER JOIN `{project_curated}.domain_customer_management.CUSTOMER` c
                                ON pul.PLATFORM_USER_UID = c.PLATFORM_USER_UID
                            INNER JOIN `{project_curated}.domain_customer_management.CUSTOMER_IDENTIFIER` ci
                                ON c.CUSTOMER_UID = ci.CUSTOMER_UID
                                AND ci.TYPE = 'PCF-CUSTOMER-ID'
                    ) login_details
                        ON COALESCE(debtor_customer_id, creditor_customer_id) = login_details.CUSTOMER_NO
                        AND login_details.REC_RANK = 1
            ) AS user_detail
                ON fm_gl_req.FUNDSMOVE_INSTRUCTION_ID = user_detail.FMI_INSTRUCTION_ID
                AND user_detail.REC_RANK = 1
    """

        return user_detail_join
