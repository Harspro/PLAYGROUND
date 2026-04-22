# Class for rails : Helper Class for BPAY & BTFR
class RailTypeMappingHelper:
    @staticmethod
    def get_customer_name_from_bill_pay():
        return """CASE
                        WHEN
                            bp.B_CUST_FN IS NULL AND bp.B_CUST_LN IS NULL
                        THEN
                            NULL
                        WHEN
                            bp.B_CUST_FN IS NULL AND bp.B_CUST_LN IS NOT NULL
                        THEN
                            bp.B_CUST_LN
                        WHEN
                            bp.B_CUST_FN IS NOT NULL AND bp.B_CUST_LN IS NULL
                        THEN
                            bp.B_CUST_FN
                        ELSE
                            TRIM(COALESCE(bp.B_CUST_FN,'') || ' ' || COALESCE(bp.B_CUST_LN,''))
                    END"""

    @staticmethod
    def get_device_id_from_user_detail():
        return "user_detail.DEVICE_ID"

    @staticmethod
    def get_ip_address_from_user_detail():
        return "user_detail.IP_ADD"

    @staticmethod
    def get_user_name_from_user_detail():
        return "user_detail.USER_ID"

    @staticmethod
    def get_session_date_time_from_user_detail():
        return "user_detail.SESSION_DT"

    @staticmethod
    def get_device_type_from_user_detail():
        return "user_detail.DEVICE_TYPE"

    @staticmethod
    def get_conductor_external_cust_no_from_agg():
        return "txn_agg.customer_no"

    @staticmethod
    def get_conductor_external_cust_last_name_from_agg():
        return "txn_agg.customer_last_name"

    @staticmethod
    def get_conductor_external_cust_first_name_from_agg():
        return "txn_agg.customer_first_name"
