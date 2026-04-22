CREATE OR REPLACE VIEW pcb-{env}-processing.domain_account_management.OTHER_BLOCK_CODES
  OPTIONS (
    expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))
AS
SELECT
  cifp_account_id5,
  cifp_block_code,
  customer_uid
FROM
  pcb-{env}-processing.domain_account_management.CONCAT_BLOCK_CODES
    UNPIVOT(
      cifp_block_code
        FOR
          status_code_name IN (
            cifp_block_code_01, cifp_block_code_02, cifp_block_code_03,
            cifp_block_code_04, cifp_block_code_05, cifp_block_code_06,
            cifp_block_code_07, cifp_block_code_08, cifp_block_code_09,
            cifp_block_code_10, cifp_block_code_11, cifp_block_code_12,
            cifp_block_code_13, cifp_block_code_14, cifp_block_code_15,
            cifp_block_code_16, cifp_block_code_17, cifp_block_code_18,
            cifp_block_code_19, cifp_block_code_20, cifp_block_code_21,
            cifp_block_code_22, cifp_block_code_23, cifp_block_code_24,
            cifp_block_code_25, cifp_block_code_26, cifp_block_code_27,
            cifp_block_code_28, cifp_block_code_29, cifp_block_code_30,
            cifp_block_code_31, cifp_block_code_32, cifp_block_code_33,
            cifp_block_code_34, cifp_block_code_35, cifp_block_code_36,
            cifp_block_code_37, cifp_block_code_38, cifp_block_code_39,
            cifp_block_code_40, cifp_block_code_41, cifp_block_code_42,
            cifp_block_code_43, cifp_block_code_44, cifp_block_code_45,
            cifp_block_code_46, cifp_block_code_47, cifp_block_code_48,
            cifp_block_code_49, cifp_block_code_50, cifp_block_code_51,
            cifp_block_code_52, cifp_block_code_53, cifp_block_code_54,
            cifp_block_code_55, cifp_block_code_56, cifp_block_code_57,
            cifp_block_code_58, cifp_block_code_59, cifp_block_code_60,
            cifp_block_code_61, cifp_block_code_62, cifp_block_code_63,
            cifp_block_code_64, cifp_block_code_65, cifp_block_code_66,
            cifp_block_code_67, cifp_block_code_68, cifp_block_code_69,
            cifp_block_code_70, cifp_block_code_71, cifp_block_code_72,
            cifp_block_code_73, cifp_block_code_74, cifp_block_code_75,
            cifp_block_code_76, cifp_block_code_77, cifp_block_code_78,
            cifp_block_code_79, cifp_block_code_80, cifp_block_code_81,
            cifp_block_code_82))
WHERE
  cifp_block_code IN (
    'CR07', 'CR13', 'CRA1', 'CRA2', 'CRA3', 'CRA4', 'CRA5', 'CRA6', 'CRA7',
    'CRA8', 'CRBB', 'CRBD', 'CRCD', 'CRDC', 'CROL', 'CROT', 'CRPD', 'CRTR',
    'CO07', 'CO13', 'COBD', 'CODC', 'COFR', 'COST', 'INA', 'INC', 'SFAT',
    'SFCD', 'SFFA', 'SFFL', 'SFFN', 'SFIA', 'SFIF', 'SFLS', 'SFMP', 'SFNR',
    'SFOT', 'SFST', 'SF3D', 'SF4A', 'SF4L', 'SF4U', 'WASF');
