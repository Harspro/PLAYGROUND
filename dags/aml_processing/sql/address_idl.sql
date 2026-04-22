CREATE TEMP FUNCTION twocharcountrycode(country STRING)
AS (
  CASE UPPER(country)
      WHEN 'AFG' THEN 'AF'
      WHEN 'ALA' THEN 'AX'
      WHEN 'ALB' THEN 'AL'
      WHEN 'DZA' THEN 'DZ'
      WHEN 'ASM' THEN 'AS'
      WHEN 'AND' THEN 'AD'
      WHEN 'AGO' THEN 'AO'
      WHEN 'AIA' THEN 'AI'
      WHEN 'ATA' THEN 'AQ'
      WHEN 'ATG' THEN 'AG'
      WHEN 'ARG' THEN 'AR'
      WHEN 'ARM' THEN 'AM'
      WHEN 'ABW' THEN 'AW'
      WHEN 'AUS' THEN 'AU'
      WHEN 'AUT' THEN 'AT'
      WHEN 'AZE' THEN 'AZ'
      WHEN 'BHS' THEN 'BS'
      WHEN 'BHR' THEN 'BH'
      WHEN 'BGD' THEN 'BD'
      WHEN 'BRB' THEN 'BB'
      WHEN 'BLR' THEN 'BY'
      WHEN 'BEL' THEN 'BE'
      WHEN 'BLZ' THEN 'BZ'
      WHEN 'BEN' THEN 'BJ'
      WHEN 'BMU' THEN 'BM'
      WHEN 'BTN' THEN 'BT'
      WHEN 'BOL' THEN 'BO'
      WHEN 'BES' THEN 'BQ'
      WHEN 'BIH' THEN 'BA'
      WHEN 'BWA' THEN 'BW'
      WHEN 'BVT' THEN 'BV'
      WHEN 'BRA' THEN 'BR'
      WHEN 'IOT' THEN 'IO'
      WHEN 'BRN' THEN 'BN'
      WHEN 'BGR' THEN 'BG'
      WHEN 'BFA' THEN 'BF'
      WHEN 'BDI' THEN 'BI'
      WHEN 'CPV' THEN 'CV'
      WHEN 'KHM' THEN 'KH'
      WHEN 'CMR' THEN 'CM'
      WHEN 'CAN' THEN 'CA'
      WHEN 'CYM' THEN 'KY'
      WHEN 'CAF' THEN 'CF'
      WHEN 'TCD' THEN 'TD'
      WHEN 'CHL' THEN 'CL'
      WHEN 'CHN' THEN 'CN'
      WHEN 'CXR' THEN 'CX'
      WHEN 'CCK' THEN 'CC'
      WHEN 'COL' THEN 'CO'
      WHEN 'COM' THEN 'KM'
      WHEN 'COD' THEN 'CD'
      WHEN 'COG' THEN 'CG'
      WHEN 'COK' THEN 'CK'
      WHEN 'CRI' THEN 'CR'
      WHEN 'CIV' THEN 'CI'
      WHEN 'HRV' THEN 'HR'
      WHEN 'CUB' THEN 'CU'
      WHEN 'CUW' THEN 'CW'
      WHEN 'CYP' THEN 'CY'
      WHEN 'CZE' THEN 'CZ'
      WHEN 'DNK' THEN 'DK'
      WHEN 'DJI' THEN 'DJ'
      WHEN 'DMA' THEN 'DM'
      WHEN 'DOM' THEN 'DO'
      WHEN 'ECU' THEN 'EC'
      WHEN 'EGY' THEN 'EG'
      WHEN 'SLV' THEN 'SV'
      WHEN 'GNQ' THEN 'GQ'
      WHEN 'ERI' THEN 'ER'
      WHEN 'EST' THEN 'EE'
      WHEN 'SWZ' THEN 'SZ'
      WHEN 'ETH' THEN 'ET'
      WHEN 'FLK' THEN 'FK'
      WHEN 'FRO' THEN 'FO'
      WHEN 'FJI' THEN 'FJ'
      WHEN 'FIN' THEN 'FI'
      WHEN 'FRA' THEN 'FR'
      WHEN 'GUF' THEN 'GF'
      WHEN 'PYF' THEN 'PF'
      WHEN 'ATF' THEN 'TF'
      WHEN 'GAB' THEN 'GA'
      WHEN 'GMB' THEN 'GM'
      WHEN 'GEO' THEN 'GE'
      WHEN 'DEU' THEN 'DE'
      WHEN 'GHA' THEN 'GH'
      WHEN 'GIB' THEN 'GI'
      WHEN 'GRC' THEN 'GR'
      WHEN 'GRL' THEN 'GL'
      WHEN 'GRD' THEN 'GD'
      WHEN 'GLP' THEN 'GP'
      WHEN 'GUM' THEN 'GU'
      WHEN 'GTM' THEN 'GT'
      WHEN 'GGY' THEN 'GG'
      WHEN 'GIN' THEN 'GN'
      WHEN 'GNB' THEN 'GW'
      WHEN 'GUY' THEN 'GY'
      WHEN 'HTI' THEN 'HT'
      WHEN 'HMT' THEN 'HM'
      WHEN 'VAT' THEN 'VA'
      WHEN 'HND' THEN 'HN'
      WHEN 'HKG' THEN 'HK'
      WHEN 'HUN' THEN 'HU'
      WHEN 'ISL' THEN 'IS'
      WHEN 'IND' THEN 'IN'
      WHEN 'IDN' THEN 'ID'
      WHEN 'IRN' THEN 'IR'
      WHEN 'IRQ' THEN 'IQ'
      WHEN 'IRL' THEN 'IE'
      WHEN 'IMN' THEN 'IM'
      WHEN 'ISR' THEN 'IL'
      WHEN 'ITA' THEN 'IT'
      WHEN 'JAM' THEN 'JM'
      WHEN 'JPN' THEN 'JP'
      WHEN 'JEY' THEN 'JE'
      WHEN 'JOR' THEN 'JO'
      WHEN 'KAZ' THEN 'KZ'
      WHEN 'KEN' THEN 'KE'
      WHEN 'KIR' THEN 'KI'
      WHEN 'PRK' THEN 'KP'
      WHEN 'KOR' THEN 'KR'
      WHEN 'KWT' THEN 'KW'
      WHEN 'KGZ' THEN 'KG'
      WHEN 'LAO' THEN 'LA'
      WHEN 'LVA' THEN 'LV'
      WHEN 'LBN' THEN 'LB'
      WHEN 'LSO' THEN 'LS'
      WHEN 'LBR' THEN 'LR'
      WHEN 'LBY' THEN 'LY'
      WHEN 'LIE' THEN 'LI'
      WHEN 'LTU' THEN 'LT'
      WHEN 'LUX' THEN 'LU'
      WHEN 'MAC' THEN 'MO'
      WHEN 'MKD' THEN 'MK'
      WHEN 'MDG' THEN 'MG'
      WHEN 'MWI' THEN 'MW'
      WHEN 'MYS' THEN 'MY'
      WHEN 'MDV' THEN 'MV'
      WHEN 'MLI' THEN 'ML'
      WHEN 'MLT' THEN 'MT'
      WHEN 'MHL' THEN 'MH'
      WHEN 'MTQ' THEN 'MQ'
      WHEN 'MRT' THEN 'MR'
      WHEN 'MUS' THEN 'MU'
      WHEN 'MYT' THEN 'YT'
      WHEN 'MEX' THEN 'MX'
      WHEN 'FSM' THEN 'FM'
      WHEN 'MDA' THEN 'MD'
      WHEN 'MCO' THEN 'MC'
      WHEN 'MNG' THEN 'MN'
      WHEN 'MNE' THEN 'ME'
      WHEN 'MSR' THEN 'MS'
      WHEN 'MAR' THEN 'MA'
      WHEN 'MOZ' THEN 'MZ'
      WHEN 'MMR' THEN 'MM'
      WHEN 'NAM' THEN 'NA'
      WHEN 'NRU' THEN 'NR'
      WHEN 'NPL' THEN 'NP'
      WHEN 'NLD' THEN 'NL'
      WHEN 'NCL' THEN 'NC'
      WHEN 'NZL' THEN 'NZ'
      WHEN 'NIC' THEN 'NI'
      WHEN 'NER' THEN 'NE'
      WHEN 'NGA' THEN 'NG'
      WHEN 'NIU' THEN 'NU'
      WHEN 'NFK' THEN 'NF'
      WHEN 'MNP' THEN 'MP'
      WHEN 'NOR' THEN 'NO'
      WHEN 'OMN' THEN 'OM'
      WHEN 'PAK' THEN 'PK'
      WHEN 'PLW' THEN 'PW'
      WHEN 'PSE' THEN 'PS'
      WHEN 'PAN' THEN 'PA'
      WHEN 'PNG' THEN 'PG'
      WHEN 'PRY' THEN 'PY'
      WHEN 'PER' THEN 'PE'
      WHEN 'PHL' THEN 'PH'
      WHEN 'PCN' THEN 'PN'
      WHEN 'POL' THEN 'PL'
      WHEN 'PRT' THEN 'PT'
      WHEN 'PRI' THEN 'PR'
      WHEN 'QAT' THEN 'QA'
      WHEN 'REU' THEN 'RE'
      WHEN 'ROU' THEN 'RO'
      WHEN 'RUS' THEN 'RU'
      WHEN 'RWA' THEN 'RW'
      WHEN 'BLM' THEN 'BL'
      WHEN 'SHN' THEN 'SH'
      WHEN 'KNA' THEN 'KN'
      WHEN 'LCA' THEN 'LC'
      WHEN 'MAF' THEN 'MF'
      WHEN 'SPM' THEN 'PM'
      WHEN 'VCT' THEN 'VC'
      WHEN 'WSM' THEN 'WS'
      WHEN 'SMR' THEN 'SM'
      WHEN 'STP' THEN 'ST'
      WHEN 'SAU' THEN 'SA'
      WHEN 'SEN' THEN 'SN'
      WHEN 'SRB' THEN 'RS'
      WHEN 'SYC' THEN 'SC'
      WHEN 'SLE' THEN 'SL'
      WHEN 'SGP' THEN 'SG'
      WHEN 'SXM' THEN 'SX'
      WHEN 'SVK' THEN 'SK'
      WHEN 'SVN' THEN 'SI'
      WHEN 'SLB' THEN 'SB'
      WHEN 'SOM' THEN 'SO'
      WHEN 'ZAF' THEN 'ZA'
      WHEN 'SGS' THEN 'GS'
      WHEN 'SSD' THEN 'SS'
      WHEN 'ESP' THEN 'ES'
      WHEN 'LKA' THEN 'LK'
      WHEN 'SDN' THEN 'SD'
      WHEN 'SUR' THEN 'SR'
      WHEN 'SJM' THEN 'SJ'
      WHEN 'SWE' THEN 'SE'
      WHEN 'CHE' THEN 'CH'
      WHEN 'SYR' THEN 'SY'
      WHEN 'TWN' THEN 'TW'
      WHEN 'TJK' THEN 'TJ'
      WHEN 'TZA' THEN 'TZ'
      WHEN 'THA' THEN 'TH'
      WHEN 'TLS' THEN 'TS'
      WHEN 'TGO' THEN 'TG'
      WHEN 'TKL' THEN 'TK'
      WHEN 'TON' THEN 'TO'
      WHEN 'TTO' THEN 'TT'
      WHEN 'TUN' THEN 'TN'
      WHEN 'TUR' THEN 'TR'
      WHEN 'TKM' THEN 'TM'
      WHEN 'TCA' THEN 'TC'
      WHEN 'TUV' THEN 'TV'
      WHEN 'UGA' THEN 'UG'
      WHEN 'UKR' THEN 'UA'
      WHEN 'ARE' THEN 'AE'
      WHEN 'GBR' THEN 'GB'
      WHEN 'UMI' THEN 'UM'
      WHEN 'USA' THEN 'US'
      WHEN 'URY' THEN 'UY'
      WHEN 'UZB' THEN 'UZ'
      WHEN 'VUT' THEN 'VU'
      WHEN 'VEN' THEN 'VE'
      WHEN 'VNM' THEN 'VN'
      WHEN 'VGB' THEN 'VG'
      WHEN 'VIR' THEN 'VI'
      WHEN 'WLF' THEN 'WF'
      WHEN 'ESH' THEN 'EH'
      WHEN 'YEM' THEN 'YE'
      WHEN 'ZMB' THEN 'ZM'
      WHEN 'ZWE' THEN 'ZW'
    END
);



WITH
all_cust AS (
  SELECT DISTINCT 
    CI.CUSTOMER_UID AS CUSTOMER_UID,
    CI.CUSTOMER_IDENTIFIER_NO
  FROM
    `pcb-{env}-curated.domain_customer_management.CUSTOMER_IDENTIFIER` CI
  WHERE
    CI.TYPE = 'PCF-CUSTOMER-ID'
    AND CI.DISABLED_IND = 'N'
),
excluded_cust AS (
  SELECT DISTINCT 
    SUBSTR(AM00.MAST_ACCOUNT_ID, 2) AS ACCOUNT_NO
  FROM
    `pcb-{env}-curated.domain_account_management.TSYS_PCB_MC_ACCTMASTER_DAILY_AM00_REC` AM00
  WHERE
    (
      am00.AM00_DATE_CLOSE IS NOT NULL
      AND am00.AM00_DATE_CLOSE > 0
      AND am00.AM00_DATE_CLOSE < CAST(
        FORMAT_DATE('%Y%j', DATE_SUB(CURRENT_DATE('America/Toronto'), INTERVAL 7 YEAR))
        AS INT64)
      AND AM00.MAST_ACCOUNT_SUFFIX = 0
    )
),
account_customer AS (
  SELECT DISTINCT
    AC.CUSTOMER_UID AS CUSTOMER_UID,
    a.MAST_ACCOUNT_ID AS ACCOUNT_NO
  FROM
    `pcb-{env}-curated.domain_account_management.ACCOUNT_CUSTOMER` AC
    LEFT JOIN `pcb-{env}-curated.domain_account_management.ACCOUNT` A ON AC.ACCOUNT_UID = A.ACCOUNT_UID
),
filtered_cust AS (
  SELECT DISTINCT 
    ACTIVE_AC.CUSTOMER_UID,
    CUSTOMER_IDENTIFIER_NO
  FROM
    (
      SELECT
        A.CUSTOMER_UID,
        A.CUSTOMER_IDENTIFIER_NO
      FROM
        all_cust AS A
        INNER JOIN (
          SELECT
            *
          FROM
            account_customer
          WHERE
            ACCOUNT_NO NOT IN (
              SELECT
                ACCOUNT_NO
              FROM
                excluded_cust
            )
        ) AS AC ON A.CUSTOMER_UID = AC.CUSTOMER_UID
    ) ACTIVE_AC
),
address as (
    SELECT DISTINCT 
      CT.CUSTOMER_UID AS CUSTOMER_UID,
      (
        CASE
          WHEN CT.CONTEXT = 'PRIMARY' THEN 'HOME'
          WHEN CT.CONTEXT = 'BUSINESS' THEN 'EMPLOYER'
          WHEN CT.CONTEXT = 'MAILING' THEN 'OTHER'
          WHEN CT.CONTEXT = 'TEMP-MAILING' THEN 'OTHER'
          ELSE 'UNKNOWN'
        END
      ) AS ADDRESS_TYPE,
      (
        CASE
          WHEN CT.CONTEXT = 'PRIMARY' THEN 'PRIMARY'
          WHEN CT.CONTEXT = 'BUSINESS' THEN 'N/A'
          WHEN CT.CONTEXT = 'MAILING' THEN 'N/A'
          WHEN CT.CONTEXT = 'TEMP-MAILING' THEN 'TEMPORARY'
          ELSE 'UNKNOWN'
        END
      ) AS ADDRESS_USE_TYPE,
      (
        CASE
          WHEN CT.CONTEXT = 'PRIMARY' THEN 'ACTIVE'
          WHEN CT.CONTEXT = 'BUSINESS' THEN 'ACTIVE'
          WHEN CT.CONTEXT = 'MAILING' THEN 'ACTIVE'
          WHEN CT.CONTEXT = 'TEMP-MAILING' THEN
            CASE
                WHEN CURRENT_DATETIME() BETWEEN (ALT_C.START_DT) and (ALT_C.END_DT) THEN 'ACTIVE'
                ELSE 'INACTIVE'
            END
          ELSE 'UNKNOWN'
        END
      ) AS ADDRESS_STATUS_TYPE,
      AC.ADDRESS_LINE_1,
      AC.ADDRESS_LINE_2,
      AC.CITY,
      AC.PROVINCE_STATE,
      AC.POSTAL_ZIP_CODE,
      AC.COUNTRY
    FROM
        `pcb-{env}-curated.domain_customer_management.CONTACT` CT
    INNER JOIN `pcb-{env}-curated.domain_customer_management.ADDRESS_CONTACT` AC
      ON CT.CONTACT_UID = AC.CONTACT_UID
    LEFT JOIN `pcb-{env}-curated.domain_customer_management.ALTERNATE_CONTACT` ALT_C
      ON ALT_C.ALT_CONTACT_UID = CT.CONTACT_UID
    WHERE
        CT.type='ADDRESS'
),

address_feed AS (
  SELECT
    '320' AS Institution_Number,
    customer.CUSTOMER_IDENTIFIER_NO AS Customer_Number,
    address.ADDRESS_TYPE AS Address_Type,
    address.ADDRESS_USE_TYPE AS Address_Use_Type,
    address.ADDRESS_STATUS_TYPE AS Address_Status_Type,
    address.ADDRESS_LINE_1 AS Street_Address_1,
    address.ADDRESS_LINE_2 AS Street_Address_2,
    address.CITY AS City,
    address.PROVINCE_STATE AS State,
    address.POSTAL_ZIP_CODE AS ZIP_code,
    NULL AS Province,
    NULL AS Postal_Code,
    twocharcountrycode(address.COUNTRY) as Country_Code
  FROM
    filtered_cust AS customer
    JOIN address
      ON address.CUSTOMER_UID = customer.CUSTOMER_UID
)
SELECT
  *
FROM
  address_feed;