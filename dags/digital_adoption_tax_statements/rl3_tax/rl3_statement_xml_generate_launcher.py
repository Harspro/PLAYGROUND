"""
RL-3 Tax Statement XML Generation DAG

This DAG generates RL-3 tax slip XML files for Revenu Québec based on T5 tax slip data.
The process includes:
1. Querying all unique submissions (SbmtRefId) from RL3 processing tables
2. For each submission, generating one XML file in the Revenu Québec format
3. Validating each XML file against the XSD schema
4. Moving all validated XML files to the outbound bucket

Prerequisites:
    - RL3_TAX_HEADER table must be populated in processing project
    - RL3_TAX_SLIP table must be populated in processing project
    - RL3_TAX_TRAILER table must be populated in processing project
    - Each submission must have matching SbmtRefId in all three tables

DAG Parameters:
    - year (optional): The tax year for XSD validation and schema version.
                      If not provided, defaults to current year.
                      Example: {"year": 2025}

Configuration:
    - Schema versions are configured in rl3_statement_config.yaml
    - Add new year/version mappings when receiving XSD from Revenu Québec
    - Example: schema_versions: {2025: "2025.1", 2026: "2026.3"}

Processing:
    - Processes ALL unique SbmtRefId values found in RL3_TAX_HEADER
    - Creates one XML file per submission (SbmtRefId)
    - Each XML file contains: header (1 record), slips (N records), trailer (1 record)
    - All files are validated and moved together
    - Uses full SAX streaming (StreamingXMLWriter) for incremental XML generation
    - Uses BigQuery result iterators to minimize memory footprint
    - Slip data is processed one row at a time without loading all into memory

XML Structure:
    - Encoding: UTF-8
    - Namespace: http://www.mrq.gouv.qc.ca/T5
    - Schema Version: Configured per year in rl3_statement_config.yaml (e.g., "2025.1", "2026.3")
    - Root: Transmission -> P (header) -> Groupe03 -> [R/A/D (slips)] -> T (trailer)
    - Note: P (header) is outside Groupe03; R/A/D (slips) and T (trailer) are inside Groupe03
    - XSD Path: dags/digital_adoption_tax_statements/xsd/rl3_xmlschm_<year>/Transmission.xsd

Slip Types:
    - R: Regular (TypeEnvoi=1, rpt_tcd='O')
    - A: Amended (TypeEnvoi=4, rpt_tcd='A')
    - D: Cancelled (TypeEnvoi=6, rpt_tcd='C')
"""

from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from datetime import datetime, timedelta
from google.cloud import storage
from typing import Final, List, Dict, Optional, Any, ContextManager, BinaryIO, Tuple
from util.miscutils import read_env_filepattern, read_yamlfile_env_suffix
from util.xml_utils import StreamingXMLWriter, get_field_value, is_null_or_empty, is_not_null, validate_xml
from util.gcs_utils import move_gcs_objects
from airflow import DAG, settings
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException
import pendulum
import util.constants as consts
import pytz
import xmlschema
from dag_factory.abc import BaseDagBuilder
from dag_factory import DAGFactory
from dag_factory.environment_config import EnvironmentConfig
import logging

logger = logging.getLogger(__name__)


RL3_STATEMENT_CONFIG_FILE: Final = 'rl3_statement_config.yaml'
DAG_ID: Final = 'rl3_generate_xml'

DAG_DEFAULT_ARGS = {
    'owner': "team-digital-adoption-alerts",
    'capability': "Account Management",
    'severity': 'P2',
    'sub_capability': 'NA',
    'business_impact': 'This dag generates the XML RL-3 data from BQ table',
    'customer_impact': 'In the event of a failure RL-3 tax files will not be available',
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 3,
    'retry_delay': timedelta(seconds=10)
}


def get_xsd_path(year: int) -> str:
    """Get the XSD file path for the given year."""
    return f'{settings.DAGS_FOLDER}/digital_adoption_tax_statements/xsd/rl3_xmlschm_{year}/Transmission.xsd'


def process_header(header_row: Any, writer: StreamingXMLWriter) -> None:
    """Process header (P) and write to XML stream using SAX.

    Args:
        header_row: BigQuery Row object (not pandas dataframe)
        writer: StreamingXMLWriter instance
    """
    writer.start_element('P')

    # Add Annee (REQUIRED)
    writer.write_field('Annee', header_row, 'Annee', required=True, transform=lambda x: str(int(x)))

    # Add TypeEnvoi (REQUIRED)
    writer.write_field('TypeEnvoi', header_row, 'TypeEnvoi', required=True, transform=lambda x: str(int(x)))

    # Preparateur section (required)
    writer.start_element('Preparateur')

    # PreparateurNo (REQUIRED)
    writer.write_field('No', header_row, 'PreparateurNo', required=True)

    # PreparateurType (optional)
    writer.write_field('Type', header_row, 'PreparateurType', transform=lambda x: str(int(x)))

    # PreparateurNom1 (REQUIRED)
    writer.write_field('Nom1', header_row, 'PreparateurNom1', required=True)

    # PreparateurNom2 (optional)
    writer.write_field('Nom2', header_row, 'PreparateurNom2')

    # Preparateur Address (optional)
    if any(is_not_null(get_field_value(header_row, f'PreparateurAdresse{field}')) for field in ['Ligne1', 'Ligne2', 'Ville', 'Province', 'CodePostal']):
        writer.start_element('Adresse')
        writer.write_field('Ligne1', header_row, 'PreparateurAdresseLigne1')
        writer.write_field('Ligne2', header_row, 'PreparateurAdresseLigne2')
        writer.write_field('Ville', header_row, 'PreparateurAdresseVille')
        writer.write_field('Province', header_row, 'PreparateurAdresseProvince')
        writer.write_field('CodePostal', header_row, 'PreparateurAdresseCodePostal')
        writer.end_element('Adresse')

    writer.end_element('Preparateur')

    # Informatique section (optional) - NO Langue field per XSD
    if any(is_not_null(get_field_value(header_row, f'Informatique{field}')) for field in ['Nom', 'IndRegional', 'Tel', 'PosteTel']):
        writer.start_element('Informatique')
        writer.write_field('Nom', header_row, 'InformatiqueNom')
        writer.write_field('IndRegional', header_row, 'InformatiqueIndRegional', transform=lambda x: str(int(x)))
        writer.write_field('Tel', header_row, 'InformatiqueTel')
        writer.write_field('PosteTel', header_row, 'InformatiquePosteTel', transform=lambda x: str(int(x)))
        writer.end_element('Informatique')

    # Comptabilite section (optional) - NO Langue field per XSD
    if any(is_not_null(get_field_value(header_row, f'Comptabilite{field}')) for field in ['Nom', 'IndRegional', 'Tel', 'PosteTel']):
        writer.start_element('Comptabilite')
        writer.write_field('Nom', header_row, 'ComptabiliteNom')
        writer.write_field('IndRegional', header_row, 'ComptabiliteIndRegional', transform=lambda x: str(int(x)))
        writer.write_field('Tel', header_row, 'ComptabiliteTel')
        writer.write_field('PosteTel', header_row, 'ComptabilitePosteTel', transform=lambda x: str(int(x)))
        writer.end_element('Comptabilite')

    # NoCertification (REQUIRED)
    writer.write_field('NoCertification', header_row, 'NoCertification', required=True)

    # Software info (optional)
    writer.write_field('NomLogiciel', header_row, 'NomLogiciel')
    writer.write_field('VersionLogiciel', header_row, 'VersionLogiciel')

    # CourrielResponsable (optional)
    writer.write_field('CourrielResponsable', header_row, 'CourrielResponsable')

    # IdPartenaireReleves (REQUIRED)
    writer.write_field('IdPartenaireReleves', header_row, 'IdPartenaireReleves', required=True)

    # IdProduitReleves (REQUIRED)
    writer.write_field('IdProduitReleves', header_row, 'IdProduitReleves', required=True)

    # NoCasEssai (optional)
    if is_not_null(get_field_value(header_row, 'NoCasEssai')):
        writer.write_element('NoCasEssai', str(get_field_value(header_row, 'NoCasEssai')))

    writer.end_element('P')  # Close P element


def _validate_no_releve_value(slip_row: Any) -> Tuple[Optional[str], List[str]]:
    """Validate NoReleve value without writing to XML.

    Args:
        slip_row: Slip row data

    Returns:
        tuple: (validated_no_releve_value, list_of_errors)
        - validated_no_releve_value: The validated NoReleve value (padded to 9 digits) or None if invalid
        - list_of_errors: List of error messages (empty if valid)
    """
    errors = []
    no_releve_value = get_field_value(slip_row, 'NoReleve', required=True)

    # Validate it's numeric by attempting int conversion
    try:
        no_releve_int = int(no_releve_value)
    except (ValueError, TypeError):
        errors.append("NoReleve must be numeric")
        return None, errors

    # Pad with leading zeros to ensure exactly 9 digits
    no_releve_value = str(no_releve_int).zfill(9)

    # Check it doesn't exceed 9 digits
    if len(no_releve_value) > 9:
        errors.append(f"NoReleve exceeds 9 digits. Got {len(no_releve_value)} digits")
        return None, errors

    return no_releve_value, errors


def _validate_luhn(sin: str) -> bool:
    """Validate SIN/NAS using Luhn algorithm (checksum validation).

    The Luhn algorithm is used to validate Canadian Social Insurance Numbers.
    It works by:
    1. Double every second digit from the right
    2. If the result is > 9, subtract 9
    3. Sum all digits
    4. If the total modulo 10 equals 0, it's valid

    Note: This function assumes sin is already validated as 9 digits by _validate_nas.

    Args:
        sin: 9-digit SIN string (pre-validated)

    Returns:
        True if valid, False otherwise

    Example:
        046454286 -> Valid (sum=50, 50%10=0)
        123456789 -> Invalid (sum=45, 45%10=5)
    """
    digits = [int(d) for d in sin]
    # Double every second digit from right (indices 1, 3, 5, 7 for a 9-digit SIN)
    for i in range(1, len(digits), 2):
        digits[i] *= 2
        if digits[i] > 9:
            digits[i] -= 9
    return sum(digits) % 10 == 0


def _validate_nas_value(slip_row: Any, no_releve_value: str) -> Tuple[Optional[str], List[str]]:
    """Validate NAS (Social Insurance Number) without writing to XML.

    Validates:
    1. NAS is exactly 9 digits (no padding - must be exactly 9 characters)
    2. NAS contains only digits
    3. NAS is not zero (all zeros)
    4. NAS passes Luhn algorithm checksum validation

    Note: PersonneNAS is expected to be a 9-digit zero-padded string from the database.

    Args:
        slip_row: Slip row data
        no_releve_value: NoReleve value for context in error messages

    Returns:
        tuple: (validated_nas_value, list_of_errors)
        - validated_nas_value: The validated NAS value (exactly 9 digits) or None if invalid
        - list_of_errors: List of error messages (empty if valid)
    """
    errors = []
    try:
        nas_raw = get_field_value(slip_row, 'PersonneNAS')

        # Handle string (expected from DB) - just clean any whitespace
        nas_value = str(nas_raw).strip()

        # Validate NAS is exactly 9 digits (no padding - must be exactly 9 characters)
        if len(nas_value) != 9:
            errors.append(f"NAS must be exactly 9 digits. Got {len(nas_value)} digits")
            return None, errors

        # Validate NAS contains only digits
        if not nas_value.isdigit():
            errors.append("NAS must contain only digits")
            return None, errors

        # Validate NAS is not all zeros
        if nas_value == '000000000':
            errors.append("NAS value is all zeros")
            return None, errors

        # Validate NAS checksum using Luhn algorithm
        if not _validate_luhn(nas_value):
            errors.append(
                f"NAS fails Luhn checksum validation for slip NoReleve={no_releve_value}. "
                f"NAS is not a valid Canadian SIN."
            )
            return None, errors

        return nas_value, errors
    except (ValueError, TypeError) as e:
        errors.append(f"Invalid NAS for slip NoReleve={no_releve_value}: {e}")
        return None, errors


def _process_personne(writer: StreamingXMLWriter, slip_row: Any, no_releve_value: str, slip_element_name: str = 'R') -> None:
    """Process Personne (individual) section.

    Args:
        writer: StreamingXMLWriter instance
        slip_row: Slip row data
        no_releve_value: NoReleve value for context in error messages
        slip_element_name: Type of slip ('R', 'A', or 'D')
    """
    writer.start_element('Personne')

    # NAS (REQUIRED, must be exactly 9 digits) - already validated in first pass
    nas_value, _ = _validate_nas_value(slip_row, no_releve_value)
    writer.write_element('NAS', nas_value)

    # NomFamille (REQUIRED for Personne)
    writer.write_field('NomFamille', slip_row, 'PersonneNomFamille', required=True,
                       context=f"NoReleve={no_releve_value}")

    # Prenom (REQUIRED for Personne)
    writer.write_field('Prenom', slip_row, 'PersonnePrenom', required=True,
                       context=f"NoReleve={no_releve_value}")

    # Initiale (optional, but NOT for D-type cancelled slips)
    if slip_element_name != 'D':
        writer.write_field('Initiale', slip_row, 'PersonneInitiale')

    writer.end_element('Personne')


def _process_raison_sociale(writer: StreamingXMLWriter, slip_row: Any, no_releve_value: str) -> None:
    """Process RaisonSociale (corporation) section.

    Args:
        writer: StreamingXMLWriter instance
        slip_row: Slip row data
        no_releve_value: NoReleve value for context in error messages
    """
    writer.start_element('RaisonSociale')

    # AutreNoId (optional)
    writer.write_field('AutreNoId', slip_row, 'RaisonSocialeAutreNoId')

    # Nom1 (REQUIRED for RaisonSociale)
    writer.write_field('Nom1', slip_row, 'RaisonSocialeNom1', required=True,
                       context=f"NoReleve={no_releve_value}")

    # Nom2 (optional)
    writer.write_field('Nom2', slip_row, 'RaisonSocialeNom2')

    writer.end_element('RaisonSociale')


def _process_adresse(writer: StreamingXMLWriter, slip_row: Any) -> None:
    """Process Adresse (address) section if any address fields are present.

    Args:
        writer: StreamingXMLWriter instance
        slip_row: Slip row data
    """
    if any(is_not_null(get_field_value(slip_row, f'Adresse{field}'))
           for field in ['Ligne1', 'Ligne2', 'Ville', 'Province', 'CodePostal']):
        writer.start_element('Adresse')
        writer.write_field('Ligne1', slip_row, 'AdresseLigne1')
        writer.write_field('Ligne2', slip_row, 'AdresseLigne2')
        writer.write_field('Ville', slip_row, 'AdresseVille')
        writer.write_field('Province', slip_row, 'AdresseProvince')
        writer.write_field('CodePostal', slip_row, 'AdresseCodePostal')
        writer.end_element('Adresse')


def _process_beneficiaire(writer: StreamingXMLWriter, slip_row: Any, slip_element_name: str, no_releve_value: str) -> None:
    """Process Beneficiaire (beneficiary) section.

    Args:
        writer: StreamingXMLWriter instance
        slip_row: Slip row data
        slip_element_name: Type of slip ('R', 'A', or 'D')
        no_releve_value: NoReleve value for context in error messages

    Raises:
        AirflowFailException: If neither Personne nor RaisonSociale is present
    """
    writer.start_element('Beneficiaire')

    # For R and A types: Type and No come BEFORE Personne/RaisonSociale
    # For D type: ONLY Personne/RaisonSociale (no Type or No)
    if slip_element_name != 'D':
        # BeneficiaireType (REQUIRED for R and A types)
        writer.write_field('Type', slip_row, 'BeneficiaireType', required=True,
                           context=f"NoReleve={no_releve_value}", transform=lambda x: str(int(x)))

        # BeneficiaireNo (optional for R and A types)
        writer.write_field('No', slip_row, 'BeneficiaireNo')

    # Determine if Personne or RaisonSociale (MUST have one or the other)
    if is_not_null(get_field_value(slip_row, 'PersonneNAS')) and slip_row.get('PersonneNAS') != 0:
        _process_personne(writer, slip_row, no_releve_value, slip_element_name)
    elif is_not_null(get_field_value(slip_row, 'RaisonSocialeNom1')):
        _process_raison_sociale(writer, slip_row, no_releve_value)
    else:
        # Must have either Personne or RaisonSociale
        raise AirflowFailException(f"Slip must have either PersonneNAS or RaisonSocialeNom1 for NoReleve={no_releve_value}")

    # Adresse section (optional for D type, required for R and A)
    if slip_element_name != 'D':
        _process_adresse(writer, slip_row)

    writer.end_element('Beneficiaire')


def _process_montants(writer: StreamingXMLWriter, slip_row: Any) -> None:
    """Process Montants (amounts) section.

    Args:
        writer: StreamingXMLWriter instance
        slip_row: Slip row data
    """
    writer.start_element('Montants')

    # Add monetary amounts
    amt_columns = {
        'A1_DividendeDetermine': 'A1_DividendeDetermine',
        'A2_DividendeOrdinaire': 'A2_DividendeOrdinaire',
        'B_DividendeImposable': 'B_DividendeImposable',
        'C_CreditImpotDividende': 'C_CreditImpotDividende',
        'D_InteretSourceCdn': 'D_InteretSourceCdn',
        'E_AutreRevenuCdn': 'E_AutreRevenuCdn',
        'F_RevenuBrutEtranger': 'F_RevenuBrutEtranger',
        'G_ImpotEtranger': 'G_ImpotEtranger',
        'H_RedevanceCdn': 'H_RedevanceCdn',
        'I_DividendeGainCapital': 'I_DividendeGainCapital',
        'J_RevenuAccumuleRente': 'J_RevenuAccumuleRente',
        'K_InteretBilletsLies': 'K_InteretBilletsLies'
    }

    for col, xml_name in amt_columns.items():
        if is_not_null(get_field_value(slip_row, col)) and float(slip_row[col]) != 0:
            writer.write_element(xml_name, f"{float(slip_row[col]):.2f}")

    # DeviseEtrangere
    if is_not_null(get_field_value(slip_row, 'DeviseEtrangere')):
        writer.write_element('DeviseEtrangere', str(get_field_value(slip_row, 'DeviseEtrangere')))

    writer.end_element('Montants')


def _validate_slip(slip_row: Any, slip_element_name: str, no_releve_limit: Optional[int] = None,
                   no_releve_pdf_limit: Optional[int] = None) -> List[str]:
    """Validate a single slip and collect all validation errors.

    Args:
        slip_row: Slip row data
        slip_element_name: Type of slip ('R', 'A', or 'D')
        no_releve_limit: Optional maximum NoReleve number allowed
        no_releve_pdf_limit: Optional maximum NoRelevePDF number allowed

    Returns:
        List of error messages (empty if slip is valid)
    """
    errors = []

    # Validate Annee (REQUIRED)
    annee_value = get_field_value(slip_row, 'Annee', required=True)
    if is_null_or_empty(annee_value):
        errors.append("Annee is required but is null or empty")
    else:
        try:
            int(annee_value)
        except (ValueError, TypeError):
            errors.append("Annee must be numeric")

    # Validate NoReleve
    no_releve_value, no_releve_errors = _validate_no_releve_value(slip_row)
    errors.extend(no_releve_errors)

    if no_releve_value is None:
        # If NoReleve is invalid, skip remaining validations that depend on it
        return errors

    # Validate NoReleve doesn't exceed the limit (if provided)
    if no_releve_limit is not None:
        try:
            no_releve_first_8 = int(no_releve_value[:8])
            if no_releve_first_8 > no_releve_limit:
                errors.append(
                    f"NoReleve first 8 digits ({no_releve_first_8}) exceeds the maximum allowed limit of {no_releve_limit}"
                )
        except (ValueError, TypeError) as e:
            errors.append(f"Error validating NoReleve limit: {e}")

    # Validate NoRelevePDF doesn't exceed the limit (if provided)
    if no_releve_pdf_limit is not None:
        no_releve_pdf_value = get_field_value(slip_row, 'NoRelevePDF')
        if is_not_null(no_releve_pdf_value):
            try:
                no_releve_pdf_first_8 = int(str(no_releve_pdf_value)[:8])
                if no_releve_pdf_first_8 > no_releve_pdf_limit:
                    errors.append(
                        f"NoRelevePDF first 8 digits ({no_releve_pdf_first_8}) exceeds the maximum allowed limit of {no_releve_pdf_limit}. "
                        f"NoReleve={no_releve_value}"
                    )
            except (ValueError, TypeError) as e:
                errors.append(f"Error validating NoRelevePDF limit: {e}")

    # Validate Beneficiaire section
    if slip_element_name != 'D':
        # BeneficiaireType (REQUIRED for R and A types)
        beneficiaire_type = get_field_value(slip_row, 'BeneficiaireType', required=True)
        if is_null_or_empty(beneficiaire_type):
            errors.append(f"BeneficiaireType is required for {slip_element_name} type slips but is null or empty. NoReleve={no_releve_value}")

    # Determine if Personne or RaisonSociale (MUST have one or the other)
    has_personne = is_not_null(get_field_value(slip_row, 'PersonneNAS')) and slip_row.get('PersonneNAS') != 0
    has_raison_sociale = is_not_null(get_field_value(slip_row, 'RaisonSocialeNom1'))

    if not has_personne and not has_raison_sociale:
        errors.append(f"Slip must have either PersonneNAS or RaisonSocialeNom1 for NoReleve={no_releve_value}")
    elif has_personne:
        # Validate PersonneNAS
        nas_value, nas_errors = _validate_nas_value(slip_row, no_releve_value)
        errors.extend(nas_errors)

        # Validate PersonneNomFamille (REQUIRED for Personne)
        if is_null_or_empty(get_field_value(slip_row, 'PersonneNomFamille')):
            errors.append(f"PersonneNomFamille is required for Personne but is null or empty. NoReleve={no_releve_value}")

        # Validate PersonnePrenom (REQUIRED for Personne)
        if is_null_or_empty(get_field_value(slip_row, 'PersonnePrenom')):
            errors.append(f"PersonnePrenom is required for Personne but is null or empty. NoReleve={no_releve_value}")
    elif has_raison_sociale:
        # Validate RaisonSocialeNom1 (REQUIRED for RaisonSociale)
        if is_null_or_empty(get_field_value(slip_row, 'RaisonSocialeNom1')):
            errors.append(f"RaisonSocialeNom1 is required for RaisonSociale but is null or empty. NoReleve={no_releve_value}")

    # NoReleveDerniereTrans for amended slips (A type) - REQUIRED
    if slip_element_name == 'A':
        no_releve_derniere_trans_value = get_field_value(slip_row, 'NoReleveDerniereTrans')

        if is_null_or_empty(no_releve_derniere_trans_value):
            errors.append(
                f"NoReleveDerniereTrans is required for amended slips (A type) but is null or blank. "
                f"NoReleve={no_releve_value}"
            )
        else:
            try:
                no_releve_derniere_trans_int = int(no_releve_derniere_trans_value)
                no_releve_derniere_trans = str(no_releve_derniere_trans_int).zfill(9)
                if len(no_releve_derniere_trans) > 9:
                    errors.append(f"NoReleveDerniereTrans exceeds 9 digits. Got {len(no_releve_derniere_trans)} digits")
            except (ValueError, TypeError):
                errors.append("NoReleveDerniereTrans must be numeric")

    return errors


def process_slips(slips_iterator: Any, writer: StreamingXMLWriter, type_envoi: int,
                  no_releve_limit: Optional[int] = None, no_releve_pdf_limit: Optional[int] = None) -> int:
    """Process slips (R, A, or D) and write to XML stream using SAX.

    First validates all slips and collects errors. If any errors are found, raises an exception
    with all errors. Otherwise, writes XML for all slips.

    Args:
        slips_iterator: Iterator of BigQuery Row objects (not pandas dataframe)
        writer: StreamingXMLWriter instance
        type_envoi: The type of slips (1=Original, 4=Amended, 6=Cancelled)
        no_releve_limit: Optional maximum NoReleve number allowed (from dag_run.conf)
        no_releve_pdf_limit: Optional maximum NoRelevePDF number allowed (from dag_run.conf)

    Returns:
        int: Number of slips processed

    Raises:
        AirflowFailException: If any validation errors are found, contains all errors
    """

    # Determine slip element name based on TypeEnvoi
    if type_envoi == 1:
        slip_element_name = 'R'  # Regular
    elif type_envoi == 4:
        slip_element_name = 'A'  # Amended
    elif type_envoi == 6:
        slip_element_name = 'D'  # Cancelled
    else:
        slip_element_name = 'R'  # Default to Regular

    # Convert iterator to list so we can iterate twice (validate, then write)
    slips_list = list(slips_iterator)

    # First pass: Validate all slips and collect errors
    # Use a dict to deduplicate errors by NoReleve (to avoid showing same error multiple times for duplicate slips)
    errors_by_no_releve = {}
    total_slip_count_with_errors = 0

    for slip_row in slips_list:
        slip_errors = _validate_slip(slip_row, slip_element_name, no_releve_limit, no_releve_pdf_limit)
        if slip_errors:
            total_slip_count_with_errors += 1
            # Add NoReleve context to each error if available
            no_releve_value = get_field_value(slip_row, 'NoReleve', required=False)
            if no_releve_value:
                try:
                    no_releve_padded = str(int(no_releve_value)).zfill(9)
                    error_key = no_releve_padded
                    error_msg = f"[NoReleve={no_releve_padded}] " + "; ".join(slip_errors)
                except (ValueError, TypeError):
                    error_key = str(no_releve_value)
                    error_msg = f"[NoReleve={no_releve_value}] " + "; ".join(slip_errors)
            else:
                error_key = None
                error_msg = "; ".join(slip_errors)

            # Store error message (will overwrite duplicates with same NoReleve, keeping last one)
            if error_key:
                errors_by_no_releve[error_key] = error_msg
            else:
                # For slips without NoReleve, add directly (can't deduplicate)
                errors_by_no_releve[f"_no_releve_{len(errors_by_no_releve)}"] = error_msg

    # If any errors found, raise exception with all unique errors
    if errors_by_no_releve:
        unique_error_count = len(errors_by_no_releve)
        error_message = f"Validation failed for {total_slip_count_with_errors} slip(s) ({unique_error_count} unique NoReleve(s)). Errors:\n" + "\n".join(f"  {i+1}. {error}" for i, error in enumerate(errors_by_no_releve.values()))
        raise AirflowFailException(error_message)

    # Second pass: Write XML for all slips (all already validated, no need to re-validate)
    slip_count = 0
    for slip_row in slips_list:
        slip_count += 1
        writer.start_element(slip_element_name)

        # Annee (REQUIRED) - already validated
        writer.write_field('Annee', slip_row, 'Annee', required=True, transform=lambda x: str(int(x)))

        # NoReleve (REQUIRED, must be exactly 9 digits) - already validated, just get value and write
        no_releve_value, _ = _validate_no_releve_value(slip_row)
        writer.write_element('NoReleve', no_releve_value)

        # Beneficiaire section (REQUIRED) - already validated
        _process_beneficiaire(writer, slip_row, slip_element_name, no_releve_value)

        # Montants section (not included for D type - cancelled slips)
        if slip_element_name != 'D':
            _process_montants(writer, slip_row)

            # CaseRensCompl section (optional, can have up to 4)
            if is_not_null(get_field_value(slip_row, 'CodeRensCompl')) and is_not_null(get_field_value(slip_row, 'DonneeRensCompl')):
                writer.start_element('CaseRensCompl')
                writer.write_element('CodeRensCompl', str(get_field_value(slip_row, 'CodeRensCompl')))
                writer.write_element('DonneeRensCompl', str(get_field_value(slip_row, 'DonneeRensCompl')))
                writer.end_element('CaseRensCompl')

        # NoReleveDerniereTrans for amended slips (A type) - already validated, just format and write
        if slip_element_name == 'A':
            no_releve_derniere_trans_value = get_field_value(slip_row, 'NoReleveDerniereTrans')
            no_releve_derniere_trans_int = int(no_releve_derniere_trans_value)
            no_releve_derniere_trans = str(no_releve_derniere_trans_int).zfill(9)
            writer.write_element('NoReleveDerniereTrans', no_releve_derniere_trans)

        writer.end_element(slip_element_name)  # Close R/A/D element

    return slip_count


def process_trailer(trailer_row: Any, writer: StreamingXMLWriter) -> None:
    """Process trailer (T) and write to XML stream using SAX.

    Args:
        trailer_row: BigQuery Row object (not pandas dataframe)
        writer: StreamingXMLWriter instance
    """
    writer.start_element('T')

    # Annee (REQUIRED)
    writer.write_field('Annee', trailer_row, 'Annee', required=True, transform=lambda x: str(int(x)))

    # NbReleves (REQUIRED)
    writer.write_field('NbReleves', trailer_row, 'NbReleves', required=True, transform=lambda x: str(int(x)))

    # PayeurMandataire section (REQUIRED)
    writer.start_element('PayeurMandataire')

    # NoId (REQUIRED - must be exactly 10 digits, stored as string in DB)
    writer.write_field('NoId', trailer_row, 'NoId', required=True, transform=lambda x: str(x).zfill(10))

    # TypeDossier (REQUIRED - fixed value 'RS')
    writer.write_field('TypeDossier', trailer_row, 'TypeDossier', required=True)

    # NoDossier (REQUIRED)
    writer.write_field('NoDossier', trailer_row, 'NoDossier', required=True)

    # NEQ (optional)
    writer.write_field('NEQ', trailer_row, 'NEQ', transform=lambda x: str(int(x)))

    # NoSuccursale (optional)
    writer.write_field('NoSuccursale', trailer_row, 'NoSuccursale')

    # Nom (REQUIRED)
    writer.write_field('Nom', trailer_row, 'Nom', required=True)

    # Payeur Address (REQUIRED with at least Ligne1)
    writer.start_element('Adresse')

    # Ligne1 (REQUIRED)
    writer.write_field('Ligne1', trailer_row, 'AdresseLigne1', required=True)

    # Ligne2 (optional)
    writer.write_field('Ligne2', trailer_row, 'AdresseLigne2')

    # Ville (optional)
    writer.write_field('Ville', trailer_row, 'AdresseVille')

    # Province (optional)
    writer.write_field('Province', trailer_row, 'AdresseProvince')

    # CodePostal (optional)
    writer.write_field('CodePostal', trailer_row, 'AdresseCodePostal')

    writer.end_element('Adresse')  # Close Adresse
    writer.end_element('PayeurMandataire')  # Close PayeurMandataire
    writer.end_element('T')  # Close T


def load_xsd(xsd_file_path: str) -> xmlschema.XMLSchema:
    """Load and parse the XSD file using xmlschema."""
    # Initialize xmlschema.XMLSchema with the main XSD file
    return xmlschema.XMLSchema(xsd_file_path)


class RL3XmlGeneratorDagBuilder(BaseDagBuilder):
    """DAG builder for RL-3 XML generation and validation.

    This builder creates a DAG that:
    1. Generates RL-3 XML files from BigQuery data (streaming to GCS)
    2. Validates XML files against XSD schema (streaming validation)
    3. Moves validated files to outbound bucket
    """

    def __init__(self, environment_config: EnvironmentConfig):
        """Initialize RL-3 DAG builder with environment configuration.

        Args:
            environment_config: Environment configuration provided by DAGFactory
        """
        super().__init__(environment_config)

        # Load RL3-specific configuration using environment settings
        rl3_config = read_yamlfile_env_suffix(
            f'{settings.DAGS_FOLDER}/digital_adoption_tax_statements/{RL3_STATEMENT_CONFIG_FILE}',
            self.environment_config.deploy_env,
            self.environment_config.storage_suffix
        )

        # Use GCP project IDs from environment config
        self.processing_project_id = self.environment_config.gcp_config.get(consts.PROCESSING_ZONE_PROJECT_ID)
        self.landing_project_id = self.environment_config.gcp_config.get(consts.LANDING_ZONE_PROJECT_ID)

        # RL3-specific GCS buckets
        self.staging_bucket = rl3_config['staging_bucket']
        self.outbound_bucket = rl3_config['outbound_bucket']
        self.schema_versions = rl3_config.get('schema_versions', {})
        self.deploy_env = self.environment_config.deploy_env

    def get_schema_version(self, year: int) -> str:
        """Get the schema version for the given year from configuration.

        Args:
            year: The tax year (e.g., 2025, 2026) - can be int or str

        Returns:
            str: The schema version (e.g., "2025.1", "2026.3")

        Raises:
            AirflowFailException: If the year is not found in the schema_versions configuration
        """
        # Convert year to int to ensure consistent type matching with YAML config keys
        year_int = int(year)

        if year_int not in self.schema_versions:
            raise AirflowFailException(
                f"Schema version not configured for year {year_int}. "
                f"Please add the version to rl3_statement_config.yaml. "
                f"Available years: {list(self.schema_versions.keys())}"
            )
        return self.schema_versions[year_int]

    def get_xml_file_name(self, sbmt_ref_id: str, type_envoi: int) -> str:
        """Generate the destination file name based on header data.

        Args:
            sbmt_ref_id: Submission reference ID
            type_envoi: Type of submission (1=original, 4=amended, 6=cancelled)

        Returns:
            str: The destination file name
        """
        # Determine type letter based on TypeEnvoi
        if type_envoi == 1:
            type_letter = 'o'  # original
        elif type_envoi == 4:
            type_letter = 'a'  # amended
        elif type_envoi == 6:
            type_letter = 'c'  # cancelled
        else:
            type_letter = 'u'  # unknown

        current_timestamp = datetime.now().astimezone(pytz.timezone('America/Toronto')).strftime('%Y%m%d%H%M%S')
        file_name = f"revenu_quebec_outbound_rl3_slip/pcb_rq_rl3_slip_{type_letter}_{sbmt_ref_id}_{current_timestamp}_{{file_env}}.xml"
        destination_file_name = read_env_filepattern(file_name, self.deploy_env)
        return destination_file_name

    def create_xml_for_submission(self, client: bigquery.Client, sbmt_ref_id: str, year: int,
                                  no_releve_limit: Optional[int] = None,
                                  no_releve_pdf_limit: Optional[int] = None) -> str:
        """Create RL-3 XML file for a single submission using full SAX streaming and upload to GCS.

        This function uses SAX (StreamingXMLWriter) for incremental XML generation without loading
        the entire XML structure into memory. It also streams BigQuery query results, processing
        one row at a time.

        Args:
            client: BigQuery client
            sbmt_ref_id: Submission reference ID
            year: Tax year
            no_releve_limit: Optional maximum NoReleve number allowed
            no_releve_pdf_limit: Optional maximum NoRelevePDF number allowed

        Returns:
            str: The destination file name in GCS
        """
        # Query data for this specific SbmtRefId
        query_header_data = f"""
            SELECT *
            FROM `{self.processing_project_id}.domain_tax_slips.RL3_TAX_HEADER`
            WHERE SbmtRefId = '{sbmt_ref_id}'
            ORDER BY CreateDate DESC
            LIMIT 1
        """

        query_slips_data = f"""
            SELECT *
            FROM `{self.processing_project_id}.domain_tax_slips.RL3_TAX_SLIP`
            WHERE SbmtRefId = '{sbmt_ref_id}'
            ORDER BY NoReleve
        """

        query_trailer_data = f"""
            SELECT *
            FROM `{self.processing_project_id}.domain_tax_slips.RL3_TAX_TRAILER`
            WHERE SbmtRefId = '{sbmt_ref_id}'
            ORDER BY CreateDate DESC
            LIMIT 1
        """

        # Execute queries and get result iterators (streaming, not loaded into memory)
        header_results = list(client.query(query_header_data).result())
        slips_results = client.query(query_slips_data).result()  # Keep as iterator for streaming
        trailer_results = list(client.query(query_trailer_data).result())

        # Validate we have data for this submission
        if not header_results:
            raise AirflowFailException(f"No header data found for SbmtRefId: {sbmt_ref_id}")
        if not trailer_results:
            raise AirflowFailException(f"No trailer data found for SbmtRefId: {sbmt_ref_id}")

        # Get the first (and only) header and trailer rows
        header_row = header_results[0]
        trailer_row = trailer_results[0]

        # Get TypeEnvoi from header
        type_envoi = get_field_value(header_row, 'TypeEnvoi')

        # Get the file name
        destination_file_name = self.get_xml_file_name(sbmt_ref_id, type_envoi)

        # Get schema version for the year from configuration
        schema_version = self.get_schema_version(year)

        # Setup GCS streaming
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.staging_bucket)
        blob = bucket.blob(destination_file_name)

        logger.info(f"Creating XML for submission {sbmt_ref_id}, streaming directly to gs://{self.staging_bucket}/{destination_file_name}")

        # Stream XML directly to GCS (no memory buffer)
        # Wrap in try/except to clean up partial files on error
        try:
            with blob.open("wb") as gcs_file:
                # Create XML using SAX streaming writer that writes directly to GCS
                writer = StreamingXMLWriter(gcs_file, encoding='UTF-8')

                # Start XML document (adds <?xml version="1.0" encoding="UTF-8"?>)
                writer.start_document()

                # Start root Transmission element with attributes
                transmission_attrs = {
                    "VersionSchema": schema_version,
                    "xmlns": "http://www.mrq.gouv.qc.ca/T5",
                    "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
                    "xsi:schemaLocation": "http://www.mrq.gouv.qc.ca/T5 Transmission.xsd"
                }
                writer.start_element('Transmission', transmission_attrs)

                # Process header (P element) - streaming (OUTSIDE Groupe03)
                process_header(header_row, writer)

                # Start Groupe03 wrapper element (contains slips AND trailer per RL-3 XSD schema)
                writer.start_element('Groupe03')

                # Process slips (R/A/D elements) - streaming (INSIDE Groupe03)
                slip_count = process_slips(slips_results, writer, type_envoi, no_releve_limit, no_releve_pdf_limit)

                # Validate slip count matches trailer NbReleves
                nb_releves = get_field_value(trailer_row, 'NbReleves')
                if nb_releves is not None:
                    nb_releves_int = int(nb_releves)
                    if slip_count != nb_releves_int:
                        raise AirflowFailException(
                            f"Slip count mismatch for submission {sbmt_ref_id}: "
                            f"Processed {slip_count} slips but trailer NbReleves is {nb_releves_int}"
                        )

                # Process trailer (T element) - streaming (INSIDE Groupe03, after slips)
                process_trailer(trailer_row, writer)

                # Close Groupe03
                writer.end_element('Groupe03')

                # Close root element
                writer.end_element('Transmission')

                # End XML document
                writer.end_document()

        except Exception:
            # Clean up partial file on any error
            try:
                if blob.exists():
                    blob.delete()
                    logger.info(f"Deleted partial file: gs://{self.staging_bucket}/{destination_file_name}")
            except Exception as delete_error:
                logger.warning(f"Failed to delete partial file: {delete_error}")
            raise  # Re-raise the original exception (Airflow will log it)

        logger.info(f"Successfully created XML for submission {sbmt_ref_id}: {destination_file_name} ({slip_count} slips)")
        return destination_file_name

    def create_xml(self, **kwargs) -> List[str]:
        """Main task to create all RL-3 XML files using full SAX streaming.

        Queries BigQuery for all submissions and creates an XML file for each using SAX streaming.
        XML files are streamed directly to GCS without loading into memory.

        Returns:
            list: List of destination file names created in GCS
        """
        # Get year from dag_run configuration
        dag_run = kwargs.get('dag_run')
        if dag_run and dag_run.conf and 'year' in dag_run.conf:
            year = dag_run.conf['year']
        else:
            # Default to current year if not provided
            year = datetime.now().year

        # Get optional no_releve_limit from dag_run configuration
        no_releve_limit = None
        if dag_run and dag_run.conf and 'noreleve_limit' in dag_run.conf:
            no_releve_limit = int(dag_run.conf['noreleve_limit'])
            logger.info(f"NoReleve limit set to: {no_releve_limit}")

        # Get optional no_releve_pdf_limit from dag_run configuration
        no_releve_pdf_limit = None
        if dag_run and dag_run.conf and 'norelevepdf_limit' in dag_run.conf:
            no_releve_pdf_limit = int(dag_run.conf['norelevepdf_limit'])
            logger.info(f"NoRelevePDF limit set to: {no_releve_pdf_limit}")

        client = bigquery.Client()

        # Get all unique SbmtRefId values from the header table (streaming query)
        query_submissions = f"""
            SELECT DISTINCT SbmtRefId
            FROM `{self.processing_project_id}.domain_tax_slips.RL3_TAX_HEADER`
            ORDER BY SbmtRefId
        """

        submissions_results = list(client.query(query_submissions).result())

        if not submissions_results:
            logger.info("No submissions found in RL3_TAX_HEADER table. No XML files to create.")
            return []

        # Process each submission and create XML files
        # Collect all errors from all submissions before failing
        created_files = []
        submission_errors = []

        for row in submissions_results:
            sbmt_ref_id = get_field_value(row, 'SbmtRefId')
            logger.info(f"Processing submission: {sbmt_ref_id}")

            try:
                file_name = self.create_xml_for_submission(client, sbmt_ref_id, year, no_releve_limit, no_releve_pdf_limit)
                created_files.append(file_name)
                logger.info(f"Successfully created XML for submission {sbmt_ref_id}: {file_name}")
            except Exception as e:
                # Collect error message (Airflow will log the exception automatically)
                error_msg = str(e)
                submission_errors.append(f"[SbmtRefId={sbmt_ref_id}] {error_msg}")

        # If any submissions failed, raise exception with all errors
        if submission_errors:
            error_message = f"Validation failed for {len(submission_errors)} submission(s). Errors:\n" + "\n".join(f"  {i+1}. {error}" for i, error in enumerate(submission_errors))
            raise AirflowFailException(error_message)

        logger.info(f"Total XML files created: {len(created_files)}")
        return created_files

    def open_xml_stream(self, file_path: str) -> ContextManager[BinaryIO]:
        """Open XML file from GCS as a stream for validation without loading into memory.

        Uses blob.open() to return a file-like object that streams data incrementally.
        This is memory-efficient for large XML files.

        Args:
            file_path: Path to the XML file in GCS

        Returns:
            file-like object: Binary stream of XML content (context manager)
        """
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.staging_bucket)
        blob = bucket.blob(file_path)
        # Return a streaming file-like object instead of loading entire content into memory
        return blob.open("rb")

    def xml_validation_task(self, **kwargs) -> List[Dict[str, Any]]:
        """Main task to validate all XML files with XSD.

        Validates all XML files created by the create_xml task.
        """
        # Get year from dag_run configuration
        dag_run = kwargs.get('dag_run')
        if dag_run and dag_run.conf and 'year' in dag_run.conf:
            year = dag_run.conf['year']
        else:
            # Default to current year if not provided
            year = datetime.now().year

        # Retrieve the list of XML file paths from the XCom of the previous task
        task_instance = kwargs['task_instance']
        xml_file_paths = task_instance.xcom_pull(task_ids='create_xml')

        if not xml_file_paths:
            logger.info("No XML files to validate. Skipping validation step.")
            return []

        # Get the XSD path for the specified year
        xsd_path = get_xsd_path(year)

        # Load the XSD schema with xmlschema (load once, use for all files)
        xsd_schema = load_xsd(xsd_path)

        # Validate each XML file using streaming (memory-efficient)
        validation_results = []
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.staging_bucket)

        for file_path in xml_file_paths:
            logger.info(f"Validating file: {file_path} (streaming mode)")
            try:
                # Stream XML from GCS without loading into memory
                with self.open_xml_stream(file_path) as xml_stream:
                    # Validate XML against the XSD schema (uses iterparse internally)
                    validate_xml(xml_stream, xsd_schema)

                validation_results.append({'file': file_path, 'status': 'valid'})
                logger.info(f"Successfully validated: {file_path}")
            except Exception as e:
                logger.error(f"Validation failed for {file_path}: {str(e)}")
                validation_results.append({'file': file_path, 'status': 'failed', 'error': str(e)})

                # Delete invalid file from staging bucket
                try:
                    blob = bucket.blob(file_path)
                    if blob.exists():
                        blob.delete()
                        logger.info(f"Deleted invalid file: gs://{self.staging_bucket}/{file_path}")
                except Exception as delete_error:
                    logger.warning(f"Failed to delete invalid file {file_path}: {delete_error}")

                raise

        logger.info(f"Total files validated: {len(validation_results)}")
        return validation_results

    def move_files_to_outbound(self, **kwargs) -> List[str]:
        """Move validated XML files from staging to outbound bucket.

        Moves all validated files from staging bucket to outbound landing bucket
        using the common GCS utility function.
        """
        task_instance = kwargs['task_instance']
        xml_file_paths = task_instance.xcom_pull(task_ids='create_xml')

        # Use the common GCS utility to move files
        moved_files = move_gcs_objects(
            source_bucket_name=self.staging_bucket,
            destination_bucket_name=self.outbound_bucket,
            file_paths=xml_file_paths,
            delete_source=True  # Move operation (delete after copy)
        )

        return moved_files

    def build(self, dag_id: str, config: dict) -> DAG:
        """Build the RL-3 XML generation DAG.

        Args:
            dag_id: The DAG ID
            config: Configuration dictionary (not used, kept for interface compatibility)

        Returns:
            Configured DAG instance
        """
        with DAG(
            dag_id=dag_id,
            default_args=DAG_DEFAULT_ARGS,
            schedule=None,
            start_date=datetime(2024, 1, 1, tzinfo=pendulum.timezone('America/Toronto')),
            catchup=False,
            max_active_runs=1,
            is_paused_upon_creation=True
        ) as dag:

            start_point = EmptyOperator(task_id=consts.START_TASK_ID)
            end_point = EmptyOperator(task_id=consts.END_TASK_ID)

            create_xml_task = PythonOperator(
                task_id='create_xml',
                python_callable=self.create_xml
            )

            xsd_validation_task = PythonOperator(
                task_id='validate_xml_with_xsd',
                python_callable=self.xml_validation_task,
                retries=0
            )

            move_files_task = PythonOperator(
                task_id='move_files_to_outbound_landing',
                python_callable=self.move_files_to_outbound
            )

            # Set task dependencies
            start_point >> create_xml_task >> xsd_validation_task >> move_files_task >> end_point

        return dag


# Create DAG using factory pattern
globals().update(DAGFactory().create_dag(RL3XmlGeneratorDagBuilder, DAG_ID))
