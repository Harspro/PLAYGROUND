import pytest
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
import xml.etree.ElementTree as ET
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import io
from datetime import datetime
import pytz
import xmlschema
from google.cloud import bigquery, storage

from digital_adoption_tax_statements.rl3_tax.rl3_statement_xml_generate_launcher import (
    get_xsd_path, process_header, process_slips, process_trailer,
    load_xsd, RL3XmlGeneratorDagBuilder, DAG_ID, _validate_luhn
)
from dag_factory import DAGFactory
from airflow.exceptions import AirflowFailException
from util.xml_utils import StreamingXMLWriter, validate_xml


@pytest.fixture
def dag_builder_instance():
    """Fixture for creating an RL3XmlGeneratorDagBuilder instance with mocked configuration."""
    with patch('digital_adoption_tax_statements.rl3_tax.rl3_statement_xml_generate_launcher.read_yamlfile_env_suffix') as mock_read_yaml:
        # Mock the RL3 configuration
        mock_read_yaml.return_value = {
            'staging_bucket': 'test-staging-bucket',
            'outbound_bucket': 'test-outbound-bucket',
            'schema_versions': {
                2025: '2025.1',
                2026: '2026.1'
            }
        }

        # Create a mock environment config
        mock_env_config = MagicMock()
        mock_env_config.deploy_env = 'test'
        mock_env_config.storage_suffix = 'test'

        # Create a mock gcp_config with a working .get() method
        mock_gcp_config = MagicMock()
        mock_gcp_config.get.side_effect = lambda key: {
            'processing_zone_project_id': 'test-processing-project',
            'landing_zone_project_id': 'test-landing-project'
        }.get(key)
        mock_env_config.gcp_config = mock_gcp_config

        # Create the DAG builder instance
        builder = RL3XmlGeneratorDagBuilder(mock_env_config)
        yield builder


@pytest.fixture
def mock_bigquery_client():
    """Fixture for mocking BigQuery client."""
    with patch('digital_adoption_tax_statements.rl3_tax.rl3_statement_xml_generate_launcher.bigquery.Client') as mock_client_class:
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        yield mock_client


@pytest.fixture
def mock_storage_client():
    """Fixture for mocking Google Cloud Storage client."""
    with patch('digital_adoption_tax_statements.rl3_tax.rl3_statement_xml_generate_launcher.storage.Client') as mock_client_class:
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()

        mock_client_class.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_client.get_bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        yield {
            'client': mock_client,
            'bucket': mock_bucket,
            'blob': mock_blob
        }


@pytest.fixture
def mock_datetime():
    """Fixture for mocking datetime functionality."""
    with patch('digital_adoption_tax_statements.rl3_tax.rl3_statement_xml_generate_launcher.datetime') as mock_dt:
        mock_now = MagicMock()
        mock_now.year = 2025  # Mock current year as 2025 (a configured year)
        mock_now.astimezone.return_value.strftime.return_value = '20251114120000'
        mock_dt.now.return_value = mock_now
        yield mock_dt


@pytest.fixture
def mock_read_env_filepattern():
    """Fixture for mocking read_env_filepattern function."""
    with patch('digital_adoption_tax_statements.rl3_tax.rl3_statement_xml_generate_launcher.read_env_filepattern') as mock_func:
        mock_func.return_value = 'revenu_quebec_outbound_rl3_slip/pcb_rq_rl3_slip_o_TEST123_20251114120000_test.xml'
        yield mock_func


@pytest.fixture
def mock_xmlschema():
    """Fixture for mocking xmlschema.XMLSchema."""
    with patch('digital_adoption_tax_statements.rl3_tax.rl3_statement_xml_generate_launcher.xmlschema.XMLSchema') as mock_schema_class:
        mock_schema = MagicMock()
        mock_schema_class.return_value = mock_schema
        yield mock_schema


@pytest.fixture
def mock_bigquery_row():
    """Create a mock BigQuery Row object that supports both dict-like and attribute access."""
    def create_row(data_dict):
        """Create a mock Row object from a dictionary."""
        mock_row = MagicMock()
        # Store data_dict for closures to access
        _data = data_dict.copy()

        # Support dict-like access: row.get('field', default)
        def get_method(key, default=None):
            return _data.get(key, default)

        mock_row.get = get_method

        # Support attribute access: row.field
        for key, value in data_dict.items():
            setattr(mock_row, key, value)

        # Support dict key access: row['field']
        # Use side_effect to handle the __getitem__ calls
        mock_row.__getitem__ = MagicMock(side_effect=lambda key: _data[key])
        return mock_row
    return create_row


@pytest.fixture
def xml_writer_helper():
    """Helper fixture for creating XML writers and extracting XML content."""
    def create_writer_and_parser():
        """Create a writer and return (writer, get_xml_func)."""
        xml_buffer = io.BytesIO()
        writer = StreamingXMLWriter(xml_buffer, encoding='UTF-8')

        def get_xml():
            """Get XML content as string and parse it."""
            xml_buffer.seek(0)
            xml_content = xml_buffer.read().decode('utf-8')
            # Parse the XML to ElementTree for assertions
            try:
                root = ET.fromstring(xml_content)
                return root
            except ET.ParseError:
                # If not valid XML yet (partial), return the string
                return xml_content

        return writer, get_xml

    return create_writer_and_parser


@pytest.fixture
def sample_dataframes(mock_bigquery_row):
    """Fixture providing sample test data.

    Returns both dataframes (for backwards compatibility) and Row objects (for new tests).
    """
    header_data = {
        'Annee': 2025,
        'TypeEnvoi': 1,
        'SbmtRefId': 'TEST123',
        'PreparateurNo': 'NP088880',
        'PreparateurType': 2,
        'PreparateurNom1': "President's Choice Bank",
        'PreparateurNom2': None,
        'PreparateurAdresseLigne1': '500 Lakeshore Blvd. West',
        'PreparateurAdresseLigne2': 'Suite 600',
        'PreparateurAdresseVille': 'Toronto',
        'PreparateurAdresseProvince': 'ON',
        'PreparateurAdresseCodePostal': 'M5V1A5',
        'ComptabiliteNom': 'Rod Lyn',
        'ComptabiliteIndRegional': 416,
        'ComptabiliteTel': '420-2103',
        'ComptabilitePosteTel': None,
        'NoCertification': 'RQ-25-99-999',
        'IdPartenaireReleves': '0000000000001DC1',
        'IdProduitReleves': '0000000000003964',
        'NoCasEssai': 'RAD_2025_03_001'
    }

    slip_data_1 = {
        'Annee': 2025,
        'NoReleve': '111111111',
        'BeneficiaireType': 1,
        'BeneficiaireNo': '0000000377845',
        'PersonneNAS': '046454286',  # Valid SIN that passes Luhn checksum
        'PersonneNomFamille': 'FROEW',
        'PersonnePrenom': 'JOSEPH',
        'PersonneInitiale': None,
        'RaisonSocialeNom1': None,
        'RaisonSocialeNom2': None,
        'RaisonSocialeAutreNoId': None,
        'AdresseLigne1': '82 H St',
        'AdresseLigne2': None,
        'AdresseVille': 'La Romaine',
        'AdresseProvince': 'QC',
        'AdresseCodePostal': 'G0G1M0',
        'A1_DividendeDetermine': None,
        'A2_DividendeOrdinaire': None,
        'B_DividendeImposable': None,
        'C_CreditImpotDividende': None,
        'D_InteretSourceCdn': 849.18,
        'E_AutreRevenuCdn': None,
        'F_RevenuBrutEtranger': None,
        'G_ImpotEtranger': None,
        'H_RedevanceCdn': None,
        'I_DividendeGainCapital': None,
        'J_RevenuAccumuleRente': None,
        'K_InteretBilletsLies': None,
        'DeviseEtrangere': 'CAD',
        'CodeRensCompl': None,
        'DonneeRensCompl': None,
        'NoReleveDerniereTrans': None
    }

    slip_data_2 = {
        'Annee': 2025,
        'NoReleve': '111111112',
        'BeneficiaireType': 1,
        'BeneficiaireNo': '0000000378950',
        'PersonneNAS': '130692544',  # Valid SIN that passes Luhn checksum
        'PersonneNomFamille': 'ALBANESE',
        'PersonnePrenom': 'DOROSHY',
        'PersonneInitiale': None,
        'RaisonSocialeNom1': None,
        'RaisonSocialeNom2': None,
        'RaisonSocialeAutreNoId': None,
        'AdresseLigne1': '38 Habitant Cres',
        'AdresseLigne2': None,
        'AdresseVille': 'Whitby',
        'AdresseProvince': 'ON',
        'AdresseCodePostal': 'L1P1E2',
        'A1_DividendeDetermine': None,
        'A2_DividendeOrdinaire': None,
        'B_DividendeImposable': None,
        'C_CreditImpotDividende': None,
        'D_InteretSourceCdn': 1180.89,
        'E_AutreRevenuCdn': None,
        'F_RevenuBrutEtranger': None,
        'G_ImpotEtranger': None,
        'H_RedevanceCdn': None,
        'I_DividendeGainCapital': None,
        'J_RevenuAccumuleRente': None,
        'K_InteretBilletsLies': None,
        'DeviseEtrangere': 'CAD',
        'CodeRensCompl': None,
        'DonneeRensCompl': None,
        'NoReleveDerniereTrans': None
    }

    # Amended slip data (includes NoReleveDerniereTrans - required for amended slips)
    amended_slip_data_1 = {
        'Annee': 2025,
        'NoReleve': '111111111',
        'BeneficiaireType': 1,
        'BeneficiaireNo': '0000000377845',
        'PersonneNAS': '046454286',  # Valid SIN that passes Luhn checksum
        'PersonneNomFamille': 'FROEW',
        'PersonnePrenom': 'JOSEPH',
        'PersonneInitiale': None,
        'RaisonSocialeNom1': None,
        'RaisonSocialeNom2': None,
        'RaisonSocialeAutreNoId': None,
        'AdresseLigne1': '82 H St',
        'AdresseLigne2': None,
        'AdresseVille': 'La Romaine',
        'AdresseProvince': 'QC',
        'AdresseCodePostal': 'G0G1M0',
        'A1_DividendeDetermine': None,
        'A2_DividendeOrdinaire': None,
        'B_DividendeImposable': None,
        'C_CreditImpotDividende': None,
        'D_InteretSourceCdn': 849.18,
        'E_AutreRevenuCdn': None,
        'F_RevenuBrutEtranger': None,
        'G_ImpotEtranger': None,
        'H_RedevanceCdn': None,
        'I_DividendeGainCapital': None,
        'J_RevenuAccumuleRente': None,
        'K_InteretBilletsLies': None,
        'DeviseEtrangere': 'CAD',
        'CodeRensCompl': None,
        'DonneeRensCompl': None,
        'NoReleveDerniereTrans': '100000001'  # Required for amended slips
    }

    amended_slip_data_2 = {
        'Annee': 2025,
        'NoReleve': '111111112',
        'BeneficiaireType': 1,
        'BeneficiaireNo': '0000000378950',
        'PersonneNAS': '130692544',  # Valid SIN that passes Luhn checksum
        'PersonneNomFamille': 'ALBANESE',
        'PersonnePrenom': 'DOROSHY',
        'PersonneInitiale': None,
        'RaisonSocialeNom1': None,
        'RaisonSocialeNom2': None,
        'RaisonSocialeAutreNoId': None,
        'AdresseLigne1': '38 Habitant Cres',
        'AdresseLigne2': None,
        'AdresseVille': 'Whitby',
        'AdresseProvince': 'ON',
        'AdresseCodePostal': 'L1P1E2',
        'A1_DividendeDetermine': None,
        'A2_DividendeOrdinaire': None,
        'B_DividendeImposable': None,
        'C_CreditImpotDividende': None,
        'D_InteretSourceCdn': 1180.89,
        'E_AutreRevenuCdn': None,
        'F_RevenuBrutEtranger': None,
        'G_ImpotEtranger': None,
        'H_RedevanceCdn': None,
        'I_DividendeGainCapital': None,
        'J_RevenuAccumuleRente': None,
        'K_InteretBilletsLies': None,
        'DeviseEtrangere': 'CAD',
        'CodeRensCompl': None,
        'DonneeRensCompl': None,
        'NoReleveDerniereTrans': '100000002'  # Required for amended slips
    }

    trailer_data = {
        'Annee': 2025,
        'NbReleves': 2,
        'NoId': '1149157944',
        'TypeDossier': 'RS',
        'NoDossier': '0000',
        'NEQ': None,
        'NoSuccursale': None,
        'Nom': "President's Choice Bank",
        'AdresseLigne1': '500 Lakeshore Blvd. West',
        'AdresseLigne2': 'Suite 600',
        'AdresseVille': 'Toronto',
        'AdresseProvince': 'ON',
        'AdresseCodePostal': 'M5V1A5'
    }

    return {
        # Row objects for new streaming tests
        'header_row': mock_bigquery_row(header_data),
        'slip_rows': [mock_bigquery_row(slip_data_1), mock_bigquery_row(slip_data_2)],
        'amended_slip_rows': [mock_bigquery_row(amended_slip_data_1), mock_bigquery_row(amended_slip_data_2)],
        'trailer_row': mock_bigquery_row(trailer_data),
        # Dataframes for backwards compatibility with existing tests
        'header': pd.DataFrame({k: [v] for k, v in header_data.items()}),
        'slips': pd.DataFrame({
            'Annee': [2025, 2025],
            'NoReleve': ['111111111', '111111112'],  # Now strings in database
            'BeneficiaireType': [1, 1],
            'BeneficiaireNo': ['0000000377845', '0000000378950'],
            'PersonneNAS': ['046454286', '130692544'],  # Valid SINs that pass Luhn checksum
            'PersonneNomFamille': ['FROEW', 'ALBANESE'],
            'PersonnePrenom': ['JOSEPH', 'DOROSHY'],
            'PersonneInitiale': [None, None],
            'RaisonSocialeNom1': [None, None],
            'AdresseLigne1': ['82 H St', '38 Habitant Cres'],
            'AdresseLigne2': [None, None],
            'AdresseVille': ['La Romaine', 'Whitby'],
            'AdresseProvince': ['QC', 'ON'],
            'AdresseCodePostal': ['G0G1M0', 'L1P1E2'],
            'A1_DividendeDetermine': [None, None],
            'A2_DividendeOrdinaire': [None, None],
            'B_DividendeImposable': [None, None],
            'C_CreditImpotDividende': [None, None],
            'D_InteretSourceCdn': [849.18, 1180.89],
            'E_AutreRevenuCdn': [None, None],
            'F_RevenuBrutEtranger': [None, None],
            'G_ImpotEtranger': [None, None],
            'H_RedevanceCdn': [None, None],
            'I_DividendeGainCapital': [None, None],
            'J_RevenuAccumuleRente': [None, None],
            'K_InteretBilletsLies': [None, None],
            'DeviseEtrangere': ['CAD', 'CAD'],
            'CodeRensCompl': [None, None],
            'DonneeRensCompl': [None, None],
            'NoReleveDerniereTrans': [None, None]
        }),
        'trailer': pd.DataFrame({
            'Annee': [2025],
            'NbReleves': [2],
            'NoId': ['1149157944'],
            'TypeDossier': ['RS'],
            'NoDossier': ['0000'],
            'NEQ': [None],
            'NoSuccursale': [None],
            'Nom': ["President's Choice Bank"],
            'AdresseLigne1': ['500 Lakeshore Blvd. West'],
            'AdresseLigne2': ['Suite 600'],
            'AdresseVille': ['Toronto'],
            'AdresseProvince': ['ON'],
            'AdresseCodePostal': ['M5V1A5']
        })
    }


@pytest.fixture
def mock_task_instance():
    """Fixture for mocking Airflow task instance."""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = 'test-file.xml'
    return mock_ti


@pytest.fixture
def mock_get_xml_file_name():
    """Fixture for mocking get_xml_file_name method."""
    with patch.object(RL3XmlGeneratorDagBuilder, 'get_xml_file_name') as mock_func:
        mock_func.return_value = 'test-file.xml'
        yield mock_func


@pytest.fixture
def mock_gcs_blob_for_writing():
    """Fixture for mocking GCS blob.open for writing (streaming)."""
    with patch('digital_adoption_tax_statements.rl3_tax.rl3_statement_xml_generate_launcher.storage.Client') as mock_client_class:
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_stream = MagicMock(spec=io.BytesIO)

        mock_client_class.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.open.return_value.__enter__.return_value = mock_stream

        yield {
            'client': mock_client,
            'bucket': mock_bucket,
            'blob': mock_blob,
            'stream': mock_stream
        }


@pytest.fixture
def mock_open_xml_stream():
    """Fixture for mocking open_xml_stream method."""
    with patch.object(RL3XmlGeneratorDagBuilder, 'open_xml_stream') as mock_func:
        # Return a BytesIO object (file-like) instead of a string
        xml_content = b'<?xml version="1.0" encoding="UTF-8"?><test>xml content</test>'
        mock_func.return_value.__enter__.return_value = io.BytesIO(xml_content)
        yield mock_func


@pytest.fixture
def mock_load_xsd():
    """Fixture for mocking load_xsd function."""
    with patch('digital_adoption_tax_statements.rl3_tax.rl3_statement_xml_generate_launcher.load_xsd') as mock_func:
        mock_schema = MagicMock()
        mock_func.return_value = mock_schema
        yield mock_func


@pytest.fixture
def mock_validate_xml():
    """Fixture for mocking validate_xml function."""
    with patch('digital_adoption_tax_statements.rl3_tax.rl3_statement_xml_generate_launcher.validate_xml') as mock_func:
        yield mock_func


@pytest.fixture
def mock_get_schema_version():
    """Fixture for mocking get_schema_version method."""
    with patch.object(RL3XmlGeneratorDagBuilder, 'get_schema_version') as mock_func:
        # Default return values for common years
        def side_effect(year):
            versions = {2025: "2025.1", 2026: "2026.1"}
            if year in versions:
                return versions[year]
            raise AirflowFailException(f"Schema version not configured for year {year}")
        mock_func.side_effect = side_effect
        yield mock_func


class TestRL3StatementXMLGenerateLauncher:
    """Test suite for RL3 Statement XML Generate Launcher DAG."""

    @pytest.fixture
    def dag(self):
        """Fixture to create DAG instance using DAGFactory."""
        dags = DAGFactory().create_dag(RL3XmlGeneratorDagBuilder, DAG_ID)
        return dags[DAG_ID]

    def test_dag_structure(self, dag):
        """Test DAG structure and configuration."""
        # Check DAG ID
        assert dag.dag_id == 'rl3_generate_xml'

        # Check default args
        assert dag.default_args['owner'] == 'team-digital-adoption-alerts'
        assert dag.default_args['capability'] == 'Account Management'
        assert dag.default_args['severity'] == 'P2'
        assert dag.default_args['max_active_runs'] == 1
        assert dag.default_args['retries'] == 3

        # Check schedule
        assert dag.schedule_interval is None

        # Check is_paused_upon_creation
        assert dag.is_paused_upon_creation is True

        # Check catchup
        assert dag.catchup is False

        # Check tags (automatically added by DAGFactory)
        assert 'team-digital-adoption-alerts' in dag.tags
        assert 'P2' in dag.tags

    def test_dag_tasks_and_dependencies(self, dag):
        """Test that all expected tasks are present in the DAG."""
        expected_task_ids = [
            'start',
            'create_xml',
            'validate_xml_with_xsd',
            'move_files_to_outbound_landing',
            'end'
        ]

        actual_task_ids = set(dag.task_dict.keys())
        assert set(expected_task_ids).issubset(actual_task_ids)

        # Check task dependencies
        for task, downstream_task in zip(expected_task_ids, expected_task_ids[1:]):
            current_task = dag.get_task(task)
            assert current_task.downstream_list[0].task_id == downstream_task

    def test_task_types(self, dag):
        """Test that tasks are of correct types."""
        assert isinstance(dag.get_task('start'), EmptyOperator)
        assert isinstance(dag.get_task('create_xml'), PythonOperator)
        assert isinstance(dag.get_task('validate_xml_with_xsd'), PythonOperator)
        assert isinstance(dag.get_task('move_files_to_outbound_landing'), PythonOperator)
        assert isinstance(dag.get_task('end'), EmptyOperator)

    def test_get_xsd_path(self):
        """Test XSD path generation for a given year."""
        # Act
        result_2025 = get_xsd_path(2025)
        result_2026 = get_xsd_path(2026)

        # Assert
        assert 'rl3_xmlschm_2025/Transmission.xsd' in result_2025
        assert 'rl3_xmlschm_2026/Transmission.xsd' in result_2026

    def test_get_schema_version(self, dag_builder_instance):
        """Test schema version retrieval from configuration."""
        # Act
        result_2025 = dag_builder_instance.get_schema_version(2025)
        result_2026 = dag_builder_instance.get_schema_version(2026)

        # Assert - based on config: 2025: "2025.1", 2026: "2026.1"
        assert result_2025 == "2025.1"
        assert result_2026 == "2026.1"

    def test_get_schema_version_missing_year(self, dag_builder_instance):
        """Test schema version with unconfigured year."""
        # Act & Assert
        with pytest.raises(AirflowFailException, match="Schema version not configured for year 2030"):
            dag_builder_instance.get_schema_version(2030)

    def test_get_xml_file_name_original(self, dag_builder_instance, mock_datetime, mock_read_env_filepattern):
        """Test XML file name generation for original slips."""
        # Arrange
        sbmt_ref_id = 'TEST123'
        type_envoi = 1  # Original

        # Act
        result = dag_builder_instance.get_xml_file_name(sbmt_ref_id, type_envoi)

        # Assert
        assert 'pcb_rq_rl3_slip_o_TEST123_20251114120000_test.xml' in result

    def test_get_xml_file_name_amended(self, dag_builder_instance, mock_datetime, mock_read_env_filepattern):
        """Test XML file name generation for amended slips."""
        # Arrange
        sbmt_ref_id = 'TEST456'
        type_envoi = 4  # Amended

        mock_read_env_filepattern.return_value = 'revenu_quebec_outbound_rl3_slip/pcb_rq_rl3_slip_a_TEST456_20251114120000_test.xml'

        # Act
        result = dag_builder_instance.get_xml_file_name(sbmt_ref_id, type_envoi)

        # Assert
        assert 'pcb_rq_rl3_slip_a' in result
        assert '.xml' in result

    def test_get_xml_file_name_cancelled(self, dag_builder_instance, mock_datetime, mock_read_env_filepattern):
        """Test XML file name generation for cancelled slips."""
        # Arrange
        sbmt_ref_id = 'TEST789'
        type_envoi = 6  # Cancelled

        mock_read_env_filepattern.return_value = 'revenu_quebec_outbound_rl3_slip/pcb_rq_rl3_slip_c_TEST789_20251114120000_test.xml'

        # Act
        result = dag_builder_instance.get_xml_file_name(sbmt_ref_id, type_envoi)

        # Assert
        assert 'pcb_rq_rl3_slip_c' in result
        assert '.xml' in result

    def test_process_header(self, sample_dataframes, xml_writer_helper):
        """Test processing header data into XML."""
        # Arrange
        header_row = sample_dataframes['header_row']
        writer, get_xml = xml_writer_helper()

        # Act
        process_header(header_row, writer)

        # Assert - Parse the generated XML
        p = get_xml()  # This returns the P element
        assert p is not None
        assert p.tag == 'P'
        assert p.find('Annee').text == '2025'
        assert p.find('TypeEnvoi').text == '1'

        # Check Preparateur
        preparateur = p.find('Preparateur')
        assert preparateur is not None
        assert preparateur.find('No').text == 'NP088880'
        assert preparateur.find('Type').text == '2'
        assert preparateur.find('Nom1').text == "President's Choice Bank"

        # Check Preparateur Address
        prep_addr = preparateur.find('Adresse')
        assert prep_addr is not None
        assert prep_addr.find('Ligne1').text == '500 Lakeshore Blvd. West'
        assert prep_addr.find('Ville').text == 'Toronto'
        assert prep_addr.find('Province').text == 'ON'

        # Check Comptabilite
        comptabilite = p.find('Comptabilite')
        assert comptabilite is not None
        assert comptabilite.find('Nom').text == 'Rod Lyn'
        assert comptabilite.find('IndRegional').text == '416'

        # Check required fields
        assert p.find('NoCertification').text == 'RQ-25-99-999'
        assert p.find('IdPartenaireReleves').text == '0000000000001DC1'
        assert p.find('IdProduitReleves').text == '0000000000003964'

    def test_process_header_missing_required_field(self, mock_bigquery_row, xml_writer_helper):
        """Test processing header data with missing required field."""
        # Arrange
        header_data = {
            'Annee': None,  # Required field missing
            'TypeEnvoi': 1,
            'PreparateurNo': 'NP088880',
            'PreparateurNom1': 'Test',
            'NoCertification': 'RQ-25-99-999',
            'IdPartenaireReleves': '0000000000001DC1',
            'IdProduitReleves': '0000000000003964'
        }
        header_row = mock_bigquery_row(header_data)
        writer, get_xml = xml_writer_helper()

        # Act & Assert
        with pytest.raises(AirflowFailException, match="Field 'Annee' is required but is null or blank"):
            process_header(header_row, writer)

    def test_process_slips(self, sample_dataframes, xml_writer_helper):
        """Test processing slip data into XML."""
        # Arrange
        slip_rows = sample_dataframes['slip_rows']
        writer, get_xml = xml_writer_helper()
        type_envoi = 1  # Regular

        # Wrap slips in Groupe03 for testing
        writer.start_element('Groupe03')

        # Act
        slip_count = process_slips(slip_rows, writer, type_envoi)

        # Close Groupe03
        writer.end_element('Groupe03')

        # Assert count
        assert slip_count == 2

        # Parse XML
        groupe03 = get_xml()
        r_slips = groupe03.findall('R')
        assert len(r_slips) == 2

        # Check first slip
        first_slip = r_slips[0]
        assert first_slip.find('Annee').text == '2025'
        assert first_slip.find('NoReleve').text == '111111111'

        # Check Beneficiaire
        beneficiaire = first_slip.find('Beneficiaire')
        assert beneficiaire is not None
        assert beneficiaire.find('Type').text == '1'
        assert beneficiaire.find('No').text == '0000000377845'

        # Check Personne
        personne = beneficiaire.find('Personne')
        assert personne is not None
        assert personne.find('NAS').text == '046454286'
        assert personne.find('NomFamille').text == 'FROEW'
        assert personne.find('Prenom').text == 'JOSEPH'

        # Check Adresse
        adresse = beneficiaire.find('Adresse')
        assert adresse is not None
        assert adresse.find('Ligne1').text == '82 H St'
        assert adresse.find('Ville').text == 'La Romaine'
        assert adresse.find('Province').text == 'QC'

        # Check Montants
        montants = first_slip.find('Montants')
        assert montants is not None
        assert montants.find('D_InteretSourceCdn').text == '849.18'
        assert montants.find('DeviseEtrangere').text == 'CAD'

    def test_process_slips_invalid_noreleve(self, mock_bigquery_row, xml_writer_helper):
        """Test processing slip with invalid NoReleve (exceeds 9 digits)."""
        # Arrange
        slip_data = {
            'Annee': 2025,
            'NoReleve': '12345678901',  # 11 digits - exceeds maximum (now string in database)
            'BeneficiaireType': 1,
            'PersonneNAS': '046454286',  # Valid SIN
            'PersonneNomFamille': 'Test',
            'PersonnePrenom': 'User'
        }
        slip_rows = [mock_bigquery_row(slip_data)]
        writer, get_xml = xml_writer_helper()

        # Act & Assert
        with pytest.raises(AirflowFailException, match="NoReleve exceeds 9 digits"):
            process_slips(slip_rows, writer, 1)

    def test_process_slips_noreleve_with_padding(self, sample_dataframes, mock_bigquery_row, xml_writer_helper):
        """Test processing slip with NoReleve that needs padding (fewer than 9 digits)."""
        # Arrange
        slip_data = {
            'Annee': 2025,
            'NoReleve': '123',  # Only 3 digits - should be padded to 000000123
            'BeneficiaireType': 1,
            'PersonneNAS': '046454286',  # Valid SIN
            'PersonneNomFamille': 'Test',
            'PersonnePrenom': 'User',
            'PersonneInitiale': None,
            'BeneficiaireNo': None,
            'AdresseLigne1': '123 Test St',
            'AdresseLigne2': None,
            'AdresseVille': 'Montreal',
            'AdresseProvince': 'QC',
            'AdresseCodePostal': 'H1H1H1',
            'AdressePays': 'CAN',
            'Case_A_Mnt': 1000.00,
            'CaseRensCompl': None,
            'CodeRensCompl': None,
            'DonneeRensCompl': None,
            'NoReleveDerniereTrans': None
        }
        slip_rows = [mock_bigquery_row(slip_data)]
        writer, get_xml = xml_writer_helper()

        # Wrap in Groupe03
        writer.start_element('Groupe03')

        # Act
        process_slips(slip_rows, writer, 1)

        # Close Groupe03
        writer.end_element('Groupe03')

        # Assert
        root = get_xml()  # root IS the Groupe03 element we created

        r_slips = root.findall('R')
        assert len(r_slips) == 1

        first_slip = r_slips[0]
        # Verify NoReleve was padded to 9 digits with leading zeros
        assert first_slip.find('NoReleve').text == '000000123'

    def test_process_slips_invalid_nas_too_short(self, mock_bigquery_row, xml_writer_helper):
        """Test processing slip with NAS that is too short (less than 9 digits).

        NAS must be exactly 9 digits - no padding is performed.
        """
        # Arrange
        slip_data = {
            'Annee': 2025,
            'NoReleve': '111111111',
            'BeneficiaireType': 1,
            'PersonneNAS': '12345678',  # Only 8 digits - should fail immediately
            'PersonneNomFamille': 'Test',
            'PersonnePrenom': 'User'
        }
        slip_rows = [mock_bigquery_row(slip_data)]
        writer, get_xml = xml_writer_helper()

        # Act & Assert - Fails immediately with length validation error
        with pytest.raises(AirflowFailException, match="NAS must be exactly 9 digits"):
            process_slips(slip_rows, writer, 1)

    def test_process_slips_invalid_nas_too_long(self, mock_bigquery_row, xml_writer_helper):
        """Test processing slip with NAS that is too long (more than 9 digits)."""
        # Arrange
        slip_data = {
            'Annee': 2025,
            'NoReleve': '111111111',
            'BeneficiaireType': 1,
            'PersonneNAS': '1234567890',  # 10 digits
            'PersonneNomFamille': 'Test',
            'PersonnePrenom': 'User'
        }
        slip_rows = [mock_bigquery_row(slip_data)]
        writer, get_xml = xml_writer_helper()

        # Act & Assert
        with pytest.raises(AirflowFailException, match="NAS must be exactly 9 digits"):
            process_slips(slip_rows, writer, 1)

    def test_process_slips_invalid_nas_non_digits(self, mock_bigquery_row, xml_writer_helper):
        """Test processing slip with NAS that contains non-digit characters.

        Note: NAS must contain only digits. Non-digit characters cause immediate failure.
        """
        # Arrange
        slip_data = {
            'Annee': 2025,
            'NoReleve': '111111111',
            'BeneficiaireType': 1,
            'PersonneNAS': '12345678A',  # Contains letter - invalid
            'PersonneNomFamille': 'Test',
            'PersonnePrenom': 'User'
        }
        slip_rows = [mock_bigquery_row(slip_data)]
        writer, get_xml = xml_writer_helper()

        # Act & Assert - Fails because NAS contains non-digit characters
        with pytest.raises(AirflowFailException, match="NAS must contain only digits"):
            process_slips(slip_rows, writer, 1)

    # ==================== LUHN ALGORITHM (SIN CHECKSUM) VALIDATION TESTS ====================

    def test_validate_luhn_valid_sins(self):
        """Test Luhn algorithm with valid SINs."""
        # Known valid SINs (pass Luhn checksum)
        valid_sins = [
            '046454286',  # Standard valid SIN
            '130692544',  # Another valid SIN
        ]
        for sin in valid_sins:
            assert _validate_luhn(sin) is True, f"SIN {sin} should be valid"

    def test_validate_luhn_invalid_sins(self):
        """Test Luhn algorithm with invalid SINs."""
        # Invalid SINs (fail Luhn checksum)
        invalid_sins = [
            '123456789',  # Fails checksum
            '111111111',  # Fails checksum
            '999999999',  # Fails checksum
            '123456780',  # Fails checksum
        ]
        for sin in invalid_sins:
            assert _validate_luhn(sin) is False, f"SIN {sin} should be invalid"

    def test_validate_luhn_assumes_valid_input(self):
        """Test that _validate_luhn assumes pre-validated 9-digit input from _validate_nas_value.

        Note: _validate_luhn is only called from _validate_nas_value after length/format validation,
        so it doesn't need to handle invalid formats. This test documents that assumption.
        """
        # Valid 9-digit SINs are handled correctly
        assert _validate_luhn('046454286') is True  # Valid
        assert _validate_luhn('123456789') is False  # Invalid checksum

    def test_process_slips_invalid_nas_luhn_checksum(self, mock_bigquery_row, xml_writer_helper):
        """Test processing slip with NAS that fails Luhn checksum validation."""
        # Arrange - 123456789 is 9 digits but fails Luhn checksum
        slip_data = {
            'Annee': 2025,
            'NoReleve': '111111111',
            'BeneficiaireType': 1,
            'PersonneNAS': '123456789',  # 9 digits but invalid checksum
            'PersonneNomFamille': 'Test',
            'PersonnePrenom': 'User'
        }
        slip_rows = [mock_bigquery_row(slip_data)]
        writer, get_xml = xml_writer_helper()

        # Act & Assert
        with pytest.raises(AirflowFailException, match="NAS fails Luhn checksum validation"):
            process_slips(slip_rows, writer, 1)

    def test_process_slips_valid_nas_luhn_checksum(self, mock_bigquery_row, xml_writer_helper):
        """Test processing slip with valid NAS that passes Luhn checksum."""
        # Arrange - 046454286 passes Luhn checksum
        slip_data = {
            'Annee': 2025,
            'NoReleve': '111111111',
            'BeneficiaireType': 1,
            'PersonneNAS': '046454286',  # Valid SIN that passes Luhn
            'PersonneNomFamille': 'Test',
            'PersonnePrenom': 'User',
            'D_InteretSourceCdn': 100.00,
            'DeviseEtrangere': 'CAD'
        }
        slip_rows = [mock_bigquery_row(slip_data)]
        writer, get_xml = xml_writer_helper()

        # Wrap in Groupe03
        writer.start_element('Groupe03')

        # Act - should not raise
        slip_count = process_slips(slip_rows, writer, 1)

        # Close Groupe03
        writer.end_element('Groupe03')

        # Assert
        assert slip_count == 1
        groupe03 = get_xml()
        r_slip = groupe03.find('R')
        personne = r_slip.find('Beneficiaire/Personne')
        # NAS should be padded to 9 digits
        assert personne.find('NAS').text == '046454286'

    def test_process_slips_missing_required_personne_fields(self, mock_bigquery_row, xml_writer_helper):
        """Test processing slip with missing required Personne fields."""
        # Arrange
        slip_data = {
            'Annee': 2025,
            'NoReleve': '111111111',
            'BeneficiaireType': 1,
            'PersonneNAS': '046454286',  # Valid SIN
            'PersonneNomFamille': None,  # Required field missing
            'PersonnePrenom': 'User'
        }
        slip_rows = [mock_bigquery_row(slip_data)]
        writer, get_xml = xml_writer_helper()

        # Act & Assert
        with pytest.raises(AirflowFailException, match="PersonneNomFamille is required for Personne but is null or empty"):
            process_slips(slip_rows, writer, 1)

    def test_process_slips_amended_type(self, sample_dataframes, xml_writer_helper):
        """Test processing amended slips (A type)."""
        # Arrange - Use amended slip rows which have NoReleveDerniereTrans (required for amended slips)
        slip_rows = sample_dataframes['amended_slip_rows']
        writer, get_xml = xml_writer_helper()
        type_envoi = 4  # Amended

        # Wrap in Groupe03
        writer.start_element('Groupe03')

        # Act
        slip_count = process_slips(slip_rows, writer, type_envoi)

        # Close Groupe03
        writer.end_element('Groupe03')

        # Assert
        assert slip_count == 2
        groupe03 = get_xml()
        a_slips = groupe03.findall('A')
        assert len(a_slips) == 2

        # Verify NoReleveDerniereTrans is included in amended slips
        first_slip = a_slips[0]
        assert first_slip.find('NoReleveDerniereTrans').text == '100000001'

    def test_process_slips_cancelled_type(self, sample_dataframes, xml_writer_helper):
        """Test processing cancelled slips (D type)."""
        # Arrange
        slip_rows = sample_dataframes['slip_rows']
        writer, get_xml = xml_writer_helper()
        type_envoi = 6  # Cancelled

        # Wrap in Groupe03
        writer.start_element('Groupe03')

        # Act
        slip_count = process_slips(slip_rows, writer, type_envoi)

        # Close Groupe03
        writer.end_element('Groupe03')

        # Assert
        assert slip_count == 2
        groupe03 = get_xml()
        d_slips = groupe03.findall('D')
        assert len(d_slips) == 2

        # Cancelled slips should not have Montants section
        first_slip = d_slips[0]
        assert first_slip.find('Montants') is None

    def test_process_trailer(self, sample_dataframes, xml_writer_helper):
        """Test processing trailer data into XML."""
        # Arrange
        trailer_row = sample_dataframes['trailer_row']
        writer, get_xml = xml_writer_helper()

        # Act
        process_trailer(trailer_row, writer)

        # Assert
        t = get_xml()  # Returns T element
        assert t is not None
        assert t.tag == 'T'
        assert t.find('Annee').text == '2025'
        assert t.find('NbReleves').text == '2'

        # Check PayeurMandataire
        payeur = t.find('PayeurMandataire')
        assert payeur is not None
        assert payeur.find('NoId').text == '1149157944'
        assert payeur.find('TypeDossier').text == 'RS'
        assert payeur.find('NoDossier').text == '0000'
        assert payeur.find('Nom').text == "President's Choice Bank"

        # Check Payeur Address
        payeur_addr = payeur.find('Adresse')
        assert payeur_addr is not None
        assert payeur_addr.find('Ligne1').text == '500 Lakeshore Blvd. West'
        assert payeur_addr.find('Ville').text == 'Toronto'

    def test_process_trailer_missing_required_field(self, mock_bigquery_row, xml_writer_helper):
        """Test processing trailer data with missing required field."""
        # Arrange
        trailer_data = {
            'Annee': 2025,
            'NbReleves': None,  # Required field missing
            'NoId': '1149157944',
            'TypeDossier': 'RS',
            'NoDossier': '0000',
            'Nom': 'Test',
            'AdresseLigne1': '123 Main St'
        }
        trailer_row = mock_bigquery_row(trailer_data)
        writer, get_xml = xml_writer_helper()

        # Act & Assert
        with pytest.raises(AirflowFailException, match="Field 'NbReleves' is required but is null or blank"):
            process_trailer(trailer_row, writer)

    def test_open_xml_stream(self, dag_builder_instance, mock_storage_client):
        """Test opening XML stream from Google Cloud Storage."""
        # Arrange
        xml_content = b'<?xml version="1.0" encoding="UTF-8"?><test>xml content</test>'
        mock_file_stream = io.BytesIO(xml_content)
        mock_storage_client['blob'].open.return_value = mock_file_stream
        file_path = 'test-file.xml'

        # Act
        result = dag_builder_instance.open_xml_stream(file_path)

        # Assert
        mock_storage_client['blob'].open.assert_called_once_with("rb")
        assert result == mock_file_stream

    def test_load_xsd(self, mock_xmlschema):
        """Test loading XSD schema."""
        # Act
        result = load_xsd('test.xsd')

        # Assert
        assert result == mock_xmlschema

    def test_validate_xml_valid(self):
        """Test XML validation with valid XML."""
        # Arrange
        mock_schema = MagicMock()
        mock_schema.is_valid.return_value = True
        xml_content = '<?xml version="1.0" encoding="UTF-8"?><test>valid xml</test>'

        # Act
        validate_xml(xml_content, mock_schema)

        # Assert
        mock_schema.is_valid.assert_called_once_with(xml_content)

    def test_validate_xml_invalid(self):
        """Test XML validation with invalid XML."""
        # Arrange
        mock_schema = MagicMock()
        mock_schema.is_valid.return_value = False
        mock_error = MagicMock()
        mock_error.reason = "value doesn't match any pattern of ['\\d{9}']"
        mock_error.elem = MagicMock()
        mock_error.elem.tag = '{http://www.mrq.gouv.qc.ca/T5}NoReleve'
        mock_error.elem.text = '12345'
        mock_error.path = '/Transmission/Groupe03/R[1]/NoReleve'
        mock_schema.iter_errors.return_value = [mock_error]
        xml_stream = io.BytesIO(b'<invalid>xml</invalid>')

        # Act & Assert
        with pytest.raises(AirflowFailException, match="XML Validation Errors"):
            validate_xml(xml_stream, mock_schema)

    def test_xml_validation_task(self, dag_builder_instance, mock_task_instance, mock_load_xsd, mock_open_xml_stream, mock_validate_xml, mock_storage_client):
        """Test the main XML validation task."""
        # Arrange
        mock_task_instance.xcom_pull.return_value = ['file1.xml', 'file2.xml']
        kwargs = {'task_instance': mock_task_instance}

        # Act
        result = dag_builder_instance.xml_validation_task(**kwargs)

        # Assert
        assert len(result) == 2
        assert result[0]['status'] == 'valid'
        assert result[1]['status'] == 'valid'
        # validate_xml should be called twice (once per file)
        assert mock_validate_xml.call_count == 2

    def test_task_configurations(self, dag):
        """Test specific task configurations."""
        # Test validation task has retries=0
        validate_xml_task = dag.get_task('validate_xml_with_xsd')
        assert validate_xml_task.retries == 0

        # Test move files task exists
        move_task = dag.get_task('move_files_to_outbound_landing')
        assert isinstance(move_task, PythonOperator)

    def test_process_header_with_null_optional_fields(self, mock_bigquery_row, xml_writer_helper):
        """Test processing header data with null optional values."""
        # Arrange
        header_data = {
            'Annee': 2025,
            'TypeEnvoi': 1,
            'PreparateurNo': 'NP088880',
            'PreparateurType': None,  # optional
            'PreparateurNom1': 'Test Company',
            'PreparateurNom2': None,  # optional
            'InformatiqueNom': None,  # optional
            'ComptabiliteNom': None,  # optional
            'NoCertification': 'RQ-25-99-999',
            'NomLogiciel': None,  # optional
            'IdPartenaireReleves': '0000000000001DC1',
            'IdProduitReleves': '0000000000003964'
        }
        header_row = mock_bigquery_row(header_data)
        writer, get_xml = xml_writer_helper()

        # Act
        process_header(header_row, writer)

        # Assert
        p = get_xml()
        assert p is not None
        assert p.tag == 'P'
        assert p.find('Annee').text == '2025'

        # Optional fields should not be present
        preparateur = p.find('Preparateur')
        assert preparateur.find('Type') is None

        # No Informatique or Comptabilite sections
        assert p.find('Informatique') is None
        assert p.find('Comptabilite') is None

    def test_process_slips_with_raison_sociale(self, mock_bigquery_row, xml_writer_helper):
        """Test processing slip data with RaisonSociale (corporation)."""
        # Arrange
        slip_data = {
            'Annee': 2025,
            'NoReleve': '111111111',
            'BeneficiaireType': 2,
            'BeneficiaireNo': 'CORP001',
            'PersonneNAS': None,
            'RaisonSocialeNom1': 'Test Corporation Inc.',
            'RaisonSocialeNom2': 'Division A',
            'RaisonSocialeAutreNoId': 'ID123',
            'D_InteretSourceCdn': 1000.00,
            'DeviseEtrangere': 'CAD',
            'AdresseLigne1': None,
            'AdresseLigne2': None,
            'AdresseVille': None,
            'AdresseProvince': None,
            'AdresseCodePostal': None
        }
        slip_row = mock_bigquery_row(slip_data)
        slips_iterator = [slip_row]  # Iterator with one slip
        writer, get_xml = xml_writer_helper()

        # Wrap in Groupe03
        writer.start_element('Groupe03')

        # Act
        slip_count = process_slips(slips_iterator, writer, 1)

        # Close Groupe03
        writer.end_element('Groupe03')

        # Assert count
        assert slip_count == 1

        # Assert
        groupe03 = get_xml()
        r_slip = groupe03.find('R')
        beneficiaire = r_slip.find('Beneficiaire')
        raison_sociale = beneficiaire.find('RaisonSociale')
        assert raison_sociale is not None
        assert raison_sociale.find('Nom1').text == 'Test Corporation Inc.'
        assert raison_sociale.find('Nom2').text == 'Division A'
        assert raison_sociale.find('AutreNoId').text == 'ID123'

        # Should not have Personne
        assert beneficiaire.find('Personne') is None

    def test_process_slips_missing_beneficiaire_info(self, mock_bigquery_row, xml_writer_helper):
        """Test processing slip without Personne or RaisonSociale."""
        # Arrange
        slip_data = {
            'Annee': 2025,
            'NoReleve': '111111111',
            'BeneficiaireType': 1,
            'PersonneNAS': None,
            'RaisonSocialeNom1': None
        }
        slip_rows = [mock_bigquery_row(slip_data)]
        writer, get_xml = xml_writer_helper()

        # Act & Assert
        with pytest.raises(AirflowFailException, match="must have either PersonneNAS or RaisonSocialeNom1"):
            process_slips(slip_rows, writer, 1)

    def test_create_xml_with_year_parameter(self, dag_builder_instance, mock_bigquery_client, sample_dataframes, mock_get_xml_file_name, mock_gcs_blob_for_writing, mock_get_schema_version, mock_bigquery_row):
        """Test create_xml with year parameter from dag_run."""
        # Arrange
        mock_dag_run = MagicMock()
        mock_dag_run.conf = {'year': 2026}

        # Mock the query for submissions (returns iterator of Row objects)
        submission_row = mock_bigquery_row({'SbmtRefId': 'TEST123'})

        # Mock query results as iterators
        mock_bigquery_client.query.side_effect = [
            MagicMock(result=lambda: [submission_row]),  # submissions query
            MagicMock(result=lambda: [sample_dataframes['header_row']]),  # header query
            MagicMock(result=lambda: iter(sample_dataframes['slip_rows'])),  # slips query (iterator)
            MagicMock(result=lambda: [sample_dataframes['trailer_row']])  # trailer query
        ]

        # Act
        result = dag_builder_instance.create_xml(dag_run=mock_dag_run)

        # Assert
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0] == 'test-file.xml'
        # Verify get_schema_version was called with year 2026
        mock_get_schema_version.assert_called_with(2026)

    def test_create_xml_defaults_to_current_year(self, dag_builder_instance, mock_datetime, mock_bigquery_client, sample_dataframes, mock_get_xml_file_name, mock_gcs_blob_for_writing, mock_get_schema_version, mock_bigquery_row):
        """Test create_xml defaults to current year when no parameter provided."""
        # Arrange - mock_datetime fixture sets current year to 2025 (a configured year)
        submission_row = mock_bigquery_row({'SbmtRefId': 'TEST123'})

        mock_bigquery_client.query.side_effect = [
            MagicMock(result=lambda: [submission_row]),  # submissions query
            MagicMock(result=lambda: [sample_dataframes['header_row']]),  # header query
            MagicMock(result=lambda: iter(sample_dataframes['slip_rows'])),  # slips query (iterator)
            MagicMock(result=lambda: [sample_dataframes['trailer_row']])  # trailer query
        ]

        # Act
        result = dag_builder_instance.create_xml()

        # Assert
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0] == 'test-file.xml'
        # Verify get_schema_version was called with mocked current year (2025)
        mock_get_schema_version.assert_called_with(2025)

    def test_xml_validation_task_with_year_parameter(self, dag_builder_instance, mock_task_instance, mock_load_xsd, mock_open_xml_stream, mock_validate_xml, mock_storage_client):
        """Test XML validation task with year parameter from dag_run."""
        # Arrange
        mock_dag_run = MagicMock()
        mock_dag_run.conf = {'year': 2026}
        mock_task_instance.xcom_pull.return_value = ['file1.xml']

        # Act
        result = dag_builder_instance.xml_validation_task(task_instance=mock_task_instance, dag_run=mock_dag_run)

        # Assert
        # Verify XSD path for year 2026 was used
        mock_load_xsd.assert_called_once()
        xsd_path_arg = mock_load_xsd.call_args[0][0]
        assert 'rl3_xmlschm_2026/Transmission.xsd' in xsd_path_arg
        assert len(result) == 1
        assert result[0]['status'] == 'valid'

    def test_xml_validation_task_defaults_to_current_year(self, dag_builder_instance, mock_datetime, mock_task_instance, mock_load_xsd, mock_open_xml_stream, mock_validate_xml, mock_storage_client):
        """Test XML validation task defaults to current year when no parameter provided."""
        # Arrange - mock_datetime fixture sets current year to 2025 (a configured year)
        mock_task_instance.xcom_pull.return_value = ['file1.xml']

        # Act
        result = dag_builder_instance.xml_validation_task(task_instance=mock_task_instance)

        # Assert
        # Verify XSD path for current year was used (2025)
        mock_load_xsd.assert_called_once()
        xsd_path_arg = mock_load_xsd.call_args[0][0]
        assert 'rl3_xmlschm_2025/Transmission.xsd' in xsd_path_arg
        assert len(result) == 1
        assert result[0]['status'] == 'valid'

    def test_create_xml_with_unconfigured_year(self, dag_builder_instance, mock_bigquery_client, sample_dataframes, mock_gcs_blob_for_writing, mock_get_schema_version, mock_get_xml_file_name, mock_bigquery_row):
        """Test create_xml with unconfigured year raises proper error."""
        # Arrange
        mock_dag_run = MagicMock()
        mock_dag_run.conf = {'year': 2030}  # Year not in config

        submission_row = mock_bigquery_row({'SbmtRefId': 'TEST123'})

        mock_bigquery_client.query.side_effect = [
            MagicMock(result=lambda: [submission_row]),  # submissions query
            MagicMock(result=lambda: [sample_dataframes['header_row']]),  # header query
            MagicMock(result=lambda: iter(sample_dataframes['slip_rows'])),  # slips query (iterator)
            MagicMock(result=lambda: [sample_dataframes['trailer_row']])  # trailer query
        ]

        # Act & Assert
        # The fixture will raise AirflowFailException for year 2030
        with pytest.raises(AirflowFailException, match="Schema version not configured for year 2030"):
            dag_builder_instance.create_xml(dag_run=mock_dag_run)

    # ==================== NO_RELEVE_LIMIT VALIDATION TESTS ====================

    def test_process_slips_with_no_releve_limit_passes(self, mock_bigquery_row, xml_writer_helper):
        """Test that slips within no_releve_limit pass validation."""
        # Arrange
        slip_data = {
            'Annee': 2025,
            'NoReleve': '001234567',  # First 8 digits = 00123456 (123456)
            'BeneficiaireType': 1,
            'PersonneNAS': '046454286',  # Valid SIN
            'PersonneNomFamille': 'Test',
            'PersonnePrenom': 'User',
            'D_InteretSourceCdn': 100.00,
            'DeviseEtrangere': 'CAD'
        }
        slip_rows = [mock_bigquery_row(slip_data)]
        writer, get_xml = xml_writer_helper()
        no_releve_limit = 200000  # Limit is higher than first 8 digits (123456)

        # Wrap in Groupe03
        writer.start_element('Groupe03')

        # Act
        slip_count = process_slips(slip_rows, writer, 1, no_releve_limit)

        # Close Groupe03
        writer.end_element('Groupe03')

        # Assert - should complete without error
        assert slip_count == 1

    def test_process_slips_with_no_releve_limit_exceeds(self, mock_bigquery_row, xml_writer_helper):
        """Test that slips exceeding no_releve_limit fail validation."""
        # Arrange
        slip_data = {
            'Annee': 2025,
            'NoReleve': '001234567',  # First 8 digits = 00123456 (123456)
            'BeneficiaireType': 1,
            'PersonneNAS': '046454286',  # Valid SIN
            'PersonneNomFamille': 'Test',
            'PersonnePrenom': 'User'
        }
        slip_rows = [mock_bigquery_row(slip_data)]
        writer, get_xml = xml_writer_helper()
        no_releve_limit = 100000  # Limit is lower than first 8 digits (123456)

        # Act & Assert
        with pytest.raises(AirflowFailException, match="NoReleve first 8 digits.*exceeds the maximum allowed limit"):
            process_slips(slip_rows, writer, 1, no_releve_limit)

    def test_create_xml_with_no_releve_limit_parameter(self, dag_builder_instance, mock_bigquery_client, sample_dataframes, mock_get_xml_file_name, mock_gcs_blob_for_writing, mock_get_schema_version, mock_bigquery_row):
        """Test create_xml with no_releve_limit parameter from dag_run."""
        # Arrange
        mock_dag_run = MagicMock()
        mock_dag_run.conf = {'year': 2025, 'noreleve_limit': 99999999}  # High limit

        submission_row = mock_bigquery_row({'SbmtRefId': 'TEST123'})

        mock_bigquery_client.query.side_effect = [
            MagicMock(result=lambda: [submission_row]),
            MagicMock(result=lambda: [sample_dataframes['header_row']]),
            MagicMock(result=lambda: iter(sample_dataframes['slip_rows'])),
            MagicMock(result=lambda: [sample_dataframes['trailer_row']])
        ]

        # Act
        result = dag_builder_instance.create_xml(dag_run=mock_dag_run)

        # Assert
        assert isinstance(result, list)
        assert len(result) == 1

    # ==================== NO_RELEVE_PDF_LIMIT VALIDATION TESTS ====================

    def test_process_slips_with_no_releve_pdf_limit_passes(self, mock_bigquery_row, xml_writer_helper):
        """Test that slips with NoRelevePDF within limit pass validation."""
        # Arrange
        slip_data = {
            'Annee': 2025,
            'NoReleve': '111111111',
            'NoRelevePDF': '001234567',  # First 8 digits = 00123456 (123456)
            'BeneficiaireType': 1,
            'PersonneNAS': '046454286',  # Valid SIN
            'PersonneNomFamille': 'Test',
            'PersonnePrenom': 'User',
            'D_InteretSourceCdn': 100.00,
            'DeviseEtrangere': 'CAD'
        }
        slip_rows = [mock_bigquery_row(slip_data)]
        writer, get_xml = xml_writer_helper()
        no_releve_pdf_limit = 200000  # Limit is higher than first 8 digits (123456)

        # Wrap in Groupe03
        writer.start_element('Groupe03')

        # Act
        slip_count = process_slips(slip_rows, writer, 1, None, no_releve_pdf_limit)

        # Close Groupe03
        writer.end_element('Groupe03')

        # Assert - should complete without error
        assert slip_count == 1

    def test_process_slips_with_no_releve_pdf_limit_exceeds(self, mock_bigquery_row, xml_writer_helper):
        """Test that slips with NoRelevePDF exceeding limit fail validation."""
        # Arrange
        slip_data = {
            'Annee': 2025,
            'NoReleve': '111111111',
            'NoRelevePDF': '001234567',  # First 8 digits = 00123456 (123456)
            'BeneficiaireType': 1,
            'PersonneNAS': '046454286',  # Valid SIN
            'PersonneNomFamille': 'Test',
            'PersonnePrenom': 'User'
        }
        slip_rows = [mock_bigquery_row(slip_data)]
        writer, get_xml = xml_writer_helper()
        no_releve_pdf_limit = 100000  # Limit is lower than first 8 digits (123456)

        # Act & Assert
        with pytest.raises(AirflowFailException, match="NoRelevePDF first 8 digits.*exceeds the maximum allowed limit"):
            process_slips(slip_rows, writer, 1, None, no_releve_pdf_limit)

    def test_process_slips_with_no_releve_pdf_null_skips_validation(self, mock_bigquery_row, xml_writer_helper):
        """Test that slips with null NoRelevePDF skip PDF limit validation."""
        # Arrange
        slip_data = {
            'Annee': 2025,
            'NoReleve': '111111111',
            'NoRelevePDF': None,  # Null - should skip validation
            'BeneficiaireType': 1,
            'PersonneNAS': '046454286',  # Valid SIN
            'PersonneNomFamille': 'Test',
            'PersonnePrenom': 'User',
            'D_InteretSourceCdn': 100.00,
            'DeviseEtrangere': 'CAD'
        }
        slip_rows = [mock_bigquery_row(slip_data)]
        writer, get_xml = xml_writer_helper()
        no_releve_pdf_limit = 1  # Very low limit - but should be skipped since NoRelevePDF is null

        # Wrap in Groupe03
        writer.start_element('Groupe03')

        # Act
        slip_count = process_slips(slip_rows, writer, 1, None, no_releve_pdf_limit)

        # Close Groupe03
        writer.end_element('Groupe03')

        # Assert - should complete without error (validation skipped)
        assert slip_count == 1

    def test_process_slips_no_releve_pdf_not_in_xml(self, mock_bigquery_row, xml_writer_helper):
        """Test that NoRelevePDF is NOT written to the XML output."""
        # Arrange
        slip_data = {
            'Annee': 2025,
            'NoReleve': '111111111',
            'NoRelevePDF': '222222222',  # Should NOT appear in XML
            'BeneficiaireType': 1,
            'PersonneNAS': '046454286',  # Valid SIN
            'PersonneNomFamille': 'Test',
            'PersonnePrenom': 'User',
            'D_InteretSourceCdn': 100.00,
            'DeviseEtrangere': 'CAD'
        }
        slip_rows = [mock_bigquery_row(slip_data)]
        writer, get_xml = xml_writer_helper()

        # Wrap in Groupe03
        writer.start_element('Groupe03')

        # Act
        process_slips(slip_rows, writer, 1)

        # Close Groupe03
        writer.end_element('Groupe03')

        # Assert - NoRelevePDF should NOT be in the XML
        groupe03 = get_xml()
        r_slip = groupe03.find('R')
        assert r_slip.find('NoRelevePDF') is None
        # But NoReleve should be there
        assert r_slip.find('NoReleve').text == '111111111'

    def test_create_xml_with_no_releve_pdf_limit_parameter(self, dag_builder_instance, mock_bigquery_client, sample_dataframes, mock_get_xml_file_name, mock_gcs_blob_for_writing, mock_get_schema_version, mock_bigquery_row):
        """Test create_xml with norelevepdf_limit parameter from dag_run."""
        # Arrange
        mock_dag_run = MagicMock()
        mock_dag_run.conf = {'year': 2025, 'norelevepdf_limit': 99999999}  # High limit

        submission_row = mock_bigquery_row({'SbmtRefId': 'TEST123'})

        mock_bigquery_client.query.side_effect = [
            MagicMock(result=lambda: [submission_row]),
            MagicMock(result=lambda: [sample_dataframes['header_row']]),
            MagicMock(result=lambda: iter(sample_dataframes['slip_rows'])),
            MagicMock(result=lambda: [sample_dataframes['trailer_row']])
        ]

        # Act
        result = dag_builder_instance.create_xml(dag_run=mock_dag_run)

        # Assert
        assert isinstance(result, list)
        assert len(result) == 1

    def test_create_xml_with_both_limits(self, dag_builder_instance, mock_bigquery_client, sample_dataframes, mock_get_xml_file_name, mock_gcs_blob_for_writing, mock_get_schema_version, mock_bigquery_row):
        """Test create_xml with both noreleve_limit and norelevepdf_limit parameters."""
        # Arrange
        mock_dag_run = MagicMock()
        mock_dag_run.conf = {
            'year': 2025,
            'noreleve_limit': 99999999,
            'norelevepdf_limit': 99999999
        }

        submission_row = mock_bigquery_row({'SbmtRefId': 'TEST123'})

        mock_bigquery_client.query.side_effect = [
            MagicMock(result=lambda: [submission_row]),
            MagicMock(result=lambda: [sample_dataframes['header_row']]),
            MagicMock(result=lambda: iter(sample_dataframes['slip_rows'])),
            MagicMock(result=lambda: [sample_dataframes['trailer_row']])
        ]

        # Act
        result = dag_builder_instance.create_xml(dag_run=mock_dag_run)

        # Assert
        assert isinstance(result, list)
        assert len(result) == 1

    # ==================== MOVE FILES TO OUTBOUND TESTS ====================

    def test_move_files_to_outbound(self, dag_builder_instance, mock_task_instance):
        """Test move_files_to_outbound method."""
        # Arrange
        mock_task_instance.xcom_pull.return_value = ['file1.xml', 'file2.xml']

        with patch('digital_adoption_tax_statements.rl3_tax.rl3_statement_xml_generate_launcher.move_gcs_objects') as mock_move:
            mock_move.return_value = ['file1.xml', 'file2.xml']

            # Act
            result = dag_builder_instance.move_files_to_outbound(task_instance=mock_task_instance)

            # Assert
            assert result == ['file1.xml', 'file2.xml']
            mock_move.assert_called_once_with(
                source_bucket_name='test-staging-bucket',
                destination_bucket_name='test-outbound-bucket',
                file_paths=['file1.xml', 'file2.xml'],
                delete_source=True
            )

    def test_move_files_to_outbound_empty_list(self, dag_builder_instance, mock_task_instance):
        """Test move_files_to_outbound with empty file list."""
        # Arrange
        mock_task_instance.xcom_pull.return_value = []

        with patch('digital_adoption_tax_statements.rl3_tax.rl3_statement_xml_generate_launcher.move_gcs_objects') as mock_move:
            mock_move.return_value = []

            # Act
            result = dag_builder_instance.move_files_to_outbound(task_instance=mock_task_instance)

            # Assert
            assert result == []

    # ==================== CREATE XML EDGE CASES ====================

    def test_create_xml_no_submissions_found(self, dag_builder_instance, mock_bigquery_client):
        """Test create_xml when no submissions are found in header table."""
        # Arrange
        mock_bigquery_client.query.return_value.result.return_value = []

        # Act
        result = dag_builder_instance.create_xml()

        # Assert
        assert result == []

    def test_create_xml_missing_header_data(self, dag_builder_instance, mock_bigquery_client, sample_dataframes, mock_gcs_blob_for_writing, mock_get_schema_version, mock_bigquery_row):
        """Test create_xml fails when header data is missing for a submission."""
        # Arrange
        submission_row = mock_bigquery_row({'SbmtRefId': 'TEST123'})

        mock_bigquery_client.query.side_effect = [
            MagicMock(result=lambda: [submission_row]),  # submissions query
            MagicMock(result=lambda: []),  # header query - EMPTY
            MagicMock(result=lambda: iter(sample_dataframes['slip_rows'])),
            MagicMock(result=lambda: [sample_dataframes['trailer_row']])
        ]

        # Act & Assert
        with pytest.raises(AirflowFailException, match="No header data found for SbmtRefId"):
            dag_builder_instance.create_xml()

    def test_create_xml_missing_trailer_data(self, dag_builder_instance, mock_bigquery_client, sample_dataframes, mock_gcs_blob_for_writing, mock_get_schema_version, mock_bigquery_row):
        """Test create_xml fails when trailer data is missing for a submission."""
        # Arrange
        submission_row = mock_bigquery_row({'SbmtRefId': 'TEST123'})

        mock_bigquery_client.query.side_effect = [
            MagicMock(result=lambda: [submission_row]),  # submissions query
            MagicMock(result=lambda: [sample_dataframes['header_row']]),  # header query
            MagicMock(result=lambda: iter(sample_dataframes['slip_rows'])),
            MagicMock(result=lambda: [])  # trailer query - EMPTY
        ]

        # Act & Assert
        with pytest.raises(AirflowFailException, match="No trailer data found for SbmtRefId"):
            dag_builder_instance.create_xml()

    # ==================== XML VALIDATION TASK EDGE CASES ====================

    def test_xml_validation_task_empty_file_list(self, dag_builder_instance, mock_task_instance, mock_load_xsd, mock_storage_client):
        """Test XML validation task with empty file list."""
        # Arrange
        mock_task_instance.xcom_pull.return_value = []

        # Act
        result = dag_builder_instance.xml_validation_task(task_instance=mock_task_instance)

        # Assert
        assert result == []
        mock_load_xsd.assert_not_called()

    def test_xml_validation_task_deletes_invalid_file(self, dag_builder_instance, mock_task_instance, mock_load_xsd, mock_open_xml_stream, mock_storage_client):
        """Test XML validation task deletes invalid files from staging."""
        # Arrange
        mock_task_instance.xcom_pull.return_value = ['invalid_file.xml']

        with patch('digital_adoption_tax_statements.rl3_tax.rl3_statement_xml_generate_launcher.validate_xml') as mock_validate:
            mock_validate.side_effect = AirflowFailException("XML Validation Error")
            mock_storage_client['blob'].exists.return_value = True

            # Act & Assert
            with pytest.raises(AirflowFailException):
                dag_builder_instance.xml_validation_task(task_instance=mock_task_instance)

            # Verify blob.delete() was called
            mock_storage_client['blob'].delete.assert_called_once()

    # ==================== SLIP VALIDATION TESTS ====================

    def test_process_slips_non_numeric_noreleve(self, mock_bigquery_row, xml_writer_helper):
        """Test processing slip with non-numeric NoReleve."""
        # Arrange
        slip_data = {
            'Annee': 2025,
            'NoReleve': 'ABC123DEF',  # Non-numeric
            'BeneficiaireType': 1,
            'PersonneNAS': '046454286',  # Valid SIN
            'PersonneNomFamille': 'Test',
            'PersonnePrenom': 'User'
        }
        slip_rows = [mock_bigquery_row(slip_data)]
        writer, get_xml = xml_writer_helper()

        # Act & Assert
        with pytest.raises(AirflowFailException, match="NoReleve must be numeric"):
            process_slips(slip_rows, writer, 1)

    def test_process_slips_amended_missing_noreleve_derniere_trans(self, mock_bigquery_row, xml_writer_helper):
        """Test that amended slips without NoReleveDerniereTrans fail."""
        # Arrange
        slip_data = {
            'Annee': 2025,
            'NoReleve': '111111111',
            'BeneficiaireType': 1,
            'PersonneNAS': '046454286',  # Valid SIN
            'PersonneNomFamille': 'Test',
            'PersonnePrenom': 'User',
            'D_InteretSourceCdn': 100.00,
            'DeviseEtrangere': 'CAD',
            'NoReleveDerniereTrans': None  # Missing - required for amended
        }
        slip_rows = [mock_bigquery_row(slip_data)]
        writer, get_xml = xml_writer_helper()
        type_envoi = 4  # Amended

        # Act & Assert
        with pytest.raises(AirflowFailException, match="NoReleveDerniereTrans is required for amended slips"):
            process_slips(slip_rows, writer, type_envoi)

    def test_process_slips_amended_non_numeric_noreleve_derniere_trans(self, mock_bigquery_row, xml_writer_helper):
        """Test that amended slips with non-numeric NoReleveDerniereTrans fail."""
        # Arrange
        slip_data = {
            'Annee': 2025,
            'NoReleve': '111111111',
            'BeneficiaireType': 1,
            'PersonneNAS': '046454286',  # Valid SIN
            'PersonneNomFamille': 'Test',
            'PersonnePrenom': 'User',
            'D_InteretSourceCdn': 100.00,
            'DeviseEtrangere': 'CAD',
            'NoReleveDerniereTrans': 'INVALID'  # Non-numeric
        }
        slip_rows = [mock_bigquery_row(slip_data)]
        writer, get_xml = xml_writer_helper()
        type_envoi = 4  # Amended

        # Act & Assert
        with pytest.raises(AirflowFailException, match="NoReleveDerniereTrans must be numeric"):
            process_slips(slip_rows, writer, type_envoi)

    def test_process_slips_with_case_rens_compl(self, mock_bigquery_row, xml_writer_helper):
        """Test processing slip with CaseRensCompl section."""
        # Arrange
        slip_data = {
            'Annee': 2025,
            'NoReleve': '111111111',
            'BeneficiaireType': 1,
            'PersonneNAS': '046454286',  # Valid SIN
            'PersonneNomFamille': 'Test',
            'PersonnePrenom': 'User',
            'D_InteretSourceCdn': 100.00,
            'DeviseEtrangere': 'CAD',
            'CodeRensCompl': 'A1',
            'DonneeRensCompl': 'Additional info'
        }
        slip_rows = [mock_bigquery_row(slip_data)]
        writer, get_xml = xml_writer_helper()

        # Wrap in Groupe03
        writer.start_element('Groupe03')

        # Act
        slip_count = process_slips(slip_rows, writer, 1)

        # Close Groupe03
        writer.end_element('Groupe03')

        # Assert
        assert slip_count == 1
        groupe03 = get_xml()
        r_slip = groupe03.find('R')
        case_rens = r_slip.find('CaseRensCompl')
        assert case_rens is not None
        assert case_rens.find('CodeRensCompl').text == 'A1'
        assert case_rens.find('DonneeRensCompl').text == 'Additional info'

    def test_process_slips_cancelled_no_montants(self, mock_bigquery_row, xml_writer_helper):
        """Test that cancelled slips don't include Montants section."""
        # Arrange
        slip_data = {
            'Annee': 2025,
            'NoReleve': '111111111',
            'BeneficiaireType': 1,
            'PersonneNAS': '046454286',  # Valid SIN
            'PersonneNomFamille': 'Test',
            'PersonnePrenom': 'User',
            'D_InteretSourceCdn': 100.00,  # This should be ignored for cancelled
            'DeviseEtrangere': 'CAD'
        }
        slip_rows = [mock_bigquery_row(slip_data)]
        writer, get_xml = xml_writer_helper()
        type_envoi = 6  # Cancelled

        # Wrap in Groupe03
        writer.start_element('Groupe03')

        # Act
        slip_count = process_slips(slip_rows, writer, type_envoi)

        # Close Groupe03
        writer.end_element('Groupe03')

        # Assert
        assert slip_count == 1
        groupe03 = get_xml()
        d_slip = groupe03.find('D')
        assert d_slip is not None
        # Cancelled slips should NOT have Montants
        assert d_slip.find('Montants') is None
        # Cancelled slips should NOT have CaseRensCompl
        assert d_slip.find('CaseRensCompl') is None

    # ==================== TRAILER VALIDATION TESTS ====================

    def test_process_trailer_with_optional_fields(self, mock_bigquery_row, xml_writer_helper):
        """Test processing trailer with optional fields populated."""
        # Arrange
        trailer_data = {
            'Annee': 2025,
            'NbReleves': 5,
            'NoId': '1234567890',
            'TypeDossier': 'RS',
            'NoDossier': '0001',
            'NEQ': '1234567890',  # Optional
            'NoSuccursale': '001',  # Optional
            'Nom': 'Test Company',
            'AdresseLigne1': '123 Main St',
            'AdresseLigne2': 'Suite 100',  # Optional
            'AdresseVille': 'Montreal',
            'AdresseProvince': 'QC',
            'AdresseCodePostal': 'H1H1H1'
        }
        trailer_row = mock_bigquery_row(trailer_data)
        writer, get_xml = xml_writer_helper()

        # Act
        process_trailer(trailer_row, writer)

        # Assert
        t = get_xml()
        assert t.tag == 'T'

        payeur = t.find('PayeurMandataire')
        assert payeur.find('NEQ').text == '1234567890'
        assert payeur.find('NoSuccursale').text == '001'

        adresse = payeur.find('Adresse')
        assert adresse.find('Ligne2').text == 'Suite 100'

    # ==================== MULTIPLE SUBMISSIONS TEST ====================

    def test_create_xml_multiple_submissions(self, dag_builder_instance, mock_bigquery_client, sample_dataframes, mock_get_xml_file_name, mock_gcs_blob_for_writing, mock_get_schema_version, mock_bigquery_row):
        """Test create_xml with multiple submissions creates multiple files."""
        # Arrange
        submission_row_1 = mock_bigquery_row({'SbmtRefId': 'TEST001'})
        submission_row_2 = mock_bigquery_row({'SbmtRefId': 'TEST002'})

        # Mock get_xml_file_name to return different names
        mock_get_xml_file_name.side_effect = ['file1.xml', 'file2.xml']

        mock_bigquery_client.query.side_effect = [
            MagicMock(result=lambda: [submission_row_1, submission_row_2]),  # Two submissions
            # First submission queries
            MagicMock(result=lambda: [sample_dataframes['header_row']]),
            MagicMock(result=lambda: iter(sample_dataframes['slip_rows'])),
            MagicMock(result=lambda: [sample_dataframes['trailer_row']]),
            # Second submission queries
            MagicMock(result=lambda: [sample_dataframes['header_row']]),
            MagicMock(result=lambda: iter(sample_dataframes['slip_rows'])),
            MagicMock(result=lambda: [sample_dataframes['trailer_row']])
        ]

        # Act
        result = dag_builder_instance.create_xml()

        # Assert
        assert len(result) == 2
        assert result[0] == 'file1.xml'
        assert result[1] == 'file2.xml'

    # ==================== SLIP COUNT VALIDATION TEST ====================

    def test_process_slips_returns_correct_count(self, mock_bigquery_row, xml_writer_helper):
        """Test that process_slips returns accurate slip count."""
        # Arrange - Create 5 slips
        slips = []
        for i in range(5):
            slip_data = {
                'Annee': 2025,
                'NoReleve': f'11111111{i}',
                'BeneficiaireType': 1,
                'PersonneNAS': '046454286',  # Valid SIN
                'PersonneNomFamille': f'Test{i}',
                'PersonnePrenom': 'User',
                'D_InteretSourceCdn': 100.00,
                'DeviseEtrangere': 'CAD'
            }
            slips.append(mock_bigquery_row(slip_data))

        writer, get_xml = xml_writer_helper()

        # Wrap in Groupe03
        writer.start_element('Groupe03')

        # Act
        slip_count = process_slips(slips, writer, 1)

        # Close Groupe03
        writer.end_element('Groupe03')

        # Assert
        assert slip_count == 5

    # ==================== ADDRESS PROCESSING TESTS ====================

    def test_process_slips_with_full_address(self, mock_bigquery_row, xml_writer_helper):
        """Test processing slip with complete address information."""
        # Arrange
        slip_data = {
            'Annee': 2025,
            'NoReleve': '111111111',
            'BeneficiaireType': 1,
            'PersonneNAS': '046454286',  # Valid SIN
            'PersonneNomFamille': 'Test',
            'PersonnePrenom': 'User',
            'AdresseLigne1': '123 Main Street',
            'AdresseLigne2': 'Apt 456',
            'AdresseVille': 'Montreal',
            'AdresseProvince': 'QC',
            'AdresseCodePostal': 'H1H1H1',
            'D_InteretSourceCdn': 100.00,
            'DeviseEtrangere': 'CAD'
        }
        slip_rows = [mock_bigquery_row(slip_data)]
        writer, get_xml = xml_writer_helper()

        # Wrap in Groupe03
        writer.start_element('Groupe03')

        # Act
        slip_count = process_slips(slip_rows, writer, 1)

        # Close Groupe03
        writer.end_element('Groupe03')

        # Assert
        assert slip_count == 1
        groupe03 = get_xml()
        r_slip = groupe03.find('R')
        adresse = r_slip.find('Beneficiaire/Adresse')
        assert adresse is not None
        assert adresse.find('Ligne1').text == '123 Main Street'
        assert adresse.find('Ligne2').text == 'Apt 456'
        assert adresse.find('Ville').text == 'Montreal'
        assert adresse.find('Province').text == 'QC'
        assert adresse.find('CodePostal').text == 'H1H1H1'

    # ==================== MONTANTS SECTION TESTS ====================

    def test_process_slips_with_all_montants(self, mock_bigquery_row, xml_writer_helper):
        """Test processing slip with all Montants fields populated."""
        # Arrange
        slip_data = {
            'Annee': 2025,
            'NoReleve': '111111111',
            'BeneficiaireType': 1,
            'PersonneNAS': '046454286',  # Valid SIN
            'PersonneNomFamille': 'Test',
            'PersonnePrenom': 'User',
            'A1_DividendeDetermine': 1000.50,
            'A2_DividendeOrdinaire': 500.25,
            'B_DividendeImposable': 750.00,
            'C_CreditImpotDividende': 100.00,
            'D_InteretSourceCdn': 200.00,
            'E_AutreRevenuCdn': 150.00,
            'F_RevenuBrutEtranger': 300.00,
            'G_ImpotEtranger': 50.00,
            'H_RedevanceCdn': 75.00,
            'I_DividendeGainCapital': 125.00,
            'J_RevenuAccumuleRente': 80.00,
            'K_InteretBilletsLies': 60.00,
            'DeviseEtrangere': 'USD'
        }
        slip_rows = [mock_bigquery_row(slip_data)]
        writer, get_xml = xml_writer_helper()

        # Wrap in Groupe03
        writer.start_element('Groupe03')

        # Act
        slip_count = process_slips(slip_rows, writer, 1)

        # Close Groupe03
        writer.end_element('Groupe03')

        # Assert
        assert slip_count == 1
        groupe03 = get_xml()
        r_slip = groupe03.find('R')
        montants = r_slip.find('Montants')
        assert montants is not None
        assert montants.find('A1_DividendeDetermine').text == '1000.50'
        assert montants.find('A2_DividendeOrdinaire').text == '500.25'
        assert montants.find('DeviseEtrangere').text == 'USD'

    def test_process_slips_collects_all_errors(self, mock_bigquery_row, xml_writer_helper):
        """Test that process_slips collects all validation errors before failing."""
        # Arrange - create multiple slips with different errors
        slip1_data = {
            'Annee': 2025,
            'NoReleve': '12345678901',  # Error: exceeds 9 digits
            'BeneficiaireType': 1,
            'PersonneNAS': '046454286',
            'PersonneNomFamille': 'Test1',
            'PersonnePrenom': 'User1'
        }
        slip2_data = {
            'Annee': 2025,
            'NoReleve': '222222222',
            'BeneficiaireType': 1,
            'PersonneNAS': '123456789',  # Error: fails Luhn checksum
            'PersonneNomFamille': 'Test2',
            'PersonnePrenom': 'User2'
        }
        slip3_data = {
            'Annee': 2025,
            'NoReleve': '333333333',
            'BeneficiaireType': 1,
            'PersonneNAS': None,  # Error: missing PersonneNAS and RaisonSocialeNom1
            'RaisonSocialeNom1': None
        }
        slip_rows = [
            mock_bigquery_row(slip1_data),
            mock_bigquery_row(slip2_data),
            mock_bigquery_row(slip3_data)
        ]
        writer, get_xml = xml_writer_helper()

        # Act & Assert
        with pytest.raises(AirflowFailException) as exc_info:
            process_slips(slip_rows, writer, 1)

        # Verify error message contains all errors
        error_message = str(exc_info.value)
        assert "Validation failed for 3 slip(s)" in error_message
        assert "NoReleve exceeds 9 digits" in error_message
        assert "NAS fails Luhn checksum validation" in error_message
        assert "must have either PersonneNAS or RaisonSocialeNom1" in error_message
        # Verify error message format includes numbered list
        assert "1." in error_message
        assert "2." in error_message
        assert "3." in error_message
