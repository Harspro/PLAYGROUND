import pytest
from unittest.mock import MagicMock
from dag_factory.terminus_dag_factory import add_tags
from dag_factory.sre_parameters import SREParameter


@pytest.fixture
def mock_dag():
    # Create a mock DAG object
    dag = MagicMock()

    # Define mock default_args with example tags
    dag.default_args = {
        SREParameter.OWNER.value: 'owner_tag',
        SREParameter.SEVERITY.value: 'high_severity'
    }

    return dag


def test_add_tags_with_no_existing_tags(mock_dag):
    # Set up dag with no existing tags
    mock_dag.tags = None

    enriched_dag = add_tags(mock_dag)

    expected_tags = ['owner_tag', 'high_severity']
    # Assertions
    assert enriched_dag.tags == expected_tags
    assert mock_dag.tags == expected_tags


def test_add_tags_with_existing_tags(mock_dag):
    # Set up dag with existing tags
    mock_dag.tags = ['existing_tag']

    enriched_dag = add_tags(mock_dag)

    expected_tags = ['owner_tag', 'high_severity', 'existing_tag']
    # Assertions
    assert enriched_dag.tags == expected_tags
    assert mock_dag.tags == expected_tags


def test_add_tags_ignore_duplicate(mock_dag):
    # Set up dag with an existing tag that is in default_args
    mock_dag.tags = ['existing_tag', 'owner_tag']

    enriched_dag = add_tags(mock_dag)

    expected_tags = ['high_severity', 'existing_tag', 'owner_tag']
    # Assertions
    assert enriched_dag.tags == expected_tags
    assert mock_dag.tags == expected_tags
