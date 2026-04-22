import airflow.models
import pytest


@pytest.fixture(scope="session")
def dag_bag():
    return airflow.models.DagBag(include_examples=False)


def test_no_import_errors(dag_bag):
    """
    Following code find any import error in DAG
    """

    import_errors = dag_bag.import_errors
    assert len(import_errors) == 0, f'Import Errors: {repr(import_errors)}'


def test_dag_owner_present(dag_bag):
    """
    Following code find any DAG without owner in default arguments
    """

    for dag in dag_bag.dags:
        owner = dag_bag.dags[dag].default_args.get('owner')
        error_msg = 'Owner not set for DAG {id}'.format(id=dag)
        assert owner != '', error_msg


def test_dag_capability_present(dag_bag):
    """
    Following code find any DAG without capability in default arguments
    """

    for dag in dag_bag.dags:
        capability = dag_bag.dags[dag].default_args.get('capability')
        error_msg = 'capability not set for DAG {id}'.format(id=dag)
        assert capability != '', error_msg


def test_dag_severity_present(dag_bag):
    """
    Following code find any DAG without severity in default arguments
    """

    for dag in dag_bag.dags:
        severity = dag_bag.dags[dag].default_args.get('severity')
        error_msg = 'severity not set for DAG {id}'.format(id=dag)
        assert severity != '', error_msg


def test_dag_sub_capability_present(dag_bag):
    """
    Following code find any DAG without sub_capability in default arguments
    """

    for dag in dag_bag.dags:
        sub_capability = dag_bag.dags[dag].default_args.get('sub_capability')
        error_msg = 'sub_capability not set for DAG {id}'.format(id=dag)
        assert sub_capability != '', error_msg


def test_dag_business_impact_present(dag_bag):
    """
    Following code find any DAG without business_impact in default arguments
    """

    for dag in dag_bag.dags:
        business_impact = dag_bag.dags[dag].default_args.get('business_impact')
        error_msg = 'business_impact not set for DAG {id}'.format(id=dag)
        assert business_impact != '', error_msg


def test_dag_customer_impact_present(dag_bag):
    """
    Following code find any DAG without customer_impact in default arguments
    """

    for dag in dag_bag.dags:
        customer_impact = dag_bag.dags[dag].default_args.get('customer_impact')
        error_msg = 'customer_impact not set for DAG {id}'.format(id=dag)
        assert customer_impact != '', error_msg


def test_dag_is_paused_upon_creation(dag_bag):
    """
    Following code finds if any DAG does not set is_paused_upon_creation=True
    """

    dagid_nopause = []
    for dag_id, dag in dag_bag.dags.items():
        if not dag.is_paused_upon_creation:
            dagid_nopause.append(dag_id)
    num_nopause = len(dagid_nopause)
    expected = num_nopause == 0
    error_msg = 'is_paused_upon_creation not set to True for {num} DAGs {id}'.format(num=num_nopause, id=dagid_nopause)
    assert expected, error_msg
