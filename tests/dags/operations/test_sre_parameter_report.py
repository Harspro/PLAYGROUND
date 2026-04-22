import os
import pytest
import pendulum
from airflow.models import DagBag
from datetime import datetime


@pytest.fixture(scope="session")
def dag_bag():
    return DagBag(include_examples=False)


@pytest.mark.skipif(not os.environ.get('FOR_SRE'), reason="sre report is only needed for SRE team.")
def test_generating_sre_parameters_report(dag_bag: DagBag) -> None:
    """
    Generate a report of SRE parameters (CSV format).
    The output is named as 'sre_parameters_report.log'.
    The file will be generated in the project root folder.
    To generate the SRE report, please run the test like below (assume the local setup has already been done).
    > export FOR_SRE=True
    > ./test.sh
    """

    with open('sre_parameters_report.log', 'w') as f:
        header = ','.join([
            'dag_id', 'owner', 'capability', 'severity',
            'business_impact', 'customer_impact', 'rec_load_timestamp'
        ])

        f.write(f"{header}\n")
        for dag_id in dag_bag.dags:
            default_args = dag_bag.dags[dag_id].default_args
            row = ','.join([
                dag_id,
                default_args.get('owner', ''),
                default_args.get('capability', ''),
                default_args.get('severity', ''),
                default_args.get('business_impact', ''),
                default_args.get('customer_impact', ''),
                datetime.now().astimezone(tz=pendulum.timezone('America/Toronto')).strftime('%Y-%m-%d %H:%M:%S')
            ])

            f.write(f"{row}\n")
