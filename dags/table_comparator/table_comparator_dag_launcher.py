# for airflow scanning
from table_comparator.table_comparator_dag_loader import TableComparatorLoader

globals().update(TableComparatorLoader('comparator_config.yaml').generate_dags())
