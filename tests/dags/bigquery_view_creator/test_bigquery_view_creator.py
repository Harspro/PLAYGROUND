import re
from typing import Final, Dict, Any
from unittest.mock import patch, Mock

from bigquery_view_creator.bigquery_view_creator import BigQueryViewCreator


class TestCreateMaterializedView:
    view_sql_sample: Final[str] = "SELECT column1, column2 FROM project.dataset.table"
    table_ref_sample: Final[str] = "project.dataset.table"
    view_ref_sample: Final[str] = "project.dataset.view"
    view_file_sample: Final[str] = "sample_view.sql"

    materialized_view_config_sample: Final[Dict[str, Any]] = {
        'partition_field': 'date_column',
        'clustering_fields': ['column1', 'column2'],
        'options': {
            'option1': 'value1',
            'option2': 'value2'
        }
    }

    @patch('google.cloud.bigquery.Client')
    @patch('bigquery_view_creator.bigquery_view_creator.read_file_env', return_value=view_sql_sample)
    @patch('bigquery_view_creator.bigquery_view_creator.logger')
    def test_create_materialized_view_with_sql_file(
            self,
            mock_logging,
            mock_read_file_env,
            mock_bq_client):
        instance = BigQueryViewCreator()
        instance.view_files_dir = "some_directory"

        mock_client = Mock()
        mock_bq_client.return_value = mock_client

        # Mock table with time partitioning
        mock_table = Mock()
        mock_table.time_partitioning = Mock()
        mock_table.time_partitioning.type_ = 'DAY'
        mock_table.time_partitioning.field = self.materialized_view_config_sample['partition_field']
        mock_table.time_partitioning.expiration_ms = 86400000  # 1 day in milliseconds
        mock_table.time_partitioning.require_partition_filter = True
        mock_client.get_table.return_value = mock_table

        # Mock clustering fields (if used)
        mock_table.clustering_fields = self.materialized_view_config_sample['clustering_fields']

        # Execute the function
        instance.create_materialized_view(
            table_ref=self.table_ref_sample,
            view_ref=self.view_ref_sample,
            view_sql="",
            view_file=self.view_file_sample,
            materialized_view_config=self.materialized_view_config_sample
        )

        # Check that the correct query was constructed
        expected_query = f"""CREATE OR REPLACE MATERIALIZED VIEW `{self.view_ref_sample}`
                                PARTITION BY {self.materialized_view_config_sample['partition_field']}
                                CLUSTER BY column1, column2
                                OPTIONS (
                                  description = "\\nPartitioned by: DAY\\nPartitioned on field: date_column\\nPartition expiration: 1970-01-01 19:00:00-05:00\\nPartition filter: Required\\nClustered by:\\ncolumn1\\ncolumn2",
                                  labels= [('table_ref','table')],
                                  option1 = value1, option2 = value2
                                )
                                AS
                                    {self.view_sql_sample}
                        """

        # Normalize the queries. It's too difficult to compare otherwise
        def _normalize_whitespace(s):
            return re.sub(r'\s+', ' ', s).strip()

        expected_query = _normalize_whitespace(expected_query)
        actual_query = _normalize_whitespace(str(mock_client.query.call_args[0][0]))
        assert expected_query == actual_query, f"Expected: {expected_query}\nActual: {actual_query}"
        mock_logging.info.assert_any_call(
            f"partition: {self.materialized_view_config_sample['partition_field']}, "
            f"cluster: {self.materialized_view_config_sample['clustering_fields']}")
        mock_logging.info.assert_any_call("Materialized View Options: option1 = value1, option2 = value2")
