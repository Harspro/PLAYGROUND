from table_comparator.table_comparator_dag_base import TableComparatorBase
from table_comparator.utils import (
    validate_input_parameters,
    get_table_schemas,
    determine_comparison_columns,
    generate_sql_aliases,
    generate_filter_clauses,
    generate_schema_mismatch_query,
    generate_count_queries,
    read_sql_file
)
import time
import logging
logger = logging.getLogger(__name__)


class TableComparatorLoader(TableComparatorBase):
    def __init__(self, config_filename: str):
        super().__init__(config_filename)

    def generate_comparison_queries(self, **kwargs):
        ti = kwargs['ti']
        params = self.extract_params(kwargs)
        self.validate_inputs(params)
        logger.info(
            f"Starting table comparison: {params['source_table_fq']} vs {params['target_table_fq']}")
        start_time = time.time()

        # Get schemas
        source_cols_schema, target_cols_schema = self.get_schemas(params)
        params['source_cols_schema'] = source_cols_schema
        params['target_cols_schema'] = target_cols_schema

        # Determine columns
        final_compare_cols_list = self.determine_final_compare_cols(params)
        params['final_compare_cols_list'] = final_compare_cols_list

        # Generate SQL aliases
        sql_aliases = self.generate_sql_aliases(params)
        params['sql_aliases'] = sql_aliases

        # Generate filter clauses
        source_filter_clause_str, target_filter_clause_str = self.generate_filter_clauses(
            params)
        params['source_filter_clause_str'] = source_filter_clause_str
        params['target_filter_clause_str'] = target_filter_clause_str

        # Table schema creation query
        self.push_create_table_schema_query(ti, params)

        # Mismatch report query
        self.push_mismatch_report_query(ti, params)

        # Count queries
        self.push_count_queries(ti, params)

        # Schema mismatch query
        self.push_schema_mismatch_query(ti, params)

        # View creation query
        self.push_create_view_query(ti, params)

        # Logging
        elapsed_time = time.time() - start_time
        logger.info(
            f"Query generation completed in {elapsed_time:.2f} seconds")
        logger.info(f"Comparison columns: {final_compare_cols_list}")
        logger.info(f"Primary key columns: {params['pk_columns_list']}")
        if params['global_expressions_by_type']:
            logger.info(
                f"Type-based expressions configured for: {list(params['global_expressions_by_type'].keys())}")

    def extract_params(self, kwargs):
        return {
            'source_table_fq': kwargs['source_table_name'],
            'target_table_fq': kwargs['target_table_name'],
            'pk_columns_list': kwargs['primary_key_columns'],
            'filters': kwargs.get('filters'),
            'user_compare_cols_list': kwargs.get('comparison_columns'),
            'exclude_columns_list': kwargs.get('exclude_columns'),
            'mismatch_report_table_fq': kwargs.get('mismatch_report_table_name'),
            'mismatch_report_view_name': kwargs.get('mismatch_report_view_name'),
            'column_expressions': kwargs.get('column_expressions'),
            'global_expressions_by_type': kwargs.get('global_expressions_by_type'),
        }

    def validate_inputs(self, params):
        validate_input_parameters(
            source_table_fq=params['source_table_fq'],
            target_table_fq=params['target_table_fq'],
            pk_columns_list=params['pk_columns_list']
        )

    def get_schemas(self, params):
        return get_table_schemas(
            source_table_fq=params['source_table_fq'],
            target_table_fq=params['target_table_fq']
        )

    def determine_final_compare_cols(self, params):
        return determine_comparison_columns(
            user_compare_cols_list=params['user_compare_cols_list'],
            source_cols_schema=params['source_cols_schema'],
            target_cols_schema=params['target_cols_schema'],
            pk_columns_list=params['pk_columns_list'],
            exclude_columns_list=params['exclude_columns_list']
        )

    def generate_sql_aliases(self, params):
        return generate_sql_aliases(
            pk_columns_list=params['pk_columns_list'],
            final_compare_cols_list=params['final_compare_cols_list'],
            column_expressions=params['column_expressions'],
            source_cols_schema=params['source_cols_schema'],
            target_cols_schema=params['target_cols_schema'],
            global_expressions_by_type=params['global_expressions_by_type']
        )

    def generate_filter_clauses(self, params):
        return generate_filter_clauses(
            filters=params['filters'],
            source_cols_schema=params['source_cols_schema'],
            target_cols_schema=params['target_cols_schema']
        )

    def push_create_table_schema_query(self, ti, params):
        mismatch_report_table_fq = params['mismatch_report_table_fq']
        if mismatch_report_table_fq:
            schema_sql_template = read_sql_file(
                "create_mismatch_report_table_schema.sql")
            # Generate schema for primary key columns preserving their actual
            # types
            pk_columns_schema = []
            source_cols_schema = params['source_cols_schema']
            target_cols_schema = params['target_cols_schema']

            for col in params['pk_columns_list']:
                # Try to get type from source table first, then target table
                col_type = None
                if col in source_cols_schema:
                    col_type = source_cols_schema[col].field_type
                elif col in target_cols_schema:
                    col_type = target_cols_schema[col].field_type
                else:
                    # Fallback to STRING if column not found in either table
                    logger.warning(
                        f"Primary key column {col} not found in source or target schemas, defaulting to STRING")
                    col_type = 'STRING'

                pk_columns_schema.append(f"`{col}` {col_type}")

            substitutions = {
                "mismatch_report_table_fq": mismatch_report_table_fq,
                "pk_columns_schema": ", ".join(pk_columns_schema),
                "pk_columns_for_clustering": ", ".join([f"`{col}`" for col in params['pk_columns_list']]),
            }
            create_table_query = schema_sql_template.format(**substitutions)
            ti.xcom_push(
                key='create_table_schema_query',
                value=create_table_query)
        else:
            ti.xcom_push(key='create_table_schema_query', value="")

    def push_mismatch_report_query(self, ti, params):
        mismatch_report_table_fq = params['mismatch_report_table_fq']
        sql_aliases = params['sql_aliases']
        if mismatch_report_table_fq:
            mismatch_sql_template = read_sql_file("create_mismatch_report.sql")
            substitutions = {
                "mismatch_report_table_fq": mismatch_report_table_fq,
                "pk_columns_for_clustering": ", ".join([f"`{col}`" for col in params['pk_columns_list']]),
                "source_table_fq": params['source_table_fq'],
                "target_table_fq": params['target_table_fq'],
                "pk_columns_source_aliased": (", ".join(sql_aliases['pk_cols_s_aliased']) + (
                    ", " if sql_aliases['pk_cols_s_aliased'] and sql_aliases['comp_cols_s_aliased'] else "")) if sql_aliases['pk_cols_s_aliased'] else "",
                "compare_columns_source_aliased": ", ".join(sql_aliases['comp_cols_s_aliased']),
                "original_columns_source_aliased": (", " + ", ".join(sql_aliases['original_cols_s_aliased'])) if sql_aliases.get('original_cols_s_aliased') else "",
                "pk_columns_target_aliased": (", ".join(sql_aliases['pk_cols_t_aliased']) + (
                    ", " if sql_aliases['pk_cols_t_aliased'] and sql_aliases['comp_cols_t_aliased'] else "")) if sql_aliases['pk_cols_t_aliased'] else "",
                "compare_columns_target_aliased": ", ".join(sql_aliases['comp_cols_t_aliased']),
                "original_columns_target_aliased": (", " + ", ".join(sql_aliases['original_cols_t_aliased'])) if sql_aliases.get('original_cols_t_aliased') else "",
                "source_filter_clause": params['source_filter_clause_str'],
                "target_filter_clause": params['target_filter_clause_str'],
                "pk_select_coalesce": sql_aliases['pk_select_coalesce'],
                "first_pk_column_source_aliased": sql_aliases['first_pk_s_alias'],
                "first_pk_column_target_aliased": sql_aliases['first_pk_t_alias'],
                "mismatch_predicate_sql": sql_aliases['mismatch_predicate_sql'],
                "diff_details_structs_sql": sql_aliases['diff_details_structs_sql'],
                "optional_mismatch_select_clause": "",
                "join_on_clause_aliased": sql_aliases['join_on_aliased'],
            }
            final_mismatch_query = mismatch_sql_template.format(
                **substitutions)
            ti.xcom_push(key='mismatch_query', value=final_mismatch_query)
        else:
            ti.xcom_push(key='mismatch_query', value="")

    def push_count_queries(self, ti, params):
        count_queries = generate_count_queries(
            mismatch_report_table_fq=params['mismatch_report_table_fq'],
            final_compare_cols_list=params['final_compare_cols_list'],
            pk_columns_list=params['pk_columns_list'],
            source_table_fq=params['source_table_fq'],
            target_table_fq=params['target_table_fq'],
            source_filter_clause_str=params['source_filter_clause_str'],
            target_filter_clause_str=params['target_filter_clause_str'],
            mismatch_predicate_sql=params['sql_aliases']['mismatch_predicate_sql'])
        ti.xcom_push(key='count_mismatched_query',
                     value=count_queries['count_mismatched_query'])
        ti.xcom_push(key='count_missing_in_source_query',
                     value=count_queries['count_missing_in_source_query'])
        ti.xcom_push(key='count_missing_in_target_query',
                     value=count_queries['count_missing_in_target_query'])

    def push_schema_mismatch_query(self, ti, params):
        schema_mismatch_query = generate_schema_mismatch_query(
            source_table_fq=params['source_table_fq'],
            target_table_fq=params['target_table_fq']
        )
        ti.xcom_push(key='schema_mismatch_query', value=schema_mismatch_query)

    def push_create_view_query(self, ti, params):
        mismatch_report_table_fq = params['mismatch_report_table_fq']

        # Early return if no mismatch report table specified
        if not mismatch_report_table_fq:
            logger.warning(
                "No mismatch report table specified, skipping view creation")
            ti.xcom_push(key='create_view_query', value="")
            return

        # Validate table format
        table_parts = mismatch_report_table_fq.split('.')
        if len(table_parts) != 3:
            logger.warning(
                f"Invalid table format: {mismatch_report_table_fq}, skipping view creation")
            ti.xcom_push(key='create_view_query', value="")
            return

        # Determine view name - use default if not provided
        mismatch_report_view_name = params.get('mismatch_report_view_name')
        if not mismatch_report_view_name or mismatch_report_view_name.strip() == "":
            project, dataset, table_name = table_parts
            mismatch_report_view_name = f"pcb-{self.deploy_env}-curated.{dataset}.{table_name}"
            logger.info(
                f"Using default view name: {mismatch_report_view_name}")

        # Generate and push the view creation query
        view_sql_template = read_sql_file("create_mismatch_report_view.sql")
        substitutions = {
            "view_name": mismatch_report_view_name,
            "mismatch_report_table_name": mismatch_report_table_fq,
        }
        final_view_query = view_sql_template.format(**substitutions)
        ti.xcom_push(key='create_view_query', value=final_view_query)

    def report_results(self, **kwargs):
        ti = kwargs['ti']
        overall_start_time = time.time()

        mismatched_count = self.get_xcom_count(
            ti, 'get_mismatched_count_task', 'return_value', "mismatched count")
        missing_in_source = self.get_xcom_count(
            ti,
            'get_missing_in_source_count_task',
            'return_value',
            "missing in source count")
        missing_in_target = self.get_xcom_count(
            ti,
            'get_missing_in_target_count_task',
            'return_value',
            "missing in target count")
        schema_mismatches_result = self.get_xcom_schema_mismatches(
            ti, 'get_schema_mismatches_task', 'return_value')

        self.log_comparison_header(kwargs)
        self.log_expressions_info(kwargs)
        self.log_counts(mismatched_count, missing_in_source, missing_in_target)
        self.log_schema_mismatches(schema_mismatches_result)
        self.log_mismatch_report_table(kwargs)
        self.log_performance_summary(overall_start_time)
        self.log_summary_stats(
            mismatched_count,
            missing_in_source,
            missing_in_target)
        logger.info("--- End of Report ---")

    def get_xcom_count(self, ti, task_id, key, label):
        try:
            result = ti.xcom_pull(task_ids=task_id, key=key)
            count = result[0][0] if result and result[0] else 0
        except Exception as e:
            logger.info(f"Warning: Could not retrieve {label}: {e}")
            count = 0
        return count

    def get_xcom_schema_mismatches(self, ti, task_id, key):
        try:
            result = ti.xcom_pull(task_ids=task_id, key=key)
        except Exception as e:
            logger.info(f"Warning: Could not retrieve schema mismatches: {e}")
            result = None
        return result

    def log_comparison_header(self, kwargs):
        logger.info("\n--- BigQuery Table Comparison Results ---")
        logger.info(f"Source Table: {kwargs['source_table_name']}")
        logger.info(f"Target Table: {kwargs['target_table_name']}")
        logger.info(f"Primary Keys: {kwargs['primary_key_columns']}")
        if kwargs.get('exclude_columns'):
            logger.info(f"Excluded Columns: {kwargs['exclude_columns']}")

    def log_expressions_info(self, kwargs):
        if kwargs.get('global_expressions_by_type'):
            logger.info("Applied Type-Based Global Expressions:")
            for type_category, expressions in kwargs['global_expressions_by_type'].items(
            ):
                logger.info(f"  - {type_category}: {', '.join(expressions)}")
        if kwargs.get('column_expressions'):
            logger.info("Column-Specific Expressions:")
            for col, expr_config in kwargs['column_expressions'].items():
                expressions = expr_config.get('expressions', [])
                description = expr_config.get('description', 'No description')
                logger.info(
                    f"  - {col}: {', '.join(expressions)} ({description})")
        if kwargs.get('filters'):
            logger.info("Applied Filters:")
            for filter_condition in kwargs['filters']:
                logger.info(f"  - {filter_condition}")

    def log_counts(
            self,
            mismatched_count,
            missing_in_source,
            missing_in_target):
        logger.info(f"\nMismatched Records Count: {mismatched_count}")
        logger.info(
            f"Records Missing in Source (present in target only): {missing_in_source}")
        logger.info(
            f"Records Missing in Target (present in source only): {missing_in_target}")

    def log_schema_mismatches(self, schema_mismatches_result):
        if schema_mismatches_result:
            logger.info("\n--- Schema Mismatches ---")
            for row in schema_mismatches_result:
                logger.info(
                    f"Column: {row[0]} | Source Type: {row[1]} | Target Type: {row[3]} | Discrepancy: {row[5]}"
                )
        else:
            logger.info(
                "\nNo schema mismatches found or check yielded no results.")

    def log_mismatch_report_table(self, kwargs):
        if kwargs.get('mismatch_report_table_name'):
            logger.info(
                f"\nDetailed mismatch report (if generated) is in: {kwargs['mismatch_report_table_name']}"
            )
        if kwargs.get('mismatch_report_view_name'):
            logger.info(
                f"View on mismatch report table: {kwargs['mismatch_report_view_name']}"
            )
        elif kwargs.get('mismatch_report_table_name'):
            # If no view name specified but table exists, indicate default view
            # will be created
            table_parts = kwargs['mismatch_report_table_name'].split('.')
            if len(table_parts) == 3:
                project, dataset, table_name = table_parts
                default_view_name = f"pcb-{self.deploy_env}-curated.{dataset}.{table_name}"
                logger.info(
                    f"Default view will be created at: {default_view_name}"
                )

    def log_performance_summary(self, overall_start_time):
        overall_elapsed_time = time.time() - overall_start_time
        logger.info("\n--- Performance Summary ---")
        logger.info(
            f"Total execution time: {overall_elapsed_time:.2f} seconds")

    def log_summary_stats(
            self,
            mismatched_count,
            missing_in_source,
            missing_in_target):
        total_records_checked = mismatched_count + missing_in_source + missing_in_target
        if total_records_checked > 0:
            logger.info(f"Total records with issues: {total_records_checked}")
            logger.info(
                f"Mismatch rate: {(mismatched_count / total_records_checked * 100):.2f}%"
            )
