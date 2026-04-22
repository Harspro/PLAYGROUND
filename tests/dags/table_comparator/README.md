# Table Comparator Test Suite

This directory contains comprehensive test cases for the `table_comparator` DAG module, focusing on code coverage rather than performance or integration testing.

## Test Structure

### Files

- `__init__.py` - Package initialization
- `test_table_comparator_dag_base.py` - Tests for the base DAG class
- `test_table_comparator_dag_loader.py` - Tests for the DAG loader implementation
- `test_utils.py` - Tests for utility functions
- `test_table_comparator_dag_launcher.py` - Tests for the DAG launcher module
- `test_integration.py` - Simple integration tests
- `test_config.yaml` - Test configuration file
- `README.md` - This documentation

### Test Coverage

#### TableComparatorBase Tests
- Initialization and configuration loading
- DAG schedule handling
- DAG generation and creation
- Task creation for all DAG components
- Abstract method implementations

#### TableComparatorLoader Tests
- Complete workflow testing
- Parameter extraction and validation
- Schema and column determination
- SQL alias generation
- Filter clause generation
- XCom operations (push/pull)
- Reporting and logging functions
- Error handling scenarios

#### Utils Tests
- Date and table name validation
- Expression validation and building
- Data type categorization
- Column expression handling
- File operations (SQL file reading)
- BigQuery job execution
- Input parameter validation
- Schema retrieval
- Comparison column determination
- SQL generation functions

#### DAG Launcher Tests
- Module import and structure
- DAG generation with various configurations
- Multiple DAG handling
- Empty configuration scenarios

#### Integration Tests
- Complete workflow from configuration to execution
- DAG generation integration
- Report results integration

## Running Tests

### Prerequisites
- Python 3.8+
- pytest
- Required dependencies (see main project requirements)

### Running All Tests
```bash
# From the project root
pytest tests/dags/table_comparator/ -v

# With coverage
pytest tests/dags/table_comparator/ --cov=table_comparator --cov-report=html -v
```

### Running Specific Test Files
```bash
# Test base class only
pytest tests/dags/table_comparator/test_table_comparator_dag_base.py -v

# Test loader only
pytest tests/dags/table_comparator/test_table_comparator_dag_loader.py -v

# Test utils only
pytest tests/dags/table_comparator/test_utils.py -v
```

### Running Specific Test Classes
```bash
# Test specific class
pytest tests/dags/table_comparator/test_utils.py::TestValidationFunctions -v

# Test specific method
pytest tests/dags/table_comparator/test_utils.py::TestValidationFunctions::test_validate_date_format -v
```

## Test Patterns

### Mocking Strategy
- External dependencies (BigQuery, file system) are mocked
- Airflow components (DAG, operators) are mocked
- Configuration loading is mocked
- XCom operations are mocked

### Fixtures
- `mock_gcp_config` - Mock GCP configuration
- `mock_job_config` - Mock job configuration
- `mock_params` - Mock parameters for testing
- `mock_schemas` - Mock table schemas
- `mock_ti` - Mock task instance

### Parameterized Tests
- Date format validation with various inputs
- Table name format validation
- Data type categorization
- Error scenarios

### Error Handling
- Invalid input parameters
- Missing files or tables
- BigQuery errors
- XCom failures

## Test Configuration

The `test_config.yaml` file provides a sample configuration for testing:
- Source and target table definitions
- Primary key columns
- Date filtering configuration
- Column expressions and exclusions
- Global type-based expressions
- DAG scheduling configuration

## Coverage Goals

These tests aim to achieve high code coverage by testing:
- All public methods and functions
- All conditional branches
- Error handling paths
- Edge cases and boundary conditions
- Different input combinations

## Notes

- Tests are designed to be fast and isolated
- No actual BigQuery operations are performed
- No actual file system operations are performed
- All external dependencies are mocked
- Tests focus on logic and functionality rather than performance 