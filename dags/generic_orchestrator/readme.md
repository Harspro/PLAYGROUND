---

# Generic Orchestrator

## Overview

  The `GenericOrchestrator` class is an Airflow DAG generator designed to orchestrate complex workflows using configuration-driven methods. By leveraging YAML configuration files, this class builds and manages DAGs that integrate various tasks and sub-DAG executions in a scalable and flexible manner.

## Key Features

  - **Configuration-Driven**: Builds DAGs based on configurations specified in YAML files, allowing easy customization and modification without changing the core code.
  - **Task Groups**: Supports organizing tasks into groups with specified execution modes, either parallel or sequential.
  - **Sub-DAG Management**: Utilizes `TriggerDagRunOperator` to orchestrate and manage sub-DAG executions with customizable parameters. Please make sure that the sub-dags provided are already existing with a valid DAG ID and are unpaused.
  - **Built-in Logging**: Provides comprehensive logging to monitor DAG executions and troubleshoot issues.

## Default Arguments

  The class uses default arguments for DAG creation, which can be overridden by configurations:

  - **Owner**: `team-defenders-alerts`
  - **Retries**: 1
  - **Retry Delay**: 5 minutes
  - **Email Notifications**: Disabled by default

## Usage

  ### Initialization

  To initialize the class, provide the YAML configuration file name and optionally the directory containing the config file:

  ```python
  orchestrator = GenericOrchestrator(
      config_filename='your_config.yaml',
      config_dir='path/to/configs'  # Defaults to DAGS_FOLDER/config
  )
  ```

  ### DAG Creation

  The `create_dags()` method generates a DAG for each configuration defined in the YAML file:

  ```python
  dags = orchestrator.create_dags()
  for dag_id, dag in dags.items():
      globals()[dag_id] = dag
  ```

  ### Configuration Structure

  The YAML configuration should define DAG names as top-level keys. Each DAG configuration should contain:

  - **DEFAULT_ARGS**: Dictionary of default arguments to customize DAG behavior.
  - **DAG Attributes**: Such as `schedule_interval`, `tags`, `description`.
  - **TASK_GROUPS**: Array of task group definitions with execution modes and sub-DAGs.

  Example YAML: (Find the configs here : dags/config/generic_orchestrator_configs/generic_orchestrator_config.yaml)

  ```yaml
  example_dag:
    DEFAULT_ARGS:
      owner: 'example-owner'
      retries: 3
      retry_delay: 300
    DAG_ATTRIBUTES:
      schedule_interval: '@daily'
      tags: ['example', 'airflow']
    TASK_GROUPS:
      - group_id: 'group1'
        execution_mode: 'parallel'
        description: 'Group description'
        dags:
          - sub_dag_id: 'sub_dag_1'
            params:
              {key1: 'value1'}
          - sub_dag_id: 'sub_dag_2'
            params:
              {key2: 'value2'}
  ```

**Author:**  
  Yash Sharma

---

This README serves as a comprehensive guide, explaining how to use and configure the `GenericOrchestrator` effectively within an Airflow environment. Adjust parameters and configurations as needed for your specific use case.