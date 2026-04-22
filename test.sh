#!/bin/bash
set -xe

WORK_DIR=$PWD

export TZ="UTC" # align local timezone with default timezone in containers
export PIP_DEFAULT_TIMEOUT=1000
export AIRFLOW__CORE__UNIT_TEST_MODE=True
export AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
export AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=45
export AIRFLOW__CORE__DAGS_FOLDER=$WORK_DIR/dags
export AIRFLOW_VAR_DATAPROC_CONFIG='{"location":"northamerica-northeast1","project_id":"pcb-dev-processing"}'
export AIRFLOW_VAR_GCP_CONFIG='{"bq_query_location":"northamerica-northeast1","curated_zone_project_id":"pcb-dev-curated","deploy_env_storage_suffix":"-dev","deployment_environment_name":"dev","deployment_environment_number":"002","landing_zone_connection_id":"google_cloud_default","landing_zone_project_id":"pcb-dev-landing","network_tag":"nwdpcprocessingdev002","processing_zone_connection_id":"google_cloud_default","processing_zone_project_id":"pcb-dev-processing"}'
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor

python --version

pip install -r $WORK_DIR/tests/requirements.txt

for folder in "config" "dags" "plugins" "tests"; do
  echo "### ==> Run flake8 in dir ${folder}"
  flake8 --statistics --ignore=E501,W503,F401 ${folder}
done

echo "### ==> Run Airflow Init"
airflow db migrate
##for local repeat testing
#airflow db reset

echo "### ==> Run PYTEST"
# add PYTHONPATH so that the script works in local too
export PYTHONPATH=$PYTHONPATH:$WORK_DIR/dags/:$WORK_DIR/tests/
# use html report for local
#pytest -ra --tb=native --cov-report term --cov=dags --cov-report html $WORK_DIR/tests/
pytest -ra --tb=native --cov-report term --cov=dags --cov-report xml:coverage.xml $WORK_DIR/tests/
