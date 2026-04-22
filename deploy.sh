#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Please specify deployment_env. usage: $0 <deployment_env>"
    exit 1
fi

set -x

deployment_env=$1
echo "deploying to ${deployment_env}"
echo "CI_COMMIT_REF_NAME is ${CI_COMMIT_REF_NAME}"
echo "CI_COMMIT_BRANCH is ${CI_COMMIT_BRANCH}"

# gcloud auth activate-service-account --key-file ${GOOGLE_APPLICATION_CREDENTIALS}
airflow_gcs=$(gcloud composer environments describe ${composer_environment} --project=${CLOUDSDK_CORE_PROJECT}  --format="value(config.dagGcsPrefix)" | cut -d"/" -f3)
for folder in "dags" "plugins" "config"; do
  echo "Sync file://${folder}/ ==> gs://${airflow_gcs}/${folder}/"
  gsutil -o "GSUtil:parallel_process_count=1" rsync -d -r -x "airflow_monitoring.py" file://${folder} gs://${airflow_gcs}/${folder}
done

if [[ $deployment_env != "prod" ]]; then
  echo "Overwriting airflowignore file in ${deployment_env} "
  [[ -f "./dags/.airflowignore_nonprod" ]] && gsutil cp ./dags/.airflowignore_nonprod gs://${airflow_gcs}/dags/.airflowignore

  echo "Overwriting pip.confg file in ${deployment_env} "
  [[ -f "./config/pip/pip_nonprod.conf" ]] && gsutil cp ./config/pip/pip_nonprod.conf gs://${airflow_gcs}/config/pip/pip.conf
fi