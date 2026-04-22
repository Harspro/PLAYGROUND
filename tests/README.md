# Test Composer Airflow DAGs

## Test Image Information
To test the Airflow DAGs in CI pipeline, a test Composer image with the necessary dependencies is required. This image ensures that DAGs are executed in an environment identical to the one used in GCP.

This test image is hosted in the following location: https://repo.nonprod.pcfcloud.io/ui/repos/tree/General/docker/build/test-composer-airflow-image

### Post Composer Upgrade Instructions
If there is an upgrade to the Composer version, please following these steps to update the test image to align with the upgraded version.

1. Clone this repository: [Test Composer Airflow Image](https://gitlab.lblw.ca/pcf-engineering/platforms/base-images/test-composer-airflow-image)
2. Update Composer dependencies by modifying `docker/constraints_composer-***-airflow-***.txt` to include the updated Composer version dependencies from https://cloud.google.com/composer/docs/concepts/versioning/composer-versions
3. Ensure the `docker/requirements.txt` file is aligned with the updated dependencies as well
4. Update version information in `docker-compose.yml`
    * update `COMPOSER_VERSION`
    * update `AIRFLOW_VERSION`
    * adjust the image version to be published to artifactory as `image: ${ARTIFACTORY_DOCKER_STABLE}/build/test-composer-airflow-image:{COMPOSER_VERSION}-{AIRFLOW_VERSION}`
5. Rebuild and publish the image
6. Make sure the security scan passes before merging your changes

Following these steps will ensure that the test image remains compatible with the upgraded Composer environment in GCP.