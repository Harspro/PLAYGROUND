.ONESHELL:
.SILENT:
.EXPORT_ALL_VARIABLES:

###############################################################################
# Settings
###############################################################################

include settings.env

COMPOSER_VERSION ?=
AIRFLOW_VERSION ?=

AIRFLOW_TESTS_IMAGE := repo.nonprod.pcfcloud.io/docker/composer/tests-composer-airflow:${COMPOSER_VERSION}-${AIRFLOW_VERSION}

###############################################################################
# settings
###############################################################################

settings:
	$(call header,Settings)
	echo "###############################################################################"
	echo "# COMPOSER_VERSION=${COMPOSER_VERSION}"
	echo "# AIRFLOW_VERSION=${AIRFLOW_VERSION}"
	echo "# AIRFLOW_TESTS_IMAGE=${AIRFLOW_TESTS_IMAGE}"
	echo "# docker_user=${docker_user}"
	echo "# docker_pass=${docker_pass}"
	echo "###############################################################################"

###############################################################################
# Docker
###############################################################################

docker_user ?= deployer
docker_pass ?= $(shell pass pcbank/jfrog/deployer)

docker: docker-auth docker-build docker-push docker-clean

docker-auth:
	$(call header,Docker Auth)
	docker login repo.nonprod.pcfcloud.io -u ${docker_user} -p ${docker_pass}

docker-build:
	$(call header,Docker Build)
	docker build \
	--platform linux/amd64 \
	--build-arg COMPOSER_VERSION=$(COMPOSER_VERSION) \
	--build-arg AIRFLOW_VERSION=$(AIRFLOW_VERSION) \
	--tag $(AIRFLOW_TESTS_IMAGE) \
	--file Dockerfile .

docker-push:
	$(call header,Docker Push)
	docker push ${AIRFLOW_TESTS_IMAGE}

docker-clean:
	$(call header,Docker Clean)
	docker image prune --force
