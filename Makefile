SHELL := /bin/bash
PWD := $(shell pwd)

-include .env
export

DOCKER_COMPOSE_FILE := $(or $(COMPOSE_FILE), docker-compose.yaml)

# ============================== DOCKER COMPOSE FLOW ============================== #

# Build all Docker images found in src/*/Dockerfile and tag them with the directory name.
DOCKERFILES := $(shell find ./src -name Dockerfile)
docker-build-image:
	@for dockerfile in $(DOCKERFILES); do \
		dir=$$(dirname $$dockerfile); \
		tag=$$(basename $$dir); \
		docker build -f $$dockerfile -t "$$tag:latest" .; \
	done

# Start all services defined in the docker-compose file, building images if necessary.
docker-compose-up: docker-build-image
	docker compose -f $(DOCKER_COMPOSE_FILE) up -d --build --remove-orphans
.PHONY: docker-compose-up

# Stop and remove all services defined in the docker-compose file.
docker-compose-down:
	docker compose -f $(DOCKER_COMPOSE_FILE) stop -t 60
	docker compose -f $(DOCKER_COMPOSE_FILE) down
.PHONY: docker-compose-down

# Follow logs of all services defined in the docker-compose file, showing the last 500 lines.
docker-compose-logs:
	docker compose -f $(DOCKER_COMPOSE_FILE) logs -f -n 500
.PHONY: docker-compose-logs

# ============================== TESTING ============================== #

# Run unit tests using pytest with verbose output (Middleware tests).
unit-tests:
	pytest --verbose
.PHONY: unit-tests

# Run integration tests by comparing sorted query results with expected output.
QUERY_RESULTS := \
    client_0_Q1X_result \
    client_0_Q21_result \
    client_0_Q22_result \
    client_0_Q3X_result \
    client_0_Q4X_result
integration-tests:
	@mkdir -p integration-tests/data/query_results
	@for result in $(QUERY_RESULTS); do \
		sort .results/query_results/$$result.txt > integration-tests/data/query_results/$$result.txt; \
	done
	python3 ./integration-tests/compare_results.py --expected ./integration-tests/data/expected_output --actual ./integration-tests/data/query_results
.PHONY: integration-tests

# End-of-file propagation tests.
eof-propagation-tests:
	@echo "🧪 Tests en proceso"

.PHONY: eof-propagatio
