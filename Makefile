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
	docker compose -f $(DOCKER_COMPOSE_FILE) logs -f
.PHONY: docker-compose-logs

# ============================== TESTING ============================== #

# Run unit tests using pytest with verbose output (Middleware tests).
unit-tests:
	pytest --verbose
.PHONY: unit-tests

# Run integration tests by comparing sorted query results with expected output.
ifndef EXPECTED_VARIANT
$(error Debes especificar EXPECTED_VARIANT. Ejemplo: make integration-tests EXPECTED_VARIANT=full_data)
endif
EXPECTED_BASE := ./integration-tests/data/expected_output
EXPECTED_PATH := $(EXPECTED_BASE)/$(EXPECTED_VARIANT)
ACTUAL_DIR ?= ./integration-tests/data/query_results
QUERY_RESULTS := \
    client_0_Q1X_result \
    client_0_Q21_result \
    client_0_Q22_result \
    client_0_Q3X_result \
    client_0_Q4X_result
RESULT_SUFFIX := _result.txt
integration-tests:
	@echo "==> Ejecutando tests de integración con variante: $(EXPECTED_VARIANT)"
	@echo "==> Usando expected outputs desde: $(EXPECTED_PATH)"
	@echo "==> Copiando resultados actuales a: $(ACTUAL_DIR)"
	@mkdir -p $(ACTUAL_DIR)
	@for result in $(QUERY_RESULTS); do \
		if [ ! -f .results/query_results/$$result.txt ]; then \
			echo "[ADVERTENCIA] No existe .results/query_results/$$result.txt"; \
		else \
			sort .results/query_results/$$result.txt > $(ACTUAL_DIR)/$$result.txt; \
		fi \
	done
	python3 ./integration-tests/compare_results.py \
		--expected $(EXPECTED_PATH) \
		--actual   $(ACTUAL_DIR) \
		--suffix   "$(RESULT_SUFFIX)"
.PHONY: integration-tests

# End-of-file propagation tests.
eof-propagation-tests:
	@echo "🧪 Tests en proceso"

.PHONY: eof-propagatio
