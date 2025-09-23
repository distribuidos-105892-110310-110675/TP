SHELL := /bin/bash
PWD := $(shell pwd)

docker-build-image:
	docker build -f ./src/server/Dockerfile -t "server:latest" .
	docker build -f ./src/client/Dockerfile -t "client:latest" .
.PHONY: docker-build-image

docker-compose-up: docker-build-image
	docker compose -f docker-compose.yaml up -d --build --remove-orphans
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose.yaml stop -t 3
	docker compose -f docker-compose.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose.yaml logs -f
.PHONY: docker-compose-logs

tests:
	echo "@TODO: implement test running"
	pytest --verbose
		
	exit 1
.PHONY: tests