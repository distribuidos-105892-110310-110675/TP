SHELL := /bin/bash
PWD := $(shell pwd)

docker-build-image:
	docker build -f ./src/server/Dockerfile -t "server:latest" .
	docker build -f ./src/client/Dockerfile -t "client:latest" .
	docker build -f ./src/controllers/filters/filter_by_year/Dockerfile -t "filter_by_year:latest" .
	docker build -f ./src/controllers/filters/filter_by_hour/Dockerfile -t "filter_by_hour:latest" .
	docker build -f ./src/controllers/filters/filter_by_amount/Dockerfile -t "filter_by_amount:latest" .
	docker build -f ./src/controllers/maps/map_month_semester/Dockerfile -t "map_month_semester:latest" .
	docker build -f ./src/controllers/maps/map_month_year/Dockerfile -t "map_month_year:latest" .
	docker build -f ./src/controllers/sorts/sort_by_count/Dockerfile -t "sort_by_count:latest" .
	docker build -f ./src/controllers/sorts/sort_by_sum/Dockerfile -t "sort_by_sum:latest" .
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
	pytest --verbose
.PHONY: tests