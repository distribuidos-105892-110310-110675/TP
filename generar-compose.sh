#!/bin/bash

# ============================== PRIVATE - UTILS ============================== #

function add-line() {
  local compose_filename=$1
  local text=$2

  echo "$text" >> "$compose_filename"
}

function add-empty-line() {
  local compose_filename=$1

  add-line $compose_filename ""
}

# ============================== PRIVATE - NAME BUILDER ============================== #

function add-name() {
  local compose_filename=$1
  
  echo "name: tp" > "$compose_filename"
}

# ============================== PRIVATE - SERVICES BUILDER ============================== #

function add-server-service() {
  local compose_filename=$1

  add-line $compose_filename "  server:"
  add-line $compose_filename "    container_name: server"
  add-line $compose_filename "    image: server:latest"
  add-line $compose_filename "    entrypoint: python3 -m server.main"
  add-line $compose_filename "    environment:"
  add-line $compose_filename "      - PYTHONUNBUFFERED=1"
  add-line $compose_filename "      - LOGGING_LEVEL=DEBUG"
  add-line $compose_filename "      - SERVER_PORT=5000"
  add-line $compose_filename "      - SERVER_LISTEN_BACKLOG=5"
  add-line $compose_filename "    networks:"
  add-line $compose_filename "      - custom_net"
}

function add-client-service() {
  local compose_filename=$1

  add-line $compose_filename "  client:"
  add-line $compose_filename "    container_name: client"
  add-line $compose_filename "    image: client:latest"
  add-line $compose_filename "    entrypoint: python3 -m client.main"
  add-line $compose_filename "    environment:"
  add-line $compose_filename "      - PYTHONUNBUFFERED=1"
  add-line $compose_filename "      - LOGGING_LEVEL=DEBUG"
  add-line $compose_filename "      - CLIENT_ID=1"
  add-line $compose_filename "      - SERVER_HOST=server"
  add-line $compose_filename "      - SERVER_PORT=5000"
  add-line $compose_filename "      - DATA_PATH=/agency-1.csv"
  add-line $compose_filename "      - BATCH_MAX_KIB=8"
  add-line $compose_filename "    networks:"
  add-line $compose_filename "      - custom_net"
  add-line $compose_filename "    volumes:"
  add-line $compose_filename "      - type: bind"
  add-line $compose_filename "        source: ./.data/agency-1.csv"
  add-line $compose_filename "        target: /agency-1.csv"
  add-line $compose_filename "        read_only: true"
  add-line $compose_filename "    deploy:"
  add-line $compose_filename "      restart_policy:"
  add-line $compose_filename "        condition: on-failure"
  add-line $compose_filename "        delay: 5s"
  add-line $compose_filename "        max_attempts: 1"
  add-line $compose_filename "    depends_on:"
  add-line $compose_filename "      server:"
  add-line $compose_filename "        condition: service_started"
}

function add-services() {
  local compose_filename=$1

  add-line $compose_filename "services:"

  add-server-service $compose_filename
  add-empty-line $compose_filename
  add-client-service $compose_filename
}

# ============================== PRIVATE - NETWORKS BUILDER ============================== #

function add-networks() {
  local compose_filename=$1

  add-line $compose_filename "networks:"
  add-line $compose_filename "  custom_net:"
  add-line $compose_filename "    ipam:"
  add-line $compose_filename "      driver: default"
  add-line $compose_filename "      config:"
  add-line $compose_filename "        - subnet: 172.25.125.0/24"
}

# ============================== PRIVATE - DOCKER COMPOSE FILE BUILDER ============================== #

function build-docker-compose-file() {
  local compose_filename=$1

  echo "Generando archivo $compose_filename ..."
  
  add-name $compose_filename
  add-services $compose_filename
  add-empty-line $compose_filename
  add-networks $compose_filename

  echo "Generando archivo $compose_filename ... [DONE]"
}

# ============================== MAIN ============================== #

if [ $# -ne 1 ]; then
  echo "Uso: $0 <compose_filename.yaml>"
  echo "Ejemplo: $0 docker-compose.yaml"
  exit 1
fi

compose_filename_param=$1

echo "Nombre del archivo de salida: $compose_filename_param"

build-docker-compose-file $compose_filename_param