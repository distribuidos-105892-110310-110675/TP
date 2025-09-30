#!/bin/bash

# ============================== PRIVATE - UTILS ============================== #

function add-line() {
  local compose_filename=$1
  local text=$2

  echo "$text" >> "$compose_filename"
}

function add-empty-line() {
  local compose_filename=$1

  add-line $compose_filename ''
}

function add-comment() {
  local compose_filename=$1
  local comment_text=$2

  add-line $compose_filename "# ============================== $comment_text ============================== #"
  add-empty-line $compose_filename
}

# ============================== PRIVATE - NAME ============================== #

function add-name() {
  local compose_filename=$1
  
  echo 'name: tp' > "$compose_filename"
}

# ============================== PRIVATE - RABBITMQ ============================== #

function add-rabbitmq-service() {
  local compose_filename=$1

  add-line $compose_filename '  rabbitmq-message-middleware:'
  add-line $compose_filename '    image: "rabbitmq:4-management"'
  add-line $compose_filename '    ports:'
  add-line $compose_filename '      - "5672:5672"'
  add-line $compose_filename '      - "15672:15672"'
  add-line $compose_filename '    environment:'
  add-line $compose_filename '      - RABBITMQ_DEFAULT_USER=guest'
  add-line $compose_filename '      - RABBITMQ_DEFAULT_PASS=guest'
  add-line $compose_filename '    networks:'
  add-line $compose_filename '      - custom_net'
  add-line $compose_filename '    healthcheck:'
  add-line $compose_filename '      test: rabbitmq-diagnostics check_port_connectivity'
  add-line $compose_filename '      interval: 30s'
  add-line $compose_filename '      timeout: 5s'
  add-line $compose_filename '      retries: 5'
  add-line $compose_filename '      start_period: 30s'
}

# ============================== PRIVATE - CLIENT & SERVER ============================== #

function add-client-service() {
  local compose_filename=$1

  add-line $compose_filename '  client:'
  add-line $compose_filename '    container_name: client'
  add-line $compose_filename '    image: client:latest'
  add-line $compose_filename '    entrypoint: python3 -m client.main'
  add-line $compose_filename '    environment:'
  add-line $compose_filename '      - PYTHONUNBUFFERED=1'
  add-line $compose_filename '      - LOGGING_LEVEL=INFO'
  add-line $compose_filename '      - CLIENT_ID=0'
  add-line $compose_filename '      - SERVER_HOST=server'
  add-line $compose_filename '      - SERVER_PORT=5000'
  add-line $compose_filename '      - DATA_PATH=/data'
  add-line $compose_filename '      - RESULTS_PATH=/results'
  add-line $compose_filename '      - BATCH_MAX_SIZE=20'
  add-line $compose_filename '    networks:'
  add-line $compose_filename '      - custom_net'
  add-line $compose_filename '    volumes:'
  add-line $compose_filename '      - type: bind'
  add-line $compose_filename '        source: ./.data'
  add-line $compose_filename '        target: /data'
  add-line $compose_filename '        read_only: true'
  add-line $compose_filename '      - type: bind'
  add-line $compose_filename '        source: ./.results'
  add-line $compose_filename '        target: /results'
  add-line $compose_filename '        read_only: false'
  add-line $compose_filename '    deploy:'
  add-line $compose_filename '      restart_policy:'
  add-line $compose_filename '        condition: on-failure'
  add-line $compose_filename '        delay: 5s'
  add-line $compose_filename '        max_attempts: 1'
  add-line $compose_filename '    depends_on:'
  add-line $compose_filename '      server:'
  add-line $compose_filename '        condition: service_started'
}

function add-server-service() {
  local compose_filename=$1

  add-line $compose_filename '  server:'
  add-line $compose_filename '    container_name: server'
  add-line $compose_filename '    image: server:latest'
  add-line $compose_filename '    entrypoint: python3 -m server.main'
  add-line $compose_filename '    environment:'
  add-line $compose_filename '      - PYTHONUNBUFFERED=1'
  add-line $compose_filename '      - LOGGING_LEVEL=DEBUG'
  add-line $compose_filename '      - SERVER_PORT=5000'
  add-line $compose_filename '      - SERVER_LISTEN_BACKLOG=5'
  add-line $compose_filename '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_filename '      - MENU_ITEMS_CLN_AMOUNT=1'
  add-line $compose_filename '      - STORES_CLN_AMOUNT=1'
  add-line $compose_filename '      - TRANSACTION_ITEMS_CLN_AMOUNT=1'
  add-line $compose_filename '      - TRANSACTIONS_CLN_AMOUNT=1'
  add-line $compose_filename '      - USERS_CLN_AMOUNT=1'
  add-line $compose_filename '      - Q1X_OB_AMOUNT=1'
  add-line $compose_filename '      - Q21_OB_AMOUNT=1'
  add-line $compose_filename '      - Q22_OB_AMOUNT=1'
  add-line $compose_filename '      - Q3X_OB_AMOUNT=1'
  add-line $compose_filename '      - Q4X_OB_AMOUNT=1'
  add-line $compose_filename '    networks:'
  add-line $compose_filename '      - custom_net'
  add-line $compose_filename '    deploy:'
  add-line $compose_filename '      restart_policy:'
  add-line $compose_filename '        condition: on-failure'
  add-line $compose_filename '        delay: 5s'
  add-line $compose_filename '        max_attempts: 1'
  add-line $compose_filename '    depends_on:'
  add-line $compose_filename '      rabbitmq-message-middleware:'
  add-line $compose_filename '        condition: service_healthy'
}

# ============================== PRIVATE - CLEANERS ============================== #

function add-menu-cleaner() {
  local compose_filename=$1

  add-line $compose_filename '  menu_items_cleaner_items_0:'
  add-line $compose_filename '    container_name: menu_items_cleaner_items_0'
  add-line $compose_filename '    image: menu_items_cleaner:latest'
  add-line $compose_filename '    entrypoint: python3 -m controllers.cleaners.menu_items_cleaner.main'
  add-line $compose_filename '    environment:'
  add-line $compose_filename '      - PYTHONUNBUFFERED=1'
  add-line $compose_filename '      - LOGGING_LEVEL=INFO'
  add-line $compose_filename '      - CLEANER_ID=0'
  add-line $compose_filename '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_filename '      - JOINS_AMOUNT=1'
  add-line $compose_filename '    networks:'
  add-line $compose_filename '      - custom_net'
  add-line $compose_filename '    depends_on:'
  add-line $compose_filename '      rabbitmq-message-middleware:'
  add-line $compose_filename '        condition: service_healthy'
}

function add-stores-cleaner() {
  local compose_filename=$1

  add-line $compose_filename '  stores_cleaner_items_0:'
  add-line $compose_filename '    container_name: stores_cleaner_items_0'
  add-line $compose_filename '    image: stores_cleaner:latest'
  add-line $compose_filename '    entrypoint: python3 -m controllers.cleaners.stores_cleaner.main'
  add-line $compose_filename '    environment:'
  add-line $compose_filename '      - PYTHONUNBUFFERED=1'
  add-line $compose_filename '      - LOGGING_LEVEL=INFO'
  add-line $compose_filename '      - CLEANER_ID=0'
  add-line $compose_filename '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_filename '      - JOINS_AMOUNT=1'
  add-line $compose_filename '    networks:'
  add-line $compose_filename '      - custom_net'
  add-line $compose_filename '    depends_on:'
  add-line $compose_filename '      rabbitmq-message-middleware:'
  add-line $compose_filename '        condition: service_healthy'
}

function add-transaction-items-cleaner() {
  local compose_filename=$1

  add-line $compose_filename '  transaction_items_cleaner_0:'
  add-line $compose_filename '    container_name: transaction_items_cleaner_0'
  add-line $compose_filename '    image: transaction_items_cleaner:latest'
  add-line $compose_filename '    entrypoint: python3 -m controllers.cleaners.transaction_items_cleaner.main'
  add-line $compose_filename '    environment:'
  add-line $compose_filename '      - PYTHONUNBUFFERED=1'
  add-line $compose_filename '      - LOGGING_LEVEL=DEBUG'
  add-line $compose_filename '      - CLEANER_ID=0'
  add-line $compose_filename '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_filename '      - FILTERS_AMOUNT=1'
  add-line $compose_filename '    networks:'
  add-line $compose_filename '      - custom_net'
  add-line $compose_filename '    depends_on:'
  add-line $compose_filename '      rabbitmq-message-middleware:'
  add-line $compose_filename '        condition: service_healthy'
}

function add-transactions-cleaner() {
  local compose_filename=$1

  add-line $compose_filename '  transactions_cleaner_0:'
  add-line $compose_filename '    container_name: transactions_cleaner_0'
  add-line $compose_filename '    image: transactions_cleaner:latest'
  add-line $compose_filename '    entrypoint: python3 -m controllers.cleaners.transactions_cleaner.main'
  add-line $compose_filename '    environment:'
  add-line $compose_filename '      - PYTHONUNBUFFERED=1'
  add-line $compose_filename '      - LOGGING_LEVEL=DEBUG'
  add-line $compose_filename '      - CLEANER_ID=0'
  add-line $compose_filename '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_filename '      - FILTERS_AMOUNT=1'
  add-line $compose_filename '    networks:'
  add-line $compose_filename '      - custom_net'
  add-line $compose_filename '    depends_on:'
  add-line $compose_filename '      rabbitmq-message-middleware:'
  add-line $compose_filename '        condition: service_healthy'
}

function add-users-cleaner() {
  local compose_filename=$1

  add-line $compose_filename '  users_cleaner_0:'
  add-line $compose_filename '    container_name: users_cleaner_0'
  add-line $compose_filename '    image: users_cleaner:latest'
  add-line $compose_filename '    entrypoint: python3 -m controllers.cleaners.users_cleaner.main'
  add-line $compose_filename '    environment:'
  add-line $compose_filename '      - PYTHONUNBUFFERED=1'
  add-line $compose_filename '      - LOGGING_LEVEL=DEBUG'
  add-line $compose_filename '      - CLEANER_ID=0'
  add-line $compose_filename '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_filename '      - JOINS_AMOUNT=1'
  add-line $compose_filename '    networks:'
  add-line $compose_filename '      - custom_net'
  add-line $compose_filename '    depends_on:'
  add-line $compose_filename '      rabbitmq-message-middleware:'
  add-line $compose_filename '        condition: service_healthy'
}

function add-cleaners() {
  local compose_filename=$1

  add-menu-cleaner $compose_filename
  add-empty-line $compose_filename
  add-stores-cleaner $compose_filename
  add-empty-line $compose_filename
  add-transaction-items-cleaner $compose_filename
  add-empty-line $compose_filename
  add-transactions-cleaner $compose_filename
  add-empty-line $compose_filename
  add-users-cleaner $compose_filename
}

# ============================== PRIVATE - OUTPUT BUILDER ============================== #

function add-query-1x-output-builder() {
  local compose_filename=$1

  add-line $compose_filename '  query_1x_output_builder_0:'
  add-line $compose_filename '    container_name: query_1x_output_builder_0'
  add-line $compose_filename '    image: query_1x_output_builder:latest'
  add-line $compose_filename '    entrypoint: python3 -m controllers.output_builders.query_1x_output_builder.main'
  add-line $compose_filename '    environment:'
  add-line $compose_filename '      - PYTHONUNBUFFERED=1'
  add-line $compose_filename '      - LOGGING_LEVEL=DEBUG'
  add-line $compose_filename '      - OUTPUT_BUILDER_ID=0'
  add-line $compose_filename '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_filename '      - PREV_CONTROLLERS_AMOUNT=1'
  add-line $compose_filename '    networks:'
  add-line $compose_filename '      - custom_net'
  add-line $compose_filename '    depends_on:'
  add-line $compose_filename '      rabbitmq-message-middleware:'
  add-line $compose_filename '        condition: service_healthy'
}

function add-query-21-output-builder() {
  local compose_filename=$1

  add-line $compose_filename '  query_21_output_builder_0:'
  add-line $compose_filename '    container_name: query_21_output_builder_0'
  add-line $compose_filename '    image: query_21_output_builder:latest'
  add-line $compose_filename '    entrypoint: python3 -m controllers.output_builders.query_21_output_builder.main'
  add-line $compose_filename '    environment:'
  add-line $compose_filename '      - PYTHONUNBUFFERED=1'
  add-line $compose_filename '      - LOGGING_LEVEL=DEBUG'
  add-line $compose_filename '      - OUTPUT_BUILDER_ID=0'
  add-line $compose_filename '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_filename '      - PREV_CONTROLLERS_AMOUNT=1'
  add-line $compose_filename '    networks:'
  add-line $compose_filename '      - custom_net'
  add-line $compose_filename '    depends_on:'
  add-line $compose_filename '      rabbitmq-message-middleware:'
  add-line $compose_filename '        condition: service_healthy'
}

function add-query-22-output-builder() {
  local compose_filename=$1

  add-line $compose_filename '  query_22_output_builder_0:'
  add-line $compose_filename '    container_name: query_22_output_builder_0'
  add-line $compose_filename '    image: query_22_output_builder:latest'
  add-line $compose_filename '    entrypoint: python3 -m controllers.output_builders.query_22_output_builder.main'
  add-line $compose_filename '    environment:'
  add-line $compose_filename '      - PYTHONUNBUFFERED=1'
  add-line $compose_filename '      - LOGGING_LEVEL=DEBUG'
  add-line $compose_filename '      - OUTPUT_BUILDER_ID=0'
  add-line $compose_filename '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_filename '      - PREV_CONTROLLERS_AMOUNT=1'
  add-line $compose_filename '    networks:'
  add-line $compose_filename '      - custom_net'
  add-line $compose_filename '    depends_on:'
  add-line $compose_filename '      rabbitmq-message-middleware:'
  add-line $compose_filename '        condition: service_healthy'
}

function add-query-3x-output-builder() {
  local compose_filename=$1

  add-line $compose_filename '  query_3x_output_builder_0:'
  add-line $compose_filename '    container_name: query_3x_output_builder_0'
  add-line $compose_filename '    image: query_3x_output_builder:latest'
  add-line $compose_filename '    entrypoint: python3 -m controllers.output_builders.query_3x_output_builder.main'
  add-line $compose_filename '    environment:'
  add-line $compose_filename '      - PYTHONUNBUFFERED=1'
  add-line $compose_filename '      - LOGGING_LEVEL=DEBUG'
  add-line $compose_filename '      - OUTPUT_BUILDER_ID=0'
  add-line $compose_filename '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_filename '      - PREV_CONTROLLERS_AMOUNT=1'
  add-line $compose_filename '    networks:'
  add-line $compose_filename '      - custom_net'
  add-line $compose_filename '    depends_on:'
  add-line $compose_filename '      rabbitmq-message-middleware:'
  add-line $compose_filename '        condition: service_healthy'
}

function add-query-4x-output-builder() {
  local compose_filename=$1

  add-line $compose_filename '  query_4x_output_builder_0:'
  add-line $compose_filename '    container_name: query_4x_output_builder_0'
  add-line $compose_filename '    image: query_4x_output_builder:latest'
  add-line $compose_filename '    entrypoint: python3 -m controllers.output_builders.query_4x_output_builder.main'
  add-line $compose_filename '    environment:'
  add-line $compose_filename '      - PYTHONUNBUFFERED=1'
  add-line $compose_filename '      - LOGGING_LEVEL=DEBUG'
  add-line $compose_filename '      - OUTPUT_BUILDER_ID=0'
  add-line $compose_filename '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_filename '      - PREV_CONTROLLERS_AMOUNT=1'
  add-line $compose_filename '    networks:'
  add-line $compose_filename '      - custom_net'
  add-line $compose_filename '    depends_on:'
  add-line $compose_filename '      rabbitmq-message-middleware:'
  add-line $compose_filename '        condition: service_healthy'
}

function add-output-builders() {
  local compose_filename=$1

  add-query-1x-output-builder $compose_filename
  add-empty-line $compose_filename
  add-query-21-output-builder $compose_filename
  add-empty-line $compose_filename
  add-query-22-output-builder $compose_filename
  add-empty-line $compose_filename
  add-query-3x-output-builder $compose_filename
  add-empty-line $compose_filename
  add-query-4x-output-builder $compose_filename

}

# ============================== PRIVATE - SERVICES ============================== #

function add-services() {
  local compose_filename=$1

  add-line $compose_filename 'services:'
  add-empty-line $compose_filename

  add-comment $compose_filename 'RABBITMQ SERVICE'
  add-rabbitmq-service $compose_filename
  add-empty-line $compose_filename
  
  add-comment $compose_filename 'CLIENT & SERVER SERVICES'
  add-client-service $compose_filename
  add-empty-line $compose_filename
  add-server-service $compose_filename
  add-empty-line $compose_filename

  add-comment $compose_filename 'CLEANERS SERVICES'
  add-cleaners $compose_filename
  add-empty-line $compose_filename

  add-comment $compose_filename 'OUTPUT BUILDER SERVICES'
  add-output-builders $compose_filename
}

# ============================== PRIVATE - NETWORKS ============================== #

function add-networks() {
  local compose_filename=$1

  add-line $compose_filename 'networks:'
  add-line $compose_filename '  custom_net:'
  add-line $compose_filename '    ipam:'
  add-line $compose_filename '      driver: default'
  add-line $compose_filename '      config:'
  add-line $compose_filename '        - subnet: 172.25.125.0/24'
}

# ============================== PRIVATE - DOCKER COMPOSE FILE BUILD ============================== #

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
batch_size=$2

echo "Nombre del archivo de salida: $compose_filename_param"

build-docker-compose-file $compose_filename_param