#!/bin/bash

# ============================== PRIVATE - UTILS ============================== #

function add-line() {
  local fixed_text=$1
  local variable_text=$2
  local extra=$3

  echo "$text$variable_text$extra" >> "$COMPOSE_FILE"
}

function add-empty-line() {
  add-line ''
}

function add-comment() {
  local comment_text=$1

  add-line "# ============================== $comment_text ============================== #"
  add-empty-line $COMPOSE_FILE
}

# ============================== PRIVATE - NAME ============================== #

function add-name() {
  echo 'name: tp' > "$COMPOSE_FILE"
}

# ============================== PRIVATE - RABBITMQ ============================== #

function add-rabbitmq-service() {
  add-line '  rabbitmq-message-middleware:'
  add-line '    image: "rabbitmq:4-management"'
  add-line '    ports:'
  add-line '      - "5672:5672"'
  add-line '      - "15672:15672"'
  add-line '    environment:'
  local rabbit_default_user='      - RABBITMQ_DEFAULT_USER='
  local rabbit_default_pass='      - RABBITMQ_DEFAULT_PASS='
  add-line $rabbit_default_user $RABBITMQ_USER
  add-line $rabbit_default_pass $RABBITMQ_PASS
  add-line '    networks:'
  add-line '      - custom_net'
  add-line '    healthcheck:'
  add-line '      test: rabbitmq-diagnostics check_port_connectivity'
  add-line '      interval: 30s'
  add-line '      timeout: 5s'
  add-line '      retries: 5'
  add-line '      start_period: 30s'
}

# ============================== PRIVATE - CLIENT & SERVER ============================== #

function add-client-service() {
  local current_client=$1
  local client='  client_'
  local semicolon=':'
  add-line $current_client $client $semicolon
  local container_name='    container_name: client_'
  add-line $container_name $current_client
  add-line '    image: client:latest'
  add-line '    entrypoint: python3 -m client.main'
  add-line '    environment:'
  local python_unbuffered='      - PYTHONUNBUFFERED='
  add-line $python_unbuffered $PYTHONUNBUFFERED
  local logging_level='      - LOGGING_LEVEL='
  add-line $logging_level $LOGGING_LEVEL
  local client_id='      - CLIENT_ID='
  add-line $client_id $current_client
  add-line '      - SERVER_HOST=server'
  add-line '      - SERVER_PORT=5000'
  add-line '      - DATA_PATH=/data'
  add-line '      - RESULTS_PATH=/results'
  local max_batch='      - BATCH_MAX_SIZE='
  add-line $max_batch $BATCH_MAX_SIZE
  add-line '    networks:'
  add-line '      - custom_net'
  add-line '    volumes:'
  add-line '      - type: bind'
  add-line '        source: ./.data'
  add-line '        target: /data'
  add-line '        read_only: true'
  add-line '      - type: bind'
  add-line '        source: ./.results'
  add-line '        target: /results'
  add-line '        read_only: false'
  add-line '    deploy:'
  add-line '      restart_policy:'
  add-line '        condition: on-failure'
  add-line '        delay: 5s'
  add-line '        max_attempts: 1'
  add-line '    depends_on:'
  add-line '      server:'
  add-line '        condition: service_started'
}

function add-server-service() {
  add-line  '  server:'
  add-line  '    container_name: server'
  add-line  '    image: server:latest'
  add-line  '    entrypoint: python3 -m server.main'
  add-line  '    environment:'
  local python_unbuffered='      - PYTHONUNBUFFERED='
  add-line  $python_unbuffered $PYTHONUNBUFFERED
  local logging_level='      - LOGGING_LEVEL='
  add-line  $logging_level $LOGGING_LEVEL
  add-line  '      - SERVER_PORT=5000'
  local server_backlog='      - SERVER_LISTEN_BACKLOG='
  add-line  $server_backlog $SERVER_LISTEN_BACKLOG
  add-line  '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line  '      - MENU_ITEMS_CLN_AMOUNT=1'
  add-line  '      - STORES_CLN_AMOUNT=1'
  local transaction_items_cleaners='      - TRANSACTION_ITEMS_CLN_AMOUNT='
  add-line  $transaction_items_cleaners $TRANSACTION_ITEMS_CLN_AMOUNT
  local transactions_cleaners='      - TRANSACTIONS_CLN_AMOUNT='
  add-line  $transactions_cleaners $TRANSACTIONS_CLN_AMOUNT
  local users_cleaners='      - USERS_CLN_AMOUNT='
  add-line  $users_cleaners $USERS_CLN_AMOUNT
  local q1_output_builder='      - Q1X_OB_AMOUNT='
  add-line $q1_output_builder $Q1X_OB_AMOUNT
  local q21_output_builder='      - Q21_OB_AMOUNT='
  add-line $q21_output_builder $Q21_OB_AMOUNT
  local q22_output_builder='      - Q22_OB_AMOUNT='
  add-line $q22_output_builder $Q22_OB_AMOUNT
  local q3_output_builder='      - Q3X_OB_AMOUNT='
  add-line $q3_output_builder $Q3X_OB_AMOUNT
  local q4_output_builder='      - Q4X_OB_AMOUNT='
  add-line $q4_output_builder $Q4X_OB_AMOUNT
  add-line  '    networks:'
  add-line  '      - custom_net'
  add-line  '    deploy:'
  add-line  '      restart_policy:'
  add-line  '        condition: on-failure'
  add-line  '        delay: 5s'
  add-line  '        max_attempts: 1'
  add-line  '    depends_on:'
  add-line  '      rabbitmq-message-middleware:'
  add-line  '        condition: service_healthy'
}

# ============================== PRIVATE - CLEANERS ============================== #

function add-menu-cleaner() {
  add-line '  menu_items_cleaner_items_0:'
  add-line '    container_name: menu_items_cleaner_items_0'
  add-line '    image: menu_items_cleaner:latest'
  add-line '    entrypoint: python3 -m controllers.cleaners.menu_items_cleaner.main'
  add-line '    environment:'
  add-line '      - PYTHONUNBUFFERED=1'
  add-line '      - LOGGING_LEVEL=INFO'
  add-line '      - CONTROLLER_ID=0'
  add-line '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  local joins_amount='      - JOINS_AMOUNT='
  add-line $joins_amount $Q2_JOINERS_AMOUNT
  add-line '    networks:'
  add-line '      - custom_net'
  add-line '    depends_on:'
  add-line '      rabbitmq-message-middleware:'
  add-line '        condition: service_healthy'
}

function add-stores-cleaner() {
  add-line '  stores_cleaner_items_0:'
  add-line '    container_name: stores_cleaner_items_0'
  add-line '    image: stores_cleaner:latest'
  add-line '    entrypoint: python3 -m controllers.cleaners.stores_cleaner.main'
  add-line '    environment:'
  add-line '      - PYTHONUNBUFFERED=1'
  add-line '      - LOGGING_LEVEL=INFO'
  add-line '      - CONTROLLER_ID=0'
  add-line '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  local joins_amount=add-line '      - JOINS_AMOUNT='
  local greater_join_amount
  if (( $Q3_JOINERS_AMOUNT > $Q4_TRANSACTIONS_WITH_STORES_JOINERS_AMOUNT)); then
    greater_join_amount=$Q3_JOINERS_AMOUNT
  else 
    greater_join_amount=$Q4_TRANSACTIONS_WITH_STORES_JOINERS_AMOUNT
  fi
  add-line $joins_amount $greater_join_amount
  add-line '    networks:'
  add-line '      - custom_net'
  add-line '    depends_on:'
  add-line '      rabbitmq-message-middleware:'
  add-line '        condition: service_healthy'
}

function add-transaction-items-cleaner() {
  local current_id=$1
  local transaction_items_cleaners='  transaction_items_cleaner_'
  local semicolon=':'
  add-line $transaction_items_cleaners $current_id $semicolon
  local container_name='    container_name: transaction_items_cleaner_'
  add-line $container_name $current_id
  add-line '    image: transaction_items_cleaner:latest'
  add-line '    entrypoint: python3 -m controllers.cleaners.transaction_items_cleaner.main'
  add-line '    environment:'
  local python_unbuffered='      - PYTHONUNBUFFERED='
  add-line $python_unbuffered $PYTHONUNBUFFERED
  local logging_level='      - LOGGING_LEVEL='
  add-line $logging_level $LOGGING_LEVEL
  local controller_id='      - CONTROLLER_ID='
  add-line $controller_id $current_id
  add-line '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  local filter_amount='      - FILTERS_AMOUNT='
  add-line $filter_amount $FILTER_TRANSACTION_ITEMS_BY_YEAR_AMOUNT
  add-line '    networks:'
  add-line '      - custom_net'
  add-line '    depends_on:'
  add-line '      rabbitmq-message-middleware:'
  add-line '        condition: service_healthy'
}

function add-transactions-cleaner() {
  local current_id=$1
  local transactions_cleaners='  transactions_cleaner_'
  local semicolon=':'
  add-line $transactions_cleaners $current_id $semicolon
  local container_name='    container_name: transactions_cleaner_'
  add-line $container_name $current_id
  add-line '    image: transactions_cleaner:latest'
  add-line '    entrypoint: python3 -m controllers.cleaners.transactions_cleaner.main'
  add-line '    environment:'
  local python_unbuffered='      - PYTHONUNBUFFERED='
  add-line $python_unbuffered $PYTHONUNBUFFERED
  local logging_level='      - LOGGING_LEVEL='
  add-line $logging_level $LOGGING_LEVEL
  local controller_id='      - CONTROLLER_ID='
  add-line $controller_id $current_id
  add-line '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  local filters_amount='      - FILTERS_AMOUNT='
  add-line $filters_amount $FILTER_TRANSACTIONS_BY_YEAR_AMOUNT
  add-line '    networks:'
  add-line '      - custom_net'
  add-line '    depends_on:'
  add-line '      rabbitmq-message-middleware:'
  add-line '        condition: service_healthy'
}

function add-users-cleaner() {
  local current_id=$1
  local users_cleaners='  users_cleaner_'
  local semicolon=':'
  add-line $users_cleaners $current_id $semicolon
  local container_name='    container_name: users_cleaner_'
  add-line $container_name $current_id
  add-line '    image: users_cleaner:latest'
  add-line '    entrypoint: python3 -m controllers.cleaners.users_cleaner.main'
  add-line '    environment:'
  local python_unbuffered='      - PYTHONUNBUFFERED='
  add-line $python_unbuffered $PYTHONUNBUFFERED
  local logging_level='      - LOGGING_LEVEL='
  add-line $logging_level $LOGGING_LEVEL
  local controller_id='      - CONTROLLER_ID='
  add-line $controller_id $current_id
  add-line '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  local joins_amount='      - JOINS_AMOUNT='
  add-line $joins_amount $Q4_TRANSACTIONS_WITH_USERS_JOINERS_AMOUNT
  add-line '    networks:'
  add-line '      - custom_net'
  add-line '    depends_on:'
  add-line '      rabbitmq-message-middleware:'
  add-line '        condition: service_healthy'
}

function add-cleaners() {
  add-menu-cleaner
  add-empty-line
  add-stores-cleaner 
  add-empty-line 
  for ((i=0; i < $TRANSACTION_ITEMS_CLN_AMOUNT; i++)); do
    add-transaction-items-cleaner $i
    add-empty-line
  done
  for ((i=0; i < $TRANSACTIONS_CLN_AMOUNT; i++)); do
    add-transactions-cleaner $i
    add-empty-line
  done
  for ((i=0; i < $USERS_CLN_AMOUNT; i++)); do
    add-users-cleaner $i
    add-empty-line
  done
}

# ============================== PRIVATE - OUTPUT BUILDER ============================== #

function add-query-1x-output-builder() {
  local current_id=$1
  local output_builder='  query_1x_output_builder_'
  local semicolon=':'
  add-line $output_builder $current_id $semicolon
  local container_name='    container_name: query_1x_output_builder_'
  add-line $container_name $current_id
  add-line '    image: query_1x_output_builder:latest'
  add-line '    entrypoint: python3 -m controllers.output_builders.query_1x_output_builder.main'
  add-line '    environment:'
  local python_unbuffered='      - PYTHONUNBUFFERED='
  add-line $python_unbuffered $PYTHONUNBUFFERED
  local logging_level='      - LOGGING_LEVEL='
  add-line $logging_level $LOGGING_LEVEL
  local output_builder_id='      - OUTPUT_BUILDER_ID='
  add-line $output_builder_id $current_id
  add-line '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  local prev_controllers='      - PREV_CONTROLLERS_AMOUNT='
  add-line $prev_controllers $FILTER_TRANSACTIONS_BY_FINAL_AMNT_AMOUNT
  add-line '    networks:'
  add-line '      - custom_net'
  add-line '    depends_on:'
  add-line '      rabbitmq-message-middleware:'
  add-line '        condition: service_healthy'
}

function add-query-21-output-builder() {
  local current_id=$1
  local output_builder='  query_21_output_builder_'
  local semicolon=':'
  add-line $output_builder $current_id $semicolon
  local container_name='    container_name: query_21_output_builder_'
  add-line $container_name $current_id
  add-line '    image: query_21_output_builder:latest'
  add-line '    entrypoint: python3 -m controllers.output_builders.query_21_output_builder.main'
  add-line '    environment:'
  local python_unbuffered='      - PYTHONUNBUFFERED='
  add-line $python_unbuffered $PYTHONUNBUFFERED
  local logging_level='      - LOGGING_LEVEL='
  add-line $logging_level $LOGGING_LEVEL
  local output_builder_id='      - OUTPUT_BUILDER_ID='
  add-line $output_builder_id $current_id
  add-line '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  local prev_controllers='      - PREV_CONTROLLERS_AMOUNT='
  add-line $prev_controllers $Q2_JOINERS_AMOUNT
  add-line '    networks:'
  add-line '      - custom_net'
  add-line '    depends_on:'
  add-line '      rabbitmq-message-middleware:'
  add-line '        condition: service_healthy'
}

function add-query-22-output-builder() {
  local current_id=$1
  local output_builder='  query_22_output_builder_'
  local semicolon=':'
  add-line $output_builder $current_id $semicolon
  local container_name='    container_name: query_22_output_builder_'
  add-line $container_name $current_id
  add-line '    image: query_22_output_builder:latest'
  add-line '    entrypoint: python3 -m controllers.output_builders.query_22_output_builder.main'
  add-line '    environment:'
  local python_unbuffered='      - PYTHONUNBUFFERED='
  add-line $python_unbuffered $PYTHONUNBUFFERED
  local logging_level='      - LOGGING_LEVEL='
  add-line $logging_level $LOGGING_LEVEL
  local output_builder_id='      - OUTPUT_BUILDER_ID='
  add-line $output_builder_id $current_id
  add-line '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  local prev_controllers='      - PREV_CONTROLLERS_AMOUNT='
  add-line $prev_controllers $Q2_JOINERS_AMOUNT
  add-line '    networks:'
  add-line '      - custom_net'
  add-line '    depends_on:'
  add-line '      rabbitmq-message-middleware:'
  add-line '        condition: service_healthy'
}

function add-query-3x-output-builder() {
  local current_id=$1
  local output_builder='  query_3x_output_builder_'
  local semicolon=':'
  add-line $output_builder $current_id $semicolon
  local container_name='    container_name: query_3x_output_builder_'
  add-line $container_name $current_id
  add-line '    image: query_3x_output_builder:latest'
  add-line '    entrypoint: python3 -m controllers.output_builders.query_3x_output_builder.main'
  add-line '    environment:'
  local python_unbuffered='      - PYTHONUNBUFFERED='
  add-line $python_unbuffered $PYTHONUNBUFFERED
  local logging_level='      - LOGGING_LEVEL='
  add-line $logging_level $LOGGING_LEVEL
  local output_builder_id='      - OUTPUT_BUILDER_ID='
  add-line $output_builder_id $current_id
  add-line '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  local prev_controllers='      - PREV_CONTROLLERS_AMOUNT='
  add-line $prev_controllers $Q3_JOINERS_AMOUNT
  add-line '    networks:'
  add-line '      - custom_net'
  add-line '    depends_on:'
  add-line '      rabbitmq-message-middleware:'
  add-line '        condition: service_healthy'
}

function add-query-4x-output-builder() {
  local current_id=$1
  local output_builder='  query_4x_output_builder_'
  local semicolon=':'
  add-line $output_builder $current_id $semicolon
  local container_name='    container_name: query_4x_output_builder_'
  add-line $container_name $current_id
  add-line '    image: query_4x_output_builder:latest'
  add-line '    entrypoint: python3 -m controllers.output_builders.query_4x_output_builder.main'
  add-line '    environment:'
  local python_unbuffered='      - PYTHONUNBUFFERED='
  add-line $python_unbuffered $PYTHONUNBUFFERED
  local logging_level='      - LOGGING_LEVEL='
  add-line $logging_level $LOGGING_LEVEL
  local output_builder_id='      - OUTPUT_BUILDER_ID='
  add-line $output_builder_id $current_id
  add-line '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  local prev_controllers='      - PREV_CONTROLLERS_AMOUNT='
  add-line $prev_controllers $Q4_TRANSACTIONS_WITH_STORES_JOINERS_AMOUNT
  add-line '    networks:'
  add-line '      - custom_net'
  add-line '    depends_on:'
  add-line '      rabbitmq-message-middleware:'
  add-line '        condition: service_healthy'
}

function add-output-builders() {
  for (( i=1; i<=$Q1X_OB_AMOUNT; i++ )); do
    add-query-1x-output-builder $i
    add-empty-line
  done
  for (( i=1; i<=$Q21_OB_AMOUNT; i++ )); do
    add-query-21-output-builder $i
    add-empty-line
  done
  for (( i=1; i<=$Q22_OB_AMOUNT; i++ )); do
    add-query-22-output-builder $i
    add-empty-line
  done
  for (( i=1; i<=$Q3X_OB_AMOUNT; i++ )); do
    add-query-3x-output-builder $i
    add-empty-line
  done
  for (( i=1; i<=$Q4X_OB_AMOUNT; i++ )); do
    add-query-4x-output-builder $i
    add-empty-line
  done
}

# ============================== PRIVATE - SERVICES ============================== #

function add-services() {
  local workers_amount=$1

  add-line 'services:'
  add-empty-line

  add-comment 'RABBITMQ SERVICE'
  add-rabbitmq-service
  add-empty-line
  
  add-comment 'CLIENT & SERVER SERVICES'
  add-client-service
  add-empty-line
  add-server-service
  add-empty-line

  add-comment 'CLEANERS SERVICES'
  add-cleaners
  add-empty-line

  add-comment 'FILTERS SERVICES'

  add-comment 'MAPPERS SERVICES'

  add-comment 'REDUCERS SERVICES'

  add-comment 'SORTERS SERVICES'

  add-comment 'JOINERS SERVICES'

  add-comment 'OUTPUT BUILDER SERVICES'
  add-output-builders
}

# ============================== PRIVATE - NETWORKS ============================== #

function add-networks() {
  add-line 'networks:'
  add-line '  custom_net:'
  add-line '    ipam:'
  add-line '      driver: default'
  add-line '      config:'
  add-line '        - subnet: 172.25.125.0/24'
}

# ============================== PRIVATE - DOCKER COMPOSE FILE BUILD ============================== #

function build-docker-compose-file() {
  echo "Generando archivo $COMPOSE_FILE ..."
  
  add-name
  add-services
  add-empty-line
  add-networks

  echo "Generando archivo $COMPOSE_FILE ... [DONE]"
}

# ============================== MAIN ============================== #

# take .env variables
if [ -f .env ]; then
  export $(cat .env | grep -v '#' | awk '/=/ {print $1}')
fi

if [ $# -ne 1 ]; then
  echo "Uso: $0 <compose_filename.yaml>"
  exit 1
fi

# compose_filename_param=$1

echo "Nombre del archivo de salida: $COMPOSE_FILE"

build-docker-compose-file