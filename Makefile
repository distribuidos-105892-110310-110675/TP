SHELL := /bin/bash
PWD := $(shell pwd)

docker-build-image:
	docker build -f ./src/client/Dockerfile -t "client:latest" .
	docker build -f ./src/server/Dockerfile -t "server:latest" .
	

	docker build -f ./src/controllers/cleaners/menu_items_cleaner/Dockerfile -t "menu_items_cleaner:latest" .
	docker build -f ./src/controllers/cleaners/stores_cleaner/Dockerfile -t "stores_cleaner:latest" .
	docker build -f ./src/controllers/cleaners/transaction_items_cleaner/Dockerfile -t "transaction_items_cleaner:latest" .
	docker build -f ./src/controllers/cleaners/transactions_cleaner/Dockerfile -t "transactions_cleaner:latest" .
	docker build -f ./src/controllers/cleaners/users_cleaner/Dockerfile -t "users_cleaner:latest" .

	
	docker build -f ./src/controllers/filters/filter_transactions_by_year/Dockerfile -t "filter_transactions_by_year:latest" .
	docker build -f ./src/controllers/filters/filter_transactions_by_hour/Dockerfile -t "filter_transactions_by_hour:latest" .
	docker build -f ./src/controllers/filters/filter_transactions_by_final_amount/Dockerfile -t "filter_transactions_by_final_amount:latest" .

	
	docker build -f ./src/controllers/filters/filter_items_by_year/Dockerfile -t "filter_items_by_year:latest" .
	docker build -f ./src/controllers/mappers/map_month_year_items/Dockerfile -t "map_month_year_items:latest" .

	docker build -f ./src/controllers/reducers/count_transaction_items/Dockerfile -t "count_transaction_items:latest" .
	docker build -f ./src/controllers/joiners/join_item_count_with_menu/Dockerfile -t "join_item_count_with_menu:latest" .
	docker build -f ./src/controllers/sorters/sort_desc_by_year_month_created_at_and_sellings_qty/Dockerfile -t "sort_desc_by_year_month_created_at_and_sellings_qty:latest" .
	
	docker build -f ./src/controllers/reducers/sum_transaction_items/Dockerfile -t "sum_transaction_items:latest" .
	docker build -f ./src/controllers/joiners/join_item_sum_with_menu/Dockerfile -t "join_item_sum_with_menu:latest" .
	docker build -f ./src/controllers/sorters/sort_desc_by_year_month_created_at_and_profit_sum/Dockerfile -t "sort_desc_by_year_month_created_at_and_profit_sum:latest" .

	docker build -f ./src/controllers/mappers/map_month_semester_transactions/Dockerfile -t "map_month_semester_transactions:latest" .	
	docker build -f ./src/controllers/reducers/sum_transactions_by_store/Dockerfile -t "sum_transactions_by_store:latest" .	

# 	docker build -f ./src/controllers/mappers/map_month_semester/Dockerfile -t "map_month_semester:latest" .
# 	docker build -f ./src/controllers/mappers/map_month_year/Dockerfile -t "map_month_year:latest" .
# 	docker build -f ./src/controllers/joiners/join_items_with_menu/Dockerfile -t "join_items_with_menu:latest" .
# 	docker build -f ./src/controllers/joiners/join_users_with_stores/Dockerfile -t "join_users_with_stores:latest" .


	docker build -f ./src/controllers/reducers/count_purchases_by_store_id_and_user_id/Dockerfile -t "count_purchases_by_store_id_and_user_id:latest" .
	docker build -f ./src/controllers/sorters/sort_desc_by_store_id_and_purchases_qty/Dockerfile -t "sort_desc_by_store_id_and_purchases_qty:latest" .
	docker build -f ./src/controllers/joiners/join_transactions_with_users/Dockerfile -t "join_transactions_with_users:latest" .
	docker build -f ./src/controllers/joiners/join_transactions_with_stores/Dockerfile -t "join_transactions_with_stores:latest" .


	docker build -f ./src/controllers/output_builders/query_1x_output_builder/Dockerfile -t "query_1x_output_builder:latest" .
	docker build -f ./src/controllers/output_builders/query_21_output_builder/Dockerfile -t "query_21_output_builder:latest" .
	docker build -f ./src/controllers/output_builders/query_22_output_builder/Dockerfile -t "query_22_output_builder:latest" .
	docker build -f ./src/controllers/output_builders/query_3x_output_builder/Dockerfile -t "query_3x_output_builder:latest" .
	docker build -f ./src/controllers/output_builders/query_4x_output_builder/Dockerfile -t "query_4x_output_builder:latest" .

.PHONY: docker-build-image

docker-compose-up: docker-build-image
	docker compose -f docker-compose.yaml up -d --build --remove-orphans
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose.yaml stop -t 10
	docker compose -f docker-compose.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose.yaml logs -f
.PHONY: docker-compose-logs

tests:
	pytest --verbose
.PHONY: tests