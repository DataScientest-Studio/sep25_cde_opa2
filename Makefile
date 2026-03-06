init:
	bash init.sh

up:
	docker compose up -d

down:
	docker compose down

rebuild:
	docker compose build --no-cache
	
## Streamlit
rebuild_streamlit_image:
	docker compose build --no-cache streamlit

streamlit_demo_mongo: # Pour tester dans la VM, mais sinon c'est conteneurisé
	python3 src/data/binance/extract_kline_data_ws.py --symbol BTCUSDT --interval 1m

streamlit_demo_postgres: # Pour tester dans la VM, mais sinon c'est conteneurisé
	python3 src/data/binance/transform_and_load.py

## Services de collecte et transformation des données binance (klines pour l'instant)
start_data_collector:
	docker compose up -d data-collector

stop_data_collector:
	docker compose stop data-collector

start_data_transformer:
	docker compose up -d data-transformer

stop_data_transformer:
	docker compose stop data-transformer
