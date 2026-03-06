init:
	bash init.sh

up:
	docker compose up -d

down:
	docker compose down

## Streamlit
rebuild_streamlit_image:
	docker compose build --no-cache streamlit

streamlit_demo:
	python3 src/data/binance/extract_kline_data_ws.py --symbol BTCUSDT --interval 1m