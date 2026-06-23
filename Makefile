# GESTION DU CONTEXTE
# Mode DEV
.PHONY: dev
dev:
	@docker compose down
	@ln -sf .env.dev .env
	@echo "🟢 Passage en DEVELOPPEMENT..."
	@docker compose up -d --build
	@echo "💻 Environnement de DEV prêt !"

# Mode PROD
.PHONY: prod
prod:
	@docker compose down
	@ln -sf .env.prod .env
	@echo "🔵 Passage en PRODUCTION..."
	@docker compose pull
	@docker compose up -d
	@echo "🚀 Environnement de PRODUCTION prêt et à jour !"

# Vérification du STATUS en cours
.PHONY: status
status:
	@echo "=== CONTEXTE ACTUEL ==="
	@if [ -L .env ]; then \
		echo "Fichier actif : $$(readlink .env)"; \
	else \
		echo "❌ Aucun contexte lié (pas de fichier .env actif)."; \
	fi
	@echo "-----------------------"
	@docker compose ps

.PHONY: init
init:
	bash init.sh

.PHONY: up
up:
	docker compose up -d

.PHONY: down
down:
	docker compose down

.PHONY: pull
pull:
	docker compose pull

.PHONY: rebuild
rebuild:
	docker compose build --no-cache
	
## Streamlit
.PHONY: rebuild_streamlit_image
rebuild_streamlit_image:
	docker compose build --no-cache streamlit

# Pour tester dans la VM, mais sinon c'est conteneurisé
.PHONY: streamlit_demo_mongo
streamlit_demo_mongo:
	python3 src/data/binance/extract_kline_data_ws.py --symbol BTCUSDT --interval 1m

# Pour tester dans la VM, mais sinon c'est conteneurisé
.PHONY: streamlit_demo_postgres
streamlit_demo_postgres:
	python3 src/data/binance/transform_and_load.py

## Services de collecte et transformation des données binance (klines pour l'instant)
.PHONY: start_data_collector
start_data_collector:
	docker compose up -d data-collector

.PHONY: stop_data_collector
stop_data_collector:
	docker compose stop data-collector

.PHONY: start_data_transformer
start_data_transformer:
	docker compose up -d data-transformer

.PHONY: stop_data_transformer
stop_data_transformer:
	docker compose stop data-transformer
