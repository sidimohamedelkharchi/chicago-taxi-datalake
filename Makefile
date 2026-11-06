.PHONY: env up run download test logs sql down clean

COMPOSE = docker compose

env:
	@test -f .env || cp .env.example .env

up: env ## Build and start the whole platform
	$(COMPOSE) up -d --build

run: ## Run the full pipeline (reference + every month + gold)
	$(COMPOSE) exec dagster-daemon bash scripts/run_pipeline.sh

download: env ## Optional: pre-download raw CSV to ./data/raw (local cache used by bronze)
	$(COMPOSE) run --rm --no-deps dagster-daemon python -m pipeline.ingestion.soda

test: env ## Unit tests (Spark local mode, inside the image)
	$(COMPOSE) run --rm --no-deps dagster-daemon python -m pytest -q

sql: ## Interactive SQL on the gold tables (DuckDB)
	$(COMPOSE) exec dashboard python -m dashboard.sql_shell

logs:
	$(COMPOSE) logs -f dagster-daemon

down:
	$(COMPOSE) down

clean: ## Stop and delete all data (MinIO + Dagster history)
	$(COMPOSE) down -v
