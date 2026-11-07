.PHONY: help env up down reset ps logs topics consume install test lint fmt ingest-binance \
	spark-logs sql shell

COMPOSE := docker compose
KAFKA_BIN := /opt/kafka/bin
TOPIC ?= trades.raw
q ?= SHOW TABLES IN lake.bronze

help:
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-16s %s\n", $$1, $$2}'

env: ## create .env from the example if missing
	@test -f .env || cp .env.example .env

up: env ## start the local stack
	$(COMPOSE) up -d --build

down: ## stop the stack, keep data
	$(COMPOSE) down

reset: ## stop the stack and delete all data
	$(COMPOSE) down -v

ps: ## show container status
	$(COMPOSE) ps -a

logs: ## follow logs, e.g. make logs s=ingest-binance
	$(COMPOSE) logs -f $(s)

topics: ## list topics with details
	$(COMPOSE) exec kafka $(KAFKA_BIN)/kafka-topics.sh --bootstrap-server kafka:19092 --describe

consume: ## tail a topic with keys, e.g. make consume TOPIC=trades.raw
	$(COMPOSE) exec kafka $(KAFKA_BIN)/kafka-console-consumer.sh \
		--bootstrap-server kafka:19092 --topic $(TOPIC) \
		--property print.key=true --property print.partition=true

spark-logs: ## follow the bronze streaming job
	$(COMPOSE) logs -f spark-bronze

sql: ## query the lakehouse, e.g. make sql q="SELECT count(*) FROM lake.bronze.trades"
	$(COMPOSE) exec spark-bronze python -m cryptozilla.tools.sql "$(q)"

shell: ## interactive pyspark with `spark` connected to the catalog
	$(COMPOSE) exec spark-bronze python -i -c \
		"from cryptozilla.spark import make_spark_session; spark = make_spark_session('cz-shell', master='local[2]')"

install: ## install the package with all extras and dev tools into .venv
	uv sync --all-extras

test: ## run unit tests (Spark tests are skipped without Java)
	uv run pytest

lint: ## lint and type-check
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy src

fmt: ## auto-format
	uv run ruff format .
	uv run ruff check --fix .

ingest-binance: env ## run the Binance producer on the host against local Kafka
	uv run python -m cryptozilla.ingest binance