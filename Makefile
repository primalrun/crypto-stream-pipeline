.PHONY: build up down restart logs stream kafka-ui

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose down && docker compose up -d

logs:
	docker compose logs -f

# Start the Spark Structured Streaming job (runs until Ctrl+C).
# Snowflake credentials are read from .env (loaded into spark-master at startup).
stream:
	docker compose exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		/opt/spark-jobs/stream_prices.py

# Optional: launch Kafka UI at http://localhost:8085
kafka-ui:
	docker compose --profile ui up -d kafka-ui
