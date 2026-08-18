.PHONY: up down ps logs psql install seed seed-reset generate reset-db test \
	register-connector connector-status topics consumer-lag

up:
	docker compose up -d

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f

psql:
	docker compose exec postgres psql -U $${POSTGRES_SUPERUSER:-postgres} -d $${POSTGRES_DB:-commerce}

install:
	uv sync

seed:
	uv run python scripts/seed_database.py

seed-reset:
	uv run python scripts/seed_database.py --reset

generate:
	uv run python scripts/generate_data.py

reset-db:
	docker compose down -v
	docker compose up -d postgres

test:
	uv run pytest

register-connector:
	docker compose run --rm connect-init

connector-status:
	curl -s http://localhost:$${KAFKA_CONNECT_PORT:-8083}/connectors/postgres-cdc/status | uv run python -m json.tool

# MSYS_NO_PATHCONV avoids Git Bash on Windows mangling the /opt/kafka/...
# container path below into a host path; harmless on Linux/macOS/WSL.
topics:
	MSYS_NO_PATHCONV=1 docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# NOT based on kafka-consumer-groups.sh -- confirmed empirically that Spark's
# Kafka source never commits offsets under kafka.group.id, so the Bronze
# writer's queries never appear as a listable consumer group at all. Instead,
# this greps each query's own progress logs. See docs/cdc.md.
consumer-lag:
	docker compose logs spark-bronze-writer --since 5m | grep -A 15 "Streaming query made progress"
