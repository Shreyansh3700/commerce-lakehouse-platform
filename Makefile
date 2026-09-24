.PHONY: up down ps logs psql install seed seed-reset generate reset-db test \
	register-connector connector-status topics consumer-lag silver-progress \
	gold-build-run gold-test dq-check gold-build query-gold

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

# --- Phase 3: Silver / Gold (plain PySpark batch) / Data quality ---------

silver-progress:
	docker compose logs spark-silver-writer --since 5m | grep -A 15 "Streaming query made progress"

gold-build-run:
	docker compose run --rm --profile tools gold-builder /opt/spark-app/gold_builder.py

gold-test:
	docker compose run --rm --profile tools gold-builder /opt/spark-app/gold_tests.py

# dq-check runs the DQ suite standalone for visibility (non-blocking on its
# own); gold-build is the real gate -- a critical DQ failure exits non-zero
# and short-circuits the prerequisite chain below before the Gold build ever
# runs. This mirrors what an Airflow task dependency will do in Phase 5,
# with no changes to runner.py/gold_builder.py/gold_tests.py.
dq-check:
	docker compose run --rm --profile tools dq /opt/spark-app/runner.py --suite all --fail-on critical

gold-build: dq-check gold-build-run gold-test

query-gold:
	docker compose run --rm --profile tools dq /opt/spark-app/spark_query.py --sql "SELECT * FROM nessie.gold.daily_sales ORDER BY date"
