.PHONY: up down ps logs psql install seed seed-reset generate reset-db test

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
