#!/usr/bin/env bash
# Runs automatically via docker-entrypoint-initdb.d (must be .sh for env-var substitution).
# Creates the three application roles used across all phases:
#   commerce_app       - DML on commerce.* (used by the data generator, simulates the OLTP app)
#   debezium           - LOGIN REPLICATION + SELECT (unused until Phase 2, created now so
#                        Phase 2 never has to touch Postgres config/roles again)
#   commerce_readonly  - SELECT only (used by Adminer now, Trino later)
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE ROLE commerce_app LOGIN PASSWORD '${COMMERCE_APP_PASSWORD}';
    CREATE ROLE debezium LOGIN REPLICATION PASSWORD '${DEBEZIUM_PASSWORD}';
    CREATE ROLE commerce_readonly LOGIN PASSWORD '${COMMERCE_READONLY_PASSWORD}';
EOSQL
