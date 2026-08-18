#!/bin/sh
# Idempotently registers (or updates) the postgres-cdc Debezium connector.
# Runs as a one-shot compose service (connect-init) after kafka-connect is
# healthy, and can be rerun manually via `make register-connector`.
set -eu

CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083}"
CONNECTOR_NAME="postgres-cdc"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
TEMPLATE_PATH="${SCRIPT_DIR}/postgres-connector.json"

echo "Waiting for Kafka Connect at ${CONNECT_URL} ..."
until curl -sf "${CONNECT_URL}/" >/dev/null 2>&1; do
  sleep 2
done
echo "Kafka Connect is up."

# Only ${DEBEZIUM_PASSWORD} needs substituting, so plain sed is enough --
# avoids pulling in gettext/envsubst just for one variable.
CONFIG_JSON=$(sed "s|\${DEBEZIUM_PASSWORD}|${DEBEZIUM_PASSWORD}|g" "${TEMPLATE_PATH}")

# PUT (not POST /connectors) is idempotent: creates the connector if absent,
# updates its config in place if it already exists -- safe to rerun.
echo "Registering/updating connector '${CONNECTOR_NAME}' ..."
curl -sf -X PUT "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config" \
  -H "Content-Type: application/json" \
  -d "${CONFIG_JSON}"
echo ""
echo "Connector registered."
