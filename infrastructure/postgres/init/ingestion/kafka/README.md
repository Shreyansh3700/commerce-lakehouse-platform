# Kafka

No broker config files live here -- like Postgres in Phase 1, Kafka's
KRaft/broker configuration is entirely environment variables on the `kafka`
service in the root `docker-compose.yml`, not a mounted config file.

Topic naming, partitioning, retention, and DLQ design are documented in
[`docs/cdc.md`](../../docs/cdc.md).
