# Failure Recovery (Phase 5 -- not yet implemented)

This document will cover the failure scenarios from spec section 27 (Kafka
consumer crashes, Spark job crashes, Iceberg write failures, malformed/
duplicate/out-of-order events, schema changes, network interruption) once
the relevant pipeline components exist.

Phase 1 has no failure-recovery surface yet: the data generator is a
standalone script against Postgres, with no downstream consumers to fail
or recover.
