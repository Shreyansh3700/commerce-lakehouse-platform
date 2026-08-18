# ADR 0003: Kafka in KRaft mode, no Zookeeper

## Status
Accepted

## Context
The spec (section 33) allows either "zookeeper or Kafka KRaft" for the
Kafka deployment.

## Decision
Run a single-broker Kafka in KRaft mode (`process.roles=broker,controller`),
with no separate Zookeeper service.

## Consequences
- One fewer stateful service to run, back up, and reason about.
- Zookeeper is being removed entirely in Kafka 4.x upstream; KRaft is the
  forward-looking mode.
- Single broker means replication factor 1 everywhere -- acceptable for a
  local dev/learning platform; the spec's "Scalable" NFR is about adding
  partitions/workers without redesign, not about running a multi-broker
  cluster today.
