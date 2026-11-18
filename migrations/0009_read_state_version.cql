-- Companion keyspace only; bootstrap checks the column before applying this
-- migration because this Scylla version does not support ADD IF NOT EXISTS.
ALTER TABLE {{keyspace}}.read_state_accounting ADD read_state_version bigint;
