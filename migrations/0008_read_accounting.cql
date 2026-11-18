-- Applied by the read-state adapter in a separate vnode keyspace on the SAME
-- Scylla cluster. Scylla 2025.1 tablet tables do not support LWT. The bootstrap
-- validates the identifier and substitutes {{keyspace}}; legacy tables remain.
-- All authoritative rows for one user/channel share one table and partition.
CREATE TABLE IF NOT EXISTS {{keyspace}}.read_state_accounting (
  user_id bigint,
  channel_id bigint,
  kind tinyint,
  generation uuid,
  message_id bigint,
  revision uuid,
  active_generation uuid,
  last_read_id bigint,
  acknowledged_through bigint,
  baseline bigint,
  mention_count bigint,
  PRIMARY KEY ((user_id, channel_id), kind, generation, message_id)
);
