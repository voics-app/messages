-- Content-free, idempotent markers. Kept after message deletion so the number
-- of created messages does not change when a moderator removes content.
CREATE TABLE IF NOT EXISTS guild_message_activity (
  guild_id bigint,
  utc_day bigint,
  message_id bigint,
  PRIMARY KEY ((guild_id, utc_day), message_id)
);
