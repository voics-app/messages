-- Voics messages keyspace: initial schema (spec §6.2, pinned interface P10).
-- Every statement is idempotent; the migrator records applied versions.

CREATE TYPE IF NOT EXISTS attachment (
  id bigint,
  filename text,
  content_type text,
  size bigint,
  width int,
  height int
);

-- Messages are partitioned by channel and a 10-day bucket of the snowflake
-- timestamp, newest first inside a partition.
CREATE TABLE IF NOT EXISTS messages (
  channel_id bigint,
  bucket int,
  message_id bigint,
  author_id bigint,
  type int,
  content text,
  attachments list<frozen<attachment>>,
  embeds text,
  components text,
  mentions set<bigint>,
  mention_roles set<bigint>,
  mention_everyone boolean,
  reply_to bigint,
  flags int,
  edited_at timestamp,
  pinned boolean,
  PRIMARY KEY ((channel_id, bucket), message_id)
) WITH CLUSTERING ORDER BY (message_id DESC);

-- Reactions share the messages partition key, one row per (message, emoji, user).
CREATE TABLE IF NOT EXISTS reactions (
  channel_id bigint,
  bucket int,
  message_id bigint,
  emoji text,
  user_id bigint,
  PRIMARY KEY ((channel_id, bucket), message_id, emoji, user_id)
);

-- Pins are listed per channel (at most 50), so they are partitioned by channel.
CREATE TABLE IF NOT EXISTS pins (
  channel_id bigint,
  message_id bigint,
  pinned_at timestamp,
  PRIMARY KEY ((channel_id), message_id)
);

CREATE TABLE IF NOT EXISTS read_states (
  user_id bigint,
  channel_id bigint,
  last_read_id bigint,
  mention_count int,
  PRIMARY KEY ((user_id), channel_id)
);

-- Written with USING TIMESTAMP = message id, so the newest id always wins
-- without lightweight transactions.
CREATE TABLE IF NOT EXISTS channel_last_message (
  channel_id bigint PRIMARY KEY,
  last_message_id bigint
);

-- Admin counters: message ids per hour, no content, expiring after two days.
CREATE TABLE IF NOT EXISTS message_activity (
  hour bigint,
  message_id bigint,
  PRIMARY KEY ((hour), message_id)
) WITH default_time_to_live = 172800;
