-- Full-text index of Standard messages (ADR 0018). The text is indexed, not stored: hits are
-- hydrated from ScyllaDB, which stays the source of truth, so the table can be dropped and rebuilt
-- by the reindex job at any time.

CREATE TABLE IF NOT EXISTS search_messages (
  channel_id bigint NOT NULL,
  message_id bigint NOT NULL,
  guild_id bigint,
  author_id bigint NOT NULL,
  tsv tsvector NOT NULL,
  mentions bigint[] NOT NULL DEFAULT '{}',
  -- Bit mask of the `has` kinds: link 1, embed 2, file 4, image 8, video 16, sound 32.
  has smallint NOT NULL DEFAULT 0,
  -- Edit time in unix milliseconds (0 when never edited): the version of the row.
  edited_ms bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (channel_id, message_id)
);

CREATE INDEX IF NOT EXISTS search_messages_tsv_idx ON search_messages USING GIN (tsv);
CREATE INDEX IF NOT EXISTS search_messages_mentions_idx ON search_messages USING GIN (mentions);
CREATE INDEX IF NOT EXISTS search_messages_guild_idx ON search_messages (guild_id, channel_id) WHERE guild_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS search_messages_author_idx ON search_messages (author_id, message_id DESC);

-- Where the Redpanda consumer stopped, written in the same transaction as the rows it produced.
CREATE TABLE IF NOT EXISTS search_offsets (
  topic text NOT NULL,
  partition integer NOT NULL,
  next_offset bigint NOT NULL,
  PRIMARY KEY (topic, partition)
);

-- Small key/value state, for example the backfill status.
CREATE TABLE IF NOT EXISTS search_state (
  key text PRIMARY KEY,
  value text NOT NULL
);
