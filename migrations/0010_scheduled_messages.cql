-- Qualified companion vnode tables on the existing cluster. Never change the main keyspace.
-- kind: 0 author revision/count, 1 permanent journal, 2 bounded active discovery, 3 channel count.
CREATE TABLE IF NOT EXISTS {{keyspace}}.scheduled_journal (
  author_id bigint,
  kind tinyint,
  entry text,
  revision uuid,
  active_count int,
  body text,
  PRIMARY KEY (author_id, kind, entry)
);

CREATE TABLE IF NOT EXISTS {{keyspace}}.scheduled_due (
  shard tinyint,
  wake_at_ms bigint,
  author_id bigint,
  schedule_id text,
  revision int,
  PRIMARY KEY (shard, wake_at_ms, author_id, schedule_id, revision)
);

CREATE TABLE IF NOT EXISTS {{keyspace}}.scheduled_cursors (
  shard tinyint PRIMARY KEY,
  wake_at_ms bigint,
  author_id bigint,
  schedule_id text,
  revision int
);
