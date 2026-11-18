-- Companion vnode keyspace only. Preserve the main message keyspace.
-- Row 0 is metadata; positive row IDs are authenticated voter snowflakes.
CREATE TABLE IF NOT EXISTS {{keyspace}}.poll_state (
    channel_id bigint,
    message_id bigint,
    row_id bigint,
    version bigint,
    expires_at bigint,
    allow_multiselect boolean,
    status int,
    counts frozen<list<int>>,
    participants int,
    vote_version bigint,
    vote_mask int,
    PRIMARY KEY ((channel_id, message_id), row_id)
);

CREATE TABLE IF NOT EXISTS {{keyspace}}.poll_due (
    shard int,
    expires_at bigint,
    channel_id bigint,
    message_id bigint,
    PRIMARY KEY (shard, expires_at, channel_id, message_id)
);
