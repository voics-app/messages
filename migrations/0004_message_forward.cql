-- Forwarded messages (messages#13): where the message came from plus a JSON
-- snapshot (content, attachments, embeds, mentions) frozen at forward time.

ALTER TABLE messages ADD (
  forward_channel_id bigint,
  forward_message_id bigint,
  forward_guild_id bigint,
  forward_snapshot text
);
