-- Webhook author overrides and interaction links on messages (P4 create body).
-- Adding columns is idempotent per migration version; the migrator records it.

ALTER TABLE messages ADD (
  webhook_id bigint,
  webhook_username text,
  webhook_avatar_url text,
  interaction_id bigint,
  application_id bigint
);
