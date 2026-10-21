-- Alt text and flags (IS_SPOILER) on stored attachments (client#54).
-- Rows written before this migration read both fields as NULL.

ALTER TYPE attachment ADD description text;
ALTER TYPE attachment ADD flags int;
