-- Existing messages have NULL; a poll definition is immutable after creation.
ALTER TABLE messages ADD poll_definition text;
