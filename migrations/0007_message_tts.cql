-- Existing rows have NULL and are read as false.
ALTER TABLE messages ADD tts boolean;
