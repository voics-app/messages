-- Additive metadata; existing attachments read as NULL.
ALTER TYPE attachment ADD duration_ms int;
ALTER TYPE attachment ADD waveform text;
