-- Payloads of system messages (messages#54): the call of a type 3 message
-- (participants, end time) and the thread a type 18 message announces.

ALTER TABLE messages ADD (
  call_participants list<bigint>,
  call_ended_at timestamp,
  thread_id bigint
);
