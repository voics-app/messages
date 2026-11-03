-- @here (messages#53): set when the message pings only online members.
-- mention_everyone stays true for it (Discord parity); older rows read as false.

ALTER TABLE messages ADD (
  mention_here boolean
);
