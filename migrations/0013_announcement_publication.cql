-- Kept separate from flags so a concurrent ordinary edit cannot clear publication.
ALTER TABLE messages ADD crossposted boolean;
