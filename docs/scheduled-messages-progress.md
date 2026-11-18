# Scheduled messages implementation status

This branch implements the domain and application checkpoint for messages#61. It does not expose scheduling routes or run a scheduler yet. The interface design is in [docs#182](https://github.com/discordy-voics/docs/pull/182), ADR0028 and [contracts#50](https://github.com/discordy-voics/contracts/pull/50).

The domain owns request identity, time/text rules, revision-fenced transitions, preparation recovery, dependency backoff and delivery outcomes. The application owns create/replay, edit/cancel, ownership, discovery ports and a single claimed dispatch attempt. `CreateMessage` supplies ordinary composition and publication with fresh authorization around the claim. The real HTTP access adapter bypasses both cached values and older in-flight requests for refresh.

The Scylla adapter now commits the job, author/channel quota and active discovery in one author-partition SERIAL/QUORUM conditional batch. It creates a distinct vnode companion keyspace on the existing cluster, checks replication against the main keyspace and retains the original session binding. The due index has 64 shards, bounded 100-row pages and persistent cursors. Real Scylla tests exercise competing repositories, quota release/replay, cancel/claim/recovery and cursor rewind. The dedicated scheduled message writer disables opaque driver retries for insertion.

The bounded due-index worker, server configuration/routes, client surfaces and end-to-end acceptance remain under implementation. Adapter tests alone do not establish the complete scheduling guarantee.

The intended storage guarantee is one insertion attempt after a durable claim. Lost acknowledgements or abandoned claims become visible `delivery_unknown`, without automatic resending. An original attempt may later certify storage. Publication and accounting remain best effort, as with ordinary messages. Current author account eligibility additionally requires [api#150](https://github.com/discordy-voics/api/pull/150).
