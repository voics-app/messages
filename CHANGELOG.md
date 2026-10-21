# Changelog

All notable changes to this project are documented here. The file is maintained by
release-please from Conventional Commits.

## [0.2.0](https://github.com/discordy-voics/messages/compare/messages-v0.1.0...messages-v0.2.0) (2026-10-21)


### Features

* **ack:** manual ack (Mark Unread) + mention_count in MESSAGE_ACK ([d39c00e](https://github.com/discordy-voics/messages/commit/d39c00e62da82eb46421b5934456434e0c5b7d8e))
* **ack:** manual ack moves the read marker backwards; MESSAGE_ACK carries mention_count ([302d83f](https://github.com/discordy-voics/messages/commit/302d83f41419c0e3ad02aadfca54d12d4a16d35d))
* **application:** add request coalescer, ports and message use cases ([f4514d0](https://github.com/discordy-voics/messages/commit/f4514d003de34da25de089637d79ee23aa515cbe))
* **application:** ephemeral relay and channel lookup for webhook posts ([02738f7](https://github.com/discordy-voics/messages/commit/02738f74ad884f3425a009a480c29678544d85f4))
* **attachments:** POST /channels/{id}/attachments/refresh-urls ([dfe982d](https://github.com/discordy-voics/messages/commit/dfe982d5f22615aa3075e2528048b4e2932eb913)), closes [#17](https://github.com/discordy-voics/messages/issues/17)
* **attachments:** signed, expiring attachment download URLs (messages[#17](https://github.com/discordy-voics/messages/issues/17)) ([71d856f](https://github.com/discordy-voics/messages/commit/71d856f08f849fcb36d8fa2e442153e62021ecd7))
* **attachments:** signed, expiring download URLs served by messages ([fc99654](https://github.com/discordy-voics/messages/commit/fc996547a9f200586b6b6ddb1f8a845604bab0d7)), closes [#17](https://github.com/discordy-voics/messages/issues/17)
* **authors:** hydrate message authors with the full user from api ([03e5d99](https://github.com/discordy-voics/messages/commit/03e5d99e469a3465fa75434b6ac9a193058382a6))
* **contracts:** add local wire DTOs for REST, internal APIs and event envelope ([7a14090](https://github.com/discordy-voics/messages/commit/7a140902aff28ff957209de40cf83745362e9c15))
* **domain:** add snowflakes, buckets, paging, access rules and message model ([f24e2c6](https://github.com/discordy-voics/messages/commit/f24e2c60eead3cc6614b4b231ba963af1fb2689c))
* **domain:** webhook author, interaction link and flags on messages ([15e7c3b](https://github.com/discordy-voics/messages/commit/15e7c3bb6f01d84a502102ad66dd17ee2383592a))
* group DM system message types on internal create (api[#29](https://github.com/discordy-voics/messages/issues/29)) ([3188e23](https://github.com/discordy-voics/messages/commit/3188e232a5ee9e1563f6d1c616b2d6565897d285))
* **infrastructure:** add ScyllaDB, Redpanda, MinIO and api adapters with integration tests ([e870250](https://github.com/discordy-voics/messages/commit/e8702504ae6d68253be8b85b13b28e72123501c6))
* **infrastructure:** store webhook and interaction fields, read channel routing ([78ba6ee](https://github.com/discordy-voics/messages/commit/78ba6ee8590cc45b18e1f1db90120c1ba4d072d2))
* **interface:** accept the full P4 create body and return origin fields ([3fe76b6](https://github.com/discordy-voics/messages/commit/3fe76b6acd6c7b53e4f666bc23ea84a6a6418902))
* **interface:** add axum REST and internal API, event envelopes and composition root ([3fa5bc7](https://github.com/discordy-voics/messages/commit/3fa5bc7a578f5bc3983876084fbb876bfa9b57ab))
* **interface:** add CORS policy for the desktop client ([c560575](https://github.com/discordy-voics/messages/commit/c5605758ae766ca74e75d60a87c88b329e6c64c3))
* **internal:** group DM system message types on the internal create endpoint ([5896423](https://github.com/discordy-voics/messages/commit/58964238527b853ef3d872a3c2966bef2568365c))
* **internal:** POST /internal/v1/channels/{id}/messages for bot, webhook and interaction messages ([0dc6430](https://github.com/discordy-voics/messages/commit/0dc64308f63be97cbdb5c0bc978c26f6e0905e7e))
* **messages:** link unfurling and media proxy; attachment alt text and spoilers ([926af62](https://github.com/discordy-voics/messages/commit/926af62f7fd75b1e9765aecf545f186192eaefea))
* **messages:** link unfurling, media proxy, attachment alt text and spoiler flag (messages[#12](https://github.com/discordy-voics/messages/issues/12), client[#54](https://github.com/discordy-voics/messages/issues/54)) ([eaf6e7b](https://github.com/discordy-voics/messages/commit/eaf6e7bcfb94bdfcdfa968513b08bcfc33e0828b))
* **messages:** message search endpoint and Postgres full-text index (messages[#11](https://github.com/discordy-voics/messages/issues/11)) ([f3e8f24](https://github.com/discordy-voics/messages/commit/f3e8f247bfd446e8ca9ef7e526f2b4c62dfd863a))
* **messages:** message search endpoints, Postgres full-text index and reindex (messages[#11](https://github.com/discordy-voics/messages/issues/11)) ([f8795ac](https://github.com/discordy-voics/messages/commit/f8795ac7aea3e08138fc37e12d7d267d154b11a6))
* **messages:** read CORS_ALLOWED_ORIGINS and apply the CORS layer ([da6bb9d](https://github.com/discordy-voics/messages/commit/da6bb9dc329d8380e99b9867b517d2b0c7866e3b))


### Bug Fixes

* **access:** re-check a delivery mode conflict against fresh data ([715c304](https://github.com/discordy-voics/messages/commit/715c304f26223586432ae43d4c25359d49b6c402))
* **attachments:** issue upload slots for Ephemeral channels ([27bd421](https://github.com/discordy-voics/messages/commit/27bd421dba81a448444ce0cbc2cdc12a13946c78))
* **ci:** grant docker job permissions and fix reusable workflow inputs ([28bead8](https://github.com/discordy-voics/messages/commit/28bead8bc368fb96cd885453f3f3d8b293c52042))
* upload slots for Ephemeral channels; recheck stale delivery mode on conflict ([907a1b8](https://github.com/discordy-voics/messages/commit/907a1b84ca863dbc52c8d6181430beaca1848c83))

## 0.1.0

First release: everything on develop up to this point, deployed to gen-serv.

## 0.1.0

- Initial implementation of the messages service: ScyllaDB bucketed history, reactions, pins,
  read states, attachments through MinIO, event publishing to `discordy.message.v1`, and the
  internal last-message-id and admin stats endpoints.
